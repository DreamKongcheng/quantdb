from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol
from uuid import UUID

import pandas as pd

from quantdb.errors import (
    DatasetValidationError,
    SyncError,
    SyncInterruptedError,
    is_interruption_error,
)
from quantdb.registry import (
    TRADE_CAL,
    DatasetSpec,
    Partition,
    calendar_year_partitions,
    daily_partition,
    full_partition,
    get_dataset,
    index_month_partitions,
    parse_date,
)
from quantdb.store import DuckDBStore


class DataProvider(Protocol):
    def fetch(self, spec: DatasetSpec, partition: Partition) -> pd.DataFrame: ...


class SyncProgress(Protocol):
    def dataset_started(self, dataset_id: str, total: int) -> None: ...

    def partition_started(self, dataset_id: str, partition_id: str) -> None: ...

    def partition_finished(self, result: PartitionResult) -> None: ...

    def partition_failed(
        self,
        dataset_id: str,
        partition_id: str,
        error: Exception,
    ) -> None: ...

    def partition_interrupted(
        self,
        dataset_id: str,
        partition_id: str,
        error: BaseException,
    ) -> None: ...

    def dataset_finished(self, dataset_id: str) -> None: ...


@dataclass(frozen=True)
class PartitionResult:
    dataset_id: str
    partition_id: str
    status: str
    row_count: int | None = None


@dataclass(frozen=True)
class SyncReport:
    dataset_id: str
    results: tuple[PartitionResult, ...]

    @property
    def completed(self) -> int:
        return sum(result.status == "SUCCESS" for result in self.results)

    @property
    def skipped(self) -> int:
        return sum(result.status == "SKIPPED" for result in self.results)


class SyncEngine:
    def __init__(
        self,
        store: DuckDBStore,
        provider: DataProvider,
        progress: SyncProgress | None = None,
    ) -> None:
        self.store = store
        self.provider = provider
        self.progress = progress

    def sync(
        self,
        dataset_id: str,
        *,
        start: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        refresh: bool = False,
    ) -> SyncReport:
        spec = get_dataset(dataset_id)

        if spec.partition_strategy == "trading_day":
            partitions = self._prepare_trading_day(spec, start, end)
        elif spec.partition_strategy == "calendar_year":
            partitions = calendar_year_partitions(start, end)
        elif spec.partition_strategy == "full":
            partitions = [full_partition()]
        elif spec.partition_strategy == "index_month":
            partitions = index_month_partitions(start, end)
        else:  # pragma: no cover - 注册表新增策略时的防御分支
            raise ValueError(f"数据集 {spec.id} 的分区策略尚未实现")

        return self._sync_partitions(spec, partitions, refresh=refresh)

    def _prepare_trading_day(
        self,
        spec: DatasetSpec,
        start: str | date | datetime | None,
        end: str | date | datetime | None,
    ) -> list[Partition]:
        if start is None:
            raise ValueError(f"同步 {spec.id} 必须提供 start")
        start_date = parse_date(start)
        end_date = parse_date(end) if end is not None else start_date
        if start_date > end_date:
            raise ValueError("start 不能晚于 end")

        calendar_partitions = calendar_year_partitions(start_date, end_date)
        existing_calendars = self.store.partition_ids(TRADE_CAL.id)
        missing_calendars = [
            partition for partition in calendar_partitions if partition.id not in existing_calendars
        ]
        if missing_calendars:
            self._sync_partitions(TRADE_CAL, missing_calendars, refresh=False)

        return [daily_partition(day) for day in self.store.open_dates(start_date, end_date)]

    def _sync_partitions(
        self,
        spec: DatasetSpec,
        partitions: list[Partition],
        *,
        refresh: bool,
    ) -> SyncReport:
        results: list[PartitionResult] = []
        existing_partitions = self.store.partition_ids(spec.id) if not refresh else set()
        if self.progress:
            self.progress.dataset_started(spec.id, len(partitions))
        try:
            for partition in partitions:
                if partition.id in existing_partitions:
                    result = PartitionResult(spec.id, partition.id, "SKIPPED")
                    results.append(result)
                    if self.progress:
                        self.progress.partition_finished(result)
                    continue

                if self.progress:
                    self.progress.partition_started(spec.id, partition.id)
                run_id = self.store.start_run(spec, partition)
                try:
                    frame = self.provider.fetch(spec, partition)
                    validate_frame(spec, partition, frame)
                    self.store.replace_partition(spec, partition, frame, run_id)
                except Exception as exc:
                    if is_interruption_error(exc):
                        self._mark_run_interrupted(run_id, exc)
                        if self.progress:
                            self.progress.partition_interrupted(spec.id, partition.id, exc)
                        raise SyncInterruptedError(
                            f"{spec.id} 分区 {partition.id} 查询被中断"
                        ) from exc
                    self._mark_run_failed(run_id, exc)
                    if self.progress:
                        self.progress.partition_failed(spec.id, partition.id, exc)
                    raise SyncError(f"同步 {spec.id} 分区 {partition.id} 失败：{exc}") from exc
                except BaseException as exc:
                    self._mark_run_interrupted(run_id, exc)
                    if self.progress:
                        self.progress.partition_interrupted(spec.id, partition.id, exc)
                    raise
                result = PartitionResult(spec.id, partition.id, "SUCCESS", len(frame))
                results.append(result)
                if self.progress:
                    self.progress.partition_finished(result)
        finally:
            if self.progress:
                self.progress.dataset_finished(spec.id)

        return SyncReport(spec.id, tuple(results))

    def _mark_run_failed(self, run_id: UUID, error: Exception) -> None:
        try:
            self.store.mark_run_failed(run_id, error)
        except Exception as metadata_error:
            error.add_note(f"记录 FAILED 状态时发生异常：{metadata_error}")

    def _mark_run_interrupted(self, run_id: UUID, error: BaseException) -> None:
        try:
            self.store.mark_run_interrupted(run_id, error)
        except Exception as metadata_error:
            error.add_note(f"记录 INTERRUPTED 状态时发生异常：{metadata_error}")


def validate_frame(spec: DatasetSpec, partition: Partition, frame: pd.DataFrame) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise DatasetValidationError(f"{spec.id} 未返回 pandas.DataFrame")

    missing_columns = set(spec.source_fields) - set(frame.columns)
    if missing_columns:
        names = ", ".join(sorted(missing_columns))
        raise DatasetValidationError(f"{spec.id} 缺少字段：{names}")
    if frame.empty and not spec.allow_empty:
        raise DatasetValidationError(f"{spec.id} 分区 {partition.id} 返回空数据")
    if frame.empty:
        return

    required_columns = list(dict.fromkeys((*spec.primary_key, *spec.required_columns)))
    if required_columns and frame[required_columns].isnull().any(axis=None):
        raise DatasetValidationError(f"{spec.id} 必填字段包含空值")

    if spec.primary_key:
        key_columns = list(spec.primary_key)
        duplicates = frame.duplicated(subset=key_columns, keep=False)
        if duplicates.any():
            raise DatasetValidationError(f"{spec.id} 返回重复主键，共 {int(duplicates.sum())} 行")

    if spec.partition_strategy == "trading_day":
        expected = str(partition.request_params["trade_date"])
        actual = set(frame["trade_date"].astype("string").str.strip().dropna())
        if actual != {expected}:
            raise DatasetValidationError(
                f"{spec.id} 返回了目标日期之外的数据：期望 {expected}，实际 {sorted(actual)}"
            )
    elif spec.id == TRADE_CAL.id:
        expected_exchange = str(partition.request_params["exchange"])
        exchanges = set(frame["exchange"].astype("string").str.strip().dropna())
        if exchanges != {expected_exchange}:
            raise DatasetValidationError(
                f"{spec.id} 返回了目标交易所之外的数据：{sorted(exchanges)}"
            )
    elif spec.partition_strategy == "index_month":
        expected_code = str(partition.request_params["index_code"])
        actual_codes = set(frame["index_code"].astype("string").str.strip().dropna())
        if actual_codes != {expected_code}:
            raise DatasetValidationError(
                f"{spec.id} 返回了目标指数之外的数据：{sorted(actual_codes)}"
            )
        start_date = str(partition.request_params["start_date"])
        end_date = str(partition.request_params["end_date"])
        actual_dates = frame["trade_date"].astype("string").str.strip().dropna()
        outside_range = actual_dates[(actual_dates < start_date) | (actual_dates > end_date)]
        if not outside_range.empty:
            raise DatasetValidationError(
                f"{spec.id} 返回了目标月份之外的数据：{sorted(set(outside_range))}"
            )
