from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import duckdb

from quantdb.config import resolve_database_path, resolve_tushare_token
from quantdb.errors import ReadOnlyDatabaseError
from quantdb.provider import TushareClient, TushareProvider
from quantdb.registry import get_dataset, parse_date
from quantdb.store import DuckDBStore
from quantdb.sync import DataProvider, SyncEngine, SyncProgress, SyncReport


class QuantDB:
    def __init__(
        self,
        path: str | Path | None = None,
        *,
        tushare_token: str | None = None,
        env_file: str | Path | None = ".env",
        provider: DataProvider | None = None,
        read_only: bool = False,
    ) -> None:
        self.path = resolve_database_path(path, env_file)
        self.read_only = read_only
        self.store = DuckDBStore(self.path, read_only=read_only)
        self._tushare_token = tushare_token
        self._env_file = env_file
        self._provider = provider

    def init(self) -> None:
        self._require_writable("初始化数据库")
        self.store.initialize()

    def sync(
        self,
        dataset_id: str,
        *,
        start: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        refresh: bool = False,
        progress: SyncProgress | None = None,
    ) -> SyncReport:
        self._require_writable("同步数据")
        provider = self._provider
        if provider is None:
            token = resolve_tushare_token(self._tushare_token, self._env_file)
            provider = TushareProvider(TushareClient(token))
            self._provider = provider
        return SyncEngine(self.store, provider, progress).sync(
            dataset_id,
            start=start,
            end=end,
            refresh=refresh,
        )

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        return self.store.sql(query)

    def bars(
        self,
        symbols: str | Sequence[str] | None = None,
        *,
        start: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        adjust: Literal["none", "qfq", "hfq"] = "none",
        as_of: str | date | datetime | None = None,
    ) -> duckdb.DuckDBPyRelation:
        normalized_symbols, start_date, end_date, adjust, as_of_date = self._normalize_market_query(
            symbols, start, end, adjust, as_of
        )
        return self.store.bars(
            symbols=normalized_symbols,
            start=start_date,
            end=end_date,
            adjust=adjust,
            as_of=as_of_date,
        )

    def panel(
        self,
        symbols: str | Sequence[str] | None = None,
        *,
        start: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        adjust: Literal["none", "qfq", "hfq"] = "none",
        as_of: str | date | datetime | None = None,
    ) -> duckdb.DuckDBPyRelation:
        normalized_symbols, start_date, end_date, adjust, as_of_date = self._normalize_market_query(
            symbols, start, end, adjust, as_of
        )
        return self.store.panel(
            symbols=normalized_symbols,
            start=start_date,
            end=end_date,
            adjust=adjust,
            as_of=as_of_date,
        )

    def universe(
        self,
        as_of: str | date | datetime,
        *,
        index_code: str | None = None,
        exclude_st: bool = False,
        exclude_delisting: bool = False,
    ) -> duckdb.DuckDBPyRelation:
        as_of_date = parse_date(as_of)
        normalized_index_code: str | None = None
        if index_code is not None:
            if not isinstance(index_code, str):
                raise ValueError("index_code 必须是非空字符串")
            normalized_index_code = index_code.strip()
            if not normalized_index_code:
                raise ValueError("index_code 必须是非空字符串")
        return self.store.universe(
            as_of=as_of_date,
            index_code=normalized_index_code,
            exclude_st=exclude_st,
            exclude_delisting=exclude_delisting,
        )

    def tradeability(
        self,
        as_of: str | date | datetime,
        *,
        symbols: str | Sequence[str] | None = None,
    ) -> duckdb.DuckDBPyRelation:
        return self.store.tradeability(
            as_of=parse_date(as_of),
            symbols=self._normalize_symbols(symbols),
        )

    def _normalize_market_query(
        self,
        symbols: str | Sequence[str] | None,
        start: str | date | datetime | None,
        end: str | date | datetime | None,
        adjust: Literal["none", "qfq", "hfq"],
        as_of: str | date | datetime | None,
    ) -> tuple[
        tuple[str, ...] | None,
        date | None,
        date | None,
        Literal["none", "qfq", "hfq"],
        date | None,
    ]:
        if adjust not in {"none", "qfq", "hfq"}:
            raise ValueError("adjust 只支持 'none'、'qfq' 或 'hfq'")
        if as_of is not None and adjust != "qfq":
            raise ValueError("as_of 只适用于 adjust='qfq'")

        normalized_symbols = self._normalize_symbols(symbols)

        start_date = parse_date(start) if start is not None else None
        end_date = parse_date(end) if end is not None else None
        as_of_date = parse_date(as_of) if as_of is not None else None
        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError("start 不能晚于 end")

        return normalized_symbols, start_date, end_date, adjust, as_of_date

    @staticmethod
    def _normalize_symbols(
        symbols: str | Sequence[str] | None,
    ) -> tuple[str, ...] | None:
        if symbols is None:
            normalized_symbols = None
        elif isinstance(symbols, str):
            normalized_symbols = (symbols,)
        else:
            normalized_symbols = tuple(symbols)
        if normalized_symbols is not None and not all(
            isinstance(symbol, str) and symbol for symbol in normalized_symbols
        ):
            raise ValueError("symbols 必须是非空字符串或非空字符串序列")
        return normalized_symbols

    def update(
        self,
        *,
        start: str | date | datetime = "2010-01-01",
        end: str | date | datetime | None = None,
        progress: SyncProgress | None = None,
    ) -> tuple[SyncReport, ...]:
        self._require_writable("更新数据")
        start_date = parse_date(start)
        end_date = parse_date(end) if end is not None else date.today()
        if start_date > end_date:
            raise ValueError("start 不能晚于 end")

        reports = [
            self.sync("tushare.stock_basic", refresh=True, progress=progress),
            self.sync("tushare.namechange", refresh=True, progress=progress),
            self.sync(
                "tushare.trade_cal",
                start=start_date,
                end=end_date,
                progress=progress,
            ),
        ]
        for dataset_id in (
            "tushare.daily",
            "tushare.adj_factor",
            "tushare.daily_basic",
            "tushare.suspend_d",
            "tushare.stk_limit",
        ):
            reports.append(
                self.sync(
                    dataset_id,
                    start=start_date,
                    end=end_date,
                    progress=progress,
                )
            )
        reports.append(
            self.sync(
                "tushare.index_weight",
                start=start_date,
                end=end_date,
                progress=progress,
            )
        )
        return tuple(reports)

    def health(
        self,
        *,
        start: str | date | datetime = "2010-01-01",
        end: str | date | datetime | None = None,
    ) -> duckdb.DuckDBPyRelation:
        start_date = parse_date(start)
        end_date = parse_date(end) if end is not None else date.today()
        if start_date > end_date:
            raise ValueError("start 不能晚于 end")
        return self.store.health(start_date, end_date)

    def status(self, dataset_id: str | None = None) -> duckdb.DuckDBPyRelation:
        canonical_id = get_dataset(dataset_id).id if dataset_id is not None else None
        return self.store.status(canonical_id)

    def _require_writable(self, operation: str) -> None:
        if self.read_only:
            raise ReadOnlyDatabaseError(f"只读数据库连接不能{operation}")

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> QuantDB:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
