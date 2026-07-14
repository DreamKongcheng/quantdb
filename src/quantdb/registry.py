from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

from quantdb.errors import DatasetNotFoundError

PartitionStrategy = Literal["full", "calendar_year", "trading_day"]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    duckdb_type: str
    source_date_format: str | None = None

    def select_expression(self) -> str:
        identifier = quote_identifier(self.name)
        if self.source_date_format:
            return (
                "strptime(NULLIF(TRIM(CAST("
                f"{identifier} AS VARCHAR)), ''), '{self.source_date_format}')::DATE "
                f"AS {identifier}"
            )
        return f"CAST({identifier} AS {self.duckdb_type}) AS {identifier}"


@dataclass(frozen=True)
class Partition:
    id: str
    values: Mapping[str, str | int]
    request_params: Mapping[str, str | int]
    delete_where: str | None
    delete_params: tuple[object, ...] = ()


@dataclass(frozen=True)
class DatasetSpec:
    id: str
    endpoint: str
    table: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    partition_strategy: PartitionStrategy
    allow_empty: bool = False
    dependencies: tuple[str, ...] = ()
    request_variants: tuple[Mapping[str, str], ...] = field(default_factory=lambda: ({},))

    @property
    def source_fields(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def create_table_sql(self) -> str:
        column_sql = ",\n    ".join(
            f"{quote_identifier(column.name)} {column.duckdb_type}" for column in self.columns
        )
        primary_key_sql = ", ".join(quote_identifier(name) for name in self.primary_key)
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table} (\n"
            f"    {column_sql},\n"
            f"    PRIMARY KEY ({primary_key_sql})\n"
            ")"
        )


def column(name: str, duckdb_type: str, date_format: str | None = None) -> ColumnSpec:
    return ColumnSpec(name, duckdb_type, date_format)


STOCK_BASIC = DatasetSpec(
    id="tushare.stock_basic",
    endpoint="stock_basic",
    table="tushare.stock_basic",
    columns=(
        column("ts_code", "VARCHAR"),
        column("symbol", "VARCHAR"),
        column("name", "VARCHAR"),
        column("area", "VARCHAR"),
        column("industry", "VARCHAR"),
        column("fullname", "VARCHAR"),
        column("enname", "VARCHAR"),
        column("cnspell", "VARCHAR"),
        column("market", "VARCHAR"),
        column("exchange", "VARCHAR"),
        column("curr_type", "VARCHAR"),
        column("list_status", "VARCHAR"),
        column("list_date", "DATE", "%Y%m%d"),
        column("delist_date", "DATE", "%Y%m%d"),
        column("is_hs", "VARCHAR"),
        column("act_name", "VARCHAR"),
        column("act_ent_type", "VARCHAR"),
    ),
    primary_key=("ts_code",),
    partition_strategy="full",
    request_variants=(
        {"list_status": "L"},
        {"list_status": "D"},
        {"list_status": "P"},
    ),
)

TRADE_CAL = DatasetSpec(
    id="tushare.trade_cal",
    endpoint="trade_cal",
    table="tushare.trade_cal",
    columns=(
        column("exchange", "VARCHAR"),
        column("cal_date", "DATE", "%Y%m%d"),
        column("is_open", "TINYINT"),
        column("pretrade_date", "DATE", "%Y%m%d"),
    ),
    primary_key=("exchange", "cal_date"),
    partition_strategy="calendar_year",
)

DAILY = DatasetSpec(
    id="tushare.daily",
    endpoint="daily",
    table="tushare.daily",
    columns=(
        column("ts_code", "VARCHAR"),
        column("trade_date", "DATE", "%Y%m%d"),
        column("open", "DOUBLE"),
        column("high", "DOUBLE"),
        column("low", "DOUBLE"),
        column("close", "DOUBLE"),
        column("pre_close", "DOUBLE"),
        column("change", "DOUBLE"),
        column("pct_chg", "DOUBLE"),
        column("vol", "DOUBLE"),
        column("amount", "DOUBLE"),
    ),
    primary_key=("ts_code", "trade_date"),
    partition_strategy="trading_day",
    dependencies=(STOCK_BASIC.id, TRADE_CAL.id),
)

DATASETS: dict[str, DatasetSpec] = {spec.id: spec for spec in (STOCK_BASIC, TRADE_CAL, DAILY)}


def get_dataset(dataset_id: str) -> DatasetSpec:
    canonical_id = dataset_id if "." in dataset_id else f"tushare.{dataset_id}"
    try:
        return DATASETS[canonical_id]
    except KeyError as exc:
        registered = ", ".join(sorted(DATASETS))
        raise DatasetNotFoundError(f"未注册数据集 {dataset_id!r}，当前支持：{registered}") from exc


def parse_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def full_partition() -> Partition:
    return Partition(
        id="full",
        values={"scope": "full"},
        request_params={},
        delete_where=None,
    )


def calendar_year_partitions(
    start: str | date | datetime | None,
    end: str | date | datetime | None,
    *,
    exchange: str = "SSE",
) -> list[Partition]:
    today = date.today()
    start_date = parse_date(start) if start is not None else date(today.year, 1, 1)
    end_date = parse_date(end) if end is not None else date(start_date.year, 12, 31)
    if start_date > end_date:
        raise ValueError("start 不能晚于 end")

    partitions: list[Partition] = []
    for year in range(start_date.year, end_date.year + 1):
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)
        partitions.append(
            Partition(
                id=f"exchange={exchange}/year={year}",
                values={"exchange": exchange, "year": year},
                request_params={
                    "exchange": exchange,
                    "start_date": year_start.strftime("%Y%m%d"),
                    "end_date": year_end.strftime("%Y%m%d"),
                },
                delete_where='"exchange" = ? AND "cal_date" BETWEEN ? AND ?',
                delete_params=(exchange, year_start, year_end),
            )
        )
    return partitions


def daily_partition(trade_date: date) -> Partition:
    return Partition(
        id=f"trade_date={trade_date.isoformat()}",
        values={"trade_date": trade_date.isoformat()},
        request_params={"trade_date": trade_date.strftime("%Y%m%d")},
        delete_where='"trade_date" = ?',
        delete_params=(trade_date,),
    )


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
