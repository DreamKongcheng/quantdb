from __future__ import annotations

from calendar import monthrange
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

from quantdb.errors import DatasetNotFoundError

PartitionStrategy = Literal["full", "calendar_year", "trading_day", "index_month"]

INDEX_WEIGHT_CODES = ("000300.SH", "000905.SH", "000985.CSI")


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
    required_columns: tuple[str, ...] = ()
    allow_empty: bool = False
    dependencies: tuple[str, ...] = ()
    request_variants: tuple[Mapping[str, str], ...] = field(default_factory=lambda: ({},))
    requests_per_minute: int | None = None
    deduplicate_exact_rows: bool = False

    @property
    def source_fields(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def create_table_sql(self) -> str:
        required_columns = set(self.primary_key) | set(self.required_columns)
        definitions = [
            f"{quote_identifier(column.name)} {column.duckdb_type}"
            + (" NOT NULL" if column.name in required_columns else "")
            for column in self.columns
        ]
        if self.primary_key:
            primary_key_sql = ", ".join(quote_identifier(name) for name in self.primary_key)
            definitions.append(f"PRIMARY KEY ({primary_key_sql})")
        definition_sql = ",\n    ".join(definitions)
        return f"CREATE TABLE IF NOT EXISTS {self.table} (\n    {definition_sql}\n)"


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

NAMECHANGE = DatasetSpec(
    id="tushare.namechange",
    endpoint="namechange",
    table="tushare.namechange",
    columns=(
        column("ts_code", "VARCHAR"),
        column("name", "VARCHAR"),
        column("start_date", "DATE", "%Y%m%d"),
        column("end_date", "DATE", "%Y%m%d"),
        column("ann_date", "DATE", "%Y%m%d"),
        column("change_reason", "VARCHAR"),
    ),
    primary_key=("ts_code", "start_date"),
    partition_strategy="full",
    requests_per_minute=180,
    deduplicate_exact_rows=True,
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
    dependencies=(TRADE_CAL.id,),
)

ADJ_FACTOR = DatasetSpec(
    id="tushare.adj_factor",
    endpoint="adj_factor",
    table="tushare.adj_factor",
    columns=(
        column("ts_code", "VARCHAR"),
        column("trade_date", "DATE", "%Y%m%d"),
        column("adj_factor", "DOUBLE"),
    ),
    primary_key=("ts_code", "trade_date"),
    partition_strategy="trading_day",
    dependencies=(TRADE_CAL.id,),
    requests_per_minute=180,
)

DAILY_BASIC = DatasetSpec(
    id="tushare.daily_basic",
    endpoint="daily_basic",
    table="tushare.daily_basic",
    columns=(
        column("ts_code", "VARCHAR"),
        column("trade_date", "DATE", "%Y%m%d"),
        column("close", "DOUBLE"),
        column("turnover_rate", "DOUBLE"),
        column("turnover_rate_f", "DOUBLE"),
        column("volume_ratio", "DOUBLE"),
        column("pe", "DOUBLE"),
        column("pe_ttm", "DOUBLE"),
        column("pb", "DOUBLE"),
        column("ps", "DOUBLE"),
        column("ps_ttm", "DOUBLE"),
        column("dv_ratio", "DOUBLE"),
        column("dv_ttm", "DOUBLE"),
        column("total_share", "DOUBLE"),
        column("float_share", "DOUBLE"),
        column("free_share", "DOUBLE"),
        column("total_mv", "DOUBLE"),
        column("circ_mv", "DOUBLE"),
    ),
    primary_key=("ts_code", "trade_date"),
    partition_strategy="trading_day",
    dependencies=(TRADE_CAL.id,),
    requests_per_minute=180,
)

SUSPEND_D = DatasetSpec(
    id="tushare.suspend_d",
    endpoint="suspend_d",
    table="tushare.suspend_d",
    columns=(
        column("ts_code", "VARCHAR"),
        column("trade_date", "DATE", "%Y%m%d"),
        column("suspend_timing", "VARCHAR"),
        column("suspend_type", "VARCHAR"),
    ),
    # 同一证券日可能同时有 S/R 两类事件，且 suspend_timing 可以为空，因此没有可靠自然键。
    primary_key=(),
    partition_strategy="trading_day",
    required_columns=("ts_code", "trade_date", "suspend_type"),
    allow_empty=True,
    dependencies=(TRADE_CAL.id,),
    requests_per_minute=180,
    deduplicate_exact_rows=True,
)

STK_LIMIT = DatasetSpec(
    id="tushare.stk_limit",
    endpoint="stk_limit",
    table="tushare.stk_limit",
    columns=(
        column("trade_date", "DATE", "%Y%m%d"),
        column("ts_code", "VARCHAR"),
        column("pre_close", "DOUBLE"),
        column("up_limit", "DOUBLE"),
        column("down_limit", "DOUBLE"),
    ),
    primary_key=("ts_code", "trade_date"),
    partition_strategy="trading_day",
    dependencies=(TRADE_CAL.id,),
    requests_per_minute=180,
)

INDEX_WEIGHT = DatasetSpec(
    id="tushare.index_weight",
    endpoint="index_weight",
    table="tushare.index_weight",
    columns=(
        column("index_code", "VARCHAR"),
        column("con_code", "VARCHAR"),
        column("trade_date", "DATE", "%Y%m%d"),
        column("weight", "DOUBLE"),
    ),
    primary_key=("index_code", "con_code", "trade_date"),
    partition_strategy="index_month",
    allow_empty=True,
    requests_per_minute=180,
)

DATASETS: dict[str, DatasetSpec] = {
    spec.id: spec
    for spec in (
        STOCK_BASIC,
        NAMECHANGE,
        TRADE_CAL,
        DAILY,
        ADJ_FACTOR,
        DAILY_BASIC,
        SUSPEND_D,
        STK_LIMIT,
        INDEX_WEIGHT,
    )
}


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


def index_month_partitions(
    start: str | date | datetime | None,
    end: str | date | datetime | None,
    *,
    index_codes: tuple[str, ...] = INDEX_WEIGHT_CODES,
) -> list[Partition]:
    if start is None:
        raise ValueError("同步 tushare.index_weight 必须提供 start")
    start_date = parse_date(start)
    end_date = parse_date(end) if end is not None else start_date
    if start_date > end_date:
        raise ValueError("start 不能晚于 end")

    month_start = date(start_date.year, start_date.month, 1)
    partitions: list[Partition] = []
    while month_start <= end_date:
        month_end = date(
            month_start.year,
            month_start.month,
            monthrange(month_start.year, month_start.month)[1],
        )
        # 月度权重只提交完整月份，避免月中空结果被永久视为已同步。
        if month_end > end_date:
            break
        for index_code in index_codes:
            partitions.append(
                Partition(
                    id=f"index_code={index_code}/month={month_start:%Y-%m}",
                    values={"index_code": index_code, "month": f"{month_start:%Y-%m}"},
                    request_params={
                        "index_code": index_code,
                        "start_date": month_start.strftime("%Y%m%d"),
                        "end_date": month_end.strftime("%Y%m%d"),
                    },
                    delete_where=('"index_code" = ? AND "trade_date" BETWEEN ? AND ?'),
                    delete_params=(index_code, month_start, month_end),
                )
            )
        if month_start.month == 12:
            month_start = date(month_start.year + 1, 1, 1)
        else:
            month_start = date(month_start.year, month_start.month + 1, 1)
    return partitions


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
