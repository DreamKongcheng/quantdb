from __future__ import annotations

import json
from collections.abc import Sequence
from contextlib import suppress
from datetime import date
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

import duckdb
import pandas as pd

from quantdb.errors import DatabaseConnectionError
from quantdb.registry import DATASETS, DatasetSpec, Partition, quote_identifier


class DuckDBStore:
    def __init__(self, path: str | Path, *, read_only: bool = False) -> None:
        self.path = Path(path).expanduser()
        self.read_only = read_only
        if str(self.path) != ":memory:" and not read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.connection = duckdb.connect(str(self.path), read_only=read_only)
        except duckdb.IOException as exc:
            raise DatabaseConnectionError(
                f"无法打开 DuckDB 数据库 {self.path}。文件可能正被其他进程占用：{exc}"
            ) from exc
        if not read_only:
            self.initialize()

    def initialize(self) -> None:
        self.connection.execute("CREATE SCHEMA IF NOT EXISTS meta")
        self.connection.execute("CREATE SCHEMA IF NOT EXISTS tushare")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.schema_version (
                version INTEGER PRIMARY KEY,
                description VARCHAR NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.sync_runs (
                run_id UUID PRIMARY KEY,
                dataset_id VARCHAR NOT NULL,
                partition_id VARCHAR NOT NULL,
                status VARCHAR NOT NULL CHECK (
                    status IN ('RUNNING', 'SUCCESS', 'FAILED', 'INTERRUPTED')
                ),
                request_params JSON,
                rows_received BIGINT,
                started_at TIMESTAMPTZ NOT NULL,
                finished_at TIMESTAMPTZ,
                error_type VARCHAR,
                error_message VARCHAR
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.partitions (
                dataset_id VARCHAR NOT NULL,
                partition_id VARCHAR NOT NULL,
                partition_values JSON NOT NULL,
                row_count BIGINT NOT NULL,
                run_id UUID NOT NULL,
                committed_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (dataset_id, partition_id)
            )
            """
        )
        self.recover_interrupted_runs()
        self.connection.execute(
            """
            INSERT INTO meta.schema_version
            SELECT 1, '初始化 meta 与 tushare schema', current_timestamp
            WHERE NOT EXISTS (
                SELECT 1 FROM meta.schema_version WHERE version = 1
            )
            """
        )
        for spec in DATASETS.values():
            self.connection.execute(spec.create_table_sql)
        self.connection.execute(
            """
            INSERT INTO meta.schema_version
            SELECT 2, '增加 adj_factor 与 daily_basic 数据集', current_timestamp
            WHERE NOT EXISTS (
                SELECT 1 FROM meta.schema_version WHERE version = 2
            )
            """
        )
        self._initialize_market_schema()
        self._initialize_market_metrics_schema()
        self._initialize_market_reference_schema()

    def _initialize_market_schema(self) -> None:
        self.connection.execute("CREATE SCHEMA IF NOT EXISTS market")
        self.connection.execute(
            """
            CREATE OR REPLACE VIEW market.daily_bar AS
            SELECT
                daily.ts_code,
                daily.trade_date,
                daily.open,
                daily.high,
                daily.low,
                daily.close,
                daily.pre_close,
                daily.change,
                daily.pct_chg,
                daily.vol,
                daily.amount,
                factor.adj_factor
            FROM tushare.daily AS daily
            LEFT JOIN tushare.adj_factor AS factor
                USING (ts_code, trade_date)
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE VIEW market.latest_adj_factor AS
            SELECT
                ts_code,
                max(trade_date) AS trade_date,
                arg_max(adj_factor, trade_date) AS adj_factor
            FROM tushare.adj_factor
            WHERE adj_factor IS NOT NULL
            GROUP BY ts_code
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE VIEW market.daily_bar_hfq AS
            SELECT
                ts_code,
                trade_date,
                open * adj_factor AS open,
                high * adj_factor AS high,
                low * adj_factor AS low,
                close * adj_factor AS close,
                pre_close * adj_factor AS pre_close,
                change * adj_factor AS change,
                pct_chg,
                vol,
                amount,
                adj_factor
            FROM market.daily_bar
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE VIEW market.daily_bar_qfq_latest AS
            SELECT
                bar.ts_code,
                bar.trade_date,
                bar.open * bar.adj_factor / NULLIF(anchor.adj_factor, 0) AS open,
                bar.high * bar.adj_factor / NULLIF(anchor.adj_factor, 0) AS high,
                bar.low * bar.adj_factor / NULLIF(anchor.adj_factor, 0) AS low,
                bar.close * bar.adj_factor / NULLIF(anchor.adj_factor, 0) AS close,
                bar.pre_close * bar.adj_factor
                    / NULLIF(anchor.adj_factor, 0) AS pre_close,
                bar.change * bar.adj_factor / NULLIF(anchor.adj_factor, 0) AS change,
                bar.pct_chg,
                bar.vol,
                bar.amount,
                bar.adj_factor,
                anchor.adj_factor AS anchor_adj_factor
            FROM market.daily_bar AS bar
            LEFT JOIN market.latest_adj_factor AS anchor USING (ts_code)
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE MACRO market.daily_bar_qfq_asof(as_of_date) AS TABLE (
                WITH anchor AS (
                    SELECT ts_code, arg_max(adj_factor, trade_date) AS adj_factor
                    FROM tushare.adj_factor
                    WHERE trade_date <= as_of_date
                      AND adj_factor IS NOT NULL
                    GROUP BY ts_code
                )
                SELECT
                    bar.ts_code,
                    bar.trade_date,
                    bar.open * bar.adj_factor / NULLIF(anchor.adj_factor, 0) AS open,
                    bar.high * bar.adj_factor / NULLIF(anchor.adj_factor, 0) AS high,
                    bar.low * bar.adj_factor / NULLIF(anchor.adj_factor, 0) AS low,
                    bar.close * bar.adj_factor / NULLIF(anchor.adj_factor, 0) AS close,
                    bar.pre_close * bar.adj_factor
                        / NULLIF(anchor.adj_factor, 0) AS pre_close,
                    bar.change * bar.adj_factor
                        / NULLIF(anchor.adj_factor, 0) AS change,
                    bar.pct_chg,
                    bar.vol,
                    bar.amount,
                    bar.adj_factor,
                    anchor.adj_factor AS anchor_adj_factor
                FROM market.daily_bar AS bar
                LEFT JOIN anchor USING (ts_code)
                WHERE bar.trade_date <= as_of_date
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO meta.schema_version
            SELECT 3, '增加 market 行情与复权视图', current_timestamp
            WHERE NOT EXISTS (
                SELECT 1 FROM meta.schema_version WHERE version = 3
            )
            """
        )

    def _initialize_market_metrics_schema(self) -> None:
        self.connection.execute(
            """
            CREATE OR REPLACE VIEW market.daily_metrics AS
            SELECT
                ts_code,
                trade_date,
                close,
                turnover_rate,
                turnover_rate_f,
                volume_ratio,
                pe,
                pe_ttm,
                pb,
                ps,
                ps_ttm,
                dv_ratio,
                dv_ttm,
                total_share,
                float_share,
                free_share,
                total_mv,
                circ_mv
            FROM tushare.daily_basic
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE VIEW market.daily_panel AS
            SELECT
                bar.ts_code,
                bar.trade_date,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.pre_close,
                bar.change,
                bar.pct_chg,
                bar.vol,
                bar.amount,
                bar.adj_factor,
                metrics.turnover_rate,
                metrics.turnover_rate_f,
                metrics.volume_ratio,
                metrics.pe,
                metrics.pe_ttm,
                metrics.pb,
                metrics.ps,
                metrics.ps_ttm,
                metrics.dv_ratio,
                metrics.dv_ttm,
                metrics.total_share,
                metrics.float_share,
                metrics.free_share,
                metrics.total_mv,
                metrics.circ_mv
            FROM market.daily_bar AS bar
            LEFT JOIN market.daily_metrics AS metrics
                USING (ts_code, trade_date)
            """
        )
        self.connection.execute(
            """
            INSERT INTO meta.schema_version
            SELECT 4, '增加每日指标与标准行情面板', current_timestamp
            WHERE NOT EXISTS (
                SELECT 1 FROM meta.schema_version WHERE version = 4
            )
            """
        )

    def _initialize_market_reference_schema(self) -> None:
        self.connection.execute(
            """
            CREATE OR REPLACE VIEW market.security_name_history AS
            SELECT
                ts_code,
                name,
                start_date,
                end_date,
                ann_date,
                change_reason,
                contains(upper(name), 'ST') AS is_st,
                contains(name, '退') AS is_delisting
            FROM tushare.namechange
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE MACRO market.security_status_asof(as_of_date) AS TABLE (
                WITH matching_name AS (
                    SELECT
                        history.*,
                        row_number() OVER (
                            PARTITION BY history.ts_code
                            ORDER BY history.start_date DESC, history.ann_date DESC NULLS LAST
                        ) AS match_rank
                    FROM market.security_name_history AS history
                    WHERE history.start_date <= as_of_date
                      AND (history.end_date IS NULL OR history.end_date >= as_of_date)
                )
                SELECT
                    security.ts_code,
                    security.symbol,
                    security.name AS current_name,
                    matching_name.name AS historical_name,
                    matching_name.start_date AS name_start_date,
                    matching_name.end_date AS name_end_date,
                    matching_name.ann_date AS name_ann_date,
                    matching_name.change_reason,
                    matching_name.name IS NOT NULL AS has_name_history,
                    matching_name.is_st,
                    matching_name.is_delisting,
                    security.list_date,
                    security.delist_date,
                    security.list_date <= as_of_date
                        AND (security.delist_date IS NULL OR as_of_date < security.delist_date)
                        AS is_listed
                FROM tushare.stock_basic AS security
                LEFT JOIN matching_name
                    ON matching_name.ts_code = security.ts_code
                   AND matching_name.match_rank = 1
            )
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE MACRO market.index_members_asof(
                requested_index_code, as_of_date
            ) AS TABLE (
                WITH snapshot AS (
                    SELECT max(trade_date) AS trade_date
                    FROM tushare.index_weight
                    WHERE index_code = requested_index_code
                      AND trade_date <= as_of_date
                )
                SELECT
                    weights.index_code,
                    weights.con_code,
                    weights.trade_date AS snapshot_date,
                    weights.weight
                FROM tushare.index_weight AS weights
                CROSS JOIN snapshot
                WHERE weights.index_code = requested_index_code
                  AND weights.trade_date = snapshot.trade_date
            )
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE VIEW market.trade_constraints_daily AS
            SELECT
                coalesce(limits.ts_code, suspension.ts_code) AS ts_code,
                coalesce(limits.trade_date, suspension.trade_date) AS trade_date,
                limits.pre_close,
                limits.up_limit,
                limits.down_limit,
                suspension.suspend_timing,
                suspension.suspend_type,
                coalesce(suspension.suspend_type = 'S', false) AS is_suspended,
                coalesce(suspension.suspend_type = 'R', false) AS is_resumed,
                bars.open,
                bars.high,
                bars.low,
                bars.close,
                CASE
                    WHEN bars.open IS NULL OR limits.up_limit IS NULL THEN NULL
                    ELSE bars.open >= limits.up_limit
                END AS open_at_up_limit,
                CASE
                    WHEN bars.open IS NULL OR limits.down_limit IS NULL THEN NULL
                    ELSE bars.open <= limits.down_limit
                END AS open_at_down_limit,
                CASE
                    WHEN bars.low IS NULL OR limits.up_limit IS NULL THEN NULL
                    ELSE bars.low >= limits.up_limit
                END AS locked_up_limit,
                CASE
                    WHEN bars.high IS NULL OR limits.down_limit IS NULL THEN NULL
                    ELSE bars.high <= limits.down_limit
                END AS locked_down_limit,
                CASE
                    WHEN suspension.suspend_type = 'S' THEN false
                    WHEN bars.open IS NULL OR limits.up_limit IS NULL THEN NULL
                    ELSE bars.open < limits.up_limit
                END AS can_buy_at_open,
                CASE
                    WHEN suspension.suspend_type = 'S' THEN false
                    WHEN bars.open IS NULL OR limits.down_limit IS NULL THEN NULL
                    ELSE bars.open > limits.down_limit
                END AS can_sell_at_open
            FROM tushare.stk_limit AS limits
            FULL OUTER JOIN tushare.suspend_d AS suspension
                USING (ts_code, trade_date)
            LEFT JOIN tushare.daily AS bars
                ON bars.ts_code = coalesce(limits.ts_code, suspension.ts_code)
               AND bars.trade_date = coalesce(limits.trade_date, suspension.trade_date)
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE VIEW market.stock_trade_constraints_daily AS
            WITH candidates AS (
                SELECT
                    constraints.*,
                    security.symbol,
                    security.list_date,
                    security.delist_date,
                    history.name AS historical_name,
                    history.start_date AS name_start_date,
                    history.end_date AS name_end_date,
                    history.ann_date AS name_ann_date,
                    history.change_reason,
                    history.name IS NOT NULL AS has_name_history,
                    history.is_st,
                    history.is_delisting,
                    row_number() OVER (
                        PARTITION BY constraints.ts_code, constraints.trade_date
                        ORDER BY history.start_date DESC NULLS LAST,
                                 history.ann_date DESC NULLS LAST
                    ) AS name_rank
                FROM market.trade_constraints_daily AS constraints
                JOIN tushare.stock_basic AS security
                    ON security.ts_code = constraints.ts_code
                   AND security.list_date <= constraints.trade_date
                   AND (
                       security.delist_date IS NULL
                       OR constraints.trade_date < security.delist_date
                   )
                LEFT JOIN market.security_name_history AS history
                    ON history.ts_code = constraints.ts_code
                   AND history.start_date <= constraints.trade_date
                   AND (
                       history.end_date IS NULL
                       OR history.end_date >= constraints.trade_date
                   )
            )
            SELECT
                ts_code,
                trade_date,
                symbol,
                historical_name,
                name_start_date,
                name_end_date,
                name_ann_date,
                change_reason,
                has_name_history,
                is_st,
                is_delisting,
                list_date,
                delist_date,
                pre_close,
                up_limit,
                down_limit,
                suspend_timing,
                suspend_type,
                is_suspended,
                is_resumed,
                open,
                high,
                low,
                close,
                open_at_up_limit,
                open_at_down_limit,
                locked_up_limit,
                locked_down_limit,
                can_buy_at_open,
                can_sell_at_open
            FROM candidates
            WHERE name_rank = 1
            """
        )
        self.connection.execute(
            """
            CREATE OR REPLACE MACRO market.stock_trade_constraints_asof(as_of_date) AS TABLE (
                SELECT *
                FROM market.stock_trade_constraints_daily
                WHERE trade_date = as_of_date
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO meta.schema_version
            SELECT 5, '增加证券状态、停复牌、涨跌停和指数权重数据集', current_timestamp
            WHERE NOT EXISTS (
                SELECT 1 FROM meta.schema_version WHERE version = 5
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO meta.schema_version
            SELECT 6, '增加点时点股票交易约束视图', current_timestamp
            WHERE NOT EXISTS (
                SELECT 1 FROM meta.schema_version WHERE version = 6
            )
            """
        )

    def partition_exists(self, dataset_id: str, partition_id: str) -> bool:
        row = self.connection.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM meta.partitions
                WHERE dataset_id = ? AND partition_id = ?
            )
            """,
            [dataset_id, partition_id],
        ).fetchone()
        return bool(row and row[0])

    def partition_ids(self, dataset_id: str) -> set[str]:
        rows = self.connection.execute(
            """
            SELECT partition_id
            FROM meta.partitions
            WHERE dataset_id = ?
            """,
            [dataset_id],
        ).fetchall()
        return {row[0] for row in rows}

    def start_run(self, spec: DatasetSpec, partition: Partition) -> UUID:
        run_id = uuid4()
        self.connection.execute(
            """
            INSERT INTO meta.sync_runs (
                run_id, dataset_id, partition_id, status, request_params, started_at
            ) VALUES (?, ?, ?, 'RUNNING', ?::JSON, current_timestamp)
            """,
            [
                str(run_id),
                spec.id,
                partition.id,
                json.dumps(partition.request_params, ensure_ascii=False),
            ],
        )
        return run_id

    def mark_run_failed(self, run_id: UUID, error: Exception) -> None:
        self.connection.execute(
            """
            UPDATE meta.sync_runs
            SET status = 'FAILED',
                finished_at = current_timestamp,
                error_type = ?,
                error_message = ?
            WHERE run_id = ? AND status = 'RUNNING'
            """,
            [type(error).__name__, str(error)[:4_000], str(run_id)],
        )

    def mark_run_interrupted(self, run_id: UUID, error: BaseException) -> None:
        self.connection.execute(
            """
            UPDATE meta.sync_runs
            SET status = 'INTERRUPTED',
                finished_at = current_timestamp,
                error_type = ?,
                error_message = ?
            WHERE run_id = ? AND status = 'RUNNING'
            """,
            [type(error).__name__, str(error)[:4_000] or "同步进程被中断", str(run_id)],
        )

    def recover_interrupted_runs(self) -> None:
        self.connection.execute(
            """
            UPDATE meta.sync_runs
            SET status = 'INTERRUPTED',
                finished_at = current_timestamp,
                error_type = 'ProcessInterrupted',
                error_message = '上次同步进程未正常结束，已在数据库重新打开时恢复状态'
            WHERE status = 'RUNNING'
            """
        )
        self.connection.execute(
            """
            UPDATE meta.sync_runs
            SET status = 'INTERRUPTED'
            WHERE status = 'FAILED'
              AND (
                  error_type = 'InterruptException'
                  OR (
                      error_type = 'RuntimeError'
                      AND lower(trim(error_message)) = 'query interrupted'
                  )
              )
            """
        )

    def replace_partition(
        self,
        spec: DatasetSpec,
        partition: Partition,
        frame: pd.DataFrame,
        run_id: UUID,
    ) -> None:
        incoming_name = f"_incoming_{run_id.hex}"
        registered = False
        transaction_open = False
        try:
            self.connection.register(incoming_name, frame)
            registered = True
            self.connection.execute("BEGIN TRANSACTION")
            transaction_open = True
            if partition.delete_where is None:
                self.connection.execute(f"DELETE FROM {spec.table}")
            else:
                self.connection.execute(
                    f"DELETE FROM {spec.table} WHERE {partition.delete_where}",
                    list(partition.delete_params),
                )

            columns = ", ".join(quote_identifier(column.name) for column in spec.columns)
            select_list = ",\n    ".join(column.select_expression() for column in spec.columns)
            self.connection.execute(
                f"""
                INSERT INTO {spec.table} ({columns})
                SELECT
                    {select_list}
                FROM {quote_identifier(incoming_name)}
                """
            )

            self.connection.execute(
                """
                INSERT INTO meta.partitions (
                    dataset_id, partition_id, partition_values,
                    row_count, run_id, committed_at
                ) VALUES (?, ?, ?::JSON, ?, ?, current_timestamp)
                ON CONFLICT (dataset_id, partition_id) DO UPDATE SET
                    partition_values = excluded.partition_values,
                    row_count = excluded.row_count,
                    run_id = excluded.run_id,
                    committed_at = excluded.committed_at
                """,
                [
                    spec.id,
                    partition.id,
                    json.dumps(partition.values, ensure_ascii=False),
                    len(frame),
                    str(run_id),
                ],
            )
            self.connection.execute(
                """
                UPDATE meta.sync_runs
                SET status = 'SUCCESS',
                    rows_received = ?,
                    finished_at = current_timestamp
                WHERE run_id = ?
                """,
                [len(frame), str(run_id)],
            )
            self.connection.execute("COMMIT")
            transaction_open = False
        except BaseException:
            if transaction_open:
                # 保留原始异常；连接关闭时 DuckDB 仍会清理未提交事务。
                with suppress(duckdb.Error):
                    self.connection.execute("ROLLBACK")
            raise
        finally:
            if registered:
                self.connection.unregister(incoming_name)

    def open_dates(self, start: date, end: date, *, exchange: str = "SSE") -> list[date]:
        rows = self.connection.execute(
            """
            SELECT cal_date
            FROM tushare.trade_cal
            WHERE exchange = ?
              AND cal_date BETWEEN ? AND ?
              AND is_open = 1
            ORDER BY cal_date
            """,
            [exchange, start, end],
        ).fetchall()
        return [row[0] for row in rows]

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        return self.connection.sql(query)

    def bars(
        self,
        *,
        symbols: Sequence[str] | None,
        start: date | None,
        end: date | None,
        adjust: Literal["none", "qfq", "hfq"],
        as_of: date | None,
    ) -> duckdb.DuckDBPyRelation:
        return self._market_data(
            symbols=symbols,
            start=start,
            end=end,
            adjust=adjust,
            as_of=as_of,
            include_metrics=False,
        )

    def panel(
        self,
        *,
        symbols: Sequence[str] | None,
        start: date | None,
        end: date | None,
        adjust: Literal["none", "qfq", "hfq"],
        as_of: date | None,
    ) -> duckdb.DuckDBPyRelation:
        return self._market_data(
            symbols=symbols,
            start=start,
            end=end,
            adjust=adjust,
            as_of=as_of,
            include_metrics=True,
        )

    def universe(
        self,
        *,
        as_of: date,
        index_code: str | None,
        exclude_st: bool,
        exclude_delisting: bool,
    ) -> duckdb.DuckDBPyRelation:
        params: list[object] = [as_of]
        if index_code is None:
            membership_columns = """
                NULL::VARCHAR AS index_code,
                NULL::DATE AS snapshot_date,
                NULL::DOUBLE AS weight
            """
            membership_join = ""
        else:
            membership_columns = """
                membership.index_code,
                membership.snapshot_date,
                membership.weight
            """
            membership_join = """
                JOIN market.index_members_asof(?::VARCHAR, ?::DATE) AS membership
                    ON membership.con_code = status.ts_code
            """
            params.extend((index_code, as_of))

        filters = ["status.is_listed"]
        if exclude_st:
            filters.append("status.is_st = false")
        if exclude_delisting:
            filters.append("status.is_delisting = false")

        query = f"""
            SELECT
                status.ts_code,
                status.symbol,
                status.historical_name,
                status.name_start_date,
                status.name_end_date,
                status.name_ann_date,
                status.change_reason,
                status.has_name_history,
                status.is_st,
                status.is_delisting,
                status.list_date,
                status.delist_date,
                {membership_columns}
            FROM market.security_status_asof(?::DATE) AS status
            {membership_join}
            WHERE {" AND ".join(filters)}
            ORDER BY status.ts_code
        """
        return self.connection.sql(query, params=params)

    def tradeability(
        self,
        *,
        as_of: date,
        symbols: Sequence[str] | None,
    ) -> duckdb.DuckDBPyRelation:
        params: list[object] = [as_of]
        filters: list[str] = []
        if symbols is not None:
            if symbols:
                placeholders = ", ".join("?" for _ in symbols)
                filters.append(f"constraints.ts_code IN ({placeholders})")
                params.extend(symbols)
            else:
                filters.append("false")

        query = """
            SELECT constraints.*
            FROM market.stock_trade_constraints_asof(?::DATE) AS constraints
        """
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY constraints.ts_code"
        return self.connection.sql(query, params=params)

    def _market_data(
        self,
        *,
        symbols: Sequence[str] | None,
        start: date | None,
        end: date | None,
        adjust: Literal["none", "qfq", "hfq"],
        as_of: date | None,
        include_metrics: bool,
    ) -> duckdb.DuckDBPyRelation:
        params: list[object] = []
        if adjust == "qfq" and as_of is not None:
            source = "market.daily_bar_qfq_asof(?::DATE)"
            params.append(as_of)
        else:
            source = {
                "none": "market.daily_bar",
                "qfq": "market.daily_bar_qfq_latest",
                "hfq": "market.daily_bar_hfq",
            }[adjust]

        if include_metrics:
            anchor_adj_factor = (
                "bar.anchor_adj_factor" if adjust == "qfq" else "NULL::DOUBLE AS anchor_adj_factor"
            )
            select_list = f"""
                bar.ts_code,
                bar.trade_date,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.pre_close,
                bar.change,
                bar.pct_chg,
                bar.vol,
                bar.amount,
                bar.adj_factor,
                {anchor_adj_factor},
                metrics.turnover_rate,
                metrics.turnover_rate_f,
                metrics.volume_ratio,
                metrics.pe,
                metrics.pe_ttm,
                metrics.pb,
                metrics.ps,
                metrics.ps_ttm,
                metrics.dv_ratio,
                metrics.dv_ttm,
                metrics.total_share,
                metrics.float_share,
                metrics.free_share,
                metrics.total_mv,
                metrics.circ_mv
            """
            metrics_join = """
                LEFT JOIN market.daily_metrics AS metrics
                    USING (ts_code, trade_date)
            """
        else:
            select_list = "bar.*"
            metrics_join = ""

        filters: list[str] = []
        if symbols is not None:
            if symbols:
                placeholders = ", ".join("?" for _ in symbols)
                filters.append(f"bar.ts_code IN ({placeholders})")
                params.extend(symbols)
            else:
                filters.append("false")
        if start is not None:
            filters.append("bar.trade_date >= ?::DATE")
            params.append(start)
        if end is not None:
            filters.append("bar.trade_date <= ?::DATE")
            params.append(end)

        query = f"SELECT {select_list} FROM {source} AS bar {metrics_join}"
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY bar.trade_date, bar.ts_code"
        return self.connection.sql(query, params=params)

    def health(self, start: date, end: date) -> duckdb.DuckDBPyRelation:
        return self.connection.sql(
            """
            WITH bounds AS (
                SELECT ?::DATE AS start_date, ?::DATE AS end_date
            ),
            ranged_calendar AS (
                SELECT calendar.*
                FROM tushare.trade_cal AS calendar, bounds
                WHERE calendar.exchange = 'SSE'
                  AND calendar.cal_date BETWEEN bounds.start_date AND bounds.end_date
            ),
            open_dates AS (
                SELECT cal_date
                FROM ranged_calendar
                WHERE is_open = 1
            ),
            ranged_daily AS (
                SELECT daily.*
                FROM tushare.daily AS daily, bounds
                WHERE daily.trade_date BETWEEN bounds.start_date AND bounds.end_date
            ),
            ranged_factor AS (
                SELECT factor.*
                FROM tushare.adj_factor AS factor, bounds
                WHERE factor.trade_date BETWEEN bounds.start_date AND bounds.end_date
            ),
            ranged_metrics AS (
                SELECT metrics.*
                FROM tushare.daily_basic AS metrics, bounds
                WHERE metrics.trade_date BETWEEN bounds.start_date AND bounds.end_date
            ),
            ranged_suspension AS (
                SELECT suspension.*
                FROM tushare.suspend_d AS suspension, bounds
                WHERE suspension.trade_date BETWEEN bounds.start_date AND bounds.end_date
            ),
            ranged_limits AS (
                SELECT limits.*
                FROM tushare.stk_limit AS limits, bounds
                WHERE limits.trade_date BETWEEN bounds.start_date AND bounds.end_date
            ),
            suspend_partition_dates AS (
                SELECT try_cast(partition_values ->> 'trade_date' AS DATE) AS trade_date
                FROM meta.partitions, bounds
                WHERE dataset_id = 'tushare.suspend_d'
                  AND try_cast(partition_values ->> 'trade_date' AS DATE)
                      BETWEEN bounds.start_date AND bounds.end_date
            ),
            calendar_health AS (
                SELECT
                    date_diff('day', start_date, end_date) + 1 AS expected_days,
                    (SELECT count(DISTINCT cal_date) FROM ranged_calendar) AS available_days
                FROM bounds
            ),
            coverage AS (
                SELECT
                    'tushare.stock_basic' AS dataset_id,
                    NULL::BIGINT AS expected_days,
                    NULL::BIGINT AS available_days,
                    count(*)::BIGINT AS row_count,
                    NULL::DATE AS first_date,
                    NULL::DATE AS last_date,
                    NULL::BIGINT AS unmatched_daily_rows
                FROM tushare.stock_basic

                UNION ALL

                SELECT
                    'tushare.namechange',
                    NULL::BIGINT,
                    NULL::BIGINT,
                    count(*)::BIGINT,
                    min(start_date),
                    max(coalesce(end_date, start_date)),
                    NULL::BIGINT
                FROM tushare.namechange

                UNION ALL

                SELECT
                    'tushare.trade_cal',
                    health.expected_days,
                    health.available_days,
                    count(calendar.cal_date),
                    min(calendar.cal_date),
                    max(calendar.cal_date),
                    NULL::BIGINT
                FROM calendar_health AS health
                LEFT JOIN ranged_calendar AS calendar ON true
                GROUP BY health.expected_days, health.available_days

                UNION ALL

                SELECT
                    'tushare.daily',
                    (SELECT count(*) FROM open_dates),
                    (
                        SELECT count(*)
                        FROM open_dates AS expected
                        WHERE EXISTS (
                            SELECT 1 FROM ranged_daily AS actual
                            WHERE actual.trade_date = expected.cal_date
                        )
                    ),
                    count(*),
                    min(trade_date),
                    max(trade_date),
                    NULL::BIGINT
                FROM ranged_daily

                UNION ALL

                SELECT
                    'tushare.adj_factor',
                    (SELECT count(*) FROM open_dates),
                    (
                        SELECT count(*)
                        FROM open_dates AS expected
                        WHERE EXISTS (
                            SELECT 1 FROM ranged_factor AS actual
                            WHERE actual.trade_date = expected.cal_date
                        )
                    ),
                    count(*),
                    min(trade_date),
                    max(trade_date),
                    (
                        SELECT count(*)
                        FROM ranged_daily AS daily
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM ranged_factor AS factor
                            WHERE factor.ts_code = daily.ts_code
                              AND factor.trade_date = daily.trade_date
                        )
                    )
                FROM ranged_factor

                UNION ALL

                SELECT
                    'tushare.daily_basic',
                    (SELECT count(*) FROM open_dates),
                    (
                        SELECT count(*)
                        FROM open_dates AS expected
                        WHERE EXISTS (
                            SELECT 1 FROM ranged_metrics AS actual
                            WHERE actual.trade_date = expected.cal_date
                        )
                    ),
                    count(*),
                    min(trade_date),
                    max(trade_date),
                    (
                        SELECT count(*)
                        FROM ranged_daily AS daily
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM ranged_metrics AS metrics
                            WHERE metrics.ts_code = daily.ts_code
                              AND metrics.trade_date = daily.trade_date
                        )
                    )
                FROM ranged_metrics

                UNION ALL

                SELECT
                    'tushare.suspend_d',
                    (SELECT count(*) FROM open_dates),
                    (
                        SELECT count(*)
                        FROM open_dates AS expected
                        WHERE EXISTS (
                            SELECT 1 FROM ranged_suspension AS actual
                            WHERE actual.trade_date = expected.cal_date
                        ) OR EXISTS (
                            SELECT 1 FROM suspend_partition_dates AS partition
                            WHERE partition.trade_date = expected.cal_date
                        )
                    ),
                    count(*),
                    min(trade_date),
                    max(trade_date),
                    NULL::BIGINT
                FROM ranged_suspension

                UNION ALL

                SELECT
                    'tushare.stk_limit',
                    (SELECT count(*) FROM open_dates),
                    (
                        SELECT count(*)
                        FROM open_dates AS expected
                        WHERE EXISTS (
                            SELECT 1 FROM ranged_limits AS actual
                            WHERE actual.trade_date = expected.cal_date
                        )
                    ),
                    count(*),
                    min(trade_date),
                    max(trade_date),
                    NULL::BIGINT
                FROM ranged_limits

                UNION ALL

                SELECT
                    'tushare.index_weight',
                    NULL::BIGINT,
                    NULL::BIGINT,
                    count(*)::BIGINT,
                    min(trade_date),
                    max(trade_date),
                    NULL::BIGINT
                FROM tushare.index_weight
            ),
            commits AS (
                SELECT dataset_id, max(committed_at) AS last_committed_at
                FROM meta.partitions
                GROUP BY dataset_id
            )
            SELECT
                coverage.dataset_id,
                CASE
                    WHEN coverage.dataset_id IN (
                        'tushare.stock_basic',
                        'tushare.namechange',
                        'tushare.index_weight'
                    )
                        AND coverage.row_count = 0 THEN 'EMPTY'
                    WHEN coverage.dataset_id IN (
                        'tushare.daily',
                        'tushare.adj_factor',
                        'tushare.daily_basic',
                        'tushare.suspend_d',
                        'tushare.stk_limit'
                    ) AND calendar_health.available_days < calendar_health.expected_days
                        THEN 'CALENDAR_INCOMPLETE'
                    WHEN coalesce(coverage.available_days, 0)
                            < coalesce(coverage.expected_days, 0)
                        OR (
                            coverage.dataset_id = 'tushare.adj_factor'
                            AND coalesce(coverage.unmatched_daily_rows, 0) > 0
                        ) THEN 'INCOMPLETE'
                    ELSE 'HEALTHY'
                END AS status,
                coverage.expected_days,
                coverage.available_days,
                greatest(
                    coalesce(coverage.expected_days, 0)
                        - coalesce(coverage.available_days, 0),
                    0
                ) AS missing_days,
                coverage.unmatched_daily_rows,
                coverage.row_count,
                coverage.first_date,
                coverage.last_date,
                commits.last_committed_at
            FROM coverage
            CROSS JOIN calendar_health
            LEFT JOIN commits USING (dataset_id)
            ORDER BY CASE coverage.dataset_id
                WHEN 'tushare.stock_basic' THEN 1
                WHEN 'tushare.namechange' THEN 2
                WHEN 'tushare.trade_cal' THEN 3
                WHEN 'tushare.daily' THEN 4
                WHEN 'tushare.adj_factor' THEN 5
                WHEN 'tushare.daily_basic' THEN 6
                WHEN 'tushare.suspend_d' THEN 7
                WHEN 'tushare.stk_limit' THEN 8
                WHEN 'tushare.index_weight' THEN 9
            END
            """,
            params=[start, end],
        )

    def status(self, dataset_id: str | None = None) -> duckdb.DuckDBPyRelation:
        query = """
            SELECT dataset_id, partition_id, row_count, committed_at, run_id
            FROM meta.partitions
        """
        if dataset_id is not None:
            escaped = dataset_id.replace("'", "''")
            query += f" WHERE dataset_id = '{escaped}'"
        query += " ORDER BY dataset_id, partition_id"
        return self.connection.sql(query)

    def close(self) -> None:
        self.connection.close()
