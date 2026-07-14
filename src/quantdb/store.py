from __future__ import annotations

import json
from contextlib import suppress
from datetime import date
from pathlib import Path
from uuid import UUID, uuid4

import duckdb
import pandas as pd

from quantdb.errors import DatabaseConnectionError
from quantdb.registry import DATASETS, DatasetSpec, Partition, quote_identifier


class DuckDBStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.connection = duckdb.connect(str(self.path))
        except duckdb.IOException as exc:
            raise DatabaseConnectionError(
                f"无法打开 DuckDB 数据库 {self.path}。文件可能正被其他进程占用：{exc}"
            ) from exc
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
