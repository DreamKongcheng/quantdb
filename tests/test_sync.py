from __future__ import annotations

from collections import Counter
from datetime import date

import pandas as pd
import pytest

from quantdb import QuantDB
from quantdb.errors import SyncError, SyncInterruptedError
from quantdb.registry import DAILY, daily_partition

STOCK_COLUMNS = {
    "ts_code": ["000001.SZ"],
    "symbol": ["000001"],
    "name": ["平安银行"],
    "area": ["深圳"],
    "industry": ["银行"],
    "fullname": ["平安银行股份有限公司"],
    "enname": ["Ping An Bank"],
    "cnspell": ["payh"],
    "market": ["主板"],
    "exchange": ["SZSE"],
    "curr_type": ["CNY"],
    "list_status": ["L"],
    "list_date": ["19910403"],
    "delist_date": [None],
    "is_hs": ["S"],
    "act_name": ["无实际控制人"],
    "act_ent_type": [""],
}


def daily_frame(day: str, close: object = 10.5) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [day],
            "open": [10.0],
            "high": [11.0],
            "low": [9.8],
            "close": [close],
            "pre_close": [9.9],
            "change": [0.6],
            "pct_chg": [6.06],
            "vol": [1000.0],
            "amount": [10500.0],
        }
    )


def adj_factor_frame(day: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [day],
            "adj_factor": [123.456],
        }
    )


def daily_basic_frame(day: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [day],
            "close": [10.5],
            "turnover_rate": [1.2],
            "turnover_rate_f": [1.5],
            "volume_ratio": [0.8],
            "pe": [6.0],
            "pe_ttm": [5.8],
            "pb": [0.7],
            "ps": [1.1],
            "ps_ttm": [1.0],
            "dv_ratio": [2.0],
            "dv_ttm": [2.1],
            "total_share": [19_405_918.2],
            "float_share": [19_405_575.4],
            "free_share": [10_000_000.0],
            "total_mv": [20_000_000.0],
            "circ_mv": [19_999_000.0],
        }
    )


class FakeProvider:
    def __init__(self):
        self.calls = Counter()
        self.daily_close: object = 10.5
        self.fail_daily = False
        self.interrupt_daily = False
        self.interrupt_query_daily = False

    def fetch(self, spec, partition):
        self.calls[spec.id] += 1
        if spec.id == "tushare.stock_basic":
            return pd.DataFrame(STOCK_COLUMNS)
        if spec.id == "tushare.trade_cal":
            year = partition.values["year"]
            return pd.DataFrame(
                {
                    "exchange": ["SSE", "SSE", "SSE"],
                    "cal_date": [f"{year}0101", f"{year}0102", f"{year}0103"],
                    "is_open": [0, 1, 1],
                    "pretrade_date": [None, f"{year - 1}1231", f"{year}0102"],
                }
            )
        if spec.id == "tushare.adj_factor":
            return adj_factor_frame(str(partition.request_params["trade_date"]))
        if spec.id == "tushare.daily_basic":
            return daily_basic_frame(str(partition.request_params["trade_date"]))
        if self.fail_daily:
            raise ConnectionError("network interrupted")
        if self.interrupt_daily:
            raise KeyboardInterrupt
        if self.interrupt_query_daily:
            raise RuntimeError("Query interrupted")
        return daily_frame(str(partition.request_params["trade_date"]), self.daily_close)


class RecordingProgress:
    def __init__(self):
        self.events = []

    def dataset_started(self, dataset_id, total):
        self.events.append(("dataset_started", dataset_id, total))

    def partition_started(self, dataset_id, partition_id):
        self.events.append(("partition_started", dataset_id, partition_id))

    def partition_finished(self, result):
        self.events.append(("partition_finished", result.dataset_id, result.status))

    def partition_failed(self, dataset_id, partition_id, error):
        self.events.append(("partition_failed", dataset_id, partition_id, type(error).__name__))

    def partition_interrupted(self, dataset_id, partition_id, error):
        self.events.append(
            ("partition_interrupted", dataset_id, partition_id, type(error).__name__)
        )

    def dataset_finished(self, dataset_id):
        self.events.append(("dataset_finished", dataset_id))


def test_daily_sync_automatically_prepares_trade_calendar(tmp_path):
    provider = FakeProvider()
    progress = RecordingProgress()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        report = db.sync(
            "tushare.daily",
            start="2024-01-01",
            end="2024-01-03",
            progress=progress,
        )

        assert report.completed == 2
        assert provider.calls == {
            "tushare.trade_cal": 1,
            "tushare.daily": 2,
        }
        assert db.sql("SELECT count(*) FROM tushare.daily").fetchone()[0] == 2
        assert [event for event in progress.events if event[0] == "dataset_started"] == [
            ("dataset_started", "tushare.trade_cal", 1),
            ("dataset_started", "tushare.daily", 2),
        ]
        assert [event for event in progress.events if event[0] == "dataset_finished"] == [
            ("dataset_finished", "tushare.trade_cal"),
            ("dataset_finished", "tushare.daily"),
        ]


def test_adj_factor_and_daily_basic_reuse_trading_day_sync(tmp_path):
    provider = FakeProvider()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        adj_report = db.sync(
            "tushare.adj_factor",
            start="2024-01-01",
            end="2024-01-03",
        )
        basic_report = db.sync(
            "tushare.daily_basic",
            start="2024-01-01",
            end="2024-01-03",
        )

        assert adj_report.completed == 2
        assert basic_report.completed == 2
        assert provider.calls == {
            "tushare.trade_cal": 1,
            "tushare.adj_factor": 2,
            "tushare.daily_basic": 2,
        }
        assert db.sql("SELECT count(*) FROM tushare.adj_factor").fetchone()[0] == 2
        assert db.sql("SELECT count(*) FROM tushare.daily_basic").fetchone()[0] == 2
        assert db.sql("SELECT version FROM meta.schema_version ORDER BY version").fetchall() == [
            (1,),
            (2,),
            (3,),
            (4,),
        ]


def test_update_refreshes_stock_and_fills_all_missing_daily_datasets(tmp_path):
    provider = FakeProvider()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        reports = db.update(start="2024-01-01", end="2024-01-03")

        assert [report.dataset_id for report in reports] == [
            "tushare.stock_basic",
            "tushare.trade_cal",
            "tushare.daily",
            "tushare.adj_factor",
            "tushare.daily_basic",
        ]
        assert provider.calls == {
            "tushare.stock_basic": 1,
            "tushare.trade_cal": 1,
            "tushare.daily": 2,
            "tushare.adj_factor": 2,
            "tushare.daily_basic": 2,
        }
        assert db.health(start="2024-01-01", end="2024-01-03").project(
            "dataset_id, status"
        ).fetchall() == [
            ("tushare.stock_basic", "HEALTHY"),
            ("tushare.trade_cal", "HEALTHY"),
            ("tushare.daily", "HEALTHY"),
            ("tushare.adj_factor", "HEALTHY"),
            ("tushare.daily_basic", "HEALTHY"),
        ]

        second_reports = db.update(start="2024-01-01", end="2024-01-03")

        assert provider.calls == {
            "tushare.stock_basic": 2,
            "tushare.trade_cal": 1,
            "tushare.daily": 2,
            "tushare.adj_factor": 2,
            "tushare.daily_basic": 2,
        }
        assert second_reports[0].completed == 1
        assert second_reports[1].skipped == 1
        assert [report.skipped for report in second_reports[2:]] == [2, 2, 2]


def test_failed_refresh_keeps_previous_partition(tmp_path):
    provider = FakeProvider()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        db.sync("tushare.daily", start="2024-01-02")
        old_partition = db.sql(
            "SELECT row_count, run_id FROM meta.partitions WHERE dataset_id = 'tushare.daily'"
        ).fetchone()

        provider.fail_daily = True
        with pytest.raises(SyncError, match="network interrupted"):
            db.sync("tushare.daily", start="2024-01-02", refresh=True)

        assert db.sql("SELECT close FROM tushare.daily").fetchone()[0] == 10.5
        assert (
            db.sql(
                "SELECT row_count, run_id FROM meta.partitions WHERE dataset_id = 'tushare.daily'"
            ).fetchone()
            == old_partition
        )


def test_database_write_failure_rolls_back_partition_replacement(tmp_path):
    provider = FakeProvider()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        db.sync("tushare.daily", start="2024-01-02")

        provider.daily_close = "not-a-number"
        with pytest.raises(SyncError):
            db.sync("tushare.daily", start="2024-01-02", refresh=True)

        assert db.sql("SELECT close FROM tushare.daily").fetchone()[0] == 10.5
        statuses = db.sql(
            "SELECT status FROM meta.sync_runs "
            "WHERE dataset_id = 'tushare.daily' ORDER BY started_at"
        ).fetchall()
        assert statuses == [("SUCCESS",), ("FAILED",)]


def test_keyboard_interrupt_during_fetch_keeps_old_partition(tmp_path):
    provider = FakeProvider()
    progress = RecordingProgress()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        db.sync("tushare.daily", start="2024-01-02")
        old_partition = db.sql(
            "SELECT run_id FROM meta.partitions WHERE dataset_id = 'tushare.daily'"
        ).fetchone()

        provider.interrupt_daily = True
        with pytest.raises(KeyboardInterrupt):
            db.sync("tushare.daily", start="2024-01-02", refresh=True, progress=progress)

        assert db.sql("SELECT close FROM tushare.daily").fetchone()[0] == 10.5
        assert (
            db.sql(
                "SELECT run_id FROM meta.partitions WHERE dataset_id = 'tushare.daily'"
            ).fetchone()
            == old_partition
        )
        assert db.sql(
            "SELECT status FROM meta.sync_runs "
            "WHERE dataset_id = 'tushare.daily' ORDER BY started_at DESC LIMIT 1"
        ).fetchone() == ("INTERRUPTED",)
        assert any(event[0] == "partition_interrupted" for event in progress.events)


def test_duckdb_runtime_query_interrupt_is_classified_as_interrupted(tmp_path):
    provider = FakeProvider()
    progress = RecordingProgress()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        db.sync("tushare.daily", start="2024-01-02")
        provider.interrupt_query_daily = True

        with pytest.raises(SyncInterruptedError):
            db.sync("tushare.daily", start="2024-01-02", refresh=True, progress=progress)

        assert db.sql("SELECT close FROM tushare.daily").fetchone()[0] == 10.5
        assert db.sql(
            "SELECT status, error_type, error_message FROM meta.sync_runs "
            "WHERE dataset_id = 'tushare.daily' ORDER BY started_at DESC LIMIT 1"
        ).fetchone() == ("INTERRUPTED", "RuntimeError", "Query interrupted")
        assert any(event[0] == "partition_interrupted" for event in progress.events)


class InterruptingConnection:
    def __init__(self, connection):
        self.connection = connection
        self.interrupted = False

    def execute(self, query, *args, **kwargs):
        normalized = " ".join(query.split())
        if not self.interrupted and normalized.startswith("INSERT INTO tushare.daily"):
            self.interrupted = True
            raise KeyboardInterrupt
        return self.connection.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.connection, name)


def test_keyboard_interrupt_during_write_explicitly_rolls_back_delete(tmp_path):
    provider = FakeProvider()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        db.sync("tushare.daily", start="2024-01-02")
        provider.daily_close = 99.0
        db.store.connection = InterruptingConnection(db.store.connection)

        with pytest.raises(KeyboardInterrupt):
            db.sync("tushare.daily", start="2024-01-02", refresh=True)

        assert db.sql("SELECT close FROM tushare.daily").fetchone()[0] == 10.5
        assert db.sql(
            "SELECT status FROM meta.sync_runs "
            "WHERE dataset_id = 'tushare.daily' ORDER BY started_at DESC LIMIT 1"
        ).fetchone() == ("INTERRUPTED",)


def test_reopening_database_recovers_stale_running_status(tmp_path):
    path = tmp_path / "quantdb.duckdb"
    provider = FakeProvider()
    with QuantDB(path, provider=provider) as db:
        run_id = db.store.start_run(DAILY, daily_partition(date(2024, 1, 2)))

    with QuantDB(path, provider=provider) as db:
        assert db.sql(
            f"SELECT status, error_type FROM meta.sync_runs WHERE run_id = '{run_id}'"
        ).fetchone() == ("INTERRUPTED", "ProcessInterrupted")


def test_reopening_database_repairs_old_query_interrupt_status(tmp_path):
    path = tmp_path / "quantdb.duckdb"
    provider = FakeProvider()
    with QuantDB(path, provider=provider) as db:
        run_id = db.store.start_run(DAILY, daily_partition(date(2024, 1, 2)))
        db.store.mark_run_failed(run_id, RuntimeError("Query interrupted"))

    with QuantDB(path, provider=provider) as db:
        assert db.sql(
            f"SELECT status, error_type FROM meta.sync_runs WHERE run_id = '{run_id}'"
        ).fetchone() == ("INTERRUPTED", "RuntimeError")


def test_late_error_cannot_overwrite_committed_success_status(tmp_path):
    provider = FakeProvider()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        db.sync("tushare.daily", start="2024-01-02")
        run_id = db.sql(
            "SELECT run_id FROM meta.sync_runs "
            "WHERE dataset_id = 'tushare.daily' AND status = 'SUCCESS'"
        ).fetchone()[0]

        db.store.mark_run_failed(run_id, RuntimeError("late error"))
        db.store.mark_run_interrupted(run_id, KeyboardInterrupt())

        assert db.sql(
            f"SELECT status FROM meta.sync_runs WHERE run_id = '{run_id}'"
        ).fetchone() == ("SUCCESS",)
