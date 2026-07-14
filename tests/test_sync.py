from __future__ import annotations

from collections import Counter

import pandas as pd
import pytest

from quantdb import QuantDB
from quantdb.errors import SyncError

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


class FakeProvider:
    def __init__(self):
        self.calls = Counter()
        self.daily_close: object = 10.5
        self.fail_daily = False

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
        if self.fail_daily:
            raise ConnectionError("network interrupted")
        return daily_frame(str(partition.request_params["trade_date"]), self.daily_close)


def test_daily_sync_automatically_prepares_dependencies(tmp_path):
    provider = FakeProvider()
    with QuantDB(tmp_path / "quantdb.duckdb", provider=provider) as db:
        report = db.sync("tushare.daily", start="2024-01-01", end="2024-01-03")

        assert report.completed == 2
        assert provider.calls == {
            "tushare.stock_basic": 1,
            "tushare.trade_cal": 1,
            "tushare.daily": 2,
        }
        assert db.sql("SELECT count(*) FROM tushare.daily").fetchone()[0] == 2


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
