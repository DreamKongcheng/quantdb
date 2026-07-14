from datetime import date

import pytest

from quantdb import QuantDB
from quantdb.errors import ReadOnlyDatabaseError


def seed_bars(db: QuantDB) -> None:
    db.sql(
        """
        INSERT INTO tushare.daily VALUES
            (
                '000001.SZ', DATE '2024-01-01',
                9.0, 11.0, 8.0, 10.0, 9.0, 1.0, 11.11, 100.0, 1000.0
            ),
            (
                '000001.SZ', DATE '2024-01-02',
                4.5, 5.5, 4.0, 5.0, 4.5, 0.5, 11.11, 200.0, 1000.0
            ),
            (
                '000002.SZ', DATE '2024-01-02',
                6.5, 7.5, 6.0, 7.0, 6.5, 0.5, 7.69, 300.0, 2100.0
            )
        """
    )
    db.sql(
        """
        INSERT INTO tushare.adj_factor VALUES
            ('000001.SZ', DATE '2024-01-01', 1.0),
            ('000001.SZ', DATE '2024-01-02', 2.0)
        """
    )


def test_market_views_apply_hfq_and_latest_qfq_formulas(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        seed_bars(db)

        assert db.sql(
            """
            SELECT trade_date, close, adj_factor
            FROM market.daily_bar
            WHERE ts_code = '000001.SZ'
            ORDER BY trade_date
            """
        ).fetchall() == [
            (date(2024, 1, 1), 10.0, 1.0),
            (date(2024, 1, 2), 5.0, 2.0),
        ]
        assert db.sql(
            """
            SELECT close, vol, amount, adj_factor
            FROM market.daily_bar_hfq
            WHERE ts_code = '000001.SZ'
            ORDER BY trade_date
            """
        ).fetchall() == [
            (10.0, 100.0, 1000.0, 1.0),
            (10.0, 200.0, 1000.0, 2.0),
        ]
        assert db.sql(
            """
            SELECT close, adj_factor, anchor_adj_factor
            FROM market.daily_bar_qfq_latest
            WHERE ts_code = '000001.SZ'
            ORDER BY trade_date
            """
        ).fetchall() == [
            (5.0, 1.0, 2.0),
            (5.0, 2.0, 2.0),
        ]
        assert db.sql(
            """
            SELECT ts_code, trade_date, adj_factor
            FROM market.latest_adj_factor
            """
        ).fetchall() == [("000001.SZ", date(2024, 1, 2), 2.0)]


def test_qfq_asof_uses_historical_anchor_and_excludes_future_bars(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        seed_bars(db)

        assert db.sql(
            """
            SELECT trade_date, close, anchor_adj_factor
            FROM market.daily_bar_qfq_asof(DATE '2024-01-01')
            WHERE ts_code = '000001.SZ'
            ORDER BY trade_date
            """
        ).fetchall() == [(date(2024, 1, 1), 10.0, 1.0)]

        assert db.bars(
            "000001.SZ",
            start="2024-01-01",
            end="2024-01-31",
            adjust="qfq",
            as_of="2024-01-01",
        ).project("trade_date, close, anchor_adj_factor").fetchall() == [
            (date(2024, 1, 1), 10.0, 1.0)
        ]


def test_missing_adjustment_factor_remains_visible_as_null(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        seed_bars(db)

        assert db.sql(
            """
            SELECT close, adj_factor
            FROM market.daily_bar
            WHERE ts_code = '000002.SZ'
            """
        ).fetchone() == (7.0, None)
        assert db.sql(
            """
            SELECT close, adj_factor, anchor_adj_factor
            FROM market.daily_bar_qfq_latest
            WHERE ts_code = '000002.SZ'
            """
        ).fetchone() == (None, None, None)


def test_daily_metrics_and_panel_join_metrics_onto_raw_bars(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        seed_bars(db)
        db.sql(
            """
            INSERT INTO tushare.daily_basic (
                ts_code, trade_date, close, turnover_rate, pe, pb, total_mv
            ) VALUES (
                '000001.SZ', DATE '2024-01-02', 999.0, 1.2, 6.0, 0.7, 20000000.0
            )
            """
        )

        assert db.sql(
            """
            SELECT close, turnover_rate, pe, pb, total_mv
            FROM market.daily_metrics
            WHERE ts_code = '000001.SZ' AND trade_date = DATE '2024-01-02'
            """
        ).fetchone() == (999.0, 1.2, 6.0, 0.7, 20000000.0)
        assert db.sql(
            """
            SELECT close, turnover_rate, pe, pb, total_mv
            FROM market.daily_panel
            WHERE ts_code = '000001.SZ' AND trade_date = DATE '2024-01-02'
            """
        ).fetchone() == (5.0, 1.2, 6.0, 0.7, 20000000.0)
        assert db.sql(
            """
            SELECT close, turnover_rate
            FROM market.daily_panel
            WHERE ts_code = '000002.SZ' AND trade_date = DATE '2024-01-02'
            """
        ).fetchone() == (7.0, None)


def test_health_reports_missing_dates_and_security_rows(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        seed_bars(db)
        db.sql(
            """
            INSERT INTO tushare.trade_cal VALUES
                ('SSE', DATE '2024-01-01', 1, DATE '2023-12-29'),
                ('SSE', DATE '2024-01-02', 1, DATE '2024-01-01')
            """
        )
        db.sql(
            """
            INSERT INTO tushare.daily_basic (ts_code, trade_date, close) VALUES
                ('000001.SZ', DATE '2024-01-01', 10.0),
                ('000001.SZ', DATE '2024-01-02', 5.0)
            """
        )

        health = db.health(start="2024-01-01", end="2024-01-02").project(
            "dataset_id, status, expected_days, available_days, "
            "missing_days, unmatched_daily_rows, row_count"
        )
        assert health.fetchall() == [
            ("tushare.stock_basic", "EMPTY", None, None, 0, None, 0),
            ("tushare.trade_cal", "HEALTHY", 2, 2, 0, None, 2),
            ("tushare.daily", "HEALTHY", 2, 2, 0, None, 3),
            ("tushare.adj_factor", "INCOMPLETE", 2, 2, 0, 1, 2),
            ("tushare.daily_basic", "HEALTHY", 2, 2, 0, 1, 2),
        ]


def test_bars_filters_symbols_dates_and_adjustment_mode(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        seed_bars(db)

        raw = db.bars(
            ["000001.SZ", "000002.SZ"],
            start=date(2024, 1, 2),
            end="2024-01-02",
        )
        assert raw.project("ts_code, trade_date, close").fetchall() == [
            ("000001.SZ", date(2024, 1, 2), 5.0),
            ("000002.SZ", date(2024, 1, 2), 7.0),
        ]
        assert db.bars("000001.SZ", adjust="hfq").project("close").fetchall() == [
            (10.0,),
            (10.0,),
        ]
        assert db.bars([], adjust="none").fetchall() == []


def test_panel_combines_adjusted_bars_and_daily_metrics(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        seed_bars(db)
        db.sql(
            """
            INSERT INTO tushare.daily_basic (
                ts_code, trade_date, close, turnover_rate, pe
            ) VALUES
                ('000001.SZ', DATE '2024-01-01', 999.0, 1.1, 6.1),
                ('000001.SZ', DATE '2024-01-02', 999.0, 1.2, 6.2)
            """
        )

        assert db.panel("000001.SZ", adjust="qfq").project(
            "trade_date, close, anchor_adj_factor, turnover_rate, pe"
        ).fetchall() == [
            (date(2024, 1, 1), 5.0, 2.0, 1.1, 6.1),
            (date(2024, 1, 2), 5.0, 2.0, 1.2, 6.2),
        ]
        assert db.panel(
            "000001.SZ",
            adjust="qfq",
            as_of="2024-01-01",
        ).project("trade_date, close, anchor_adj_factor, turnover_rate").fetchall() == [
            (date(2024, 1, 1), 10.0, 1.0, 1.1)
        ]
        assert db.panel("000001.SZ", adjust="hfq").project(
            "close, anchor_adj_factor, turnover_rate"
        ).fetchall() == [
            (10.0, None, 1.1),
            (10.0, None, 1.2),
        ]


def test_bars_validates_query_parameters(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        with pytest.raises(ValueError, match="adjust"):
            db.bars(adjust="invalid")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="as_of"):
            db.bars(adjust="none", as_of="2024-01-01")
        with pytest.raises(ValueError, match="start"):
            db.bars(start="2024-01-02", end="2024-01-01")
        with pytest.raises(ValueError, match="symbols"):
            db.bars([""])


def test_market_schema_initialization_is_idempotent(tmp_path):
    path = tmp_path / "quantdb.duckdb"
    with QuantDB(path) as db:
        seed_bars(db)

    with QuantDB(path) as db:
        assert db.sql("SELECT version FROM meta.schema_version ORDER BY version").fetchall() == [
            (1,),
            (2,),
            (3,),
            (4,),
        ]
        assert db.bars("000001.SZ", adjust="qfq").count("*").fetchone() == (2,)


def test_read_only_database_supports_research_queries_and_rejects_updates(tmp_path):
    path = tmp_path / "quantdb.duckdb"
    with QuantDB(path) as db:
        seed_bars(db)

    with QuantDB(path, read_only=True) as db:
        assert db.panel("000001.SZ", adjust="hfq").count("*").fetchone() == (2,)
        with pytest.raises(ReadOnlyDatabaseError, match="只读"):
            db.init()
        with pytest.raises(ReadOnlyDatabaseError, match="只读"):
            db.sync("tushare.stock_basic")
        with pytest.raises(ReadOnlyDatabaseError, match="只读"):
            db.update(start="2024-01-01", end="2024-01-02")
