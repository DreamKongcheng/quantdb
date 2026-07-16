from datetime import date

import duckdb
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


def test_point_in_time_status_index_members_and_trade_constraints(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        db.sql(
            """
            INSERT INTO tushare.stock_basic (
                ts_code, symbol, name, list_status, list_date, delist_date
            ) VALUES
                ('000001.SZ', '000001', '平安银行', 'L', DATE '1991-04-03', NULL),
                ('000002.SZ', '000002', '万科A', 'L', DATE '1991-01-29', NULL),
                (
                    '000003.SZ', '000003', '退市样本', 'D',
                    DATE '2010-01-01', DATE '2024-02-01'
                ),
                ('000004.SZ', '000004', '待上市样本', 'P', DATE '2024-03-01', NULL)
            """
        )
        db.sql(
            """
            INSERT INTO tushare.namechange VALUES
                (
                    '000001.SZ', 'ST平安', DATE '2020-01-01', DATE '2020-12-31',
                    DATE '2019-12-20', 'ST'
                ),
                (
                    '000001.SZ', '平安银行', DATE '2021-01-01', NULL,
                    DATE '2020-12-20', '撤销ST'
                )
            """
        )
        db.sql(
            """
            INSERT INTO tushare.index_weight VALUES
                ('000300.SH', '000001.SZ', DATE '2024-01-31', 1.0),
                ('000300.SH', '000002.SZ', DATE '2024-01-31', 2.0),
                ('000300.SH', '000001.SZ', DATE '2024-02-29', 1.5)
            """
        )
        db.sql(
            """
            INSERT INTO tushare.index_member_all VALUES
                (
                    '801780.SI', '银行', '801781.SI', '大型银行',
                    '801781.SI', '大型银行', '000001.SZ', '平安银行',
                    DATE '2020-01-01', DATE '2023-12-31', 'N'
                ),
                (
                    '801780.SI', '银行', '801782.SI', '股份制银行',
                    '801782.SI', '股份制银行', '000001.SZ', '平安银行',
                    DATE '2024-01-01', NULL, 'Y'
                )
            """
        )
        db.sql(
            """
            INSERT INTO tushare.daily VALUES
                (
                    '000001.SZ', DATE '2024-02-01',
                    11.0, 11.0, 11.0, 11.0, 10.0, 1.0, 10.0, 100.0, 1100.0
                )
            """
        )
        db.sql(
            """
            INSERT INTO tushare.stk_limit VALUES
                (DATE '2020-06-01', '000001.SZ', 10.0, 11.0, 9.0),
                (DATE '2024-01-31', '000003.SZ', 10.0, 11.0, 9.0),
                (DATE '2024-02-01', '000001.SZ', 10.0, 11.0, 9.0),
                (DATE '2024-02-01', '000003.SZ', 10.0, 11.0, 9.0),
                (DATE '2024-02-01', '000004.SZ', 10.0, 11.0, 9.0),
                (DATE '2024-02-01', '159001.SZ', 1.0, 1.1, 0.9)
            """
        )
        db.sql(
            """
            INSERT INTO tushare.suspend_d VALUES
                ('000002.SZ', DATE '2024-02-01', NULL, 'S'),
                ('000002.SZ', DATE '2024-02-01', NULL, 'R')
            """
        )

        assert db.sql(
            """
            SELECT historical_name, is_st, is_delisting, is_listed
            FROM market.security_status_asof(DATE '2020-06-01')
            WHERE ts_code = '000001.SZ'
            """
        ).fetchone() == ("ST平安", True, False, True)
        assert db.sql(
            """
            SELECT con_code, snapshot_date, weight
            FROM indices.members_asof('000300.SH', DATE '2024-02-15')
            ORDER BY con_code
            """
        ).fetchall() == [
            ("000001.SZ", date(2024, 1, 31), 1.0),
            ("000002.SZ", date(2024, 1, 31), 2.0),
        ]
        assert db.sql(
            """
            SELECT ts_code, l1_name, l2_name, l3_name, in_date, out_date
            FROM indices.stock_industry_asof(DATE '2023-12-31')
            """
        ).fetchall() == [
            (
                "000001.SZ",
                "银行",
                "大型银行",
                "大型银行",
                date(2020, 1, 1),
                date(2023, 12, 31),
            )
        ]
        assert db.sql(
            """
            SELECT ts_code, l2_name, in_date, out_date
            FROM indices.stock_industry_asof(DATE '2024-02-01')
            """
        ).fetchall() == [("000001.SZ", "股份制银行", date(2024, 1, 1), None)]
        assert db.sql(
            """
            SELECT
                ts_code, suspend_type, is_suspended, is_resumed,
                open_at_up_limit, locked_up_limit,
                can_buy_at_open, can_sell_at_open
            FROM market.trade_constraints_daily
            WHERE trade_date = DATE '2024-02-01'
              AND ts_code IN ('000001.SZ', '000002.SZ')
            ORDER BY ts_code
            """
        ).fetchall() == [
            ("000001.SZ", None, False, False, True, True, False, True),
            ("000002.SZ", "S,R", True, True, None, None, False, False),
        ]
        assert db.sql(
            """
            SELECT ts_code
            FROM market.trade_constraints_daily
            WHERE trade_date = DATE '2024-02-01'
            ORDER BY ts_code
            """
        ).fetchall() == [
            ("000001.SZ",),
            ("000002.SZ",),
            ("000003.SZ",),
            ("000004.SZ",),
            ("159001.SZ",),
        ]
        assert db.sql(
            """
            SELECT ts_code, has_name_history, is_suspended
            FROM market.stock_trade_constraints_asof(DATE '2024-02-01')
            ORDER BY ts_code
            """
        ).fetchall() == [
            ("000001.SZ", True, False),
            ("000002.SZ", False, True),
        ]
        assert db.sql(
            """
            SELECT ts_code, historical_name, is_st, is_delisting
            FROM market.stock_trade_constraints_asof(DATE '2020-06-01')
            """
        ).fetchone() == ("000001.SZ", "ST平安", True, False)
        assert db.sql(
            """
            SELECT ts_code
            FROM market.stock_trade_constraints_daily
            WHERE ts_code = '000003.SZ'
            ORDER BY trade_date
            """
        ).fetchall() == [("000003.SZ",)]
        assert db.universe("2024-02-01").project(
            "ts_code, historical_name, is_st, index_code, weight"
        ).fetchall() == [
            ("000001.SZ", "平安银行", False, None, None),
            ("000002.SZ", None, None, None, None),
        ]
        assert db.universe(
            "2024-02-15",
            index_code="000300.SH",
        ).project("ts_code, index_code, snapshot_date, weight").fetchall() == [
            ("000001.SZ", "000300.SH", date(2024, 1, 31), 1.0),
            ("000002.SZ", "000300.SH", date(2024, 1, 31), 2.0),
        ]
        assert db.universe(
            "2024-02-01",
            exclude_st=True,
            exclude_delisting=True,
        ).project("ts_code").fetchall() == [("000001.SZ",)]
        assert db.tradeability("2024-02-01").project(
            "ts_code, is_suspended, can_buy_at_open"
        ).fetchall() == [
            ("000001.SZ", False, False),
            ("000002.SZ", True, False),
        ]
        assert db.tradeability("2024-02-01", symbols="000002.SZ").project(
            "ts_code, is_suspended"
        ).fetchall() == [("000002.SZ", True)]
        assert db.tradeability("2024-02-01", symbols=[]).fetchall() == []


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
            ("tushare.namechange", "EMPTY", None, None, 0, None, 0),
            ("tushare.index_basic", "EMPTY", None, None, 0, None, 0),
            ("tushare.index_classify", "EMPTY", None, None, 0, None, 0),
            ("tushare.index_member_all", "EMPTY", None, None, 0, None, 0),
            ("tushare.trade_cal", "HEALTHY", 2, 2, 0, None, 2),
            ("tushare.daily", "HEALTHY", 2, 2, 0, None, 3),
            ("tushare.adj_factor", "INCOMPLETE", 2, 2, 0, 1, 2),
            ("tushare.daily_basic", "HEALTHY", 2, 2, 0, 1, 2),
            ("tushare.index_daily", "INCOMPLETE", 2, 0, 2, None, 0),
            ("tushare.index_dailybasic", "INCOMPLETE", 2, 0, 2, None, 0),
            ("tushare.suspend_d", "INCOMPLETE", 2, 0, 2, None, 0),
            ("tushare.stk_limit", "INCOMPLETE", 2, 0, 2, None, 0),
            ("tushare.index_weight", "EMPTY", None, None, 0, None, 0),
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


def test_indices_schema_and_index_bars_expose_normalized_index_data(tmp_path):
    with QuantDB(tmp_path / "quantdb.duckdb") as db:
        db.sql(
            """
            INSERT INTO tushare.index_basic (
                ts_code, name, fullname, market, publisher, base_date,
                base_point, list_date, "desc"
            ) VALUES (
                '000300.SH', '沪深300', '沪深300指数', 'SSE', '中证指数有限公司',
                DATE '2004-12-31', 1000.0, DATE '2005-04-08', '代表性规模指数'
            )
            """
        )
        db.sql(
            """
            INSERT INTO tushare.index_daily VALUES
                (
                    '000300.SH', DATE '2024-01-02',
                    3400.0, 3510.0, 3390.0, 3500.0, 3400.0,
                    100.0, 2.94, 100000.0, 200000.0
                ),
                (
                    '000905.SH', DATE '2024-01-02',
                    5400.0, 5510.0, 5390.0, 5500.0, 5400.0,
                    100.0, 1.85, 120000.0, 220000.0
                )
            """
        )
        db.sql(
            """
            INSERT INTO tushare.index_dailybasic (
                ts_code, trade_date, total_mv, float_mv,
                turnover_rate, turnover_rate_f, pe, pe_ttm, pb
            ) VALUES (
                '000300.SH', DATE '2024-01-02', 50000000000000.0, 40000000000000.0,
                0.8, 1.1, 12.5, 12.8, 1.4
            )
            """
        )

        assert db.sql("SELECT ts_code, name, description FROM indices.basic").fetchall() == [
            ("000300.SH", "沪深300", "代表性规模指数")
        ]
        assert db.index_bars("000300.SH", start="2024-01-02", end="2024-01-02").project(
            "ts_code, trade_date, close"
        ).fetchall() == [("000300.SH", date(2024, 1, 2), 3500.0)]
        assert db.index_bars([]).fetchall() == []
        assert db.index_panel(["000300.SH", "000905.SH"]).project(
            "ts_code, close, total_mv, turnover_rate, pe, pb"
        ).fetchall() == [
            ("000300.SH", 3500.0, 50_000_000_000_000.0, 0.8, 12.5, 1.4),
            ("000905.SH", 5500.0, None, None, None, None),
        ]
        assert db.sql(
            """
            SELECT count(*)
            FROM duckdb_functions()
            WHERE schema_name = 'market'
              AND function_name IN ('index_members_asof', 'stock_industry_asof')
            """
        ).fetchone() == (0,)


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
        with pytest.raises(ValueError, match="index_code"):
            db.universe("2024-01-01", index_code="")
        with pytest.raises(ValueError, match="symbols"):
            db.tradeability("2024-01-01", symbols=[""])


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
            (5,),
            (6,),
            (7,),
            (8,),
            (9,),
            (10,),
        ]
        assert db.bars("000001.SZ", adjust="qfq").count("*").fetchone() == (2,)


def test_reopening_migrates_legacy_suspend_table_without_losing_events(tmp_path):
    path = tmp_path / "quantdb.duckdb"
    with QuantDB(path):
        pass

    with duckdb.connect(str(path)) as connection:
        connection.execute("DROP TABLE tushare.suspend_d")
        connection.execute(
            """
            CREATE TABLE tushare.suspend_d (
                ts_code VARCHAR,
                trade_date DATE,
                suspend_timing VARCHAR,
                suspend_type VARCHAR,
                PRIMARY KEY (ts_code, trade_date)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO tushare.suspend_d VALUES
                ('600572.SH', DATE '2011-05-20', NULL, 'S')
            """
        )

    with QuantDB(path) as db:
        assert db.sql(
            """
            SELECT count(*)
            FROM duckdb_constraints()
            WHERE schema_name = 'tushare'
              AND table_name = 'suspend_d'
              AND constraint_type = 'PRIMARY KEY'
            """
        ).fetchone() == (0,)
        assert db.sql("SELECT * FROM tushare.suspend_d").fetchall() == [
            ("600572.SH", date(2011, 5, 20), None, "S")
        ]
        db.sql(
            """
            INSERT INTO tushare.suspend_d VALUES
                ('600572.SH', DATE '2011-05-20', NULL, 'R')
            """
        )
        assert db.sql(
            """
            SELECT suspend_type, is_suspended, is_resumed
            FROM market.trade_constraints_daily
            WHERE ts_code = '600572.SH' AND trade_date = DATE '2011-05-20'
            """
        ).fetchone() == ("S,R", True, True)

    with QuantDB(path) as db:
        assert db.sql("SELECT count(*) FROM tushare.suspend_d").fetchone() == (2,)


def test_read_only_database_supports_research_queries_and_rejects_updates(tmp_path):
    path = tmp_path / "quantdb.duckdb"
    with QuantDB(path) as db:
        seed_bars(db)

    with QuantDB(path, read_only=True) as db:
        assert db.panel("000001.SZ", adjust="hfq").count("*").fetchone() == (2,)
        assert db.universe("2024-01-01").fetchall() == []
        assert db.tradeability("2024-01-01").fetchall() == []
        with pytest.raises(ReadOnlyDatabaseError, match="只读"):
            db.init()
        with pytest.raises(ReadOnlyDatabaseError, match="只读"):
            db.sync("tushare.stock_basic")
        with pytest.raises(ReadOnlyDatabaseError, match="只读"):
            db.update(start="2024-01-01", end="2024-01-02")
