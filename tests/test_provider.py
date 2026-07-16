from datetime import date

import pandas as pd
import pytest

from quantdb.errors import FetchError
from quantdb.provider import TushareClient, TushareProvider
from quantdb.registry import (
    INDEX_BASIC,
    INDEX_BASIC_MARKETS,
    INDEX_DAILY,
    INDEX_DAILYBASIC,
    INDEX_MEMBER_ALL,
    NAMECHANGE,
    STOCK_BASIC,
    daily_partition,
    full_partition,
)


class PaginatedAPI:
    def __init__(self):
        self.calls = []

    def query(self, endpoint, **params):
        self.calls.append((endpoint, params))
        offset = params["offset"]
        if offset == 0:
            return pd.DataFrame({"id": [1, 2]})
        if offset == 2:
            return pd.DataFrame({"id": [3]})
        return pd.DataFrame({"id": []})


class FailingSecondPageAPI(PaginatedAPI):
    def query(self, endpoint, **params):
        if params["offset"] > 0:
            raise ConnectionError("connection reset")
        return super().query(endpoint, **params)


class EmptyResponseSecondPageAPI(PaginatedAPI):
    def query(self, endpoint, **params):
        if params["offset"] > 0:
            return pd.DataFrame()
        return super().query(endpoint, **params)


class ServerCappedAPI:
    def __init__(self):
        self.calls = []
        self.rows = [1, 2, 3, 4, 5]

    def query(self, endpoint, **params):
        self.calls.append((endpoint, params))
        offset = params["offset"]
        server_limit = min(params["limit"], 3)
        return pd.DataFrame({"id": self.rows[offset : offset + server_limit]})


class RepeatingPageAPI:
    def query(self, endpoint, **params):
        return pd.DataFrame({"id": [1, 2]})


class StockBasicAPI:
    def __init__(self):
        self.statuses = []

    def query(self, endpoint, **params):
        assert endpoint == "stock_basic"
        status = params["list_status"]
        fields = params["fields"].split(",")
        if params["offset"] > 0:
            return pd.DataFrame(columns=fields)
        self.statuses.append(status)
        row = dict.fromkeys(fields, "")
        row.update(
            ts_code=f"00000{len(self.statuses)}.SZ",
            symbol=f"00000{len(self.statuses)}",
            list_status=status,
            list_date="19910101",
            delist_date=None,
        )
        return pd.DataFrame([row], columns=fields)


class DuplicateNamechangeAPI:
    def query(self, endpoint, **params):
        assert endpoint == "namechange"
        fields = params["fields"].split(",")
        if params["offset"] > 0:
            return pd.DataFrame(columns=fields)
        row = {
            "ts_code": "600788.SH",
            "name": "ST达尔曼",
            "start_date": "20040510",
            "end_date": "20041101",
            "ann_date": "20040430",
            "change_reason": "ST",
        }
        return pd.DataFrame([row, row], columns=fields)


class IndexMemberAllAPI:
    def __init__(self):
        self.calls = []

    def query(self, endpoint, **params):
        assert endpoint == "index_member_all"
        self.calls.append(params)
        fields = params["fields"].split(",")
        offset = params["offset"]
        row_count = min(2_000, max(0, 5_864 - offset))
        rows = []
        for index in range(offset, offset + row_count):
            row = dict.fromkeys(fields, "")
            row.update(
                l1_code="801010.SI",
                l2_code="801011.SI",
                l3_code="801011.SI",
                ts_code=f"{index:06d}.SZ",
                in_date="20210101",
            )
            rows.append(row)
        return pd.DataFrame(rows, columns=fields)


class IndexBasicAPI:
    def __init__(self):
        self.calls = []

    def query(self, endpoint, **params):
        assert endpoint == "index_basic"
        self.calls.append(params)
        fields = params["fields"].split(",")
        if params["offset"] > 0:
            return pd.DataFrame(columns=fields)
        row = dict.fromkeys(fields, "")
        row.update(
            ts_code=f"{len(self.calls):06d}.IX",
            market=params["market"],
            base_date="20000101",
            list_date="20000102",
            exp_date=None,
        )
        return pd.DataFrame([row], columns=fields)


class IndexDailyAPI:
    def __init__(self):
        self.params = None

    def query(self, endpoint, **params):
        assert endpoint == "index_daily"
        self.params = params
        fields = params["fields"].split(",")
        if params["offset"] > 0:
            return pd.DataFrame(columns=fields)
        row = dict.fromkeys(fields, 1.0)
        row.update(ts_code="000300.SH", trade_date=params["trade_date"])
        return pd.DataFrame([row], columns=fields)


class IndexDailyBasicAPI:
    def __init__(self):
        self.params = None

    def query(self, endpoint, **params):
        assert endpoint == "index_dailybasic"
        self.params = params
        fields = params["fields"].split(",")
        if params["offset"] > 0:
            return pd.DataFrame(columns=fields)
        row = dict.fromkeys(fields, 1.0)
        row.update(ts_code="000300.SH", trade_date=params["trade_date"])
        return pd.DataFrame([row], columns=fields)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class RateLimitedOnceAPI:
    def __init__(self):
        self.calls = 0

    def query(self, endpoint, **params):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("抱歉，您访问接口(adj_factor)频率超限(200次/分钟)")
        if params["offset"] > 0:
            return pd.DataFrame({"id": []})
        return pd.DataFrame({"id": [1]})


def test_query_all_reads_every_page():
    api = PaginatedAPI()
    client = TushareClient(api=api, page_size=2, retry_attempts=1)

    result = client.query_all("example", fields=("id",), params={})

    assert result["id"].tolist() == [1, 2, 3]
    assert [call[1]["offset"] for call in api.calls] == [0, 2, 3]


def test_query_all_does_not_return_partial_data_after_network_error():
    client = TushareClient(api=FailingSecondPageAPI(), page_size=2, retry_attempts=1)

    with pytest.raises(FetchError, match="connection reset"):
        client.query_all("example", fields=("id",), params={})


def test_query_all_rejects_columnless_empty_sdk_response():
    client = TushareClient(api=EmptyResponseSecondPageAPI(), page_size=2, retry_attempts=1)

    with pytest.raises(FetchError, match="响应缺少字段"):
        client.query_all("example", fields=("id",), params={})


def test_query_all_continues_when_server_caps_below_requested_page_size():
    api = ServerCappedAPI()
    client = TushareClient(api=api, page_size=4, retry_attempts=1)

    result = client.query_all("example", fields=("id",), params={})

    assert result["id"].tolist() == [1, 2, 3, 4, 5]
    assert [call[1]["offset"] for call in api.calls] == [0, 3, 5]


def test_query_all_rejects_endpoints_that_ignore_offset():
    client = TushareClient(api=RepeatingPageAPI(), page_size=2, retry_attempts=1)

    with pytest.raises(FetchError, match="分页未推进"):
        client.query_all("example", fields=("id",), params={})


def test_stock_basic_fetches_all_statuses_before_returning():
    api = StockBasicAPI()
    provider = TushareProvider(TushareClient(api=api, retry_attempts=1))

    result = provider.fetch(STOCK_BASIC, full_partition())

    assert api.statuses == ["L", "D", "P"]
    assert result["list_status"].tolist() == ["L", "D", "P"]


def test_namechange_deduplicates_exact_upstream_rows():
    provider = TushareProvider(TushareClient(api=DuplicateNamechangeAPI(), retry_attempts=1))

    result = provider.fetch(NAMECHANGE, full_partition())

    assert result.to_dict("records") == [
        {
            "ts_code": "600788.SH",
            "name": "ST达尔曼",
            "start_date": "20040510",
            "end_date": "20041101",
            "ann_date": "20040430",
            "change_reason": "ST",
        }
    ]


def test_index_member_all_uses_its_upstream_page_limit_and_reads_every_page():
    api = IndexMemberAllAPI()
    provider = TushareProvider(TushareClient(api=api, retry_attempts=1))

    result = provider.fetch(INDEX_MEMBER_ALL, full_partition())

    assert [call["limit"] for call in api.calls] == [2_000, 2_000, 2_000, 2_000]
    assert [call["offset"] for call in api.calls] == [0, 2_000, 4_000, 5_864]
    assert len(result) == 5_864


def test_index_basic_fetches_every_market_variant():
    api = IndexBasicAPI()
    provider = TushareProvider(TushareClient(api=api, retry_attempts=1))

    result = provider.fetch(INDEX_BASIC, full_partition())

    assert [call["market"] for call in api.calls if call["offset"] == 0] == list(
        INDEX_BASIC_MARKETS
    )
    assert {call["limit"] for call in api.calls} == {5_000}
    assert result["market"].tolist() == list(INDEX_BASIC_MARKETS)


def test_index_daily_requests_all_indices_for_a_calendar_date():
    api = IndexDailyAPI()
    provider = TushareProvider(TushareClient(api=api, retry_attempts=1))

    result = provider.fetch(INDEX_DAILY, daily_partition(date(2024, 1, 2)))

    assert api.params["trade_date"] == "20240102"
    assert "ts_code" not in api.params
    assert result["ts_code"].tolist() == ["000300.SH"]


def test_index_dailybasic_uses_trade_date_and_documented_page_limit():
    api = IndexDailyBasicAPI()
    provider = TushareProvider(TushareClient(api=api, retry_attempts=1))

    result = provider.fetch(INDEX_DAILYBASIC, daily_partition(date(2024, 1, 2)))

    assert api.params["trade_date"] == "20240102"
    assert api.params["limit"] == 3_000
    assert result["ts_code"].tolist() == ["000300.SH"]


def test_query_all_throttles_every_request_for_an_endpoint():
    api = PaginatedAPI()
    clock = FakeClock()
    client = TushareClient(
        api=api,
        page_size=2,
        retry_attempts=1,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    client.query_all(
        "example",
        fields=("id",),
        params={},
        requests_per_minute=120,
    )

    assert clock.sleeps == [0.5, 0.5]


def test_query_all_cools_down_and_retries_after_rate_limit(caplog):
    api = RateLimitedOnceAPI()
    clock = FakeClock()
    client = TushareClient(
        api=api,
        retry_attempts=2,
        rate_limit_cooldown=61.0,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    result = client.query_all("adj_factor", fields=("id",), params={})

    assert result["id"].tolist() == [1]
    assert clock.sleeps == [61.0]
    assert "61.0 秒后重试" in caplog.text
