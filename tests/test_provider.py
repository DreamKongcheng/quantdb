import pandas as pd
import pytest

from quantdb.errors import FetchError
from quantdb.provider import TushareClient, TushareProvider
from quantdb.registry import STOCK_BASIC, full_partition


class PaginatedAPI:
    def __init__(self):
        self.calls = []

    def query(self, endpoint, **params):
        self.calls.append((endpoint, params))
        offset = params["offset"]
        if offset == 0:
            return pd.DataFrame({"id": [1, 2]})
        return pd.DataFrame({"id": [3]})


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


class StockBasicAPI:
    def __init__(self):
        self.statuses = []

    def query(self, endpoint, **params):
        assert endpoint == "stock_basic"
        status = params["list_status"]
        self.statuses.append(status)
        fields = params["fields"].split(",")
        row = dict.fromkeys(fields, "")
        row.update(
            ts_code=f"00000{len(self.statuses)}.SZ",
            symbol=f"00000{len(self.statuses)}",
            list_status=status,
            list_date="19910101",
            delist_date=None,
        )
        return pd.DataFrame([row], columns=fields)


def test_query_all_reads_every_page():
    api = PaginatedAPI()
    client = TushareClient(api=api, page_size=2, retry_attempts=1)

    result = client.query_all("example", fields=("id",), params={})

    assert result["id"].tolist() == [1, 2, 3]
    assert [call[1]["offset"] for call in api.calls] == [0, 2]


def test_query_all_does_not_return_partial_data_after_network_error():
    client = TushareClient(api=FailingSecondPageAPI(), page_size=2, retry_attempts=1)

    with pytest.raises(FetchError, match="connection reset"):
        client.query_all("example", fields=("id",), params={})


def test_query_all_rejects_columnless_empty_sdk_response():
    client = TushareClient(api=EmptyResponseSecondPageAPI(), page_size=2, retry_attempts=1)

    with pytest.raises(FetchError, match="响应缺少字段"):
        client.query_all("example", fields=("id",), params={})


def test_stock_basic_fetches_all_statuses_before_returning():
    api = StockBasicAPI()
    provider = TushareProvider(TushareClient(api=api, retry_attempts=1))

    result = provider.fetch(STOCK_BASIC, full_partition())

    assert api.statuses == ["L", "D", "P"]
    assert result["list_status"].tolist() == ["L", "D", "P"]
