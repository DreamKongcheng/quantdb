from __future__ import annotations

import time
from collections.abc import Callable

import pandas as pd

from quantdb.errors import FetchError
from quantdb.registry import DatasetSpec, Partition


class TushareClient:
    """Tushare SDK 的薄封装，负责分页和有限重试。"""

    def __init__(
        self,
        token: str | None = None,
        *,
        api: object | None = None,
        page_size: int = 5_000,
        retry_attempts: int = 3,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if api is None:
            if not token:
                raise ValueError("创建真实 TushareClient 时必须提供 token")
            import tushare as ts

            api = ts.pro_api(token)
        self._api = api
        self.page_size = page_size
        self.retry_attempts = retry_attempts
        self._sleep = sleep

    def query_all(
        self,
        endpoint: str,
        *,
        fields: tuple[str, ...],
        params: dict[str, str | int],
    ) -> pd.DataFrame:
        pages: list[pd.DataFrame] = []
        offset = 0
        previous_first_row_hash: int | None = None

        for _page_number in range(1_000):
            page = self._query_page(
                endpoint,
                fields=fields,
                params={**params, "limit": self.page_size, "offset": offset},
            )
            if not isinstance(page, pd.DataFrame):
                raise FetchError(f"Tushare 接口 {endpoint} 未返回 pandas.DataFrame")

            missing_columns = set(fields) - set(page.columns)
            if missing_columns:
                names = ", ".join(sorted(missing_columns))
                raise FetchError(
                    f"Tushare 接口 {endpoint} 响应缺少字段：{names}；这可能是 SDK 吞掉的 HTTP 错误"
                )

            if page.empty:
                break

            first_row_hash = int(pd.util.hash_pandas_object(page.iloc[[0]], index=False).iloc[0])
            if offset and first_row_hash == previous_first_row_hash:
                raise FetchError(f"Tushare 接口 {endpoint} 分页未推进，拒绝提交不完整数据")
            previous_first_row_hash = first_row_hash
            pages.append(page)

            if len(page) < self.page_size:
                break
            offset += len(page)
        else:
            raise FetchError(f"Tushare 接口 {endpoint} 超过最大分页数量")

        if not pages:
            return pd.DataFrame(columns=list(fields))
        return pd.concat(pages, ignore_index=True)

    def _query_page(
        self,
        endpoint: str,
        *,
        fields: tuple[str, ...],
        params: dict[str, str | int],
    ) -> pd.DataFrame:
        last_error: Exception | None = None
        for attempt in range(self.retry_attempts):
            try:
                return self._api.query(endpoint, fields=",".join(fields), **params)
            except Exception as exc:  # Tushare SDK 没有稳定的结构化异常层级
                last_error = exc
                if attempt + 1 < self.retry_attempts:
                    self._sleep(float(2**attempt))
        raise FetchError(f"Tushare 接口 {endpoint} 请求失败：{last_error}") from last_error


class TushareProvider:
    def __init__(self, client: TushareClient) -> None:
        self.client = client

    def fetch(self, spec: DatasetSpec, partition: Partition) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for variant in spec.request_variants:
            params = {**partition.request_params, **variant}
            frames.append(
                self.client.query_all(
                    spec.endpoint,
                    fields=spec.source_fields,
                    params=params,
                )
            )
        if not frames:
            return pd.DataFrame(columns=list(spec.source_fields))
        return pd.concat(frames, ignore_index=True)
