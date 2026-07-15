from __future__ import annotations

import logging
import time
from collections.abc import Callable

import pandas as pd

from quantdb.errors import FetchError
from quantdb.registry import DatasetSpec, Partition

logger = logging.getLogger(__name__)


class TushareClient:
    """Tushare SDK 的薄封装，负责分页和有限重试。"""

    def __init__(
        self,
        token: str | None = None,
        *,
        api: object | None = None,
        page_size: int = 6_000,
        retry_attempts: int = 3,
        rate_limit_cooldown: float = 61.0,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if api is None:
            if not token:
                raise ValueError("创建真实 TushareClient 时必须提供 token")
            import tushare as ts

            api = ts.pro_api(token)
        self._api = api
        self.page_size = page_size
        self.retry_attempts = retry_attempts
        self.rate_limit_cooldown = rate_limit_cooldown
        self._sleep = sleep
        self._monotonic = monotonic
        self._last_request_at: dict[str, float] = {}

    def query_all(
        self,
        endpoint: str,
        *,
        fields: tuple[str, ...],
        params: dict[str, str | int],
        requests_per_minute: int | None = None,
    ) -> pd.DataFrame:
        pages: list[pd.DataFrame] = []
        offset = 0
        previous_first_row_hash: int | None = None

        for _page_number in range(1_000):
            page = self._query_page(
                endpoint,
                fields=fields,
                params={**params, "limit": self.page_size, "offset": offset},
                requests_per_minute=requests_per_minute,
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
        requests_per_minute: int | None,
    ) -> pd.DataFrame:
        last_error: Exception | None = None
        for attempt in range(self.retry_attempts):
            self._wait_for_rate_limit(endpoint, requests_per_minute)
            try:
                return self._api.query(endpoint, fields=",".join(fields), **params)
            except Exception as exc:  # Tushare SDK 没有稳定的结构化异常层级
                last_error = exc
                if attempt + 1 < self.retry_attempts:
                    delay = (
                        self.rate_limit_cooldown if _is_rate_limit_error(exc) else float(2**attempt)
                    )
                    logger.warning(
                        "Tushare 接口 %s 请求失败，%.1f 秒后重试（%d/%d）：%s",
                        endpoint,
                        delay,
                        attempt + 1,
                        self.retry_attempts,
                        exc,
                    )
                    self._sleep(delay)
        raise FetchError(f"Tushare 接口 {endpoint} 请求失败：{last_error}") from last_error

    def _wait_for_rate_limit(self, endpoint: str, requests_per_minute: int | None) -> None:
        if requests_per_minute is None:
            return
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute 必须大于 0")

        interval = 60.0 / requests_per_minute
        now = self._monotonic()
        last_request_at = self._last_request_at.get(endpoint)
        if last_request_at is not None:
            delay = interval - (now - last_request_at)
            if delay > 0:
                self._sleep(delay)
                now = self._monotonic()
        self._last_request_at[endpoint] = now


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
                    requests_per_minute=spec.requests_per_minute,
                )
            )
        if not frames:
            return pd.DataFrame(columns=list(spec.source_fields))
        result = pd.concat(frames, ignore_index=True)
        if spec.deduplicate_exact_rows:
            result = result.drop_duplicates(subset=list(spec.source_fields), ignore_index=True)
        return result


def _is_rate_limit_error(error: Exception) -> bool:
    message = str(error)
    return "频率超限" in message or "每分钟" in message and "次数" in message
