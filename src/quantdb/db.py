from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb

from quantdb.config import resolve_database_path, resolve_tushare_token
from quantdb.provider import TushareClient, TushareProvider
from quantdb.registry import get_dataset
from quantdb.store import DuckDBStore
from quantdb.sync import DataProvider, SyncEngine, SyncProgress, SyncReport


class QuantDB:
    def __init__(
        self,
        path: str | Path | None = None,
        *,
        tushare_token: str | None = None,
        env_file: str | Path | None = ".env",
        provider: DataProvider | None = None,
    ) -> None:
        self.path = resolve_database_path(path, env_file)
        self.store = DuckDBStore(self.path)
        self._tushare_token = tushare_token
        self._env_file = env_file
        self._provider = provider

    def init(self) -> None:
        self.store.initialize()

    def sync(
        self,
        dataset_id: str,
        *,
        start: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        refresh: bool = False,
        progress: SyncProgress | None = None,
    ) -> SyncReport:
        provider = self._provider
        if provider is None:
            token = resolve_tushare_token(self._tushare_token, self._env_file)
            provider = TushareProvider(TushareClient(token))
            self._provider = provider
        return SyncEngine(self.store, provider, progress).sync(
            dataset_id,
            start=start,
            end=end,
            refresh=refresh,
        )

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        return self.store.sql(query)

    def status(self, dataset_id: str | None = None) -> duckdb.DuckDBPyRelation:
        canonical_id = get_dataset(dataset_id).id if dataset_id is not None else None
        return self.store.status(canonical_id)

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> QuantDB:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
