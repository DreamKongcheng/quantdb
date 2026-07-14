from __future__ import annotations

import os
from pathlib import Path

from dotenv import dotenv_values

from quantdb.errors import ConfigurationError


def resolve_database_path(
    explicit_path: str | Path | None = None,
    env_file: str | Path | None = ".env",
    *,
    default: str | Path = "quantdb.duckdb",
) -> Path:
    """按显式参数、环境变量、.env、默认值的顺序解析数据库路径。"""
    if explicit_path is not None and str(explicit_path).strip():
        return Path(explicit_path).expanduser()

    environment_path = os.getenv("QUANTDB_PATH")
    if environment_path and environment_path.strip():
        return Path(environment_path.strip()).expanduser()

    if env_file is not None:
        file_path = dotenv_values(Path(env_file)).get("QUANTDB_PATH")
        if file_path and file_path.strip():
            return Path(file_path.strip()).expanduser()

    return Path(default).expanduser()


def resolve_tushare_token(
    explicit_token: str | None = None,
    env_file: str | Path | None = ".env",
) -> str:
    """按显式参数、环境变量、.env 的顺序解析 token。"""
    if explicit_token and explicit_token.strip():
        return explicit_token.strip()

    environment_token = os.getenv("TUSHARE_TOKEN")
    if environment_token and environment_token.strip():
        return environment_token.strip()

    if env_file is not None:
        file_token = dotenv_values(Path(env_file)).get("TUSHARE_TOKEN")
        if file_token and file_token.strip():
            return file_token.strip()

    raise ConfigurationError("缺少 TUSHARE_TOKEN，请通过显式参数、环境变量或 .env 文件配置")
