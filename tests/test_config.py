from pathlib import Path

from quantdb.config import resolve_database_path, resolve_tushare_token


def test_explicit_token_has_highest_priority(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("TUSHARE_TOKEN=file-token\n")
    monkeypatch.setenv("TUSHARE_TOKEN", "environment-token")

    assert resolve_tushare_token("explicit-token", env_file) == "explicit-token"


def test_environment_token_overrides_dotenv(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("TUSHARE_TOKEN=file-token\n")
    monkeypatch.setenv("TUSHARE_TOKEN", "environment-token")

    assert resolve_tushare_token(env_file=env_file) == "environment-token"


def test_explicit_database_path_has_highest_priority(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("QUANTDB_PATH=from-file.duckdb\n")
    monkeypatch.setenv("QUANTDB_PATH", "from-environment.duckdb")

    assert resolve_database_path("explicit.duckdb", env_file) == Path("explicit.duckdb")


def test_environment_database_path_overrides_dotenv(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("QUANTDB_PATH=from-file.duckdb\n")
    monkeypatch.setenv("QUANTDB_PATH", "from-environment.duckdb")

    assert resolve_database_path(env_file=env_file) == Path("from-environment.duckdb")


def test_database_path_uses_dotenv_and_expands_home(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("QUANTDB_PATH=~/Data/investing/quantdb.duckdb\n")
    monkeypatch.delenv("QUANTDB_PATH", raising=False)

    assert resolve_database_path(env_file=env_file) == (
        Path.home() / "Data/investing/quantdb.duckdb"
    )
