from typer.testing import CliRunner

from quantdb.cli import app
from quantdb.errors import SyncInterruptedError
from quantdb.sync import PartitionResult, SyncReport

runner = CliRunner()


def test_init_uses_quantdb_path_from_environment(tmp_path, monkeypatch):
    database_path = tmp_path / "configured.duckdb"
    monkeypatch.setenv("QUANTDB_PATH", str(database_path))

    result = runner.invoke(app, ["init"])

    assert result.exit_code == 0
    assert database_path.exists()
    assert str(database_path) in result.output


def test_sync_help_exposes_progress_switch():
    result = runner.invoke(app, ["sync", "--help"])

    assert result.exit_code == 0
    assert "--progress" in result.output
    assert "--no-progress" in result.output


def test_sync_keyboard_interrupt_exits_with_code_130(monkeypatch):
    class InterruptingQuantDB:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def sync(self, *_args, **_kwargs):
            raise KeyboardInterrupt

    monkeypatch.setattr("quantdb.cli.QuantDB", lambda _path: InterruptingQuantDB())

    result = runner.invoke(
        app,
        ["sync", "tushare.daily", "--start", "2026-07-14", "--no-progress"],
    )

    assert result.exit_code == 130
    assert "同步已中断" in result.output


def test_sync_duckdb_interrupt_exits_with_code_130(monkeypatch):
    class InterruptingQuantDB:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def sync(self, *_args, **_kwargs):
            raise SyncInterruptedError("DuckDB query interrupted")

    monkeypatch.setattr("quantdb.cli.QuantDB", lambda _path: InterruptingQuantDB())

    result = runner.invoke(
        app,
        ["sync", "tushare.daily", "--start", "2026-07-14", "--no-progress"],
    )

    assert result.exit_code == 130
    assert "同步已中断" in result.output


def test_update_runs_all_steps_and_prints_health(monkeypatch):
    calls = []

    class UpdatingQuantDB:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def update(self, **kwargs):
            calls.append(("update", kwargs))
            return (
                SyncReport(
                    "tushare.stock_basic",
                    (PartitionResult("tushare.stock_basic", "full", "SUCCESS", 1),),
                ),
            )

        def health(self, **kwargs):
            calls.append(("health", kwargs))
            return "HEALTHY DATASETS"

    monkeypatch.setattr("quantdb.cli.QuantDB", lambda _path: UpdatingQuantDB())

    result = runner.invoke(
        app,
        [
            "update",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-03",
            "--no-progress",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        ("update", {"start": "2024-01-01", "end": "2024-01-03", "progress": None}),
        ("health", {"start": "2024-01-01", "end": "2024-01-03"}),
    ]
    assert "tushare.stock_basic: 成功 1 个分区" in result.output
    assert "HEALTHY DATASETS" in result.output


def test_health_command_forwards_date_range(monkeypatch):
    class HealthyQuantDB:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def health(self, **kwargs):
            assert kwargs == {"start": "2024-01-01", "end": "2024-01-03"}
            return "HEALTH TABLE"

    monkeypatch.setattr("quantdb.cli.QuantDB", lambda _path: HealthyQuantDB())

    result = runner.invoke(
        app,
        ["health", "--start", "2024-01-01", "--end", "2024-01-03"],
    )

    assert result.exit_code == 0
    assert "HEALTH TABLE" in result.output
