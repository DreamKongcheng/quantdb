from typer.testing import CliRunner

from quantdb.cli import app

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
