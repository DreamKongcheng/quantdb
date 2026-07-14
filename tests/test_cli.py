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
