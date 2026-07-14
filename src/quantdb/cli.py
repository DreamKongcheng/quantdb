from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from quantdb import QuantDB, TqdmSyncProgress
from quantdb.errors import QuantDBError

app = typer.Typer(no_args_is_help=True, help="本地 Tushare + DuckDB 量化数据库")

DatabaseOption = Annotated[
    Path | None,
    typer.Option("--db", help="DuckDB 数据库文件路径，优先于 QUANTDB_PATH"),
]


@app.command("init")
def init_database(db: DatabaseOption = None) -> None:
    try:
        with QuantDB(db) as database:
            database.init()
            path = database.path
    except (QuantDBError, ValueError) as exc:
        _exit_with_error("初始化失败", exc)
    typer.echo(f"已初始化 {path}")


@app.command()
def sync(
    dataset: Annotated[str, typer.Argument(help="数据集，例如 tushare.daily")],
    db: DatabaseOption = None,
    start: Annotated[str | None, typer.Option(help="开始日期 YYYY-MM-DD")] = None,
    end: Annotated[str | None, typer.Option(help="结束日期 YYYY-MM-DD")] = None,
    refresh: Annotated[bool, typer.Option(help="重新获取并替换已有分区")] = False,
    show_progress: Annotated[
        bool,
        typer.Option("--progress/--no-progress", help="显示同步进度、速度和预计剩余时间"),
    ] = True,
) -> None:
    try:
        with QuantDB(db) as database:
            progress = TqdmSyncProgress() if show_progress else None
            report = database.sync(
                dataset,
                start=start,
                end=end,
                refresh=refresh,
                progress=progress,
            )
    except (QuantDBError, ValueError) as exc:
        _exit_with_error("同步失败", exc)
    typer.echo(f"{report.dataset_id}: 成功 {report.completed} 个分区，跳过 {report.skipped} 个分区")


@app.command()
def status(
    db: DatabaseOption = None,
    dataset: Annotated[str | None, typer.Argument(help="可选的数据集名称")] = None,
) -> None:
    try:
        with QuantDB(db) as database:
            typer.echo(database.status(dataset))
    except (QuantDBError, ValueError) as exc:
        _exit_with_error("查询状态失败", exc)


@app.command("sql")
def run_sql(
    query: Annotated[str, typer.Argument(help="要执行的 SQL")],
    db: DatabaseOption = None,
) -> None:
    try:
        with QuantDB(db) as database:
            typer.echo(database.sql(query))
    except (QuantDBError, ValueError) as exc:
        _exit_with_error("SQL 执行失败", exc)


def _exit_with_error(prefix: str, error: Exception) -> None:
    typer.echo(f"{prefix}：{error}", err=True)
    raise typer.Exit(code=1) from error
