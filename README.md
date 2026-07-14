# quantdb

`quantdb` 是一个面向个人研究和回测的本地 A 股数据库。第一版使用 Tushare
采集数据，使用 DuckDB 存储，并保证每个“接口 + 逻辑分区”原子提交。

## 安装

项目使用 Python 3.11 和 uv：

```bash
uv sync
cp .env.example .env
```

在 `.env` 中配置：

```dotenv
TUSHARE_TOKEN=你的_token
QUANTDB_PATH=~/Data/investing/quantdb/quantdb.duckdb
```

## Python API

```python
from quantdb import QuantDB

# 默认读取 QUANTDB_PATH，也可以通过 QuantDB("其他路径") 显式覆盖。
db = QuantDB()

# 全量股票基础信息。已存在时使用 refresh=True 重新获取。
db.sync("tushare.stock_basic")

# 交易日历按自然年分区。
db.sync("tushare.trade_cal", start="2024-01-01", end="2026-12-31")

# 日频接口会自动补齐缺失的 trade_cal；stock_basic 是独立证券主数据。
db.sync("tushare.daily", start="2026-07-01", end="2026-07-14")

# 复权因子与每日估值、换手率等指标使用相同的交易日分区策略。
db.sync("tushare.adj_factor", start="2026-07-01", end="2026-07-14")
db.sync("tushare.daily_basic", start="2026-07-01", end="2026-07-14")

prices = db.sql("""
    SELECT *
    FROM tushare.daily
    ORDER BY trade_date, ts_code
""").pl()

# 标准行情查询。adjust 支持 none、qfq、hfq，结果仍是 DuckDB relation。
bars = db.bars(
    ["000001.SZ", "600000.SH"],
    start="2020-01-01",
    end="2024-12-31",
    adjust="qfq",
    as_of="2024-12-31",
).pl()

# 刷新股票基础信息，并补齐 2010 年以来缺失的日历和三个日频数据集。
reports = db.update()

# 动态检查指定区间的数据覆盖，不写入额外的健康状态表。
health = db.health(start="2010-01-01", end="2024-12-31")
```

已有分区默认跳过。需要从 Tushare 重新获取并原子替换时，传入
`refresh=True`。`bars()` 默认返回不复权价格；QFQ 不传 `as_of` 时使用数据库中
每只股票最新的复权因子，传入 `as_of` 时则使用该日期当时可得的最后一个复权因子，
适合需要固定回测口径的场景。

## CLI

```bash
uv run quantdb init
uv run quantdb sync tushare.stock_basic
uv run quantdb sync tushare.daily \
  --start 2026-07-01 \
  --end 2026-07-14
uv run quantdb sync tushare.adj_factor --start 2026-07-01 --end 2026-07-14
uv run quantdb sync tushare.daily_basic --start 2026-07-01 --end 2026-07-14
uv run quantdb update
uv run quantdb health
uv run quantdb status
uv run quantdb sql "SELECT count(*) FROM tushare.daily"
```

`quantdb update` 默认检查 `2010-01-01` 到今天，按以下顺序执行：

1. 原子刷新 `tushare.stock_basic`。
2. 补齐缺失的 `tushare.trade_cal` 自然年分区。
3. 依次补齐 `tushare.daily`、`tushare.adj_factor`、`tushare.daily_basic` 交易日分区。
4. 输出本次日期范围内的数据集健康状态。

可以通过 `--start`、`--end` 调整检查范围；日频接口尚未发布当天数据时，当前分区
会失败且不会提交，之后重新运行即可。

```bash
uv run quantdb update --start 2016-01-01 --end 2026-07-14
```

同步命令默认显示每个数据集的分区进度、当前分区、成功和跳过数量、数据行数、
处理速度及预计剩余时间。脚本或非交互环境中可以使用 `--no-progress` 关闭：

```bash
uv run quantdb sync tushare.daily \
  --start 2016-01-01 \
  --end 2026-07-13 \
  --no-progress
```

使用 `Ctrl+C` 中断时，已经提交的分区保持不变，当前分区显式回滚并记录为
`INTERRUPTED`。再次执行相同命令会跳过成功分区并重试未完成分区。进程被强制
终止或机器断电后，遗留的 `RUNNING` 会在下次打开数据库时恢复为 `INTERRUPTED`。

数据库路径的优先级为 `--db`、系统环境变量 `QUANTDB_PATH`、`.env` 中的
`QUANTDB_PATH`、当前目录的 `quantdb.duckdb`。路径中的 `~` 会自动展开。

## 数据库结构

数据库当前包含原始数据、同步元数据和标准行情三个 schema：

```text
meta.partitions
meta.sync_runs
meta.schema_version
tushare.stock_basic
tushare.trade_cal
tushare.daily
tushare.adj_factor
tushare.daily_basic
market.daily_bar
market.latest_adj_factor
market.daily_bar_hfq
market.daily_bar_qfq_latest
market.daily_bar_qfq_asof(as_of_date)
market.daily_metrics
market.daily_panel
```

`tushare.*` 只做必要的数据库类型转换，不做复权、填充、去极值等业务清洗。
网络请求全部完成并通过完整性校验后，系统才会开启 DuckDB 事务。事务内原子替换
对应分区，并同时更新 `meta.partitions`。

`adj_factor` 和 `daily_basic` 按 180 次/分钟主动限速，为 Tushare 的 200 次/分钟
额度保留余量。若仍因共享 token 或残留时间窗口触发限频，客户端会等待 61 秒后
自动重试，不会提交当前未完成分区。

`market.*` 通过视图实时读取原始表，不重复存储行情。后复权价格为
`raw_price * adj_factor`；最新前复权价格为
`raw_price * adj_factor / latest_adj_factor`。最新前复权会随未来公司行为改变历史值，
回测可使用固定锚点的 table macro：

```sql
SELECT *
FROM market.daily_bar_qfq_asof(DATE '2024-12-31');
```

复权只作用于 OHLC、`pre_close` 和 `change`；`pct_chg`、`vol`、`amount` 保持原值。
若某条日线缺少对应复权因子，复权价格保留为 `NULL`，不会静默按 1 填充。

`market.daily_metrics` 提供 `daily_basic` 中的换手率、估值、市值和股本指标。
`market.daily_panel` 以 `market.daily_bar` 为主表左连接这些指标，其中 `close` 始终
来自日线行情；缺失的每日指标保留为 `NULL`。面板不连接当前 `stock_basic`，避免将
当前上市状态、行业等信息无意带入历史截面。

## 数据健康

`quantdb health` 和 `db.health()` 动态检查指定日期范围，不持久化可能过期的
`dataset_health` 表。输出包括：

- `expected_days`、`available_days`、`missing_days`：日历日或预期交易日覆盖。
- `unmatched_daily_rows`：以 `daily` 为基准，没有对应复权因子或每日指标的证券日
  记录数。该字段对 `daily_basic` 仅供观察，不影响健康状态。
- `row_count`、`first_date`、`last_date`、`last_committed_at`：数据量和更新时间。
- `status`：`HEALTHY`、`INCOMPLETE`、`CALENDAR_INCOMPLETE` 或 `EMPTY`。

`meta.partitions` 仍负责记录原子同步状态；健康检查读取原始表的实际数据，因此也能
发现数据库被外部工具手动修改后造成的覆盖缺口。
