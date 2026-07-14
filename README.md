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

# daily 会自动补齐缺失的 stock_basic 和 trade_cal 依赖。
db.sync("tushare.daily", start="2026-07-01", end="2026-07-14")

prices = db.sql("""
    SELECT *
    FROM tushare.daily
    ORDER BY trade_date, ts_code
""").pl()
```

已有分区默认跳过。需要从 Tushare 重新获取并原子替换时，传入
`refresh=True`。

## CLI

```bash
uv run quantdb init
uv run quantdb sync tushare.stock_basic
uv run quantdb sync tushare.daily \
  --start 2026-07-01 \
  --end 2026-07-14
uv run quantdb status
uv run quantdb sql "SELECT count(*) FROM tushare.daily"
```

数据库路径的优先级为 `--db`、系统环境变量 `QUANTDB_PATH`、`.env` 中的
`QUANTDB_PATH`、当前目录的 `quantdb.duckdb`。路径中的 `~` 会自动展开。

## 数据库结构

第一版只创建两个 schema：

```text
meta.partitions
meta.sync_runs
meta.schema_version
tushare.stock_basic
tushare.trade_cal
tushare.daily
```

`tushare.*` 只做必要的数据库类型转换，不做复权、填充、去极值等业务清洗。
网络请求全部完成并通过完整性校验后，系统才会开启 DuckDB 事务。事务内原子替换
对应分区，并同时更新 `meta.partitions`。
