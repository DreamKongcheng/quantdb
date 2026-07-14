class QuantDBError(Exception):
    """quantdb 基础异常。"""


class ConfigurationError(QuantDBError):
    """配置缺失或无效。"""


class DatabaseConnectionError(QuantDBError):
    """DuckDB 数据库无法打开。"""


class DatasetNotFoundError(QuantDBError):
    """数据集未注册。"""


class DatasetValidationError(QuantDBError):
    """接口响应不符合数据集契约。"""


class FetchError(QuantDBError):
    """Tushare 数据获取失败。"""


class SyncError(QuantDBError):
    """一个数据分区同步失败。"""
