from io import StringIO

from quantdb.progress import TqdmSyncProgress
from quantdb.sync import PartitionResult


def test_tqdm_progress_shows_partition_counters():
    output = StringIO()
    progress = TqdmSyncProgress(file=output)

    progress.dataset_started("tushare.daily", 2)
    progress.partition_started("tushare.daily", "trade_date=2026-07-13")
    progress.partition_finished(
        PartitionResult("tushare.daily", "trade_date=2026-07-13", "SUCCESS", 5_524)
    )
    progress.partition_started("tushare.daily", "trade_date=2026-07-14")
    progress.partition_finished(
        PartitionResult("tushare.daily", "trade_date=2026-07-14", "SKIPPED")
    )
    progress.dataset_finished("tushare.daily")

    rendered = output.getvalue()
    assert "tushare.daily" in rendered
    assert "成功=1" in rendered
    assert "跳过=1" in rendered
    assert "数据行=5524" in rendered
