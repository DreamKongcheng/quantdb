from __future__ import annotations

import sys
from typing import TextIO

from tqdm import tqdm

from quantdb.sync import PartitionResult


class TqdmSyncProgress:
    """将同步事件呈现为适合终端长任务的 tqdm 进度条。"""

    def __init__(self, *, file: TextIO | None = None) -> None:
        self.file = file or sys.stderr
        self._bar: tqdm[object] | None = None
        self._successful = 0
        self._skipped = 0
        self._rows = 0

    def dataset_started(self, dataset_id: str, total: int) -> None:
        self._close_bar()
        self._successful = 0
        self._skipped = 0
        self._rows = 0
        self._bar = tqdm(
            total=total,
            desc=dataset_id,
            unit="分区",
            dynamic_ncols=True,
            leave=True,
            file=self.file,
        )

    def partition_started(self, dataset_id: str, partition_id: str) -> None:
        if self._bar is not None:
            self._bar.set_postfix_str(f"当前={partition_id}", refresh=True)

    def partition_finished(self, result: PartitionResult) -> None:
        if result.status == "SUCCESS":
            self._successful += 1
            self._rows += result.row_count or 0
        elif result.status == "SKIPPED":
            self._skipped += 1
        if self._bar is not None:
            self._bar.set_postfix(
                {
                    "成功": self._successful,
                    "跳过": self._skipped,
                    "数据行": self._rows,
                },
                refresh=False,
            )
            self._bar.update(1)

    def partition_failed(
        self,
        dataset_id: str,
        partition_id: str,
        error: Exception,
    ) -> None:
        if self._bar is not None:
            self._bar.set_postfix_str(f"失败={partition_id}", refresh=True)
            self._bar.write(f"{dataset_id} {partition_id} 失败：{error}", file=self.file)

    def dataset_finished(self, dataset_id: str) -> None:
        self._close_bar()

    def _close_bar(self) -> None:
        if self._bar is not None:
            self._bar.close()
            self._bar = None
