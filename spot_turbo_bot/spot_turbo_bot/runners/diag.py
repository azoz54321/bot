from __future__ import annotations

import asyncio
import faulthandler
import sys
import time
from typing import Optional

from ..core.config import STALL_DIAG_ON, STALL_THRESHOLD_SEC, STALL_POLL_SEC
from ..core.logger import log_line


_last_activity_ts: float = time.monotonic()
_last_report_ts: float = 0.0


def touch_activity(_: Optional[str] = None) -> None:
    global _last_activity_ts
    _last_activity_ts = time.monotonic()


class StallWatchdog:
    def __init__(self, threshold_sec: float = STALL_THRESHOLD_SEC, poll_sec: float = STALL_POLL_SEC):
        self.threshold = threshold_sec
        self.poll = poll_sec
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if not STALL_DIAG_ON:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._stop.set()
            self._task.cancel()

    async def _run(self) -> None:
        global _last_report_ts
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self.poll)
                now = time.monotonic()
                idle = now - _last_activity_ts
                if idle >= self.threshold:
                    # avoid spamming: report at most once per threshold window
                    if (now - _last_report_ts) < self.threshold:
                        continue
                    _last_report_ts = now
                    try:
                        log_line(f"[STALL] idle_for={int(idle)}s dumping stacks...")
                        faulthandler.dump_traceback(file=sys.stdout, all_threads=True)
                    except Exception:
                        # best-effort only
                        pass
        except asyncio.CancelledError:
            return

