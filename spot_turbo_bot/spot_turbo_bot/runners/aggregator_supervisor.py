from __future__ import annotations

import asyncio
import os
import random
import sys
from pathlib import Path
from typing import Optional

from ..core.logger import dbg, log_line
from ..infra.nats_adapter import get_nats_connect_config


class OneSecAggregatorSupervisor:
    """Manage a local one-second aggregator subprocess with heartbeat detection."""

    def __init__(
        self,
        *,
        heartbeat_subject: str,
        heartbeat_wait_ms: int,
        singleton_key: str,
        backoff_min_ms: int,
        backoff_max_ms: int,
        all_usdt: bool,
    ) -> None:
        self._subject = heartbeat_subject.strip()
        self._heartbeat_wait_ms = max(100, heartbeat_wait_ms)
        self._singleton_key = singleton_key.strip() or "one_sec_aggregator"
        self._backoff_min_ms = max(100, backoff_min_ms)
        self._backoff_max_ms = max(self._backoff_min_ms, backoff_max_ms)
        self._all_usdt = bool(all_usdt)

        self._lock_path = Path("runtime") / f"{self._singleton_key}.pid"
        self._lock_held = False
        self._process: Optional[asyncio.subprocess.Process] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()
        self._started_here = False
        self._ensure_task: Optional[asyncio.Task] = None
        self._ready = False
        self._ready_event = asyncio.Event()

    async def ensure_started(self) -> bool:
        if self._ensure_task is None:
            self._ensure_task = asyncio.create_task(self._ensure_started_impl())
        try:
            return await self._ensure_task
        finally:
            self._ensure_task = None

    def is_ready(self) -> bool:
        return self._ready

    async def wait_ready(self, timeout: float | None = None) -> bool:
        if self._ready:
            return True
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _ensure_started_impl(self) -> bool:
        wait_ms = self._heartbeat_wait_ms
        log_line(f"[AGG-AUTO] heartbeat check subject={self._subject or 'NONE'} wait={wait_ms}ms")
        existing = False
        if self._subject:
            try:
                existing = await self._wait_for_heartbeat()
            except Exception as exc:
                dbg(f"[AGG-AUTO] heartbeat wait error: {exc}")
        if existing:
            log_line("[AGG-AUTO] existing aggregator detected -> skip")
            self._mark_ready()
            return True
        if not self._acquire_lock():
            log_line("[AGG-AUTO] singleton busy -> skip")
            self._ready = False
            return False
        started = await self._start_process(initial=True)
        if not started:
            self._cleanup_lock()
            self._ready = False
            return False
        self._started_here = True
        self._mark_ready()
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        return True

    async def stop(self) -> None:
        self._stop_evt.set()
        if self._monitor_task:
            self._monitor_task.cancel()
        if self._process is not None:
            proc = self._process
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=1.0)
                except Exception:
                    pass
        if self._monitor_task:
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._process = None
        self._ready = False
        self._ready_event.clear()
        self._cleanup_lock()

    def started_here(self) -> bool:
        return self._started_here

    async def _wait_for_heartbeat(self) -> bool:
        from nats.aio.client import Client as NATS  # type: ignore

        config = get_nats_connect_config()
        nc = NATS()
        try:
            await nc.connect(servers=list(config.servers), **config.kwargs)
        except Exception:
            return False

        loop = asyncio.get_running_loop()
        heartbeat_seen = loop.create_future()

        async def _cb(_msg) -> None:
            if not heartbeat_seen.done():
                heartbeat_seen.set_result(True)

        try:
            sid = await nc.subscribe(self._subject, cb=_cb)
        except Exception:
            try:
                await nc.drain()
            except Exception:
                pass
            return False

        try:
            await asyncio.wait_for(heartbeat_seen, timeout=self._heartbeat_wait_ms / 1000.0)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            try:
                await nc.unsubscribe(sid)
            except Exception:
                pass
            try:
                await nc.drain()
            except Exception:
                pass

    async def _start_process(self, *, initial: bool) -> bool:
        args = [sys.executable, "-m", "spot_turbo_bot.runners.one_sec_aggregator"]
        if self._all_usdt:
            args.append("--all-usdt")
        try:
            proc = await asyncio.create_subprocess_exec(*args)
        except Exception as exc:
            log_line(f"[AGG-AUTO] failed to start subprocess: {exc}")
            return False
        self._process = proc
        self._write_pid(proc.pid)
        prefix = "timeout" if initial else "restart"
        log_line(
            f"[AGG-AUTO] heartbeat {prefix}({self._heartbeat_wait_ms}ms) -> starting subprocess --all-usdt pid={proc.pid}"
        )
        return True

    async def _monitor_loop(self) -> None:
        while not self._stop_evt.is_set():
            proc = self._process
            if proc is None:
                return
            try:
                return_code = await proc.wait()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                dbg(f"[AGG-AUTO] monitor wait error: {exc}")
                return_code = None
            self._process = None
            self._ready = False
            self._ready_event.clear()
            self._cleanup_lock()
            if self._stop_evt.is_set():
                return
            delay_ms = random.randint(self._backoff_min_ms, self._backoff_max_ms)
            log_line(
                f"[AGG-AUTO] aggregator exited code={return_code} -> restart in {delay_ms/1000:.1f}s"
            )
            await asyncio.sleep(delay_ms / 1000.0)
            if self._stop_evt.is_set():
                return
            if not self._acquire_lock():
                log_line("[AGG-AUTO] singleton busy -> skip restart")
                return
            started = await self._start_process(initial=False)
            if not started:
                self._cleanup_lock()
                return
            self._mark_ready()

    def _acquire_lock(self) -> bool:
        try:
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(self._lock_path, flags)
        except FileExistsError:
            return False
        except Exception:
            return False
        try:
            os.write(fd, str(os.getpid()).encode())
        except Exception:
            pass
        finally:
            os.close(fd)
        self._lock_held = True
        return True

    def _write_pid(self, pid: int) -> None:
        if not self._lock_held:
            return
        try:
            self._lock_path.write_text(str(pid))
        except Exception:
            pass

    def _cleanup_lock(self) -> None:
        if not self._lock_held:
            return
        try:
            self._lock_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        except TypeError:
            try:
                if self._lock_path.exists():
                    self._lock_path.unlink()
            except Exception:
                pass
        except Exception:
            pass
        self._lock_held = False

    def _mark_ready(self) -> None:
        self._ready = True
        if not self._ready_event.is_set():
            self._ready_event.set()
