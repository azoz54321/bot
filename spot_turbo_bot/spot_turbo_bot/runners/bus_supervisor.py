from __future__ import annotations

import asyncio
import os
import random
import socket
from pathlib import Path
from typing import Optional, Sequence

from ..core.logger import log_line, dbg


class NATSSupervisor:
    """Manage a local nats-server process with readiness checks and restart logic."""

    def __init__(
        self,
        *,
        bin_path: str,
        host: str,
        port: int,
        args: Sequence[str],
        ready_timeout_ms: int,
        backoff_min_ms: int,
        backoff_max_ms: int,
        singleton_key: str,
    ) -> None:
        self._bin_path = Path(bin_path)
        self._host = host
        self._port = int(port)
        self._args = tuple(args)
        self._ready_timeout_ms = max(100, int(ready_timeout_ms))
        self._backoff_min_ms = max(100, int(backoff_min_ms))
        self._backoff_max_ms = max(self._backoff_min_ms, int(backoff_max_ms))
        self._singleton_key = singleton_key.strip() or "nats_server"

        self._lock_path = Path("runtime") / f"{self._singleton_key}.pid"
        self._lock_held = False
        self._process: Optional[asyncio.subprocess.Process] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._ensure_task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()
        self._ready = False
        self._ready_event = asyncio.Event()

    async def ensure_started(self) -> bool:
        if self._ensure_task is None:
            self._ensure_task = asyncio.create_task(self._ensure_impl())
        try:
            return await self._ensure_task
        finally:
            self._ensure_task = None

    def is_ready(self) -> bool:
        return self._ready

    async def stop(self) -> None:
        self._stop_evt.set()
        if self._monitor_task:
            self._monitor_task.cancel()
        proc = self._process
        if proc is not None:
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
        self._monitor_task = None
        self._ready = False
        self._ready_event.clear()
        self._cleanup_lock()

    async def wait_ready(self, timeout: Optional[float] = None) -> bool:
        if self._ready:
            return True
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _ensure_impl(self) -> bool:
        log_line(
            f"[BUS] nats check host={self._host}:{self._port} timeout={self._ready_timeout_ms}ms"
        )
        if await self._is_ready(self._ready_timeout_ms / 1000.0):
            self._mark_ready()
            log_line(f"[BUS] nats up host={self._host}:{self._port} -> skip spawn")
            return True

        if self._port_in_use():
            log_line(f"[BUS] port {self._port} busy by another process -> skip spawn")
            self._mark_ready()
            return True

        if not self._bin_path.exists():
            log_line(f"[BUS] nats binary not found path={self._bin_path} -> waiting for external")
            self._ready = False
            return False

        if not self._acquire_lock():
            log_line("[BUS] singleton busy -> skip spawn")
            self._ready = False
            return False

        started = await self._start_process(initial=True)
        if not started:
            self._cleanup_lock()
            self._ready = False
            return False

        await self._wait_until_ready()
        return self._ready

    async def _wait_until_ready(self) -> None:
        loop = asyncio.get_running_loop()
        start = loop.time()
        deadline = start + (self._ready_timeout_ms / 1000.0)
        ready = False
        delay = 0.2
        while loop.time() < deadline and not self._stop_evt.is_set():
            if await self._is_ready(0.5):
                elapsed_ms = int((loop.time() - start) * 1000)
                log_line(f"[BUS] nats ready in {elapsed_ms}ms")
                ready = True
                break
            await asyncio.sleep(delay)
        if ready:
            self._mark_ready()
        else:
            dbg("nats ready wait expired; continuing without ready flag")
            self._ready = False

    async def _is_ready(self, timeout: float) -> bool:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port), timeout=timeout
            )
        except Exception:
            return False
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        return True

    def _port_in_use(self) -> bool:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind((self._host, self._port))
        except OSError:
            return True
        finally:
            try:
                sock.close()
            except Exception:
                pass
        return False

    async def _start_process(self, *, initial: bool) -> bool:
        try:
            env = os.environ.copy()
            proc = await asyncio.create_subprocess_exec(
                str(self._bin_path),
                *self._args,
                env=env,
            )
        except FileNotFoundError:
            log_line(f"[BUS] nats binary not found path={self._bin_path} -> waiting for external")
            return False
        except Exception as exc:
            log_line(f"[BUS] failed to start nats-server: {exc}")
            return False

        self._process = proc
        args_str = " ".join(self._args) if self._args else ""
        log_line(f"[BUS] starting nats-server pid={proc.pid} args={args_str or 'null'}")

        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor_loop())

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
                dbg(f"nats monitor wait failed: {exc}")
                return_code = None

            self._ready = False
            self._ready_event.clear()
            self._process = None
            self._cleanup_lock()

            if self._stop_evt.is_set():
                return

            delay_ms = random.randint(self._backoff_min_ms, self._backoff_max_ms)
            log_line(
                f"[BUS] nats exited code={return_code} -> restart in {delay_ms/1000:.1f}s"
            )
            await asyncio.sleep(delay_ms / 1000.0)

            if self._stop_evt.is_set():
                return

            if not self._acquire_lock():
                log_line("[BUS] singleton busy -> skip restart")
                return

            started = await self._start_process(initial=False)
            if not started:
                self._cleanup_lock()
                return
            await self._wait_until_ready()

    def _mark_ready(self) -> None:
        self._ready = True
        if not self._ready_event.is_set():
            self._ready_event.set()

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
