from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatusCode, WebSocketException

from ..core.config import (
    WS_BASE,
    USER_STREAM_KEEPALIVE_SEC,
    USERSTREAM_ENABLE,
    USERSTREAM_VERBOSE,
    USERSTREAM_STALE_MS,
    USERSTREAM_RECONNECT_BACKOFF_MS_MIN,
    USERSTREAM_RECONNECT_BACKOFF_MS_MAX,
    USERSTREAM_FALLBACK_QUERY_MS,
    USERSTREAM_STALE_MS_IDLE,
)
from ..core.logger import log_line, dbg
from ..infra.binance_http import BinanceHTTP, BinanceHTTPError
from .diag import touch_activity


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(slots=True)
class UserStreamSnapshot:
    listen_key: Optional[str]
    connected: bool
    healthy: bool
    last_event_ms: int
    message_count: int


class UserStreamManager:
    def __init__(
        self,
        http: BinanceHTTP,
        on_execution_report: Callable[[dict], None],
        on_state_event: Optional[Callable[[str, dict], None]] = None,
        pending_activity_provider: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.http = http
        self.on_execution_report = on_execution_report
        self._on_state_event = on_state_event
        self._pending_activity_provider = pending_activity_provider

        self._listen_key: Optional[str] = None
        self._listen_key_lock = asyncio.Lock()

        self._ws_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None

        self._stop_evt = asyncio.Event()
        self._connected_evt = asyncio.Event()

        self._current_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._last_event_ms: int = 0
        self._message_count: int = 0
        self._healthy: bool = False
        self._ready_logged: bool = False
        self._reconnect_attempt: int = 0

    async def start(self, *, wait_connected: bool = True, timeout: float = 10.0) -> None:
        if not USERSTREAM_ENABLE:
            log_line("[USER] user stream disabled (USERSTREAM_ENABLE=false)")
            self._connected_evt.set()
            return
        self._stop_evt.clear()
        self._connected_evt.clear()
        await self._create_listen_key(initial=True)
        self._ws_task = asyncio.create_task(self._run_ws_loop())
        self._keepalive_task = asyncio.create_task(self._run_keepalive())
        if wait_connected:
            try:
                await asyncio.wait_for(self._connected_evt.wait(), timeout=timeout)
            except asyncio.TimeoutError as exc:
                raise RuntimeError("user stream did not connect within timeout") from exc

    async def stop(self) -> None:
        self._stop_evt.set()
        tasks = [t for t in (self._ws_task, self._keepalive_task) if t]
        for task in tasks:
            task.cancel()
        if self._current_ws:
            try:
                await asyncio.wait_for(self._current_ws.close(code=4000, reason="shutdown"), timeout=1.0)
            except Exception:
                pass
        for task in tasks:
            try:
                await task
            except Exception:
                pass

    def snapshot(self) -> UserStreamSnapshot:
        return UserStreamSnapshot(
            listen_key=self._listen_key,
            connected=self._connected_evt.is_set(),
            healthy=self._healthy,
            last_event_ms=self._last_event_ms,
            message_count=self._message_count,
        )

    def is_ready(self) -> bool:
        return self._connected_evt.is_set()

    def is_healthy(self) -> bool:
        return self._healthy

    async def wait_until_ready(self, timeout: Optional[float] = None) -> bool:
        if self._connected_evt.is_set():
            return True
        try:
            await asyncio.wait_for(self._connected_evt.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _create_listen_key(self, *, initial: bool, reason: str | None = None) -> None:
        async with self._listen_key_lock:
            try:
                key = await self.http.user_stream_listen_key()
            except Exception as exc:
                log_line(f"[USER] failed to create listenKey: {exc}")
                raise
            old = self._listen_key
            self._listen_key = key
            label = "created" if initial or not old else "recreated"
            log_line(f"[USER] listenKey {label} key={key}")
            self._emit_state("listen_key", {"listenKey": key, "initial": initial, "reason": reason or ("startup" if initial else "refresh")})
            if old and old != key:
                await self._request_reconnect("listen_key_rotated")

    async def _run_keepalive(self) -> None:
        if not USERSTREAM_ENABLE:
            return
        try:
            while not self._stop_evt.is_set():
                await asyncio.sleep(USER_STREAM_KEEPALIVE_SEC)
                if self._listen_key is None or self._stop_evt.is_set():
                    continue
                try:
                    await self.http.user_stream_keepalive(self._listen_key)
                except BinanceHTTPError as e:
                    log_line(f"[USER] keepalive failed code={e.code} msg={e.msg} -> recreate listenKey")
                    self._emit_state("keepalive_failed", {"code": e.code, "msg": e.msg})
                    await self._create_listen_key(initial=False, reason="keepalive_failed")
                except Exception as exc:
                    log_line(f"[USER] keepalive failed err={exc} -> recreate listenKey")
                    self._emit_state("keepalive_failed", {"error": repr(exc)})
                    await self._create_listen_key(initial=False, reason="keepalive_exception")
                else:
                    if USERSTREAM_VERBOSE:
                        log_line(f"[USER] keepalive ok listenKey={self._listen_key}")
                    self._emit_state("keepalive_ok", {"listenKey": self._listen_key})
        except asyncio.CancelledError:
            return

    async def _run_ws_loop(self) -> None:
        if not USERSTREAM_ENABLE:
            return
        backoff_ms = USERSTREAM_RECONNECT_BACKOFF_MS_MIN
        attempt = 0
        try:
            while not self._stop_evt.is_set():
                listen_key = self._listen_key
                if not listen_key:
                    await self._create_listen_key(initial=False, reason="missing_listen_key")
                    listen_key = self._listen_key
                if not listen_key:
                    await asyncio.sleep(0.5)
                    continue

                url = f"{WS_BASE}/{listen_key}"
                try:
                    ws = await websockets.connect(
                        url,
                        ping_interval=20,
                        ping_timeout=20,
                        max_queue=1,
                        compression=None,
                    )
                except InvalidStatusCode as exc:
                    attempt += 1
                    should_refresh = 400 <= exc.status_code < 500
                    reason = f"ws_status_{exc.status_code}"
                    log_line(f"[USER] reconnecting... reason={reason}")
                    self._emit_state("reconnecting", {"reason": reason, "attempt": attempt})
                    if should_refresh:
                        await self._create_listen_key(initial=False, reason="status4xx")
                    delay = self._compute_backoff_delay(backoff_ms, attempt)
                    await asyncio.sleep(delay / 1000.0)
                    backoff_ms = min(backoff_ms * 2, USERSTREAM_RECONNECT_BACKOFF_MS_MAX)
                    continue
                except WebSocketException as exc:
                    attempt += 1
                    reason = f"ws_exc_{type(exc).__name__}"
                    log_line(f"[USER] reconnecting... reason={reason}")
                    self._emit_state("reconnecting", {"reason": reason, "attempt": attempt})
                    delay = self._compute_backoff_delay(backoff_ms, attempt)
                    await asyncio.sleep(delay / 1000.0)
                    backoff_ms = min(backoff_ms * 2, USERSTREAM_RECONNECT_BACKOFF_MS_MAX)
                    continue
                except Exception as exc:
                    attempt += 1
                    reason = f"ws_err_{exc}"
                    log_line(f"[USER] reconnecting... reason={reason}")
                    self._emit_state("reconnecting", {"reason": reason, "attempt": attempt})
                    delay = self._compute_backoff_delay(backoff_ms, attempt)
                    await asyncio.sleep(delay / 1000.0)
                    backoff_ms = min(backoff_ms * 2, USERSTREAM_RECONNECT_BACKOFF_MS_MAX)
                    continue

                try:
                    self._current_ws = ws
                    await self._handle_ws_connection(ws, attempt)
                    attempt = 0
                    backoff_ms = USERSTREAM_RECONNECT_BACKOFF_MS_MIN
                finally:
                    self._current_ws = None
                    try:
                        await ws.close()
                    except Exception:
                        pass
                if self._stop_evt.is_set():
                    break
                attempt += 1
                delay = self._compute_backoff_delay(backoff_ms, attempt)
                await asyncio.sleep(delay / 1000.0)
                backoff_ms = min(backoff_ms * 2, USERSTREAM_RECONNECT_BACKOFF_MS_MAX)
        except asyncio.CancelledError:
            return

    async def _handle_ws_connection(self, ws: websockets.WebSocketClientProtocol, attempt: int) -> None:
        connect_ts = _now_ms()
        self._last_event_ms = connect_ts
        self._message_count = 0
        self._healthy = True
        if attempt > 0:
            log_line(f"[USER] reconnected listenKey={self._listen_key}")
        else:
            log_line(f"[USER] stream connected listenKey={self._listen_key}")
        self._emit_state("connected", {"listenKey": self._listen_key, "attempt": attempt})
        if not self._connected_evt.is_set():
            self._connected_evt.set()

        stale_timeout = max(USERSTREAM_STALE_MS, USERSTREAM_FALLBACK_QUERY_MS) / 1000.0

        while not self._stop_evt.is_set():
            try:
                pending = self._has_pending_activity()
                timeout_ms = USERSTREAM_STALE_MS if pending else USERSTREAM_STALE_MS_IDLE
                timeout_ms = max(timeout_ms, USERSTREAM_FALLBACK_QUERY_MS)
                msg = await asyncio.wait_for(ws.recv(), timeout=timeout_ms / 1000.0)
            except asyncio.TimeoutError:
                if not self._has_pending_activity():
                    # Idle window: keep connection without flapping.
                    self._last_event_ms = _now_ms()
                    continue
                stale_ms = _now_ms() - self._last_event_ms
                log_line(f"[USER] stale > USERSTREAM_STALE_MS stale_ms={stale_ms}")
                self._emit_state("stale", {"stale_ms": stale_ms})
                self._healthy = False
                break
            except ConnectionClosed as exc:
                reason = f"code={exc.code} reason={exc.reason or ''}"
                log_line(f"[USER] reconnecting... reason={reason}")
                self._emit_state("reconnecting", {"reason": reason})
                self._healthy = False
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log_line(f"[USER] reconnecting... reason=recv_error {exc}")
                self._emit_state("reconnecting", {"reason": f"recv_error:{exc}"})
                self._healthy = False
                break

            now_ms = _now_ms()
            delta_ms = now_ms - self._last_event_ms if self._last_event_ms else 0
            self._last_event_ms = now_ms
            self._message_count += 1
            touch_activity("user_ws")

            try:
                data = json.loads(msg)
            except Exception:
                continue

            if self._message_count == 1:
                log_line(f"[USER] stream heartbeat e={data.get('e', '?')} Δms={delta_ms}")
                self._emit_state("first_message", {"event": data.get("e"), "delta_ms": delta_ms})

            evt = data.get("e")
            self._emit_state("message", {"event": evt, "ts": now_ms})

            if evt == "listenKeyExpired":
                log_line("[USER] listenKey expired -> rotate")
                self._healthy = False
                self._emit_state("listen_key_expired", {"ts": now_ms})
                await self._create_listen_key(initial=False, reason="expired")
                break

            if evt == "executionReport":
                try:
                    self.on_execution_report(data)
                except Exception as exc:
                    dbg(f"user stream exec handler failed: {exc}")

    async def _request_reconnect(self, reason: str) -> None:
        ws = self._current_ws
        if ws is None:
            return
        try:
            await ws.close(code=4001, reason=reason)
        except Exception:
            pass

    def _emit_state(self, event: str, info: Optional[dict]) -> None:
        if self._on_state_event:
            try:
                self._on_state_event(event, info or {})
            except Exception:
                pass

    def _compute_backoff_delay(self, base_ms: int, attempt: int) -> int:
        if attempt <= 1:
            target = base_ms
        else:
            target = min(USERSTREAM_RECONNECT_BACKOFF_MS_MAX, base_ms * (2 ** (attempt - 1)))
        jitter = random.uniform(0.85, 1.15)
        target = int(max(USERSTREAM_RECONNECT_BACKOFF_MS_MIN, min(USERSTREAM_RECONNECT_BACKOFF_MS_MAX, target * jitter)))
        return target

    def _has_pending_activity(self) -> bool:
        if self._pending_activity_provider is None:
            return True
        try:
            return bool(self._pending_activity_provider())
        except Exception:
            return True
