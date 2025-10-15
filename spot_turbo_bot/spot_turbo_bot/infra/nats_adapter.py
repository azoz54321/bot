from __future__ import annotations

import asyncio
import ssl
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

try:
    import orjson as _json  # type: ignore

    def _jloads(b: bytes) -> dict:
        return _json.loads(b)


except Exception:  # pragma: no cover
    import json as _json  # type: ignore

    def _jloads(b: bytes) -> dict:
        return _json.loads(b.decode("utf-8"))


try:
    from nkeys import nkeys as _nkeys  # type: ignore
except Exception:  # pragma: no cover
    _nkeys = None  # type: ignore[assignment]

from ..core.config import (
    NATS_AUTH_MODE,
    NATS_URLS,
    NATS_CONNECT_TIMEOUT_SEC,
    NATS_RECONNECT_MAX_TRIES,
    NATS_RECONNECT_JITTER_MS,
    NATS_NKEY_PUBLIC,
    NATS_NKEY_SEED,
    NATS_JWT,
    NATS_USERNAME,
    NATS_PASSWORD,
    NATS_TLS_REQUIRED,
    NATS_TLS_CA_PATH,
    NATS_TLS_CERT_PATH,
    NATS_TLS_KEY_PATH,
    NATS_TLS_INSECURE_SKIP_VERIFY,
)


_NATS_AUTH_MODE = (NATS_AUTH_MODE or "none").strip().lower()
_NATS_URLS = tuple(NATS_URLS) if NATS_URLS else ("nats://localhost:4222",)
_NATS_CONNECT_TIMEOUT_SEC = float(NATS_CONNECT_TIMEOUT_SEC)
_NATS_RECONNECT_MAX_TRIES = int(NATS_RECONNECT_MAX_TRIES)
_NATS_RECONNECT_WAIT_SEC = max(0.0, float(NATS_RECONNECT_JITTER_MS) / 1000.0)
_NATS_NKEY_PUBLIC = NATS_NKEY_PUBLIC.strip()
_NATS_NKEY_SEED = NATS_NKEY_SEED.strip()
_NATS_JWT = NATS_JWT.strip()
_NATS_USERNAME = NATS_USERNAME.strip()
_NATS_PASSWORD = NATS_PASSWORD
_NATS_TLS_REQUIRED = bool(NATS_TLS_REQUIRED)
_NATS_TLS_CA_PATH = NATS_TLS_CA_PATH.strip()
_NATS_TLS_CERT_PATH = NATS_TLS_CERT_PATH.strip()
_NATS_TLS_KEY_PATH = NATS_TLS_KEY_PATH.strip()
_NATS_TLS_INSECURE_SKIP_VERIFY = bool(NATS_TLS_INSECURE_SKIP_VERIFY)


@dataclass(slots=True)
class NATSConnectConfig:
    servers: tuple[str, ...]
    kwargs: dict[str, object]
    signature_cb: Optional[Callable[[bytes], bytes]] = None
    user_jwt_cb: Optional[Callable[[], str]] = None
    tls_context: Optional[ssl.SSLContext] = None


@dataclass(slots=True)
class OneSecondEvent:
    symbol: str
    sec_start: int  # UNIX micros
    sec_end: int    # UNIX micros
    open: float
    high: float
    low: float
    close: float
    updates: int
    spread_bps: float
    source: str


async def _noop_async(*_args, **_kwargs) -> None:
    return None


def _make_signature_cb(kp) -> Callable[[bytes], bytes]:
    def _sign(nonce: bytes) -> bytes:
        return kp.sign(nonce)

    return _sign


def _build_tls_context() -> Optional[ssl.SSLContext]:
    if not (_NATS_TLS_REQUIRED or _NATS_AUTH_MODE == "tls"):
        return None
    context = ssl.create_default_context()
    if _NATS_TLS_CA_PATH:
        context.load_verify_locations(cafile=_NATS_TLS_CA_PATH)
    if _NATS_TLS_CERT_PATH:
        context.load_cert_chain(certfile=_NATS_TLS_CERT_PATH, keyfile=_NATS_TLS_KEY_PATH or None)
    if _NATS_TLS_INSECURE_SKIP_VERIFY:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def _build_auth_details() -> tuple[dict[str, object], Optional[Callable[[bytes], bytes]], Optional[Callable[[], str]]]:
    mode = _NATS_AUTH_MODE
    if mode in ("", "none", "tls"):
        return {}, None, None
    if mode == "nkey":
        if not _NATS_NKEY_SEED:
            raise RuntimeError("NATS_NKEY_SEED is required when NATS_AUTH_MODE=nkey")
        if _nkeys is None:
            raise RuntimeError("nkeys package is required for NATS nkey authentication")
        kp = _nkeys.from_seed(_NATS_NKEY_SEED.encode())
        signature_cb = _make_signature_cb(kp)
        public_key = _NATS_NKEY_PUBLIC or kp.public_key.decode()
        auth_kwargs = {
            "user": public_key,
            "signature_cb": signature_cb,
            "nkeys_seed_str": _NATS_NKEY_SEED,
        }
        return auth_kwargs, signature_cb, None
    if mode == "jwt":
        if not _NATS_JWT:
            raise RuntimeError("NATS_JWT is required when NATS_AUTH_MODE=jwt")
        if not _NATS_NKEY_SEED:
            raise RuntimeError("NATS_NKEY_SEED is required when NATS_AUTH_MODE=jwt")
        if _nkeys is None:
            raise RuntimeError("nkeys package is required for NATS JWT authentication")
        kp = _nkeys.from_seed(_NATS_NKEY_SEED.encode())
        signature_cb = _make_signature_cb(kp)
        public_key = _NATS_NKEY_PUBLIC or kp.public_key.decode()

        def _user_jwt_cb() -> str:
            return _NATS_JWT

        auth_kwargs = {
            "user": public_key,
            "signature_cb": signature_cb,
            "user_jwt_cb": _user_jwt_cb,
            "nkeys_seed_str": _NATS_NKEY_SEED,
        }
        return auth_kwargs, signature_cb, _user_jwt_cb
    if mode == "userpass":
        if not _NATS_USERNAME or not _NATS_PASSWORD:
            raise RuntimeError("NATS_USERNAME and NATS_PASSWORD are required when NATS_AUTH_MODE=userpass")
        return {"user": _NATS_USERNAME, "password": _NATS_PASSWORD}, None, None
    raise RuntimeError(f"Unsupported NATS_AUTH_MODE '{mode}'")


def _build_connect_config() -> NATSConnectConfig:
    base_kwargs: dict[str, object] = {
        "allow_reconnect": True,
        "max_reconnect_attempts": _NATS_RECONNECT_MAX_TRIES,
        "connect_timeout": _NATS_CONNECT_TIMEOUT_SEC,
        "reconnect_time_wait": _NATS_RECONNECT_WAIT_SEC,
        "error_cb": _noop_async,
        "disconnected_cb": _noop_async,
        "reconnected_cb": _noop_async,
        "closed_cb": _noop_async,
    }
    auth_kwargs, signature_cb, user_jwt_cb = _build_auth_details()
    if auth_kwargs:
        base_kwargs.update(auth_kwargs)
    tls_context = _build_tls_context()
    if tls_context is not None:
        base_kwargs["tls"] = tls_context
    return NATSConnectConfig(
        servers=_NATS_URLS,
        kwargs=base_kwargs,
        signature_cb=signature_cb,
        user_jwt_cb=user_jwt_cb,
        tls_context=tls_context,
    )


_CONNECT_CONFIG = _build_connect_config()


def get_nats_connect_config() -> NATSConnectConfig:
    return _CONNECT_CONFIG


class NATSOneSecAdapter:
    """NATS adapter that subscribes to ONE_SEC.usdt.* and pushes parsed OneSecondEvent
    objects into an asyncio.Queue for downstream consumption.

    Hot path is zero-logging; metrics should be aggregated by the runner.
    """

    def __init__(
        self,
        *,
        servers: Sequence[str] | None = None,
        bus_url: Optional[str] = None,
        subject: str,
        queue_group: str,
        out_queue: asyncio.Queue[OneSecondEvent],
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self._servers = tuple(servers) if servers else ((bus_url,) if bus_url else _NATS_URLS)
        self._subject = subject
        self._queue_group = queue_group
        self._out_q = out_queue
        self._loop = loop or asyncio.get_event_loop()
        self._json_loads = _jloads
        self._nc = None
        self._sub = None
        self._stop_evt = asyncio.Event()
        self._connect_task: Optional[asyncio.Task] = None
        config = _CONNECT_CONFIG
        self._signature_cb = config.signature_cb
        self._user_jwt_cb = config.user_jwt_cb
        self._tls_context = config.tls_context
        self._connect_kwargs = config.kwargs
        if servers:
            self._servers = tuple(servers)
        elif bus_url:
            self._servers = (bus_url,)
        else:
            self._servers = config.servers

    async def start(self) -> None:
        if self._connect_task is None:
            self._connect_task = asyncio.create_task(self._connect_loop())

    async def _connect_loop(self) -> None:
        from nats.aio.client import Client as NATS  # type: ignore

        while not self._stop_evt.is_set():
            try:
                if self._nc is None or getattr(self._nc, "is_connected", False) is False:
                    self._nc = NATS()
                    try:
                        await self._nc.connect(
                            servers=list(self._servers),
                            **self._connect_kwargs,
                        )
                    except Exception:
                        await asyncio.sleep(1.0)
                        continue
                    try:
                        self._sub = await self._nc.subscribe(
                            self._subject,
                            queue=self._queue_group,
                            cb=self._on_msg,
                        )
                    except Exception:
                        try:
                            await self._nc.close()
                        except Exception:
                            pass
                        self._nc = None
                        await asyncio.sleep(1.0)
                        continue
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1.0)

    async def stop(self) -> None:
        self._stop_evt.set()
        try:
            if self._nc is not None:
                try:
                    await self._nc.drain()
                except Exception:
                    pass
        finally:
            try:
                if self._nc is not None:
                    await self._nc.close()
            except Exception:
                pass
            self._nc = None
            self._sub = None
            if self._connect_task is not None:
                try:
                    self._connect_task.cancel()
                except Exception:
                    pass
                self._connect_task = None

    async def _on_msg(self, msg) -> None:  # type: ignore[override]
        try:
            data = self._json_loads(msg.data)
            symbol = (data.get("symbol") or "").upper()
            if not symbol:
                return
            evt = OneSecondEvent(
                symbol=symbol,
                sec_start=int(data.get("sec_start", 0) or 0),
                sec_end=int(data.get("sec_end", 0) or 0),
                open=float(data.get("open", 0.0) or 0.0),
                high=float(data.get("high", 0.0) or 0.0),
                low=float(data.get("low", 0.0) or 0.0),
                close=float(data.get("close", 0.0) or 0.0),
                updates=int(data.get("updates", 0) or 0),
                spread_bps=float(data.get("spread_bps", 0.0) or 0.0),
                source=str(data.get("source", "") or ""),
            )
            try:
                self._out_q.put_nowait(evt)
            except Exception:
                pass
        except Exception:
            return
