"""Single-process TCP fault proxy built with FastAPI + asyncio.

The proxy accepts TCP connections on one port and always forwards them to a
fixed local receiver endpoint.  For every connection, each direction
(client->proxy->receiver and receiver->proxy->client) is pumped by its own
asyncio task.

Per direction the following can be configured (via the control API before a
new connection arrives, or manually on a live connection):

* pause after N cumulative bytes have been forwarded in that direction;
* resume a paused direction (or all paused directions of a connection);
* forcibly close the whole connection, interrupting paused directions and
  blocked writes immediately.

Connection lifecycle (important for "target connect is stuck"):

* As soon as the client connection is accepted by this application it is
  registered and visible in ``GET /connections`` with ``state="connecting"``.
  The connection id can therefore be found *before* the target socket exists
  and can be force-closed at any time during establishment.
* A force-close that lands while the target connect is pending cancels the
  connect, closes the client socket, and only returns after cleanup has
  finished.  If the target connect completed during the cancellation window,
  that freshly created target socket is closed as well (no pumps are started
  and no active target connection is left behind).
* A failed/timed-out target connect still leaves a closed record in
  ``GET /connections`` whose ``error`` field carries the reason.

Other guarantees implemented in this file:

* A read chunk that crosses the N-byte threshold is split, so forwarding
  stops *exactly* at N bytes.  The unsent tail (backlog) and any FIN stay on
  the proxy side while the direction is paused.
* Backpressure: while paused no further bytes are read from the socket; the
  kernel receive buffer fills and TCP flow control throttles the sender.
  Nothing is spooled to disk and no whole packets are buffered.  User-space
  buffering per direction is bounded by the stream read limit (<=256 KiB).
* A FIN (half-close) is held back until the backlog of its own direction has
  been forwarded after resume; the reverse direction keeps flowing.
* A force-close aborts both transports, cancels both pump tasks and waits
  until they have exited before reporting completion.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections import deque
from contextlib import suppress
from typing import Deque, Dict, Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# Configuration / limits
# --------------------------------------------------------------------------- #

LISTEN_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
LISTEN_PORT = int(os.getenv("PROXY_PORT", "9000"))
LISTEN_BACKLOG = int(os.getenv("PROXY_LISTEN_BACKLOG", "128"))
TARGET_HOST = os.getenv("TARGET_HOST", "receiver")
TARGET_PORT = int(os.getenv("TARGET_PORT", "9100"))

# Upper bound for a single target connect attempt.  A stuck target would
# otherwise keep a client connection in "connecting" forever.  <=0 disables it.
CONNECT_TIMEOUT = float(os.getenv("PROXY_CONNECT_TIMEOUT", "5.0"))
# Upper bound for one control-API force-close (cleanup must finish in here).
FORCE_CLOSE_TIMEOUT = float(os.getenv("PROXY_FORCE_CLOSE_TIMEOUT", "10.0"))
# Upper bound for waiting on a closed socket; abort() should make this instant.
CLOSE_WAIT_TIMEOUT = 1.0

# asyncio.StreamReader byte limit: a single read() can return at most this
# many bytes, and it is also the hard cap on user-space per-direction state
# (one reader buffer + at most one unsent split tail).
CHUNK = 64 * 1024  # 64 KiB per read
STREAM_LIMIT = 256 * 1024  # 256 KiB, required user-space cap
MAX_CLOSED_RECORDS = 200

Direction = Literal["c2r", "r2c"]
DIRECTIONS: tuple[Direction, Direction] = ("c2r", "r2c")

FORCE_CLOSE_REASON = "force-closed by control API"

# Seam for tests: lets a test delay/observe the target connect and verify
# that a socket completing inside a force-close window is closed, not leaked.
_open_connection = asyncio.open_connection


def _now() -> float:
    return time.monotonic()


# --------------------------------------------------------------------------- #
# Preset policy applied to every new connection
# --------------------------------------------------------------------------- #


class Preset(BaseModel):
    c2r_pause_after: Optional[int] = Field(
        default=None,
        ge=0,
        description="Pause client->receiver after this many bytes were "
        "forwarded. null/omitted disables it for new connections.",
    )
    r2c_pause_after: Optional[int] = Field(
        default=None,
        ge=0,
        description="Pause receiver->client after this many bytes were "
        "forwarded. null/omitted disables it for new connections.",
    )


preset = Preset()


# --------------------------------------------------------------------------- #
# Per-connection / per-direction state
# --------------------------------------------------------------------------- #


class DirState:
    """Bookkeeping for one direction of one connection."""

    def __init__(self, direction: Direction, pause_after: Optional[int]):
        self.direction: Direction = direction
        # Snapshot of the preset at connection creation time.
        self.pause_after: Optional[int] = pause_after
        self.forwarded: int = 0
        self.pending: bytes = b""  # unsent tail held while paused
        self.fin_pending: bool = False  # local peer half-closed, FIN held back
        self.eof_seen: bool = False
        self.finished: bool = False  # half-close delivered to remote peer
        self.closed: bool = False  # remote side fully closed (read returned)
        self.paused: bool = False
        self.auto_triggered: bool = False
        self.resume_event: asyncio.Event = asyncio.Event()
        self.paused_since: Optional[float] = None

    def snapshot(self) -> dict:
        return {
            "direction": self.direction,
            "pause_after": self.pause_after,
            "forwarded_bytes": self.forwarded,
            "pending_bytes": len(self.pending),
            "paused": self.paused,
            "auto_triggered": self.auto_triggered,
            "fin_pending": self.fin_pending,
            "eof_seen": self.eof_seen,
            "half_closed_delivered": self.finished,
            "peer_closed": self.closed,
            "paused_since": self.paused_since,
        }


class Conn:
    """One client connection and (once established) its target connection.

    ``state`` is one of:
      * ``connecting`` - client accepted, target connect still in flight;
      * ``connected``  - target socket up, both pump tasks running;
      * ``closing``    - close/cleanup in progress (control API waits here);
      * ``closed``     - cleanup finished and the record left the active set.
    """

    def __init__(
        self,
        conn_id: str,
        peer: tuple[str, int],
        created_at: Optional[float] = None,
    ):
        self.id = conn_id
        self.peer = peer
        self.created_at = created_at if created_at is not None else _now()
        self.state: str = "connecting"
        self.closed = False
        self.closed_at: Optional[float] = None
        # Technical failure (e.g. target connect error); kept forever.
        self.error: Optional[str] = None
        # Why the connection was closed (force-close / connect failure / EOF).
        self.close_reason: Optional[str] = None
        self.connected_at: Optional[float] = None

        self.client_reader: Optional[asyncio.StreamReader] = None
        self.client_writer: Optional[asyncio.StreamWriter] = None
        self.remote_reader: Optional[asyncio.StreamReader] = None
        self.remote_writer: Optional[asyncio.StreamWriter] = None

        # Task that resolves the target connection and (on success) starts the
        # two pump tasks.  Tracked separately from the connector task below.
        self.connect_task: Optional[asyncio.Task] = None
        # The asyncio task running asyncio.open_connection().  It may complete
        # while connect_task is being cancelled; cleanup reaps its result so a
        # freshly created target socket can never be leaked.
        self.connector_task: Optional[asyncio.Task] = None

        self.tasks: Dict[Direction, Optional[asyncio.Task]] = {
            "c2r": None,
            "r2c": None,
        }
        self.dir: Dict[Direction, DirState] = {
            "c2r": DirState("c2r", preset.c2r_pause_after),
            "r2c": DirState("r2c", preset.r2c_pause_after),
        }
        self.done: asyncio.Event = asyncio.Event()
        self._lock: asyncio.Lock = asyncio.Lock()
        self._retired: bool = False

    # -- lifecycle --------------------------------------------------------- #

    @property
    def is_closing(self) -> bool:
        return self.state in ("closing", "closed")

    def begin_close(self, reason: Optional[str]) -> None:
        """Synchronously mark the connection as closing.

        This is intentionally free of ``await``: once a control API call has
        decided to close the connection, the connect task can no longer start
        pumps or transition to ``connected`` when it resumes.
        """
        if self.state == "closed":
            return
        if self.close_reason is None:
            self.close_reason = reason
        self.state = "closing"
        self.closed = True
        # Wake any paused direction; its pump exits because conn.closed.
        for d in DIRECTIONS:
            self.dir[d].resume_event.set()

    async def finish_close(self, reason: Optional[str] = None) -> None:
        """Cancel everything, close both sockets and retire the record.

        Safe to call from several tasks (control API, supervisor, shutdown):
        the first caller does the work, later callers wait for the lock and
        return once the connection is retired.
        """
        async with self._lock:
            if self._retired:
                return
            if self.close_reason is None:
                self.close_reason = reason
            self.begin_close(reason)

            current = asyncio.current_task()
            tasks: list[asyncio.Task] = []
            connect_task = self.connect_task
            if connect_task is not None and connect_task is not current:
                if not connect_task.done():
                    connect_task.cancel()
                tasks.append(connect_task)
            for t in self.tasks.values():
                if t is not None and t is not current:
                    if not t.done():
                        t.cancel()
                    tasks.append(t)

            # abort() makes blocked read()/drain() raise immediately, so the
            # tasks above observe cancellation without extra timeouts.
            for writer in (self.client_writer, self.remote_writer):
                if writer is not None:
                    with suppress(Exception):
                        writer.transport.abort()

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            # If the target connect completed in the cancellation window its
            # socket is attached to connector_task but not to remote_writer;
            # close it explicitly (the requirement: no live target socket and
            # no forwarding for a force-closed connection).
            await self._reap_connector()

            await self._close_writer(self.client_writer)
            await self._close_writer(self.remote_writer)

            self.state = "closed"
            if self.closed_at is None:
                self.closed_at = _now()
            self._retired = True
            registry.retire(self)
            self.done.set()

    @staticmethod
    async def _close_writer(writer: Optional[asyncio.StreamWriter]) -> None:
        if writer is None:
            return
        with suppress(Exception):
            writer.close()
        with suppress(asyncio.TimeoutError, OSError, RuntimeError):
            await asyncio.wait_for(writer.wait_closed(), CLOSE_WAIT_TIMEOUT)

    async def _reap_connector(self) -> None:
        task = self.connector_task
        if task is None:
            return
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if not task.done() or task.cancelled():
            return
        try:
            result = task.result()
        except BaseException:
            return
        if not result:
            return
        _, writer = result
        if writer is not self.remote_writer:
            # Never attached by _establish: nobody else can close it.
            with suppress(Exception):
                writer.transport.abort()
            await self._close_writer(writer)

    async def open_target(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Connect to the fixed target, bounded by CONNECT_TIMEOUT (<=0: none)."""
        task = asyncio.ensure_future(
            _open_connection(TARGET_HOST, TARGET_PORT, limit=STREAM_LIMIT)
        )
        self.connector_task = task
        if CONNECT_TIMEOUT <= 0:
            return await task
        try:
            # shield: a force-close cancels this coroutine, but the connect
            # task itself is reaped by _reap_connector so its result (if any)
            # is closed rather than leaked.
            return await asyncio.wait_for(asyncio.shield(task), timeout=CONNECT_TIMEOUT)
        except asyncio.TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise

    def attach_remote(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bool:
        """Attach the target socket and start pumps unless already closing.

        Runs without any ``await`` between the state check and the task
        creation, so a concurrent force-close can never leave pumps running
        on a closed connection.
        """
        self.remote_reader = reader
        self.remote_writer = writer
        if self.state != "connecting":
            # The connection was force-closed while the connect was pending.
            # Remember the socket so the cleanup closes it, but never report
            # a completed connect nor start forwarding.
            return False
        self.connected_at = _now()
        self.state = "connected"
        self.tasks["c2r"] = asyncio.create_task(pump(self, "c2r"), name=f"{self.id}-c2r")
        self.tasks["r2c"] = asyncio.create_task(pump(self, "r2c"), name=f"{self.id}-r2c")
        return True

    # -- pause control ----------------------------------------------------- #

    def pause(self, direction: Direction) -> None:
        st = self.dir[direction]
        if st.paused or st.finished or st.closed:
            return
        st.paused = True
        st.paused_since = _now()
        st.resume_event.clear()

    def resume(self, direction: Direction) -> bool:
        st = self.dir[direction]
        if not st.paused:
            return False
        # Once resumed, the one-shot auto threshold must not fire again.
        st.pause_after = None
        st.paused = False
        st.paused_since = None
        st.resume_event.set()
        return True

    # -- snapshots --------------------------------------------------------- #

    def connect_state(self) -> str:
        if self.connected_at is not None:
            return "connected"
        if self.state == "connecting":
            return "connecting"
        if self.error:
            return "failed"
        return "aborted"

    def snapshot(self) -> dict:
        connect: dict = {
            "state": self.connect_state(),
            "target": f"{TARGET_HOST}:{TARGET_PORT}",
            "started_at": self.created_at,
            "connected_at": self.connected_at,
            "duration_seconds": (
                round(self.connected_at - self.created_at, 6)
                if self.connected_at is not None
                else None
            ),
            "timeout_seconds": CONNECT_TIMEOUT if CONNECT_TIMEOUT > 0 else None,
            "error": self.error,
        }
        return {
            "id": self.id,
            "peer": f"{self.peer[0]}:{self.peer[1]}",
            "state": self.state,
            "connect": connect,
            "created_at": self.created_at,
            "closed": self.closed,
            "closed_at": self.closed_at,
            "close_reason": self.close_reason,
            "error": self.error,
            "directions": {d: self.dir[d].snapshot() for d in DIRECTIONS},
        }


class Registry:
    def __init__(self) -> None:
        self.active: Dict[str, Conn] = {}
        self.closed: Deque[dict] = deque(maxlen=MAX_CLOSED_RECORDS)

    def add(self, conn: Conn) -> None:
        self.active[conn.id] = conn

    def get_active(self, conn_id: str) -> Conn:
        conn = self.active.get(conn_id)
        if conn is None:
            raise HTTPException(status_code=404, detail=f"unknown active connection {conn_id}")
        return conn

    def find_closed(self, conn_id: str) -> Optional[dict]:
        for record in self.closed:
            if record.get("id") == conn_id:
                return record
        return None

    def retire(self, conn: Conn) -> None:
        self.active.pop(conn.id, None)
        self.closed.appendleft(conn.snapshot())

    def list(self) -> list[dict]:
        # Newest first: active connections, then closed records.
        return [c.snapshot() for c in self.active.values()] + list(self.closed)

    def connecting_count(self) -> int:
        return sum(1 for c in self.active.values() if c.state == "connecting")


registry = Registry()


# --------------------------------------------------------------------------- #
# Pump: one direction of one connection
# --------------------------------------------------------------------------- #


async def _wait_resume(st: DirState, conn: Conn) -> None:
    """Block while the direction is paused or while the connection dies."""
    while st.paused and not conn.closed:
        try:
            await asyncio.wait_for(st.resume_event.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            continue


async def pump(conn: Conn, direction: Direction) -> None:
    """Forward exactly one direction, honouring the pause/backlog rules."""
    if direction == "c2r":
        reader, writer = conn.client_reader, conn.remote_writer
    else:
        reader, writer = conn.remote_reader, conn.client_writer
    st = conn.dir[direction]

    try:
        # A force-close may have been decided after the target socket was
        # created but before this task's first step ran.  Never forward a
        # single byte for a connection that is no longer "connected".
        if conn.state != "connected" or conn.closed:
            return
        while True:
            # While paused we do NOT touch the socket at all: no reads (which
            # would buffer more user-space data) and no FIN delivery.  The
            # kernel receive buffer fills and TCP backpressure reaches the
            # sender; the held backlog is at most one split read tail.
            if st.paused:
                await _wait_resume(st, conn)
                if conn.closed:
                    return

            # Flush the backlog that was held when the threshold tripped,
            # before reading anything new from the socket.
            if st.pending:
                data = st.pending
                st.pending = b""
                writer.write(data)
                # drain() raises ConnectionReset/aborted when force-close
                # aborts the transport, which cancels this task cleanly.
                await writer.drain()
                st.forwarded += len(data)
                if st.fin_pending:
                    _deliver_fin(writer, st)
                    return

            if st.eof_seen:
                # EOF arrived while paused; backlog (if any) is now flushed.
                if st.fin_pending:
                    _deliver_fin(writer, st)
                return

            data = await reader.read(CHUNK)
            if data == b"":
                st.eof_seen = True
                st.closed = True
                if st.paused:
                    # FIN must not cross the pause; wait, then flush backlog.
                    await _wait_resume(st, conn)
                    if conn.closed:
                        return
                    if st.pending:
                        tail = st.pending
                        st.pending = b""
                        writer.write(tail)
                        await writer.drain()
                        st.forwarded += len(tail)
                _deliver_fin(writer, st)
                return

            # Threshold handling: split the chunk so we stop exactly at N,
            # even across read-block boundaries.
            threshold = st.pause_after
            if (
                not st.auto_triggered
                and threshold is not None
                and st.forwarded + len(data) >= threshold
            ):
                cut = threshold - st.forwarded
                head, tail = data[:cut], data[cut:]
                writer.write(head)
                await writer.drain()
                st.forwarded += len(head)
                st.auto_triggered = True
                if tail:
                    # Held backlog: small, never spooled to disk.
                    st.pending = tail
                st.paused = True
                st.paused_since = _now()
                st.resume_event.clear()
                continue

            writer.write(data)
            await writer.drain()
            st.forwarded += len(data)
    except (asyncio.CancelledError, ConnectionError, RuntimeError):
        # Force-close (transport abort / task cancel) or peer reset: the
        # supervisor performs the cleanup.
        raise
    except Exception as exc:  # pragma: no cover - defensive
        conn.error = f"{direction} pump error: {exc!r}"
        raise


def _deliver_fin(writer: asyncio.StreamWriter, st: DirState) -> None:
    """Half-close the remote write side after the backlog is fully sent."""
    transport = writer.transport
    if transport.can_write_eof():
        writer.write_eof()
    st.fin_pending = False
    st.finished = True


# --------------------------------------------------------------------------- #
# Connection lifecycle
# --------------------------------------------------------------------------- #


async def _establish(conn: Conn) -> None:
    """Resolve the target connection, then atomically hand over to pumps."""
    try:
        remote_reader, remote_writer = await conn.open_target()
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        conn.error = f"connect target {TARGET_HOST}:{TARGET_PORT} failed: {exc!r}"
        raise
    # No await below until the pumps are created: force-close either happens
    # before this point (state != connecting, no pumps) or after (pumps get
    # cancelled by the force-close cleanup).
    if not conn.attach_remote(remote_reader, remote_writer):
        return


async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    conn_id = uuid.uuid4().hex[:12]
    peer = client_writer.get_extra_info("peername") or ("?", 0)
    conn = Conn(conn_id, peer)
    conn.client_reader = client_reader
    conn.client_writer = client_writer
    # Register before the target connect: from this instant the connection is
    # queryable and force-closable through the control API.
    registry.add(conn)

    conn.connect_task = asyncio.create_task(
        _establish(conn), name=f"{conn_id}-connect"
    )
    try:
        await conn.connect_task
    except asyncio.CancelledError:
        if conn.is_closing:
            # Force-close / shutdown owns the cleanup (it is waiting for this
            # task); returning lets it proceed.  Calling finish_close here
            # would deadlock on the per-connection lock.
            return
        # This supervisor itself was cancelled.
        with suppress(asyncio.CancelledError):
            await asyncio.shield(conn.finish_close(reason="supervisor cancelled"))
        raise
    except Exception:
        pass  # conn.error carries the reason

    if conn.state == "connecting":
        # Target connect failed or was aborted without a force-close.
        await conn.finish_close(
            reason="target connect failed" if conn.error else "target connect aborted"
        )
        return

    if conn.state != "connected":
        # A force-close won the race while the target was being connected.
        # It owns the cleanup; wait until the record has been retired.  The
        # timeout is defensive: if that cleanup were ever cancelled, take
        # over instead of leaving this supervisor alive forever.
        try:
            await asyncio.wait_for(conn.done.wait(), timeout=FORCE_CLOSE_TIMEOUT + 5)
        except asyncio.TimeoutError:  # pragma: no cover - defensive
            await conn.finish_close(conn.close_reason or "cleanup takeover")
        return

    tasks = [t for t in conn.tasks.values() if t is not None]
    try:
        # The connection lives until both pump tasks have exited (normal
        # half-close completion, error, or force-close cancellation).
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        with suppress(asyncio.CancelledError):
            await conn.finish_close(reason="connection closed")


# --------------------------------------------------------------------------- #
# Control API
# --------------------------------------------------------------------------- #


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.server = await asyncio.start_server(
        handle_client,
        LISTEN_HOST,
        LISTEN_PORT,
        limit=STREAM_LIMIT,
        backlog=LISTEN_BACKLOG,
    )
    try:
        yield
    finally:
        app.state.server.close()
        await app.state.server.wait_closed()
        # Close every active connection (including ones still connecting) and
        # wait for the cleanup, so the process exits without half-open
        # sockets or pending target connects.
        active = list(registry.active.values())
        for conn in active:
            conn.begin_close("proxy shutting down")
        if active:
            await asyncio.gather(
                *(conn.finish_close("proxy shutting down") for conn in active),
                return_exceptions=True,
            )


app = FastAPI(
    title="TCP 故障代理 (TCP Fault Proxy)",
    description=(
        "单进程 TCP 故障代理：固定转发至本地接收端。客户端接入后立即可在 "
        "/connections 查到（含建连中状态），可预设新连接每个方向在累计转发 "
        "N 字节后暂停，按连接恢复或强断。详见各接口说明。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


class PresetResponse(BaseModel):
    preset: Preset


class ConnListResponse(BaseModel):
    active_count: int
    connecting_count: int
    connections: list[dict]


class PauseBody(BaseModel):
    direction: Direction = Field(
        ..., description="c2r = 客户端→接收端（上传方向），r2c = 接收端→客户端（回执方向）"
    )


class ResumeBody(BaseModel):
    direction: Optional[Direction] = Field(
        default=None,
        description="要恢复的方向；省略或 null 表示恢复该连接两个方向中所有暂停的方向。",
    )


class OpResponse(BaseModel):
    ok: bool
    connection: dict


@app.get("/health", tags=["control"])
async def health() -> dict:
    return {
        "status": "ok",
        "listen": f"{LISTEN_HOST}:{LISTEN_PORT}",
        "target": f"{TARGET_HOST}:{TARGET_PORT}",
        "active_connections": len(registry.active),
        "connecting_connections": registry.connecting_count(),
        "connect_timeout_seconds": CONNECT_TIMEOUT,
        "stream_limit_bytes": STREAM_LIMIT,
        "chunk_bytes": CHUNK,
    }


@app.get("/preset", response_model=PresetResponse, tags=["control"])
async def get_preset() -> PresetResponse:
    """查看当前预设：对之后新建连接生效（不影响已存在的连接）。"""
    return PresetResponse(preset=preset)


@app.put("/preset", response_model=PresetResponse, tags=["control"])
async def set_preset(body: Preset) -> PresetResponse:
    """预设新连接各方向累计转发多少字节后自动暂停；传 null 取消。"""
    preset.c2r_pause_after = body.c2r_pause_after
    preset.r2c_pause_after = body.r2c_pause_after
    return PresetResponse(preset=preset)


@app.get("/connections", response_model=ConnListResponse, tags=["control"])
async def list_connections() -> ConnListResponse:
    """列出全部连接（活动在前、已关闭在后），含建连状态与每方向字节计数。"""
    return ConnListResponse(
        active_count=len(registry.active),
        connecting_count=registry.connecting_count(),
        connections=registry.list(),
    )


@app.get("/connections/{conn_id}", tags=["control"])
async def get_connection(conn_id: str) -> dict:
    """查询单个连接；活动连接实时快照，已清理的记录也可查到。"""
    conn = registry.active.get(conn_id)
    if conn is not None:
        return conn.snapshot()
    record = registry.find_closed(conn_id)
    if record is not None:
        return record
    raise HTTPException(status_code=404, detail=f"unknown connection {conn_id}")


@app.post("/connections/{conn_id}/pause", response_model=OpResponse, tags=["control"])
async def pause_connection(conn_id: str, body: PauseBody) -> OpResponse:
    """手动暂停某连接的某个方向（除自动阈值外的测试手段）。"""
    conn = registry.get_active(conn_id)
    conn.pause(body.direction)
    return OpResponse(ok=True, connection=conn.snapshot())


@app.post("/connections/{conn_id}/resume", response_model=OpResponse, tags=["control"])
async def resume_connection(conn_id: str, body: ResumeBody) -> OpResponse:
    """恢复某连接的指定方向；不传 direction 则恢复该连接全部暂停方向。

    恢复后积压字节按原顺序继续转发，不丢、不重、不乱序；若 FIN 被暂扣，
    会在本方向积压发完后再向对端半关闭。
    """
    conn = registry.get_active(conn_id)
    targets = (body.direction,) if body.direction else DIRECTIONS
    for d in targets:
        conn.resume(d)
    return OpResponse(ok=True, connection=conn.snapshot())


@app.post("/connections/{conn_id}/force-close", response_model=OpResponse, tags=["control"])
async def force_close(conn_id: str) -> OpResponse:
    """强断连接：建连中或已连通都可用，清理完成后才返回。

    * 建连中：取消目标建连、关闭客户端 socket；若目标套接字刚在此刻建立，
      也会被关闭，不会启动转发。
    * 已连通：abort() 让卡在 drain() 的写和卡在 read() 的读立刻抛异常，
      两个 pump 任务随之结束，两端 socket 关闭、记录退出活动列表。
    """
    conn = registry.active.get(conn_id)
    if conn is None:
        record = registry.find_closed(conn_id)
        if record is not None:
            raise HTTPException(
                status_code=409,
                detail={"message": "connection already closed", "connection": record},
            )
        raise HTTPException(status_code=404, detail=f"unknown connection {conn_id}")

    conn.begin_close(FORCE_CLOSE_REASON)
    try:
        await asyncio.wait_for(
            conn.finish_close(FORCE_CLOSE_REASON), timeout=FORCE_CLOSE_TIMEOUT
        )
    except asyncio.TimeoutError:  # pragma: no cover - defensive
        raise HTTPException(status_code=500, detail="force-close did not finish in time")
    return OpResponse(ok=True, connection=conn.snapshot())
