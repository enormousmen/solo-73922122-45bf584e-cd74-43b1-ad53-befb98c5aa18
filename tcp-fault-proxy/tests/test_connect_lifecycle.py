#!/usr/bin/env python3
"""Connect-phase lifecycle tests for the TCP fault proxy.

Covers the requirement "target connect gets stuck / races with force-close":

  A. a client connection accepted by the proxy is visible in /connections
     with state "connecting" *before* the target connect completes;
  B. force-close during a pending target connect returns only after cleanup,
     interrupts the client connection and cancels the pending connect;
  C. a force-close that races with a just-completed target connect closes the
     newly created target socket, starts no forwarding and reports a closed
     record (white-box + black-box);
  D. a target connect timeout/failure still leaves a closed record with the
     reason in the error field;
  E. a stuck connect does not hold up the control API or other connections;
  F. the normal path (pause at N, resume, half-close, reverse receipt) still
     works after these changes.

The proxy app is imported in-process with a stub target server; the control
API is served by uvicorn in the same event loop.  Stdlib + installed deps only.
"""

from __future__ import annotations

import asyncio
import hashlib
import http.client
import json
import os
import socket
import sys
import time
import uuid
from collections import deque
from contextlib import suppress

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((PASS if ok else FAIL, name, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# Ports must be known before importing the app (module-level configuration).
PROXY_TCP_PORT = free_port()
PROXY_HTTP_PORT = free_port()
STUB_TCP_PORT = free_port()

os.environ.update(
    PROXY_HOST="127.0.0.1",
    PROXY_PORT=str(PROXY_TCP_PORT),
    PROXY_CONNECT_TIMEOUT="1.5",
    TARGET_HOST="127.0.0.1",
    TARGET_PORT=str(STUB_TCP_PORT),
)

# Works under ``python -I`` (no implicit cwd/script-dir entry in sys.path).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import proxy.app as proxy_app  # noqa: E402  (env must be set first)

import uvicorn  # noqa: E402

REAL_OPEN = asyncio.open_connection


# --------------------------------------------------------------------------- #
# Stub target + fake connect dispatcher
# --------------------------------------------------------------------------- #

stub_stats = {"accepted": 0, "finished": 0, "receipt_conns": []}
stub_receipts: deque[dict] = deque(maxlen=100)

holder: dict = {
    "default": "real",
    "queue": deque(),
    "created": asyncio.Event(),
    "gate": None,
    "cancelled": 0,
    "sockets": [],
}


def arm(mode: str) -> None:
    holder["queue"].append(mode)
    holder["created"] = asyncio.Event()


async def fake_open(host, port, limit=proxy_app.STREAM_LIMIT):
    """Stand-in for asyncio.open_connection, per-connection scripted."""
    mode = holder["queue"].popleft() if holder["queue"] else holder["default"]
    if mode == "stuck":
        try:
            await asyncio.Event().wait()  # pending connect, cancellable
        except asyncio.CancelledError:
            holder["cancelled"] += 1
            raise
    if mode == "refuse":
        raise ConnectionRefusedError(111, "Connection refused (test)")
    if mode == "race":
        holder["created"].set()
        await holder["gate"].wait()
        reader, writer = await REAL_OPEN(host, port, limit=limit)
        holder["sockets"].append(writer)
        return reader, writer
    reader, writer = await REAL_OPEN(host, port, limit=limit)
    holder["sockets"].append(writer)
    return reader, writer


proxy_app._open_connection = fake_open


async def stub_handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    stub_stats["accepted"] += 1
    digest = hashlib.sha256()
    size = 0
    try:
        while True:
            data = await reader.read(65536)
            if data == b"":
                break
            size += len(data)
            digest.update(data)
        receipt = {"bytes": size, "sha256": digest.hexdigest(), "chunks": 1}
        stub_receipts.appendleft(receipt)
        stub_stats["receipt_conns"].append(receipt)
        payload = (json.dumps(receipt) + "\n").encode()
        writer.write(payload)
        await writer.drain()
        if writer.transport.can_write_eof():
            writer.write_eof()
        await asyncio.sleep(0.02)
    except (ConnectionError, OSError, RuntimeError):
        pass
    finally:
        stub_stats["finished"] += 1
        writer.close()
        with suppress(OSError, RuntimeError):
            await writer.wait_closed()


# --------------------------------------------------------------------------- #
# Control API helpers
# --------------------------------------------------------------------------- #


def _call(method: str, path: str, body: dict | None = None, timeout: float = 10.0):
    conn = http.client.HTTPConnection("127.0.0.1", PROXY_HTTP_PORT, timeout=timeout)
    payload = json.dumps(body).encode() if body is not None else None
    conn.request(
        method,
        path,
        body=payload,
        headers={"Content-Type": "application/json"} if payload else {},
    )
    resp = conn.getresponse()
    raw = resp.read().decode()
    conn.close()
    return resp.status, (json.loads(raw) if raw else {})


async def api(method: str, path: str, body: dict | None = None, timeout: float = 10.0):
    return await asyncio.to_thread(_call, method, path, body, timeout)


def conns_list(payload: dict) -> list[dict]:
    return payload.get("connections", [])


async def active_conns() -> list[dict]:
    _, payload = await api("GET", "/connections")
    return [c for c in conns_list(payload) if not c["closed"]]


async def find_conn(pred, timeout: float = 3.0) -> dict | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for c in await active_conns():
            if pred(c):
                return c
        await asyncio.sleep(0.02)
    return None


async def wait_until(cond, timeout: float = 3.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await cond():
            return True
        await asyncio.sleep(interval)
    return False


async def client_closed(reader: asyncio.StreamReader, timeout: float = 3.0) -> bool:
    try:
        data = await asyncio.wait_for(reader.read(), timeout)
        return data == b""
    except (ConnectionError, OSError):
        return True
    except asyncio.TimeoutError:
        return False


# --------------------------------------------------------------------------- #
# Scenarios
# --------------------------------------------------------------------------- #


async def scenario_stuck_connect() -> None:
    print("\n-- A/B/E: stuck target connect is visible, force-closable, isolated --")
    holder["default"] = "real"
    arm("stuck")
    client_reader, client_writer = await REAL_OPEN("127.0.0.1", PROXY_TCP_PORT)

    conn = await find_conn(lambda c: c["state"] == "connecting", timeout=2.0)
    check("accepted connection visible while target connect pending", conn is not None)
    if conn is None:
        client_writer.close()
        return
    cid = conn["id"]
    check("connection id available during connect", len(cid) == 12, cid)
    check("state is connecting", conn["state"] == "connecting")
    check("connect sub-state is connecting", conn["connect"]["state"] == "connecting",
          str(conn.get("connect")))
    check("per-direction state already present",
          set(conn["directions"]) == {"c2r", "r2c"})

    status, payload = await api("GET", f"/connections/{cid}")
    check("single-connection GET works during connect", status == 200 and payload["id"] == cid)

    t0 = time.monotonic()
    status, payload = await api("POST", f"/connections/{cid}/pause", {"direction": "c2r"})
    dt = time.monotonic() - t0
    check("control API responsive while connect pending", status == 200 and dt < 0.5,
          f"{dt * 1000:.0f} ms")
    check("pause accepted during connect",
          payload["connection"]["directions"]["c2r"]["paused"] is True)

    # A second, normal connection must not be held up by the stuck one.
    t0 = time.monotonic()
    _, p = await api("PUT", "/preset", {"c2r_pause_after": 100_000})
    client2_r, client2_w = await REAL_OPEN("127.0.0.1", PROXY_TCP_PORT)
    conn2 = await find_conn(
        lambda c: c["id"] != cid and c["state"] == "connected", timeout=2.0
    )
    check("second connection establishes while first connect is stuck",
          conn2 is not None, str(conn2 and conn2["state"]))
    dt = time.monotonic() - t0
    await api("PUT", "/preset", {"c2r_pause_after": None})
    check("second connection not blocked by stuck connect", conn2 is not None and dt < 2.0,
          f"{dt * 1000:.0f} ms")

    # Force-close the stuck connection.
    t0 = time.monotonic()
    status, payload = await api("POST", f"/connections/{cid}/force-close", timeout=5.0)
    dt = time.monotonic() - t0
    check("force-close during pending connect returns 200", status == 200, str(status))
    check("force-close during pending connect completes quickly", dt < 2.0,
          f"{dt * 1000:.0f} ms")
    check("force-close response is the closed record",
          payload.get("connection", {}).get("closed") is True,
          str(payload.get("connection", {}).get("state")))
    check("force-close reason recorded",
          payload.get("connection", {}).get("close_reason") == proxy_app.FORCE_CLOSE_REASON)
    check("client connection is closed by proxy", await client_closed(client_reader))
    check("pending target connect was cancelled", holder["cancelled"] >= 1,
          f"cancelled={holder['cancelled']}")
    check("no active connection with that id",
          all(c["id"] != cid for c in await active_conns()))
    status, record = await api("GET", f"/connections/{cid}")
    check("closed record still queryable", status == 200 and record["closed"] is True)
    check("closed record connect state is aborted",
          record["connect"]["state"] == "aborted", str(record["connect"]["state"]))

    status, _ = await api("POST", f"/connections/{cid}/force-close", timeout=5.0)
    check("second force-close on closed id answers 409 (no hang)", status == 409, str(status))

    # Concurrent force-close on the same pending connect must never hang and
    # must leave exactly one closed record.
    arm("stuck")
    reader3, writer3 = await REAL_OPEN("127.0.0.1", PROXY_TCP_PORT)
    conn3 = await find_conn(lambda c: c["state"] == "connecting", timeout=2.0)
    if conn3 is not None:
        id3 = conn3["id"]
        t0 = time.monotonic()
        results3 = await asyncio.gather(
            *(
                asyncio.to_thread(
                    _call, "POST", f"/connections/{id3}/force-close", None, 5.0
                )
                for _ in range(3)
            ),
            return_exceptions=True,
        )
        dt = time.monotonic() - t0
        statuses = sorted(
            r[0] if isinstance(r, tuple) else type(r).__name__ for r in results3
        )
        check("concurrent force-close requests all answer",
              all(isinstance(r, tuple) for r in results3), str(statuses))
        check("concurrent force-close completes quickly", dt < 2.0, f"{dt * 1000:.0f} ms")
        check("concurrent force-close never hangs (200/409 only)",
              set(statuses) <= {200, 409}, str(statuses))
        closed_records = [
            c for c in conns_list((await api("GET", "/connections"))[1])
            if c["id"] == id3 and c["closed"]
        ]
        check("exactly one closed record after concurrent force-close",
              len(closed_records) == 1, f"records={len(closed_records)}")
        check("concurrent force-close did not start forwarding",
              closed_records and closed_records[0]["connect"]["state"] == "aborted")
        check("concurrent force-close closed the client", await client_closed(reader3))
    else:
        check("stuck connection visible for concurrent force-close", False)
    writer3.close()

    # Tidy up the second connection (it may still be paused at 100000 bytes).
    if conn2 is not None:
        await api("POST", f"/connections/{conn2['id']}/force-close", timeout=5.0)
    client2_w.close()
    with suppress(OSError, RuntimeError):
        await client2_w.wait_closed()
    holder["default"] = "real"


async def scenario_connect_timeout() -> None:
    print("\n-- D: target connect timeout leaves a closed record with reason --")
    arm("stuck")
    client_reader, client_writer = await REAL_OPEN("127.0.0.1", PROXY_TCP_PORT)
    conn = await find_conn(lambda c: c["state"] == "connecting", timeout=2.0)
    check("stuck connection appears before timeout", conn is not None)
    if conn is None:
        client_writer.close()
        return
    cid = conn["id"]
    closed = await client_closed(client_reader, timeout=4.0)
    check("client closed after connect timeout", closed)
    record = None
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        status, rec = await api("GET", f"/connections/{cid}")
        if status == 200 and rec["closed"]:
            record = rec
            break
        await asyncio.sleep(0.03)
    check("timed-out connect retired", record is not None)
    if record is not None:
        check("connect failure reason preserved",
              "connect target" in (record.get("error") or "")
              and "TimeoutError" in (record.get("error") or ""),
              str(record.get("error")))
        check("connect state failed", record["connect"]["state"] == "failed",
              str(record["connect"]["state"]))
        check("failure close_reason recorded",
              record.get("close_reason") == "target connect failed",
              str(record.get("close_reason")))
    client_writer.close()


async def scenario_connect_refused() -> None:
    print("\n-- D: target connect failure leaves a closed record with reason --")
    arm("refuse")
    before = {c["id"] for c in conns_list((await api("GET", "/connections"))[1])}
    client_reader, client_writer = await REAL_OPEN("127.0.0.1", PROXY_TCP_PORT)
    check("client closed after refused target connect",
          await client_closed(client_reader, timeout=3.0))
    record = None
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        _, payload = await api("GET", "/connections")
        candidates = [c for c in conns_list(payload)
                      if c["id"] not in before and c["closed"]]
        if candidates:
            record = candidates[0]
            break
        await asyncio.sleep(0.03)
    check("connect-failure record exists", record is not None)
    if record is not None:
        check("failure record carries the reason",
              "connect target" in (record.get("error") or "")
              and "ConnectionRefused" in (record.get("error") or ""),
              str(record.get("error")))
        check("failed connection is not active",
              all(c["id"] != record["id"] for c in await active_conns()))
        check("refused connect close_reason recorded",
              record.get("close_reason") == "target connect failed",
              str(record.get("close_reason")))
    client_writer.close()
    with suppress(OSError, RuntimeError):
        await client_writer.wait_closed()


async def scenario_race_target_socket() -> None:
    print("\n-- C: force-close racing a completing target connect closes the socket --")
    for attempt in range(3):
        holder["gate"] = asyncio.Event()
        arm("race")
        client_reader, client_writer = await REAL_OPEN("127.0.0.1", PROXY_TCP_PORT)
        await asyncio.wait_for(holder["created"].wait(), timeout=3.0)
        conn = await find_conn(lambda c: c["state"] == "connecting", timeout=2.0)
        if conn is None:
            check(f"race attempt {attempt}: connection visible", False)
            client_writer.close()
            continue
        cid = conn["id"]
        # Let the target socket be created while the force-close request is
        # already in flight; whoever wins, cleanup must not leak the socket.
        asyncio.get_running_loop().call_later(0.002 * attempt, holder["gate"].set)
        status, payload = await api("POST", f"/connections/{cid}/force-close", timeout=5.0)
        check(f"race attempt {attempt}: force-close ok", status == 200, str(status))
        check(f"race attempt {attempt}: client closed", await client_closed(client_reader))
        after = await active_conns()
        check(f"race attempt {attempt}: no active connection left",
              all(c["id"] != cid for c in after))
        # The target socket created during the race must be closed: the stub
        # must have seen its accepted connection finish (EOF/reset).
        finished = await wait_until(
            lambda: asyncio.sleep(0, result=stub_stats["accepted"] == stub_stats["finished"]),
            timeout=3.0,
        )
        check(f"race attempt {attempt}: target socket closed",
              finished, f"accepted={stub_stats['accepted']} finished={stub_stats['finished']}")
        client_writer.close()
        with suppress(OSError, RuntimeError):
            await client_writer.wait_closed()

    # White-box: connector result exists but was never attached to the conn;
    # finish_close must reap and close it, and must not start pump tasks.
    remote_reader, remote_writer = await REAL_OPEN("127.0.0.1", STUB_TCP_PORT)
    client_side_reader, client_side_writer = await REAL_OPEN("127.0.0.1", STUB_TCP_PORT)
    conn = proxy_app.Conn("wb-race", ("127.0.0.1", 1))
    conn.state = "connecting"
    conn.client_reader, conn.client_writer = client_side_reader, client_side_writer
    proxy_app.registry.add(conn)
    accepted_before = stub_stats["accepted"]

    async def returns_socket():
        return remote_reader, remote_writer

    conn.connector_task = asyncio.create_task(returns_socket())
    await asyncio.sleep(0)  # connector done, result not attached anywhere
    conn.begin_close(proxy_app.FORCE_CLOSE_REASON)
    await conn.finish_close(proxy_app.FORCE_CLOSE_REASON)

    check("white-box: no pump tasks started",
          conn.tasks == {"c2r": None, "r2c": None}, str(conn.tasks))
    check("white-box: target socket is closed", remote_writer.transport.is_closing() is True)
    closed_seen = await wait_until(
        lambda: asyncio.sleep(
            0, result=stub_stats["finished"] >= accepted_before
        ),
        timeout=2.0,
    )
    check("white-box: stub observed target socket close", closed_seen,
          f"accepted={stub_stats['accepted']} finished={stub_stats['finished']}")
    check("white-box: record retired as closed",
          "wb-race" not in proxy_app.registry.active)
    snapshot = proxy_app.registry.find_closed("wb-race")
    check("white-box: record keeps force-close reason",
          snapshot is not None
          and snapshot["close_reason"] == proxy_app.FORCE_CLOSE_REASON,
          str(snapshot and snapshot.get("close_reason")))
    check("white-box: connect reported aborted, not connected",
          snapshot is not None and snapshot["connect"]["state"] == "aborted")
    for w in (client_side_writer, remote_writer):
        w.close()
        with suppress(OSError, RuntimeError):
            await w.wait_closed()


async def scenario_normal_path() -> None:
    print("\n-- F: normal path regression (pause/resume/half-close/receipt) --")
    holder["default"] = "real"
    await api("PUT", "/preset", {"c2r_pause_after": 300_000})
    payload = bytes((i * 31 + 7) & 0xFF for i in range(4096)) * 256  # 1 MiB
    expected = hashlib.sha256(payload).hexdigest()
    reader, writer = await REAL_OPEN("127.0.0.1", PROXY_TCP_PORT)
    conn = await find_conn(lambda c: c["state"] == "connected", timeout=2.0)
    check("normal connection reaches connected state", conn is not None)
    if conn is None:
        writer.close()
        return
    cid = conn["id"]
    writer.write(payload)
    await writer.drain()
    if writer.transport.can_write_eof():
        writer.write_eof()

    paused = None
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        _, snap = await api("GET", f"/connections/{cid}")
        st = snap["directions"]["c2r"]
        if st["paused"]:
            paused = st
            break
        await asyncio.sleep(0.02)
    check("auto pause triggered on normal path", paused is not None)
    if paused is not None:
        check("stopped exactly at preset N", paused["forwarded_bytes"] == 300_000,
              str(paused["forwarded_bytes"]))
        check("FIN held while paused", paused["half_closed_delivered"] is False)
        status, resp = await api("POST", f"/connections/{cid}/resume",
                                 {"direction": "c2r"})
        check("resume accepted", status == 200 and resp["ok"] is True)

    raw = await asyncio.wait_for(reader.read(), timeout=10.0)
    receipt = json.loads(raw.decode().strip().splitlines()[-1])
    check("normal path receipt byte count", receipt["bytes"] == len(payload),
          str(receipt["bytes"]))
    check("normal path receipt sha256", receipt["sha256"] == expected)
    check("client read side reached EOF", await client_closed(reader))
    writer.close()
    with suppress(OSError, RuntimeError):
        await writer.wait_closed()
    await api("PUT", "/preset", {"c2r_pause_after": None})


async def main() -> int:
    stub_server = await asyncio.start_server(stub_handle, "127.0.0.1", STUB_TCP_PORT)
    config = uvicorn.Config(
        proxy_app.app,
        host="127.0.0.1",
        port=PROXY_HTTP_PORT,
        log_level="warning",
        lifespan="on",
        timeout_keep_alive=5,
    )
    server = uvicorn.Server(config)
    serve_task = asyncio.create_task(server.serve())
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    if not server.started:
        print("FATAL: control API did not start")
        return 2
    check("control API started", True)

    try:
        await scenario_stuck_connect()
        await scenario_connect_timeout()
        await scenario_connect_refused()
        await scenario_race_target_socket()
        await scenario_normal_path()
        # Final isolation checks.
        check("no active connections left at end", await active_conns() == [],
              f"active={len(await active_conns())}")
        status, payload = await api("GET", "/health")
        check("health reports connecting counter",
              status == 200 and payload["connecting_connections"] == 0)

        async def stub_quiescent() -> bool:
            return stub_stats["accepted"] == stub_stats["finished"]

        check("all target sockets cleaned up",
              await wait_until(stub_quiescent, timeout=3.0),
              f"accepted={stub_stats['accepted']} finished={stub_stats['finished']}")
        outstanding = [w for w in holder["sockets"] if not w.transport.is_closing()]
        check("no live target sockets created by the proxy",
              outstanding == [], f"live={len(outstanding)}")
    finally:
        server.should_exit = True
        await asyncio.wait_for(serve_task, timeout=10)
        stub_server.close()
        await stub_server.wait_closed()

    failed = [r for r in results if r[0] == FAIL]
    print("\n== summary ==")
    for status, name, detail in results:
        print(f"  [{status}] {name}" + (f" -- {detail}" if detail else ""))
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception as exc:
        import traceback

        traceback.print_exc()
        print(f"FATAL: {exc!r}")
        sys.exit(2)
