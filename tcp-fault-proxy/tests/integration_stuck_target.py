#!/usr/bin/env python3
"""Integration test against a *real* unestablished target connect.

The target is TEST-NET-1 (192.0.2.1), which is not routable from this host, so
``asyncio.open_connection`` genuinely hangs in the TCP handshake - exactly the
"device-access receiver connect is stuck" situation.  Optional: requires that
the address is blackholed (no route/refusal), otherwise it is skipped.

Modes (env ``STUCK_MODE``):
  * ``force``   (default): PROXY_CONNECT_TIMEOUT=20; the client connection must
    be visible as "connecting", force-close must return quickly and close the
    client; no live connection may remain;
  * ``timeout``: PROXY_CONNECT_TIMEOUT=2; no force-close; the proxy must close
    the client after the connect timeout and keep a closed record whose error
    field holds the reason.

Run:
  python -I tests/integration_stuck_target.py            # force-close path
  STUCK_MODE=timeout python -I tests/integration_stuck_target.py
"""

from __future__ import annotations

import asyncio
import http.client
import json
import os
import socket
import sys
import time

STUCK_TARGET = os.getenv("STUCK_TARGET", "192.0.2.1")
STUCK_MODE = os.getenv("STUCK_MODE", "force")
CONNECT_TIMEOUT = "2" if STUCK_MODE == "timeout" else "20"

PROXY_TCP_PORT = 0
PROXY_HTTP_PORT = 0


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


PROXY_TCP_PORT = free_port()
PROXY_HTTP_PORT = free_port()
os.environ.update(
    PROXY_HOST="127.0.0.1",
    PROXY_PORT=str(PROXY_TCP_PORT),
    PROXY_CONNECT_TIMEOUT=CONNECT_TIMEOUT,
    TARGET_HOST=STUCK_TARGET,
    TARGET_PORT="9100",
)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import proxy.app as proxy_app  # noqa: E402

import uvicorn  # noqa: E402

FAILED = 0


def check(name: str, ok: bool, detail: str = "") -> bool:
    global FAILED
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    if not ok:
        FAILED += 1
    return ok


def api(method: str, path: str, body: dict | None = None, timeout: float = 10.0):
    conn = http.client.HTTPConnection("127.0.0.1", PROXY_HTTP_PORT, timeout=timeout)
    payload = json.dumps(body).encode() if body is not None else None
    conn.request(method, path, body=payload,
                 headers={"Content-Type": "application/json"} if payload else {})
    resp = conn.getresponse()
    raw = resp.read().decode()
    conn.close()
    return resp.status, (json.loads(raw) if raw else {})


async def api_async(method: str, path: str, body: dict | None = None, timeout=10.0):
    return await asyncio.to_thread(api, method, path, body, timeout)


async def main() -> int:
    # Sanity: the target must really be blackholed (connect attempts hang).
    probe = socket.socket()
    probe.settimeout(0.5)
    try:
        probe.connect((STUCK_TARGET, 9100))
        print(f"SKIP: {STUCK_TARGET}:9100 is reachable; not a stuck-connect scenario")
        return 0
    except OSError:
        pass
    finally:
        probe.close()

    config = uvicorn.Config(proxy_app.app, host="127.0.0.1", port=PROXY_HTTP_PORT,
                            log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    serve_task = asyncio.create_task(server.serve())
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    check("proxy control API started", server.started)

    client = socket.create_connection(("127.0.0.1", PROXY_TCP_PORT))
    client.settimeout(5)
    conn = None
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        _, payload = await api_async("GET", "/connections")
        active = [c for c in payload["connections"] if not c["closed"]]
        if active:
            conn = active[0]
            break
        await asyncio.sleep(0.02)

    ok = check("client accepted connection is visible during real stuck connect",
               conn is not None)
    if ok:
        cid = conn["id"]
        check("state is connecting", conn["state"] == "connecting", conn["state"])
        check("connect state is connecting", conn["connect"]["state"] == "connecting")
        check("id is known before target connect completes", len(cid) == 12, cid)
        t0 = time.monotonic()
        status, health = await api_async("GET", "/health", timeout=1.0)
        check("control API responsive while connect is stuck",
              status == 200 and time.monotonic() - t0 < 0.5,
              f"{(time.monotonic() - t0) * 1000:.0f} ms")

        if STUCK_MODE == "force":
            t0 = time.monotonic()
            status, payload = await api_async(
                "POST", f"/connections/{cid}/force-close", {}, timeout=5.0
            )
            dt = time.monotonic() - t0
            check("force-close during stuck connect returns 200", status == 200, str(status))
            check("force-close during stuck connect is quick", dt < 1.0,
                  f"{dt * 1000:.0f} ms")
            check("record after force-close is closed",
                  payload["connection"]["closed"] is True)
            check("close reason recorded",
                  payload["connection"]["close_reason"] == proxy_app.FORCE_CLOSE_REASON)
            try:
                eof = client.recv(1) == b""
            except OSError:
                eof = True
            check("client socket closed by proxy", eof)
            check("no active connection remains",
                  all(c["id"] != cid for c in
                      (await api_async("GET", "/connections"))[1]["connections"]
                      if not c["closed"]))
            status, _ = await api_async("POST", f"/connections/{cid}/force-close", {}, 5.0)
            check("repeat force-close on closed id -> 409, no hang", status == 409, str(status))
        else:
            try:
                eof = await asyncio.wait_for(
                    asyncio.to_thread(client.recv, 1), timeout=5.0
                ) == b""
            except (OSError, asyncio.TimeoutError):
                eof = True
            check("client closed after connect timeout", eof)
            record = None
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                status, rec = await api_async("GET", f"/connections/{cid}")
                if status == 200 and rec["closed"]:
                    record = rec
                    break
                await asyncio.sleep(0.02)
            check("timed-out connect left a closed record", record is not None)
            if record is not None:
                check("record keeps the failure reason",
                      "connect target" in (record.get("error") or "")
                      and "TimeoutError" in (record.get("error") or ""),
                      str(record.get("error")))
                check("connect state is failed", record["connect"]["state"] == "failed")
            check("no active connection remains after timeout",
                  all(c["id"] != cid for c in
                      (await api_async("GET", "/connections"))[1]["connections"]
                      if not c["closed"]))

    _, health = await api_async("GET", "/health")
    check("connecting counter back to zero", health["connecting_connections"] == 0)
    client.close()

    server.should_exit = True
    await asyncio.wait_for(serve_task, timeout=10)
    print(f"\n{'FAILED' if FAILED else 'OK'}: mode={STUCK_MODE}")
    return 1 if FAILED else 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception as exc:
        import traceback

        traceback.print_exc()
        print(f"FATAL: {exc!r}")
        sys.exit(2)
