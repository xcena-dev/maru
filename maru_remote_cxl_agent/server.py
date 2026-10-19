# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""TCP front end of the region map service.

One thread per Handler connection. A request runs the service call and gets
one reply; when the connection ends (the Handler closed it, its process died,
or TCP keepalive found the peer gone), the service drops the connection's
hold on every region.

With a key, a connection must prove it (``auth``) before it may map, unmap
or release; ``hello`` gives the connection's nonce, and the ``auth`` reply
proves the key back to the client.
"""

from __future__ import annotations

import hmac
import itertools
import logging
import secrets
import socket
import threading

from maru_remote.region_map import NONCE_BYTES, prove, recv_frame, send_frame

from .service import MapDenied, NotMapped, RegionMapService

logger = logging.getLogger(__name__)

# TCP keepalive: probe after 10 s idle, every 5 s, give up after 3 misses.
_KEEPALIVE = (10, 5, 3)


def _set_keepalive(sock: socket.socket) -> None:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    idle, interval, count = _KEEPALIVE
    for name, value in (
        ("TCP_KEEPIDLE", idle),
        ("TCP_KEEPINTVL", interval),
        ("TCP_KEEPCNT", count),
    ):
        option = getattr(socket, name, None)
        if option is not None:
            sock.setsockopt(socket.IPPROTO_TCP, option, value)


class ConnectionAuth:
    """Whether one connection proved the remote CXL agent key."""

    def __init__(self, key: bytes | None) -> None:
        """Start a connection.

        Args:
            key: Remote CXL agent key; None runs without one (every request is
                accepted).
        """
        self.key = key
        self.nonce = secrets.token_bytes(NONCE_BYTES) if key is not None else b""
        self.proved = key is None

    def check(self, message: dict) -> dict:
        """Check an ``auth`` request and build its reply."""
        if self.key is None:
            return {
                "ok": False,
                "error": "this remote CXL agent runs without a key",
                "code": "invalid",
            }
        proof, nonce = message.get("proof"), message.get("nonce")
        if (
            not isinstance(proof, bytes)
            or not isinstance(nonce, bytes)
            or not (hmac.compare_digest(proof, prove(self.key, "handler", self.nonce)))
        ):
            return {
                "ok": False,
                "error": "wrong remote CXL agent key",
                "code": "denied",
            }
        self.proved = True
        return {"ok": True, "proof": prove(self.key, "agent", nonce)}


def handle_request(
    service: RegionMapService,
    conn: int,
    message: dict,
    auth: ConnectionAuth | None = None,
) -> dict:
    """Run one request and build its reply.

    Args:
        service: The region map service.
        conn: Connection id.
        message: Decoded request.
        auth: Key state of the connection; None accepts every request.

    Returns:
        The reply map (``ok`` and the result, or ``error`` and ``code``).
    """
    auth = auth or ConnectionAuth(None)
    op = message.get("op")
    try:
        if op == "hello":
            nonce = {"nonce": auth.nonce} if auth.key is not None else {}
            return {"ok": True, **service.hello(), **nonce}
        if op == "auth":
            return auth.check(message)
        if not auth.proved:
            return {
                "ok": False,
                "error": "prove the remote CXL agent key first (auth)",
                "code": "denied",
            }
        if op == "map":
            return {"ok": True, **service.map(conn, message.get("handle"))}
        if op == "unmap":
            region_id = message.get("region_id")
            if type(region_id) is not int:
                raise ValueError("region_id must be an integer")
            service.unmap(conn, region_id)
            return {"ok": True}
        if op == "release":
            service.release(message.get("handle"))
            return {"ok": True, "released": True}
        return {"ok": False, "error": f"unknown op {op!r}", "code": "invalid"}
    except MapDenied as exc:
        return {"ok": False, "error": str(exc), "code": "denied"}
    except NotMapped as exc:
        return {"ok": False, "error": str(exc), "code": "unavailable"}
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "code": "invalid"}
    except Exception as exc:
        logger.warning("%s failed: %s", op, exc)
        return {"ok": False, "error": str(exc)[:1024], "code": "unavailable"}


def serve_connection(
    service: RegionMapService,
    conn: int,
    sock: socket.socket,
    key: bytes | None = None,
) -> None:
    """Serve one Handler connection until it ends, then drop its holds.

    Args:
        service: The region map service.
        conn: Connection id.
        sock: Connected socket.
        key: Remote CXL agent key the connection must prove; None for none.
    """
    auth = ConnectionAuth(key)
    try:
        while True:
            message = recv_frame(sock)
            if message is None:
                break
            send_frame(sock, handle_request(service, conn, message, auth))
    except (OSError, ValueError) as exc:
        logger.info("connection %d ended: %s", conn, exc)
    finally:
        try:
            sock.close()
        except OSError:
            pass
        service.drop(conn)


class RegionMapServer:
    """Accepts Handler connections for a :class:`RegionMapService`."""

    def __init__(
        self,
        service: RegionMapService,
        host: str,
        port: int,
        key: bytes | None = None,
    ) -> None:
        """Bind the listening socket.

        Args:
            service: The region map service.
            host: Bind address.
            port: Bind port (0 picks a free port).
            key: Remote CXL agent key every connection must prove; None accepts
                every connection (tests).
        """
        self.service = service
        self._key = key
        self._sock = socket.create_server((host, port), reuse_port=False)
        self.address = self._sock.getsockname()[:2]
        self._stop = threading.Event()
        self._ids = itertools.count(1)
        self._threads: list[threading.Thread] = []
        self._clients: dict[int, socket.socket] = {}
        self._clients_lock = threading.Lock()

    def serve_forever(self) -> None:
        """Accept connections until :meth:`stop`."""
        self._sock.settimeout(0.5)
        while not self._stop.is_set():
            try:
                client, peer = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._stop.is_set():
                    break
                raise
            client.settimeout(None)
            _set_keepalive(client)
            conn = next(self._ids)
            logger.info("connection %d from %s:%d", conn, *peer[:2])
            with self._clients_lock:
                self._clients[conn] = client
            thread = threading.Thread(
                target=self._serve,
                args=(conn, client),
                name=f"region-map-{conn}",
                daemon=True,
            )
            thread.start()
            self._threads = [t for t in self._threads if t.is_alive()] + [thread]

    def _serve(self, conn: int, client: socket.socket) -> None:
        try:
            serve_connection(self.service, conn, client, self._key)
        finally:
            with self._clients_lock:
                self._clients.pop(conn, None)

    def stop(self) -> None:
        """Stop accepting connections and end the open ones."""
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        with self._clients_lock:
            clients = list(self._clients.values())
        for client in clients:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        for thread in self._threads:
            thread.join(timeout=5)
