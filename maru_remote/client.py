# SPDX-License-Identifier: Apache-2.0
"""Control-channel client of a remote server: one ZMQ REQ socket plus the
NIXL peer bookkeeping a transfer needs.

The client never moves KV bytes itself; the remote storage backend in
``maru_handler.storage.remote`` does that with the transport this client
keeps pointed at the server's current NIXL metadata.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

import zmq

from . import protocol
from .transport import NixlTransport

logger = logging.getLogger(__name__)


class RemoteError(RuntimeError):
    """The server replied with ok=False, or the reply was malformed.

    Attributes:
        code: Machine-readable reason from the server (e.g. ``"POOL_FULL"``),
            or None.
    """

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.code = code


class RemoteTimeout(RemoteError):  # noqa: N818 (public name)
    """No reply arrived within the control timeout."""


class RemoteUnreachable(RemoteError):  # noqa: N818 (public name)
    """The control socket failed (connection refused, bad endpoint, ...)."""


class RemoteRestarted(RemoteError):  # noqa: N818 (public name)
    """A reply carried a different server generation than the one connected to."""


class RemoteClient:
    """Talk to one remote server.

    Control requests go over one ZMQ REQ socket, serialized by a lock so the
    client can be shared between threads. With a transport, :meth:`connect`
    loads the server's NIXL metadata and every reply that reports a newer
    ``md_version`` (the server mapped and registered another region) reloads
    it, so transfers always target regions the local agent knows about.
    Without a transport the client is control-only (existence checks).
    """

    def __init__(
        self,
        ctrl_url: str,
        transport: NixlTransport | None,
        *,
        client_id: str,
        timeout_ms: int = 5000,
    ) -> None:
        """Create the client; no request is sent until :meth:`connect`.

        Args:
            ctrl_url: ZMQ endpoint of the server (e.g. ``"tcp://h:6600"``).
            transport: Local NIXL transport, or None for a control-only client.
            client_id: Name reported to the server.
            timeout_ms: Reply deadline for each control request.
        """
        self.transport = transport
        self.client_id = client_id
        self.page_bytes = 0
        self.peer = ""
        self.md_version = -1
        self.generation = ""
        # Keys the server has evicted in its current run, as of the last reply.
        self.evictions = 0
        self.hello: dict[str, Any] = {}
        self._url = ctrl_url
        self._timeout_ms = timeout_ms
        self._ctx = zmq.Context.instance()
        self._sock: zmq.Socket | None = None
        self._lock = threading.Lock()
        self._md_lock = threading.Lock()

    def connect(self) -> dict[str, Any]:
        """(Re)connect: say hello, learn the layout and load NIXL metadata.

        A control-only client sends ``ping`` instead and only records the
        server generation. Calling it again after a restart reloads the peer.

        Returns:
            The ``hello`` reply (pool_id, generation, page_bytes, lifetimes,
            ...), or the ``ping`` reply for a control-only client.

        Raises:
            RemoteError: if the server rejects the request.
            RemoteTimeout: if the server does not answer in time.
            RemoteUnreachable: if the socket fails.
        """
        if self.transport is None:
            reply = self._call("ping", check_generation=False)
            self.generation = str(reply["generation"])
            return reply
        reply = self._call(
            "hello",
            expect=("page_bytes", "nixl_md", "md_version", "regions"),
            check_generation=False,
            client_id=self.client_id,
        )
        with self._md_lock:
            if self.peer and str(reply["generation"]) != self.generation:
                self._forget_peer()
            self.generation = str(reply["generation"])
            self.page_bytes = int(reply["page_bytes"])
            self.hello = reply
            self._apply_metadata(reply)
        return reply

    def reserve(self, sizes: list[int]) -> list[dict[str, Any]]:
        """Reserve one pool page per size for remote WRITE.

        Args:
            sizes: Bytes each page will hold (at most ``page_bytes``).

        Returns:
            One dict per page: ticket, region_id, base, offset, length.
        """
        reply = self._call(
            "reserve",
            expect=("pages", "md_version"),
            client_id=self.client_id,
            sizes=list(sizes),
        )
        self._refresh_if_stale(int(reply["md_version"]))
        return list(reply["pages"])

    def publish(self, entries: list[tuple[str, str]]) -> list[str]:
        """Publish written pages under keys; every ticket is consumed.

        Args:
            entries: ``(ticket, key)`` pairs.

        Returns:
            ``"CREATED"``, ``"ALREADY_PRESENT"`` or ``"REJECTED"`` per entry.
        """
        wire = [{"ticket": t, "key": k} for t, k in entries]
        reply = self._call("publish", expect=("statuses",), entries=wire)
        statuses = [str(s) for s in reply["statuses"]]
        if len(statuses) != len(entries):
            raise RemoteError("publish: reply has the wrong number of statuses")
        return statuses

    def abandon(self, tickets: list[str]) -> int:
        """Return reserved or quarantined pages the worker will not publish.

        Args:
            tickets: Reservation tickets from :meth:`reserve`.

        Returns:
            Number of pages freed (unknown tickets are skipped).
        """
        reply = self._call("abandon", expect=("freed",), tickets=list(tickets))
        return int(reply["freed"])

    def quarantine(self, tickets: list[str]) -> int:
        """Keep reserved pages from being reallocated until :meth:`abandon`.

        Args:
            tickets: Reservation tickets of a WRITE that missed its deadline.

        Returns:
            Number of reservations quarantined.
        """
        reply = self._call("quarantine", expect=("quarantined",), tickets=list(tickets))
        return int(reply["quarantined"])

    def exists(self, keys: list[str]) -> list[bool]:
        """Report which keys are stored.

        Args:
            keys: Keys to check.

        Returns:
            One bool per key.
        """
        reply = self._call("exists", expect=("found",), keys=list(keys))
        found = [bool(f) for f in reply["found"]]
        if len(found) != len(keys):
            raise RemoteError("exists: reply has the wrong number of results")
        return found

    def lookup(
        self, keys: list[str], ticket_id: str, *, protect: bool = True
    ) -> list[dict[str, Any] | None]:
        """Locate keys for remote READ, pinning them under ``ticket_id``.

        Args:
            keys: Keys to locate.
            ticket_id: Read-ticket name; release it with :meth:`release`.
            protect: Pin the found keys until release or ticket expiry.

        Returns:
            One entry (region_id, base, offset, length) or None per key.
        """
        reply = self._call(
            "lookup",
            expect=("entries", "md_version"),
            keys=list(keys),
            ticket_id=ticket_id,
            protect=protect,
        )
        self._refresh_if_stale(int(reply["md_version"]))
        entries: list[dict[str, Any] | None] = list(reply["entries"])
        if len(entries) != len(keys):
            raise RemoteError("lookup: reply has the wrong number of entries")
        return entries

    def release(self, ticket_id: str) -> int:
        """Unpin the keys of a read ticket.

        Args:
            ticket_id: Name passed to :meth:`lookup`.

        Returns:
            Number of keys unpinned (0 for an unknown ticket).
        """
        reply = self._call("release", expect=("released",), ticket_id=ticket_id)
        return int(reply["released"])

    def ping(self) -> float:
        """Probe liveness; returns the server clock value."""
        return float(self._call("ping", expect=("time",))["time"])

    def stats(self) -> dict[str, Any]:
        """Return the server's counters."""
        reply = self._call("stats")
        return {k: v for k, v in reply.items() if k not in ("ok", "op")}

    def close(self) -> None:
        """Close the control socket (the transport is owned by the caller)."""
        with self._lock:
            self._reset_socket()

    # ---- private -------------------------------------------------------------

    def _apply_metadata(self, reply: dict[str, Any]) -> None:
        """Load the server's NIXL metadata (caller holds the md lock).

        Args:
            reply: A ``hello`` or ``metadata`` reply.
        """
        assert self.transport is not None
        self.peer = self.transport.add_peer(bytes(reply["nixl_md"]))
        self.md_version = int(reply["md_version"])

    def _forget_peer(self) -> None:
        """Drop the peer of a previous server generation (md lock held)."""
        assert self.transport is not None
        try:
            self.transport.remove_peer(self.peer)
        except Exception as exc:  # a stale peer must not block the reconnect
            logger.warning("removing stale NIXL peer %s failed: %s", self.peer, exc)
        self.peer = ""
        self.md_version = -1

    def _refresh_if_stale(self, md_version: int) -> None:
        """Reload metadata when the server reports a newer ``md_version``.

        Args:
            md_version: Version carried by the latest reply.
        """
        if self.transport is None or md_version <= self.md_version:
            return
        with self._md_lock:
            if md_version <= self.md_version:  # another thread refreshed it
                return
            logger.info(
                "remote metadata v%d -> v%d; reloading", self.md_version, md_version
            )
            self._apply_metadata(
                self._call("metadata", expect=("nixl_md", "md_version", "regions"))
            )

    def _call(
        self,
        op: str,
        *,
        expect: tuple[str, ...] = (),
        check_generation: bool = True,
        **fields: Any,
    ) -> dict[str, Any]:
        """Send one request and wait for its reply.

        Args:
            op: Request name.
            expect: Reply fields the caller reads; a reply without one of
                them is an error rather than a ``KeyError``.
            check_generation: Send the generation this client connected to
                (the server refuses the request without executing it if it
                restarted since) and raise :class:`RemoteRestarted` when the
                reply comes from a different generation.
            **fields: Request fields.

        Returns:
            The decoded reply with ``ok`` True and every ``expect`` field.

        Raises:
            RemoteTimeout: if no reply arrives within the timeout; the REQ
                socket is replaced because its state machine is stuck.
            RemoteUnreachable: if the socket fails.
            RemoteRestarted: if the server restarted since :meth:`connect`.
            RemoteError: if the reply has ``ok`` False, cannot be decoded or
                lacks an expected field.
        """
        if check_generation and self.generation:
            fields["generation"] = self.generation
        with self._lock:
            try:
                sock = self._socket()
                sock.send(protocol.encode(op, **fields))
                if not sock.poll(self._timeout_ms, zmq.POLLIN):
                    self._reset_socket()
                    raise RemoteTimeout(
                        f"remote {op} timed out after {self._timeout_ms} ms"
                    )
                raw = sock.recv()
            except zmq.ZMQError as exc:
                self._reset_socket()
                raise RemoteUnreachable(f"remote {op}: socket error: {exc}") from exc
        try:
            reply = protocol.decode(raw)
        except ValueError as exc:
            raise RemoteError(f"remote {op}: {exc}") from exc
        generation = str(reply.get("generation", ""))
        if check_generation and self.generation and generation != self.generation:
            raise RemoteRestarted(
                f"remote {op}: server generation changed "
                f"({self.generation} -> {generation})"
            )
        if not reply.get("ok"):
            raise RemoteError(
                f"remote {op}: {reply.get('error', 'remote error')}", reply.get("code")
            )
        missing = [name for name in expect if name not in reply]
        if missing:
            raise RemoteError(f"remote {op}: reply lacks {', '.join(missing)}")
        evictions = reply.get("evictions")
        if isinstance(evictions, int) and not isinstance(evictions, bool):
            self.evictions = evictions
        return reply

    def _socket(self) -> zmq.Socket:
        """Return the REQ socket, connecting a new one if needed (lock held)."""
        if self._sock is None:
            sock = self._ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(self._url)
            self._sock = sock
        return self._sock

    def _reset_socket(self) -> None:
        """Close the REQ socket without lingering (lock held)."""
        if self._sock is not None:
            sock, self._sock = self._sock, None
            sock.close(linger=0)
