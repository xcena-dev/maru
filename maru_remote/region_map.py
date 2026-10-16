# SPDX-License-Identifier: Apache-2.0
"""Region map protocol between a worker's Handler and a region mapper.

A region mapper runs on every memory node. It does for a remote Handler what
``DaxMapper`` does inside a Handler on a CXL-attached node: check a region's
handle with the resource manager (``GET_ACCESS``) and mmap the region, and
then register the mapping with the node's NIC so the Handler can reach it by
RDMA.

One TCP connection per Handler and region mapper; each message is a 4-byte
big-endian length followed by a MessagePack map. Requests carry ``op``:

- ``hello``: returns ``{"node", "epoch", "boot"}``; the epoch is new on
  every start of the region mapper process, ``boot`` on every boot of the
  memory node.
- ``map`` with ``handle`` (``MaruHandle.to_dict()``): returns
  ``{"region_id", "addr", "length", "metadata"}``. ``addr`` is the region
  mapper's virtual address of the region (VA_R); a Handler uses it only as
  the remote address of RDMA requests. ``metadata`` is NIXL metadata naming
  this region's registration only, with connection info.
- ``unmap`` with ``region_id``: the connection no longer needs the region.
- ``release`` with ``handle``: the region's owner is giving it back; the
  region mapper deregisters and unmaps it whoever else holds it, and refuses
  later maps of the handle. Releasing a region that is not mapped succeeds.

Replies carry ``ok``; a failure has ``error`` and ``code``. The region mapper
treats a closed connection as an unmap of every region the connection mapped.
"""

from __future__ import annotations

import logging
import socket
import struct
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import msgpack

logger = logging.getLogger(__name__)

MAX_FRAME = 16 * 1024 * 1024
_LEN = struct.Struct("!I")


class RegionMapError(RuntimeError):
    """The region mapper refused a request.

    Attributes:
        code: Short reason (``"denied"``, ``"unavailable"``, ``"invalid"``...).
    """

    def __init__(self, message: str, code: str = "error") -> None:
        super().__init__(message)
        self.code = code


class RegionMapUnavailable(ConnectionError):  # noqa: N818 (a ConnectionError)
    """The region mapper could not be reached or the connection broke."""


def send_frame(sock: socket.socket, message: dict) -> None:
    """Send one length-prefixed MessagePack message.

    Args:
        sock: Connected socket.
        message: Map to send.

    Raises:
        ValueError: if the encoded message exceeds :data:`MAX_FRAME`.
        OSError: if the socket fails.
    """
    data = msgpack.packb(message, use_bin_type=True)
    if len(data) > MAX_FRAME:
        raise ValueError(f"region map message of {len(data)} bytes is too large")
    sock.sendall(_LEN.pack(len(data)) + data)


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    chunks = []
    while n:
        chunk = sock.recv(n)
        if not chunk:
            return None
        chunks.append(chunk)
        n -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock: socket.socket) -> dict | None:
    """Receive one message; None when the peer closed the connection.

    Args:
        sock: Connected socket.

    Returns:
        The decoded map, or None at end of stream.

    Raises:
        ValueError: if the frame is too large or not a map.
        OSError: if the socket fails.
    """
    head = _recv_exact(sock, _LEN.size)
    if head is None:
        return None
    (length,) = _LEN.unpack(head)
    if length > MAX_FRAME:
        raise ValueError(f"region map frame of {length} bytes is too large")
    body = _recv_exact(sock, length)
    if body is None:
        return None
    message = msgpack.unpackb(body, raw=False)
    if not isinstance(message, dict):
        raise ValueError("region map message must be a map")
    return message


@dataclass(frozen=True)
class MapReply:
    """Where a mapped region is reachable.

    Attributes:
        region_id: Region number from the handle.
        addr: The region mapper's virtual address of the region (VA_R).
        length: Region length in bytes.
        metadata: NIXL metadata of the region mapper's agent naming this
            region's registration (its remote key) and the connection info.
        epoch: The region mapper process this mapping belongs to.
        boot: The boot of the memory node at the time of the mapping.
    """

    region_id: int
    addr: int
    length: int
    metadata: bytes
    epoch: str
    boot: str = ""


def parse_address(address: str) -> tuple[str, int]:
    """Split ``host:port``.

    Args:
        address: Region mapper address.

    Returns:
        ``(host, port)``.

    Raises:
        ValueError: if the address has no port.
    """
    host, sep, port = address.rpartition(":")
    if not sep or not host or not port.isdigit():
        raise ValueError(f"region mapper address must be host:port, got {address!r}")
    return host, int(port)


class RegionMapClient:
    """A Handler's connection to one region mapper.

    The connection is opened on first use. If it breaks, every mapping the
    region mapper held for this connection is gone with it, so the caller
    must forget the addresses it got and map again; :attr:`generation`
    counts reconnections so the caller can tell. A request that finds the
    connection broken opens it once more and is sent again.
    """

    def __init__(
        self,
        address: str,
        *,
        timeout_s: float = 30.0,
        connect_timeout_s: float | None = None,
        connect: Callable[[tuple[str, int], float], Any] | None = None,
    ) -> None:
        """Create the client.

        Args:
            address: ``host:port`` of the region mapper.
            timeout_s: Socket timeout for each reply (a first map registers
                a whole region with the NIC).
            connect_timeout_s: Timeout for connecting; ``timeout_s`` if None.
            connect: Socket factory ``(address, timeout) -> socket``
                (tests inject an in-process pair).
        """
        self.address = address
        self._endpoint = parse_address(address)
        self._timeout_s = timeout_s
        self._connect_timeout_s = (
            timeout_s if connect_timeout_s is None else connect_timeout_s
        )
        self._connect = connect or socket.create_connection
        self._sock: Any = None
        self._lock = threading.Lock()
        self.epoch = ""
        self.node = ""
        self.boot = ""
        self.generation = 0

    def hello(self) -> dict:
        """Connect if needed and return the region mapper's identity.

        Returns:
            ``{"node", "epoch", "boot"}``.

        Raises:
            RegionMapUnavailable: if the region mapper cannot be reached.
        """
        with self._lock:
            return self._send({"op": "hello"})

    def map(self, handle: dict) -> MapReply:
        """Ask the region mapper to map and register a region.

        Args:
            handle: ``MaruHandle.to_dict()`` of the region.

        Returns:
            Where the region is reachable.

        Raises:
            RegionMapError: if the region mapper refused (e.g. a wrong handle).
            RegionMapUnavailable: if the region mapper cannot be reached.
        """
        with self._lock:
            reply = self._send({"op": "map", "handle": dict(handle)})
            return MapReply(
                int(reply["region_id"]),
                int(reply["addr"]),
                int(reply["length"]),
                bytes(reply["metadata"]),
                self.epoch,
                self.boot,
            )

    def unmap(self, region_id: int) -> None:
        """Tell the region mapper this Handler no longer needs a region.

        A broken connection already released the region, so it is ignored.

        Args:
            region_id: Region number.

        Raises:
            RegionMapError: if the region mapper refused.
        """
        with self._lock:
            if self._sock is None:
                return
            try:
                self._request({"op": "unmap", "region_id": int(region_id)})
            except RegionMapUnavailable:
                pass

    def release(self, handle: dict) -> None:
        """Have the region mapper deregister a region this Handler owns.

        Sent before the region goes back to MaruServer. A region mapper that
        restarted holds no registration and answers that it is released.

        Args:
            handle: ``MaruHandle.to_dict()`` of the region.

        Raises:
            RegionMapError: if the region mapper refused (``"denied"``: it
                maps another region under this id; ``"unavailable"``: the
                deregistration failed and the region is still registered).
            RegionMapUnavailable: if no connection got a reply.
        """
        with self._lock:
            self._send({"op": "release", "handle": dict(handle)})

    def close(self) -> None:
        """Close the connection, which releases every mapping it holds."""
        with self._lock:
            self._drop()

    def _send(self, message: dict) -> dict:
        """Send a request; a connection found broken is opened once more."""
        reused = self._sock is not None
        try:
            self._ensure()
            return self._request(message)
        except RegionMapUnavailable:
            if not reused:
                raise
        self._ensure()
        return self._request(message)

    def _ensure(self) -> None:
        if self._sock is not None:
            return
        try:
            sock = self._connect(self._endpoint, self._connect_timeout_s)
        except OSError as exc:
            raise RegionMapUnavailable(
                f"region mapper {self.address} is unreachable: {exc}"
            ) from exc
        try:
            sock.settimeout(self._timeout_s)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except (OSError, AttributeError):
            pass
        self._sock = sock
        self.generation += 1
        reply = self._request({"op": "hello"})
        self.epoch = str(reply["epoch"])
        self.node = str(reply["node"])
        self.boot = str(reply.get("boot", ""))

    def _request(self, message: dict) -> dict:
        try:
            send_frame(self._sock, message)
            reply = recv_frame(self._sock)
        except (OSError, ValueError) as exc:
            self._drop()
            raise RegionMapUnavailable(
                f"region mapper {self.address} connection failed: {exc}"
            ) from exc
        if reply is None:
            self._drop()
            raise RegionMapUnavailable(f"region mapper {self.address} closed")
        if not reply.get("ok"):
            raise RegionMapError(
                str(reply.get("error", "region mapper refused")),
                str(reply.get("code", "error")),
            )
        return reply

    def _drop(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
