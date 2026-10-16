# SPDX-License-Identifier: Apache-2.0
"""Region map protocol between a worker's Handler and a region mapper.

A region mapper runs on every memory node. It does for a remote Handler what
``DaxMapper`` does inside a Handler on a CXL-attached node: check a region's
handle with the resource manager (``GET_ACCESS``) and mmap the region, and
then register the mapping with the node's NIC so the Handler can reach it by
RDMA.

One TCP connection per Handler and region mapper; each message is a 4-byte
big-endian length followed by a MessagePack map. Requests carry ``op``:

- ``hello``: returns ``{"node", "epoch", "boot", "nonce"}``; the epoch is
  new on every start of the region mapper process, ``boot`` on every boot of
  the memory node, ``nonce`` on every connection.
- ``auth`` with ``proof`` and ``nonce``: the client proves it holds the
  region mapper key (:func:`prove` over the connection's nonce) and the reply
  proves the same over the client's nonce. A connection does nothing else
  until it has proved the key, so only processes that can read the key file
  map regions, as only users who can open the DAX device map them on a
  CXL-attached node.
- ``map`` with ``handle``: returns ``{"region_id", "addr", "length",
  "metadata"}``. ``addr`` is the region mapper's virtual address of the
  region (VA_R); a Handler uses it only as the remote address of RDMA
  requests. ``metadata`` is NIXL metadata naming this region's registration
  only, with connection info. The handle is either the region's handle
  (``MaruHandle.to_dict()``, which only the region's owner and MaruServer
  hold) or its read handle (:func:`read_handle`, which MaruServer gives to
  readers); a read handle maps only a region that is mapped already.
- ``unmap`` with ``region_id``: the connection no longer needs the region.
- ``release`` with the region's handle: the region is being given back; the
  region mapper deregisters and unmaps it whoever else holds it, and refuses
  later maps of the handle. Releasing a region that is not mapped succeeds.
  A read handle cannot release.

Replies carry ``ok``; a failure has ``error`` and ``code``. The region mapper
treats a closed connection as an unmap of every region the connection mapped.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import socket
import struct
import tempfile
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import msgpack

logger = logging.getLogger(__name__)

MAX_FRAME = 16 * 1024 * 1024
_LEN = struct.Struct("!I")
# Shared by the region mappers of a deployment, its workers and its
# MaruServers; readable only by the users that run them.
DEFAULT_KEY_FILE = "/etc/maru/region-mapper.key"
NONCE_BYTES = 16


def pick_mapper(mappers: list[str], region_id: int) -> str:
    """The region mapper that serves a region (rendezvous hash).

    Every Handler with the same list, and MaruServer, pick the same node for
    a region, so the owner and the readers of a region use one memory node's
    NIC.

    Args:
        mappers: Region mapper addresses of the pool.
        region_id: Region number.

    Returns:
        One address of ``mappers``.
    """
    return max(
        mappers,
        key=lambda address: hashlib.sha256(f"{address}|{region_id}".encode()).digest(),
    )


def read_token(handle: dict) -> str:
    """The read capability of a region, derived from its handle.

    Whoever knows the handle's auth token can compute it; nobody can go back
    from it to the auth token. A region mapper that mapped the region checks
    a read token against the handle the resource manager confirmed.

    Args:
        handle: ``MaruHandle.to_dict()`` of the region.

    Returns:
        32 hex digits.
    """
    key = int(handle["auth_token"]).to_bytes(8, "little")
    message = b"maru-region-read|%d|%d|%d" % (
        int(handle["region_id"]),
        int(handle["offset"]),
        int(handle["length"]),
    )
    return hmac.new(key, message, hashlib.sha256).hexdigest()[:32]


def read_handle(handle: dict) -> dict:
    """What a reader gets of a region: where it is and its read token.

    Args:
        handle: ``MaruHandle.to_dict()`` of the region.

    Returns:
        ``{"region_id", "offset", "length", "read_token"}``.
    """
    return {
        "region_id": int(handle["region_id"]),
        "offset": int(handle["offset"]),
        "length": int(handle["length"]),
        "read_token": read_token(handle),
    }


class KeyFileError(ValueError):
    """The region mapper key file is missing, malformed or open to everyone."""


def new_key() -> str:
    """A new region mapper key: 32 random bytes as hex."""
    return secrets.token_hex(32)


def load_key(path: str) -> bytes:
    """Read the region mapper key.

    Args:
        path: Key file; one line of hex digits (at least 32).

    Returns:
        The key bytes.

    Raises:
        KeyFileError: if the file cannot be read, is readable by every user,
            or does not hold a key.
    """
    try:
        with open(path) as f:
            mode = os.fstat(f.fileno()).st_mode
            text = f.read().strip()
    except OSError as exc:
        raise KeyFileError(f"cannot read the region mapper key {path}: {exc}") from exc
    if mode & 0o004:
        raise KeyFileError(
            f"the region mapper key {path} is readable by every user; chmod o-r {path}"
        )
    try:
        key = bytes.fromhex(text)
    except ValueError:
        key = b""
    if len(key) < 16:
        raise KeyFileError(
            f"{path} does not hold a region mapper key (at least 32 hex digits)"
        )
    return key


def write_key(path: str) -> None:
    """Create a key file readable by its owner only; an existing one stays.

    The key is written to a temporary file that is then linked into place,
    so an interrupted write never leaves an empty key behind.

    Args:
        path: Key file to create.

    Raises:
        FileExistsError: if the file exists.
    """
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".region-mapper-key-", dir=directory)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(new_key() + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.link(tmp, path)
    finally:
        os.unlink(tmp)


def prove(key: bytes, role: str, nonce: bytes) -> bytes:
    """Proof that ``role`` holds ``key``, bound to the peer's ``nonce``.

    Args:
        key: Region mapper key.
        role: ``"handler"`` for a client, ``"mapper"`` for the region mapper.
        nonce: The nonce the other side chose for this connection.

    Returns:
        HMAC-SHA256 digest.
    """
    return hmac.new(key, role.encode() + b"|" + bytes(nonce), hashlib.sha256).digest()


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
        key: bytes | None = None,
    ) -> None:
        """Create the client.

        Args:
            address: ``host:port`` of the region mapper.
            timeout_s: Socket timeout for each reply (a first map registers
                a whole region with the NIC).
            connect_timeout_s: Timeout for connecting; ``timeout_s`` if None.
            connect: Socket factory ``(address, timeout) -> socket``
                (tests inject an in-process pair).
            key: Region mapper key (:func:`load_key`). Each connection
                proves it and checks that the region mapper proves it too;
                None only for a region mapper that runs without a key.
        """
        self.address = address
        self._key = key
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
        try:
            self._authenticate(reply.get("nonce"))
        except (RegionMapError, RegionMapUnavailable) as exc:
            self._drop()
            raise RegionMapUnavailable(
                f"region mapper {self.address}: key check failed: {exc}"
            ) from exc
        self.epoch = str(reply["epoch"])
        self.node = str(reply["node"])
        self.boot = str(reply.get("boot", ""))

    def _authenticate(self, nonce: Any) -> None:
        """Prove the key to the region mapper and check its proof.

        Raises:
            RegionMapError: if the region mapper refused the proof.
            RegionMapUnavailable: if the two sides disagree on using a key
                or the region mapper proved another key.
        """
        if self._key is None:
            if nonce is not None:
                raise RegionMapUnavailable(
                    "it requires the region mapper key; set the key file "
                    f"(default {DEFAULT_KEY_FILE})"
                )
            return
        if nonce is None:
            raise RegionMapUnavailable("it runs without a key and cannot prove one")
        mine = secrets.token_bytes(NONCE_BYTES)
        ack = self._request(
            {
                "op": "auth",
                "proof": prove(self._key, "handler", bytes(nonce)),
                "nonce": mine,
            }
        )
        proof = ack.get("proof")
        if not isinstance(proof, bytes) or not hmac.compare_digest(
            proof, prove(self._key, "mapper", mine)
        ):
            raise RegionMapUnavailable("it proved another key")

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
