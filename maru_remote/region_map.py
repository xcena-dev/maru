# SPDX-License-Identifier: Apache-2.0
"""Region map protocol between a worker's Handler and a remote CXL agent.

A remote CXL agent runs on every memory node. It does for a remote Handler what
``DaxMapper`` does inside a Handler on a CXL-attached node: check a region's
handle with the resource manager (``GET_ACCESS``) and mmap the region, and
then register the mapping with the node's NIC so the Handler can reach it by
RDMA.

One TCP connection per Handler and remote CXL agent; each message is a 4-byte
big-endian length followed by a MessagePack map. Requests carry ``op``:

- ``hello``: returns ``{"node", "epoch", "boot", "nonce"}``; the epoch is
  new on every start of the remote CXL agent process, ``boot`` on every boot of
  the memory node and on every reset of memory that loses what it held (it
  then carries the memory's generation), ``nonce`` on every connection. An agent with a region
  extension adds ``region_extension`` (see :func:`hint_actions`).
- ``auth`` with ``proof`` and ``nonce``: the client proves it holds the
  remote CXL agent key (:func:`prove` over the connection's nonce) and the reply
  proves the same over the client's nonce. A connection does nothing else
  until it has proved the key, so only processes that can read the key file
  map regions, as only users who can open the DAX device map them on a
  CXL-attached node.
- ``map`` with ``handle``: returns ``{"region_id", "addr", "length",
  "metadata", "boot"}``. ``addr`` is the remote CXL agent's virtual address
  of the region (VA_R); a Handler uses it only as the remote address of RDMA
  requests. ``boot`` is the current boot, as ``hello`` says it. ``metadata`` is NIXL metadata naming this region's registration
  only, with connection info. The handle is either the region's handle
  (``MaruHandle.to_dict()``, which only the region's owner and MaruServer
  hold) or its read handle (:func:`read_handle`, which MaruServer gives to
  readers); a read handle maps only a region that is mapped already.
  ``fresh`` true says the owner allocated the region in this session and has
  had no map reply for it yet, so it holds no KV.
- ``unmap`` with ``region_id``: the connection no longer needs the region.
- ``release`` with the region's handle: the region is being given back; the
  remote CXL agent deregisters and unmaps it whoever else holds it, and refuses
  later maps of the handle. Releasing a region that is not mapped succeeds.
  A read handle cannot release.
- ``extension`` with ``action`` (``read_hint`` or ``read_hint_end``),
  ``request``, ``ranges`` and ``epoch``: a read hint for the agent's region
  extension; every range must lie in a region the agent maps.

Replies carry ``ok``; a failure has ``error`` and ``code``. The remote CXL agent
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
# The key the installer creates on a memory node, read by its remote CXL agent;
# shared by the remote CXL agents of a deployment.
DEFAULT_KEY_FILE = "/etc/maru/remote-cxl-agent.key"
# The copy that workers and MaruServers read, readable only by their user. A
# node apart from it: a worker installed with the agent gets its own key in
# DEFAULT_KEY_FILE, which is not the deployment's.
DEFAULT_COPY_FILE = "~/.maru/remote-cxl-agent.key"
NONCE_BYTES = 16
# Read hint messages a region extension may take (``op: extension``), and the
# version of their protocol an agent advertises in its hello
HINT_ACTIONS = ("read_hint", "read_hint_end")
HINT_PROTOCOL_VERSION = 1


def pick_agent(agents: list[str], region_id: int) -> str:
    """The remote CXL agent that serves a region (rendezvous hash).

    Every Handler with the same list, and MaruServer, pick the same node for
    a region, so the owner and the readers of a region use one memory node's
    NIC.

    Args:
        agents: Remote CXL agent addresses of the pool.
        region_id: Region number.

    Returns:
        One address of ``agents``.
    """
    return max(
        agents,
        key=lambda address: hashlib.sha256(f"{address}|{region_id}".encode()).digest(),
    )


def read_token(handle: dict) -> str:
    """The read capability of a region, derived from its handle.

    Whoever knows the handle's auth token can compute it; nobody can go back
    from it to the auth token. A remote CXL agent that mapped the region checks
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


def hint_actions(advertised: Any) -> frozenset[str]:
    """The read hint actions an agent's hello says its region extension takes.

    An agent with a region extension advertises
    ``{"version": HINT_PROTOCOL_VERSION, "actions": [...]}``; anything else
    (no extension, or an extension of an earlier protocol) takes none.
    """
    if not isinstance(advertised, dict):
        return frozenset()
    if advertised.get("version") != HINT_PROTOCOL_VERSION:
        return frozenset()
    actions = advertised.get("actions")
    if not isinstance(actions, list):
        return frozenset()
    return frozenset(a for a in actions if a in HINT_ACTIONS)


class KeyFileError(ValueError):
    """The remote CXL agent key file is missing, malformed or open to everyone."""


def new_key() -> str:
    """A new remote CXL agent key: 32 random bytes as hex."""
    return secrets.token_hex(32)


def load_key(path: str) -> bytes:
    """Read the remote CXL agent key.

    Args:
        path: Key file; one line of hex digits (at least 32). ``~`` is the
            user's home.

    Returns:
        The key bytes.

    Raises:
        KeyFileError: if the file cannot be read, is readable by every user,
            or does not hold a key.
    """
    path = os.path.expanduser(path)
    try:
        with open(path) as f:
            mode = os.fstat(f.fileno()).st_mode
            text = f.read().strip()
    except OSError as exc:
        raise KeyFileError(
            f"cannot read the remote CXL agent key {path}: {exc}"
        ) from exc
    if mode & 0o004:
        raise KeyFileError(
            f"the remote CXL agent key {path} is readable by every user; chmod o-r {path}"
        )
    try:
        key = bytes.fromhex(text)
    except ValueError:
        key = b""
    if len(key) < 16:
        raise KeyFileError(
            f"{path} does not hold a remote CXL agent key (at least 32 hex digits)"
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
    fd, tmp = tempfile.mkstemp(prefix=".remote-cxl-agent-key-", dir=directory)
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
        key: Remote CXL agent key.
        role: ``"handler"`` for a client, ``"agent"`` for the remote CXL agent.
        nonce: The nonce the other side chose for this connection.

    Returns:
        HMAC-SHA256 digest.
    """
    return hmac.new(key, role.encode() + b"|" + bytes(nonce), hashlib.sha256).digest()


class RegionMapError(RuntimeError):
    """The remote CXL agent refused a request.

    Attributes:
        code: Short reason (``"denied"``, ``"unavailable"``, ``"invalid"``...).
    """

    def __init__(self, message: str, code: str = "error") -> None:
        super().__init__(message)
        self.code = code


class RegionMapUnavailable(ConnectionError):  # noqa: N818 (a ConnectionError)
    """The remote CXL agent could not be reached or the connection broke."""


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
        addr: The remote CXL agent's virtual address of the region (VA_R).
        length: Region length in bytes.
        metadata: NIXL metadata of the remote CXL agent's agent naming this
            region's registration (its remote key) and the connection info.
        epoch: The remote CXL agent process this mapping belongs to.
        boot: The boot of the memory node at the time of the mapping.
        generation: The client's connection the reply came over; the
            remote CXL agent drops the mapping when that connection ends.
    """

    region_id: int
    addr: int
    length: int
    metadata: bytes
    epoch: str
    boot: str = ""
    generation: int = 0


def parse_address(address: str) -> tuple[str, int]:
    """Split ``host:port``.

    Args:
        address: Remote CXL agent address.

    Returns:
        ``(host, port)``.

    Raises:
        ValueError: if the address has no port.
    """
    host, sep, port = address.rpartition(":")
    if not sep or not host or not port.isdigit():
        raise ValueError(f"remote CXL agent address must be host:port, got {address!r}")
    return host, int(port)


class RegionMapClient:
    """A Handler's connection to one remote CXL agent.

    The connection is opened on first use. If it breaks, every mapping the
    remote CXL agent held for this connection is gone with it, so the caller
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
            address: ``host:port`` of the remote CXL agent.
            timeout_s: Socket timeout for each reply (a first map registers
                a whole region with the NIC).
            connect_timeout_s: Timeout for connecting; ``timeout_s`` if None.
            connect: Socket factory ``(address, timeout) -> socket``
                (tests inject an in-process pair).
            key: Remote CXL agent key (:func:`load_key`). Each connection
                proves it and checks that the remote CXL agent proves it too;
                None only for a remote CXL agent that runs without a key.
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
        # Hint actions the agent's region extension takes (from its hello)
        self.hint_actions: frozenset[str] = frozenset()

    def hello(self) -> dict:
        """Connect if needed and return the remote CXL agent's identity.

        Returns:
            ``{"node", "epoch", "boot"}``.

        Raises:
            RegionMapUnavailable: if the remote CXL agent cannot be reached.
        """
        with self._lock:
            return self._send({"op": "hello"})

    def map(
        self, handle: dict, nixl_agent: str | None = None, fresh: bool = False
    ) -> MapReply:
        """Ask the remote CXL agent to map and register a region.

        Args:
            handle: ``MaruHandle.to_dict()`` of the region.
            nixl_agent: Name of the NIXL agent that loads the reply and then
                announces itself (``NixlTransport.announce``); the remote CXL
                agent drives UCX until the announcement arrives. None for a
                caller that issues no RDMA (MaruServer).
            fresh: The caller owns the region, allocated it in this session
                and has had no map reply for it: it holds no KV yet.

        Returns:
            Where the region is reachable.

        Raises:
            RegionMapError: if the remote CXL agent refused (e.g. a wrong handle).
            RegionMapUnavailable: if the remote CXL agent cannot be reached.
        """
        message: dict[str, Any] = {"op": "map", "handle": dict(handle)}
        if nixl_agent is not None:
            message["nixl_agent"] = nixl_agent
        if fresh:
            message["fresh"] = True
        with self._lock:
            reply = self._send(message)
            # The boot may change while the connection lives (a reset of the
            # memory); an agent that does not say keeps the hello's.
            self.boot = str(reply.get("boot") or self.boot)
            return MapReply(
                int(reply["region_id"]),
                int(reply["addr"]),
                int(reply["length"]),
                bytes(reply["metadata"]),
                self.epoch,
                self.boot,
                self.generation,
            )

    def unmap(self, region_id: int) -> None:
        """Tell the remote CXL agent this Handler no longer needs a region.

        A broken connection already released the region, so it is ignored.

        Args:
            region_id: Region number.

        Raises:
            RegionMapError: if the remote CXL agent refused.
        """
        with self._lock:
            if self._sock is None:
                return
            try:
                self._request({"op": "unmap", "region_id": int(region_id)})
            except RegionMapUnavailable:
                pass

    def release(self, handle: dict) -> None:
        """Have the remote CXL agent deregister a region this Handler owns.

        Sent before the region goes back to MaruServer. A remote CXL agent that
        restarted holds no registration and answers that it is released.

        Args:
            handle: ``MaruHandle.to_dict()`` of the region.

        Raises:
            RegionMapError: if the remote CXL agent refused (``"denied"``: it
                maps another region under this id; ``"unavailable"``: the
                deregistration failed and the region is still registered).
            RegionMapUnavailable: if no connection got a reply.
        """
        with self._lock:
            self._send({"op": "release", "handle": dict(handle)})

    def extension(self, message: dict) -> dict:
        """Send a read hint to the remote CXL agent's region extension.

        Args:
            message: ``action`` (``read_hint`` or ``read_hint_end``),
                ``request``, ``ranges`` and ``epoch``.

        Returns:
            The agent's reply (without ``ok``).

        Raises:
            RegionMapError: if the agent refused it (no extension, a range
                in no region it maps, or the extension raised).
            RegionMapUnavailable: if no connection got a reply.
        """
        with self._lock:
            reply = self._send({**message, "op": "extension"})
        return {k: v for k, v in reply.items() if k != "ok"}

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
                f"remote CXL agent {self.address} is unreachable: {exc}"
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
                f"remote CXL agent {self.address}: key check failed: {exc}"
            ) from exc
        boot = str(reply.get("boot", ""))
        if not boot:
            # A reboot of the node, which loses writes its CPU caches held,
            # could not be told from a restart of the agent.
            self._drop()
            raise RegionMapUnavailable(
                f"remote CXL agent {self.address} reports no boot id of its node"
            )
        self.epoch = str(reply["epoch"])
        self.node = str(reply["node"])
        self.boot = boot
        self.hint_actions = hint_actions(reply.get("region_extension"))

    def _authenticate(self, nonce: Any) -> None:
        """Prove the key to the remote CXL agent and check its proof.

        Raises:
            RegionMapError: if the remote CXL agent refused the proof.
            RegionMapUnavailable: if the two sides disagree on using a key
                or the remote CXL agent proved another key.
        """
        if self._key is None:
            if nonce is not None:
                raise RegionMapUnavailable(
                    "it requires the remote CXL agent key; set the key file "
                    f"(default {DEFAULT_COPY_FILE})"
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
            proof, prove(self._key, "agent", mine)
        ):
            raise RegionMapUnavailable("it proved another key")

    def _request(self, message: dict) -> dict:
        try:
            send_frame(self._sock, message)
            reply = recv_frame(self._sock)
        except (OSError, ValueError) as exc:
            self._drop()
            raise RegionMapUnavailable(
                f"remote CXL agent {self.address} connection failed: {exc}"
            ) from exc
        if reply is None:
            self._drop()
            raise RegionMapUnavailable(f"remote CXL agent {self.address} closed")
        if not reply.get("ok"):
            raise RegionMapError(
                str(reply.get("error", "remote CXL agent refused")),
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
