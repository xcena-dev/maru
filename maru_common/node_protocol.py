# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService local protocol, between a node's workers and its StorageService.

Same header and MessagePack payload as MaruServer, over an AF_UNIX stream
socket. Only the ATTACH reply carries descriptors (memfds, SCM_RIGHTS), which
is why ZeroMQ is not used. Sockets are blocking without timeout; any framing
error means the connection must be closed. Each connection serializes its
sends with a lock (a reply and a NOTIFY may race).
"""

import array
import os
import socket
import struct
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import IntEnum, unique

from .protocol import HEADER_SIZE, PROTOCOL_MAGIC, PROTOCOL_VERSION, MessageHeader
from .remote_protocol import TransferEnd
from .storage_types import (
    WORKER_RDMA_CAPABILITY,
    HintAction,
    ItemOutcome,
    Key,
    KeyLocation,
    Location,
    NotifyKind,
    OpRef,
    PoolDescriptor,
    ReadPeer,
    SessionRole,
    Status,
    WaitLevel,
    WriteAgent,
    WriteDomain,
)

DEFAULT_NODE_SOCKET = "/run/maru/node.sock"
MAX_NODE_PAYLOAD = 64 * 1024 * 1024
MAX_NODE_FDS = 16
NOTIFY_SEQUENCE = 0


@unique
class NodeMessageType(IntEnum):
    """Message codes of the StorageService local endpoint."""

    ATTACH = 0x01
    DETACH = 0x02
    CREDIT = 0x10
    CREDIT_RETURN = 0x11
    WRITE_DONE = 0x12
    ABORT = 0x13
    # 0x18, 0x19, 0x1A: reserved; never reuse
    HINT = 0x1B
    READ_JOIN = 0x1C
    LOCAL_PIN = 0x20
    LOCAL_UNPIN = 0x21
    # 0x28: retired (DRAIN_ACK); never reuse it
    UNMAP_ACK = 0x29
    NOTIFY = 0x30
    PEERS = 0x31


# =============================================================================
# Records
# =============================================================================


@dataclass
class GroupLayout:
    """One KV cache group of the attaching worker."""

    object_bytes: int
    layout: str


@dataclass
class Budgets:
    """Node budgets in bytes, as resolved from the node config."""

    credit_max_bytes: int
    credit_total_max_bytes: int
    credit_refill_low: int
    credit_refill_high: int
    commit_pending_max_bytes: int
    transfer_reserve_bytes: dict[str, int] = field(default_factory=dict)


@dataclass
class TransferDefaults:
    """Transfer-mode defaults the node derives from its tiers. None keeps the
    connector's own default."""

    async_load: bool | None = None
    async_store: bool | None = None


@dataclass
class WriteDoneItem:
    key: Key
    credit: Location
    logical_bytes: int
    domain: WriteDomain | None = None
    end: TransferEnd = TransferEnd.DONE


# =============================================================================
# Messages
# =============================================================================


@dataclass
class AttachRequest:
    """ATTACH (0x01).

    ``pid``/``pid_start`` are the worker's own view; checked against SO_PEERCRED
    only when it shares the node's pid namespace. A probe (a vLLM scheduler)
    learns the node and MaruServer but gets no pool, credit or pin; its
    ``groups`` may be empty. ``step_objects``: how many objects the worker
    may store in one engine step, each taking one credit page (0: unknown);
    the node sizes the worker's credit to hold them.
    ``instance``: vLLM ``vllm_config.instance_id``, shared by its ranks;
    required for a worker requesting worker_rdma_v1.
    """

    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    incarnation: str
    namespace: str
    groups: list[GroupLayout]
    protocol: str
    capabilities: list[str] = field(default_factory=list)
    role: SessionRole = SessionRole.WORKER
    step_objects: int = 0
    instance: str = ""

    def __post_init__(self):
        if (
            self.role == SessionRole.WORKER
            and WORKER_RDMA_CAPABILITY in self.capabilities
            and not self.instance
        ):
            raise ValueError("instance is empty for worker_rdma_v1")


@dataclass
class WorkerStaging:
    size: int
    rdma_device: str
    timeout_s: float
    quarantine_max: int


@dataclass
class AttachResponse:
    """A probe's reply has no pools and no budgets. ``capabilities``: those
    of the request this node serves."""

    status: Status
    error: str | None = None
    node_id: str = ""
    node_epoch: str = ""
    server: str = ""
    page_size: int = 0
    pools: list[PoolDescriptor] = field(default_factory=list)
    budgets: Budgets | None = None
    transfer_defaults: TransferDefaults = field(default_factory=TransferDefaults)
    local_pin: bool = False
    # Node-view identity of the worker, for WORKER_OPEN
    pid: int = 0
    pid_start: int = 0
    capabilities: list[str] = field(default_factory=list)
    peers: list[ReadPeer] = field(default_factory=list)
    staging: WorkerStaging | None = None


@dataclass
class ReadJoinRequest:
    """READ_JOIN (0x1C) - Register the worker's read agent, by name."""

    agent_name: str
    agent_metadata: bytes


@dataclass
class ReadJoinResponse:
    status: Status
    error: str | None = None


@dataclass
class Peers:
    """PEERS (0x31), sequence zero: replaces the current read peer list."""

    peers: list[ReadPeer]


@dataclass
class PeersAck:
    status: Status
    error: str | None = None


@dataclass
class DetachRequest:
    """DETACH (0x02) - Sent after every credit is back and copies are done."""


@dataclass
class DetachResponse:
    status: Status
    error: str | None = None


@dataclass
class CreditRequest:
    """CREDIT (0x10)."""

    op: OpRef
    worker_session: str
    want_bytes: int


@dataclass
class CreditResponse:
    status: Status
    error: str | None = None
    credits: list[Location] = field(default_factory=list)
    write_agent: WriteAgent | None = None


@dataclass
class CreditReturnRequest:
    """CREDIT_RETURN (0x11) - Unused credit, also the answer to a return
    request."""

    credits: list[Location]


@dataclass
class CreditReturnResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class WriteDoneRequest:
    """WRITE_DONE (0x12) - Sent after the GPU copy (and flush) completed."""

    op: OpRef
    items: list[WriteDoneItem]
    wait: WaitLevel = WaitLevel.PUBLISHED


@dataclass
class WriteDoneResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class AbortRequest:
    """ABORT (0x13) - Sent after submitted GPU work on these pages drained."""

    op: OpRef
    credits: list[Location]


@dataclass
class AbortResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class HintRequest:
    """HINT (0x1B) - Read hints for the memory nodes, hint_v1 only; advice,
    answered before it is passed on. ``read_hint`` names ``ranges`` (memory
    node locations from PROBE with_locations) in read order;
    ``read_hint_end`` and ``read_promote`` name the request only. The write
    actions are REJECTED here (they ride on RESERVE)."""

    action: HintAction
    request: str
    ranges: list[Location] = field(default_factory=list)


@dataclass
class HintResponse:
    status: Status
    error: str | None = None


@dataclass
class LocalPinRequest:
    """LOCAL_PIN (0x20) - Answered with DISABLED."""

    items: list[KeyLocation] = field(default_factory=list)


@dataclass
class LocalPinResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class LocalUnpinRequest:
    """LOCAL_UNPIN (0x21) - Answered with DISABLED."""

    pin_ids: list[str] = field(default_factory=list)


@dataclass
class LocalUnpinResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class UnmapAckRequest:
    """UNMAP_ACK (0x29) - The pool's mapping and CUDA registration are gone."""

    pool_id: str
    pool_generation: int


@dataclass
class UnmapAckResponse:
    status: Status
    error: str | None = None


@dataclass
class Notify:
    """NOTIFY (0x30), StorageService to worker, no response.

    Fields by kind: credit_return (bytes), pool_add (pool), pool_remove
    (pool_id, pool_generation).
    """

    kind: NotifyKind
    bytes: int | None = None
    pool: PoolDescriptor | None = None
    pool_id: str | None = None
    pool_generation: int | None = None


@dataclass
class NotifyAck:
    """Placeholder; NOTIFY is never answered."""

    status: Status = Status.OK


NODE_MESSAGE_CLASSES = {
    NodeMessageType.READ_JOIN: (ReadJoinRequest, ReadJoinResponse),
    NodeMessageType.PEERS: (Peers, PeersAck),
    NodeMessageType.ATTACH: (AttachRequest, AttachResponse),
    NodeMessageType.DETACH: (DetachRequest, DetachResponse),
    NodeMessageType.CREDIT: (CreditRequest, CreditResponse),
    NodeMessageType.CREDIT_RETURN: (CreditReturnRequest, CreditReturnResponse),
    NodeMessageType.WRITE_DONE: (WriteDoneRequest, WriteDoneResponse),
    NodeMessageType.ABORT: (AbortRequest, AbortResponse),
    NodeMessageType.HINT: (HintRequest, HintResponse),
    NodeMessageType.LOCAL_PIN: (LocalPinRequest, LocalPinResponse),
    NodeMessageType.LOCAL_UNPIN: (LocalUnpinRequest, LocalUnpinResponse),
    NodeMessageType.UNMAP_ACK: (UnmapAckRequest, UnmapAckResponse),
    NodeMessageType.NOTIFY: (Notify, NotifyAck),
}


# =============================================================================
# Framing over AF_UNIX stream sockets
# =============================================================================


class FrameError(ConnectionError):
    """The stream broke or carried a non-message; close the connection."""


def _check_frame(frame: bytes) -> None:
    if len(frame) < HEADER_SIZE:
        raise ValueError(f"frame shorter than a header: {len(frame)}")
    header = MessageHeader.unpack(frame)
    if header.payload_length != len(frame) - HEADER_SIZE:
        raise ValueError(
            f"header says {header.payload_length} payload bytes, frame has "
            f"{len(frame) - HEADER_SIZE}"
        )
    if header.payload_length > MAX_NODE_PAYLOAD:
        raise ValueError(f"message too large: {header.payload_length} bytes")


def send_frame(sock: socket.socket, frame: bytes, fds: Sequence[int] = ()) -> None:
    """Send one encoded message; ``fds`` ride on its first byte. Hold the send lock."""
    _check_frame(frame)
    if len(fds) > MAX_NODE_FDS:
        raise ValueError(f"too many descriptors: {len(fds)} > {MAX_NODE_FDS}")
    view = memoryview(frame)
    if fds:
        sent = socket.send_fds(sock, [view], list(fds))
        if sent <= 0:
            raise FrameError("connection closed while sending descriptors")
        view = view[sent:]
    if view:
        sock.sendall(view)


def send_frame_nowait(sock: socket.socket, frame: bytes) -> int:
    """Send what a socket with no timeout takes of one encoded message now
    (no descriptors); returns the bytes sent, 0 if it would wait. Hold the
    send lock until the rest is sent."""
    _check_frame(frame)
    try:
        return sock.send(frame, socket.MSG_DONTWAIT)
    except BlockingIOError:
        return 0


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks = []
    while n:
        chunk = sock.recv(n)
        if not chunk:
            raise FrameError("connection closed in the middle of a message")
        chunks.append(chunk)
        n -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock: socket.socket, max_fds: int = 0) -> tuple[bytes, list[int]] | None:
    """Receive one message and its descriptors; None at a clean EOF.

    More than ``max_fds`` descriptors are closed and refused (only the ATTACH
    reply may bring any). The caller owns the returned descriptors.
    """
    if not 0 <= max_fds <= MAX_NODE_FDS:
        raise ValueError(f"max_fds must be within 0..{MAX_NODE_FDS}, got {max_fds}")
    data, fds, flags = _recv_with_fds(sock, HEADER_SIZE)
    if not data:
        for fd in fds:
            _close_quietly(fd)
        return None
    try:
        if flags & socket.MSG_CTRUNC:
            raise FrameError("descriptors were truncated")
        if len(fds) > max_fds:
            raise FrameError(f"{len(fds)} descriptors where at most {max_fds} may come")
        if len(data) < HEADER_SIZE:
            data += _recv_exact(sock, HEADER_SIZE - len(data))
        header = MessageHeader.unpack(data)
        if header.magic != PROTOCOL_MAGIC:
            raise FrameError(f"not a maru message (magic 0x{header.magic:04X})")
        if header.version != PROTOCOL_VERSION:
            raise FrameError(
                f"protocol version {header.version}, this side speaks {PROTOCOL_VERSION}"
            )
        if header.payload_length > MAX_NODE_PAYLOAD:
            raise FrameError(f"message too large: {header.payload_length} bytes")
        payload = _recv_exact(sock, header.payload_length)
    except BaseException:
        for fd in fds:
            _close_quietly(fd)
        raise
    return data + payload, fds


def _recv_with_fds(sock: socket.socket, n: int) -> tuple[bytes, list[int], int]:
    """recvmsg with MSG_CMSG_CLOEXEC (socket.recv_fds drops its flags argument)."""
    fds = array.array("i")
    data, ancdata, flags, _ = sock.recvmsg(
        n, socket.CMSG_SPACE(MAX_NODE_FDS * fds.itemsize), socket.MSG_CMSG_CLOEXEC
    )
    for level, kind, payload in ancdata:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            fds.frombytes(payload[: len(payload) - (len(payload) % fds.itemsize)])
    return data, list(fds), flags


def _close_quietly(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


# struct ucred: pid_t pid; uid_t uid; gid_t gid
_UCRED = struct.Struct("iII")


def peer_credentials(sock: socket.socket) -> tuple[int, int, int]:
    """(pid, uid, gid) of the peer, in this pid namespace."""
    raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, _UCRED.size)
    return _UCRED.unpack(raw)


def process_start_time(pid: int) -> int:
    """Start time of ``pid`` in clock ticks (/proc/<pid>/stat field 22)."""
    with open(f"/proc/{pid}/stat", "rb") as f:
        stat = f.read()
    # comm may hold spaces and ')': fields resume after the last ')'
    rest = stat[stat.rindex(b")") + 2 :].split()
    return int(rest[19])


def process_identity() -> tuple[str, int, int, int]:
    """(boot id, pid, start time, pid namespace inode) of this process, as the
    RM records a hold; "" or 0 where unknown."""
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            boot = f.read().strip()
    except OSError:
        boot = ""
    try:
        pid_ns = os.stat("/proc/self/ns/pid").st_ino
    except OSError:
        pid_ns = 0
    pid = os.getpid()
    return boot, pid, process_start_time(pid), pid_ns


def same_pid_namespace(pid: int) -> bool:
    """Whether ``pid`` lives in this process's pid namespace."""
    return os.stat(f"/proc/{pid}/ns/pid").st_ino == os.stat("/proc/self/ns/pid").st_ino
