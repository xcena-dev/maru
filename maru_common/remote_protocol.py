# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RemoteServer protocol, between a GPU node's remote tier and a memory node.

One TCP connection per GPU node run, opened by the key proof and HELLO; the
same header and MessagePack payload as MaruServer (``Serializer(
REMOTE_MESSAGE_CLASSES)``), responses matched by the header sequence.
The memory node reserves, commits and frees its own pages.

Requests in LEDGER_CODES change pages: they carry an OpRef whose origin is
``peer_origin`` of the HELLO, and the memory node keeps their results in an
OpLedger until acked, across connections, for its run. Release evidence is
only a transfer that ended DONE, or one never posted; anything else leaves
the page quarantined until the memory node's NIC registration ends.
"""

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum, unique

from .storage_types import (
    HintAction,
    ItemOutcome,
    Key,
    Location,
    OpRef,
    RemotePool,
    Status,
    WriteAgent,
    WriteDomain,
)

REMOTE_WIRE_VERSION = 1


def peer_origin(node_id: str, node_epoch: str) -> str:
    """The OpLedger origin, and credit holder, of a GPU node run."""
    return f"peer:{node_id}@{node_epoch}"


@unique
class RemoteMessageType(IntEnum):
    """Message codes of the RemoteServer endpoint."""

    HELLO = 0x01
    PING = 0x02
    GOODBYE = 0x03
    AGENTS = 0x04
    RESERVE = 0x10
    WRITE_DONE = 0x11
    CANCEL = 0x12
    FENCE_DOMAIN = 0x14
    HINT = 0x1A
    # 0x13, 0x18, 0x19: reserved (DROP, STAGE, UNSTAGE)


@unique
class TransferEnd(StrEnum):
    """How an RDMA transfer on a page ended, as its poster saw it."""

    DONE = "done"  # the poll saw DONE: every byte landed
    NOT_POSTED = "not_posted"  # never posted
    UNKNOWN = "unknown"  # ERR, timeout or lost: the page may still change


@unique
class HelloRefusal(StrEnum):
    """Why a HELLO was refused (BUSY: not_ready, server_epoch; else REJECTED)."""

    VERSION = "version"
    SERVER_EPOCH = "server_epoch"
    PAGE_SIZE = "page_size"
    RETIRED = "retired"  # a later run of the node said HELLO, or it said GOODBYE
    NOT_READY = "not_ready"


@unique
class RoomLimit(StrEnum):
    """Why a memory node gives a peer no room."""

    QUARANTINE = "quarantine"  # the peer's or the node's quarantine cap


@unique
class FenceResult(StrEnum):
    """Success proves registration revocation; failure includes unknown."""

    SUCCESS = "success"
    FAILURE = "failure"


# =============================================================================
# Records
# =============================================================================


@dataclass
class AgentMetadata:
    agent_name: str
    agent_metadata: bytes


@dataclass
class Written:
    """A credited page the peer posted a WRITE to; ``end`` is DONE or
    UNKNOWN (one never posted goes back by CANCEL)."""

    key: Key
    location: Location
    logical_bytes: int
    end: TransferEnd


# =============================================================================
# Messages
# =============================================================================


@dataclass
class HelloRequest:
    """HELLO (0x01) - The first request of a connection, after the key proof.

    ``capabilities``: those MaruServer accepted for the sender's run.
    ``conn_gen`` numbers the connection; it grows over the sender's run
    (all memory nodes). The memory node does not use it.
    ``ack``: as OpRef.ack, for the run's ops so far.
    """

    version: int
    node_id: str
    node_epoch: str
    host_boot_id: str
    pid: int
    server_epoch: str
    agent_name: str
    agent_metadata: bytes
    page_size: int
    conn_gen: int
    capabilities: list[str] = field(default_factory=list)
    ack: int = 0


@dataclass
class HelloResponse:
    """The memory node's run, its registered pools and the peer's room
    (bytes it would credit now).

    ``boot``: ``<host boot id>:<memory generation>``, with ``~<n>`` after the
    n-th period the generation was unknown (the boot id alone without a
    generation); a new boot means the memory may have lost what it held.
    ``hint_actions``: the HintAction values its region extension takes; the
    GPU node sends only those it knows of them.
    """

    status: Status
    error: str | None = None
    refusal: HelloRefusal | None = None
    node_id: str = ""
    node_epoch: str = ""
    server_epoch: str = ""
    capabilities: list[str] = field(default_factory=list)
    agent_name: str = ""
    pools: list[RemotePool] = field(default_factory=list)
    room: int = 0
    limit: RoomLimit | None = None
    boot: str = ""
    hint_actions: list[str] = field(default_factory=list)
    agent_metadata: bytes = b""


@dataclass
class PingRequest:
    """PING (0x02) - Liveness."""


@dataclass
class PingResponse:
    status: Status
    error: str | None = None
    room: int = 0
    limit: RoomLimit | None = None


@dataclass
class GoodbyeRequest:
    """GOODBYE (0x03) - The sender's run ends. Frees nothing: its credits
    still open are quarantined, and its node_epoch is retired."""


@dataclass
class GoodbyeResponse:
    status: Status
    error: str | None = None


@dataclass
class AgentsRequest:
    """AGENTS (0x04) - Idempotent read-agent additions and removals by name."""

    add: list[AgentMetadata] = field(default_factory=list)
    remove: list[str] = field(default_factory=list)


@dataclass
class AgentsResponse:
    status: Status
    error: str | None = None


@dataclass
class FenceDomainRequest:
    """FENCE_DOMAIN (0x14) - Revoke the instance domain's write registration."""

    op: OpRef
    domain: WriteDomain


@dataclass
class FenceDomainResponse:
    status: Status
    error: str | None = None
    result: FenceResult | None = None


@dataclass
class ReserveRequest:
    """RESERVE (0x10) - Credit ``pages`` pages to the sender's run.

    ``hint``: ``write_hint`` or ``write_wait``, among the HELLO's
    hint_actions: the region extension hears the credited pages in write
    order. With ``write_wait`` the reply waits for its answer, at most
    ``wait_ms``; the sender writes either way.
    """

    op: OpRef
    pages: int
    hint: HintAction | None = None
    wait_ms: int = 0
    domain: WriteDomain | None = None


@dataclass
class ReserveResponse:
    """``credits`` may be fewer than asked; ``room`` is what is left."""

    status: Status
    error: str | None = None
    credits: list[Location] = field(default_factory=list)
    room: int = 0
    limit: RoomLimit | None = None
    write_agent: WriteAgent | None = None
    domain_fenced: bool = False  # the memory node confirmed revocation


@dataclass
class RemoteWriteDoneRequest:
    """WRITE_DONE (0x11) - DONE items are committed, UNKNOWN ones
    quarantined."""

    op: OpRef
    items: list[Written]
    domain: WriteDomain | None = None


@dataclass
class RemoteWriteDoneResponse:
    """Items: ADDED, DUPLICATE (location: the committed one) or REJECTED;
    an UNKNOWN item OK once quarantined."""

    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class CancelRequest:
    """CANCEL (0x12) - Credits never posted to go back: those left of the
    RESERVE ``reserve_seq`` (one not received yet then gives none), or
    ``locations``."""

    op: OpRef
    reserve_seq: int | None = None
    locations: list[Location] = field(default_factory=list)
    domain: WriteDomain | None = None


@dataclass
class CancelResponse:
    status: Status
    error: str | None = None
    freed: int = 0


@dataclass
class RemoteHintRequest:
    """HINT (0x1A) - A read hint for the region extension, among the HELLO's
    hint_actions; advice, with no op. ``read_hint`` names ``ranges`` in this
    memory node's pools, in read order; ``read_hint_end`` and
    ``read_promote`` name the request only."""

    action: HintAction
    request: str
    ranges: list[Location] = field(default_factory=list)


@dataclass
class RemoteHintResponse:
    status: Status
    error: str | None = None


REMOTE_MESSAGE_CLASSES = {
    RemoteMessageType.AGENTS: (AgentsRequest, AgentsResponse),
    RemoteMessageType.FENCE_DOMAIN: (FenceDomainRequest, FenceDomainResponse),
    RemoteMessageType.HELLO: (HelloRequest, HelloResponse),
    RemoteMessageType.PING: (PingRequest, PingResponse),
    RemoteMessageType.GOODBYE: (GoodbyeRequest, GoodbyeResponse),
    RemoteMessageType.RESERVE: (ReserveRequest, ReserveResponse),
    RemoteMessageType.WRITE_DONE: (RemoteWriteDoneRequest, RemoteWriteDoneResponse),
    RemoteMessageType.CANCEL: (CancelRequest, CancelResponse),
    RemoteMessageType.HINT: (RemoteHintRequest, RemoteHintResponse),
}

# Requests recorded in the memory node's OpLedger
LEDGER_CODES = frozenset(
    {
        RemoteMessageType.RESERVE,
        RemoteMessageType.WRITE_DONE,
        RemoteMessageType.CANCEL,
        RemoteMessageType.FENCE_DOMAIN,
    }
)
