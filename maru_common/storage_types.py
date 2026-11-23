# SPDX-License-Identifier: Apache-2.0
"""Storage identities, wire types and result codes."""

import struct
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum, unique

from maru_shm.types import MaruHandle

MAX_STORAGE_BATCH = 1024


@dataclass(frozen=True)
class StorageLocation:
    """Owner-local allocation identity, never a process virtual address."""

    pool_id: str
    allocation_id: str
    generation: int
    offset: int
    length: int
    medium: str = "cpu"


class StorageError(RuntimeError):
    """A metadata operation was explicitly rejected by the server."""


class StorageUnavailableError(ConnectionError):
    """Outcome unknown: allocation ownership must be retained until resolved."""


# =============================================================================
# Multi-storage: identifiers, records and result codes
# =============================================================================

MS_PROTOCOL = "ms1"
HINT_CAPABILITY = "hint_v1"
# NODE_OPEN capabilities: the node reads and writes memory nodes' pools over
# RDMA; it is a memory node
REMOTE_CAPABILITY = "remote_v1"
MEMORY_CAPABILITY = "memory_v1"
WORKER_RDMA_CAPABILITY = "worker_rdma_v1"
# Items in one multi-storage request
MAX_BATCH = MAX_STORAGE_BATCH
# generation = epoch_seq << 40 | counter: a new run always gives higher
# generations. It travels as uint64, which bounds epoch_seq.
GENERATION_EPOCH_SHIFT = 40
GENERATION_COUNTER_MAX = (1 << GENERATION_EPOCH_SHIFT) - 1
EPOCH_SEQ_MAX = (1 << (64 - GENERATION_EPOCH_SHIFT)) - 1


def make_generation(epoch_seq: int, counter: int) -> int:
    """The generation of the ``counter``-th page given in run ``epoch_seq``."""
    if (
        not 0 <= epoch_seq <= EPOCH_SEQ_MAX
        or not 0 <= counter <= GENERATION_COUNTER_MAX
    ):
        raise ValueError(f"generation out of range: {epoch_seq=} {counter=}")
    return (epoch_seq << GENERATION_EPOCH_SHIFT) | counter


def generation_epoch_seq(generation: int) -> int:
    """The StorageService run (epoch_seq) that gave this generation."""
    return generation >> GENERATION_EPOCH_SHIFT


@unique
class PoolKind(StrEnum):
    """Pool medium; ``cpu`` is the old name of dram."""

    DRAM = "dram"
    CXL = "cxl"

    @classmethod
    def _missing_(cls, value):
        return cls.DRAM if value == "cpu" else None

    @classmethod
    def parse(cls, value: str) -> "PoolKind":
        return cls(value)


@dataclass(frozen=True)
class KindTraits:
    """Server policy for a pool medium."""

    mappable: bool
    local_rank: int
    remote_rank: int
    run_scoped: bool
    rm_region: bool


POOL_KINDS = {
    PoolKind.DRAM: KindTraits(True, 0, 2, True, False),
    PoolKind.CXL: KindTraits(True, 1, 2, False, True),
}


@unique
class Visibility(StrEnum):
    """How writes on a CXL region become visible to other hosts."""

    COHERENT = "coherent"
    FLUSH = "flush"


@unique
class Status(StrEnum):
    """Outcome of a whole request."""

    OK = "OK"
    EPOCH = "EPOCH"
    BUSY = "BUSY"
    REJECTED = "REJECTED"
    DISABLED = "DISABLED"


@unique
class ItemResult(StrEnum):
    """Outcome of one item of a request."""

    OK = "OK"
    HIT = "HIT"
    MISS = "MISS"
    ADDED = "ADDED"
    DUPLICATE = "DUPLICATE"
    RETIRED = "RETIRED"
    KEPT = "KEPT"
    STALE = "STALE"
    REJECTED = "REJECTED"


@unique
class KeptCause(StrEnum):
    """Why a retire left the location in place."""

    PINNED = "PINNED"
    MOVED = "MOVED"
    INVALID = "INVALID"


@unique
class MissReason(StrEnum):
    ABSENT = "absent"
    OFFLINE = "offline"
    BUSY = "busy"
    TIMEOUT = "timeout"
    IO_ERROR = "io_error"


@unique
class SessionRole(StrEnum):
    WORKER = "worker"
    PROBE = "probe"


@unique
class CloseMode(StrEnum):
    RESTART = "restart"
    LEAVE = "leave"


@unique
class ExitCause(StrEnum):
    EXIT = "exit"
    LOST = "lost"


@unique
class WaitLevel(StrEnum):
    PUBLISHED = "published"


@unique
class NotifyKind(StrEnum):
    CREDIT_RETURN = "credit_return"
    POOL_ADD = "pool_add"
    POOL_REMOVE = "pool_remove"


@unique
class HintAction(StrEnum):
    """Advice for a memory node's region extension: the read ones come from
    workers (HINT), the write ones ride on RESERVE."""

    READ_HINT = "read_hint"
    READ_HINT_END = "read_hint_end"
    READ_PROMOTE = "read_promote"
    WRITE_HINT = "write_hint"
    WRITE_WAIT = "write_wait"


@dataclass(frozen=True)
class Key:
    """A KV object: the engine's cache namespace and the key inside it."""

    namespace: str
    key: str


@dataclass(frozen=True)
class Location:
    """One copy of a KV (StorageLocation with ``kind`` instead of ``medium``)."""

    pool_id: str
    allocation_id: str
    generation: int
    offset: int
    length: int
    kind: PoolKind


@dataclass(frozen=True)
class KeyLocation:
    key: Key
    location: Location


@dataclass(frozen=True)
class OpRef:
    """Identifies a state-changing request; a resend gets the first result
    instead of running again.

    ``ack``: highest seq whose result the sender holds, along with all below it.
    """

    origin: str
    seq: int
    digest: bytes
    ack: int


@dataclass(frozen=True)
class PoolInfo:
    pool_id: str
    kind: PoolKind
    node_id: str
    pool_generation: int
    capacity: int
    page_size: int
    rm_domain: str | None = None
    region_id: int | None = None
    device_id: str | None = None
    # CXL only; fields that do not apply to a pool are None
    visibility: Visibility | None = None


@dataclass(frozen=True)
class DaxMapping:
    """A CXL region a worker maps: its RM domain, the RM handle, and the RM
    address the worker holds it at (rm_domain is an id, not an address)."""

    rm_domain: str
    handle: MaruHandle
    rm_address: str = ""
    # The owner node's current run: a worker's hold must name it (RM fencing)
    node_id: str = ""
    node_epoch: str = ""


def run_holder(name: str, node_epoch: str) -> str:
    """The RM holder name of ``name`` (a worker incarnation, or the
    StorageService) in one node run: each run's mappings are held apart."""
    return f"{name}@{node_epoch}"


@dataclass(frozen=True)
class PoolDescriptor:
    """How a worker maps a pool: memfd index (DRAM) or RM region (CXL)."""

    pool: PoolInfo
    memfd: int | None = None
    dax: DaxMapping | None = None


@dataclass(frozen=True)
class WorkerRef:
    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    incarnation: str
    worker_session: str | None = None


@dataclass(frozen=True)
class OrphanWorker:
    incarnation: str
    pid: int
    pid_start: int
    host_boot_id: str
    pid_ns: int


@dataclass
class RemotePool:
    """One pool registration exported by an RDMA agent."""

    pool_id: str
    kind: PoolKind
    pool_generation: int
    capacity: int
    base: int
    metadata: bytes
    pool_registration: int = 0


@dataclass
class ReadPeer:
    """Read registrations of one memory node connection."""

    node_id: str
    node_epoch: str
    agent_name: str
    agent_metadata: bytes
    pools: list[RemotePool]
    boot: str
    conn_gen: int
    generation_known: bool = False


@dataclass(frozen=True)
class WriteDomain:
    """One vLLM instance's write registration in a GPU node run.

    ``instance`` is the ATTACH instance shared by its ranks; ``number``
    distinguishes successive registrations. Fencing revokes the registration
    for every worker in the domain.
    """

    node_id: str
    node_epoch: str
    instance: str
    number: int


@dataclass
class WriteAgent:
    domain: WriteDomain
    agent_name: str
    agent_metadata: bytes
    pools: list[RemotePool]


@dataclass(frozen=True)
class ItemOutcome:
    """The result of one item. Only the fields its result uses are set."""

    result: ItemResult
    location: Location | None = None
    pin_id: str | None = None  # a read pin (HIT)
    reason: MissReason | None = None
    cause: KeptCause | None = None
    # KEPT(MOVED): the key of the location now occupying the page
    key: Key | None = None
    # Registration of the HIT location's pool in its owning node run.
    pool_registration: int = 0


@dataclass(frozen=True)
class RmDomain:
    rm_domain: str
    device_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class PoolUsage:
    """Bytes per page state; they add up to ``capacity``."""

    pool_id: str
    capacity: int
    free: int
    credited: int
    transfer: int
    pending: int
    ready: int
    retiring: int
    quarantined: int


@dataclass(frozen=True)
class AccessHints:
    """Pages MaruServer saw read, for the node's LRU, least recently read
    first. Read i is the page at ``offsets[i]`` with ``generations[i]`` in
    pool ``pool_ids[pools[i]]``; the arrays are little-endian uint16, uint64
    and uint64."""

    pool_ids: tuple[str, ...] = ()
    pools: bytes = b""
    offsets: bytes = b""
    generations: bytes = b""

    @classmethod
    def pack(cls, reads: Sequence[tuple[str, int, int]]) -> "AccessHints":
        """From (pool_id, offset, generation) reads, least recent first."""
        if not reads:
            return cls()
        pool_ids, offsets, generations = zip(*reads, strict=True)
        ids = tuple(dict.fromkeys(pool_ids))
        index = {pool_id: i for i, pool_id in enumerate(ids)}
        n = len(reads)
        return cls(
            ids,
            struct.pack(f"<{n}H", *map(index.__getitem__, pool_ids)),
            struct.pack(f"<{n}Q", *offsets),
            struct.pack(f"<{n}Q", *generations),
        )

    def __len__(self) -> int:
        return len(self.offsets) // 8

    def reads(self) -> Iterator[tuple[str, int, int]]:
        """(pool_id, offset, generation) of each read, least recent first;
        ValueError if the arrays do not agree."""
        n = len(self)
        sizes = (len(self.pools), len(self.offsets), len(self.generations))
        if sizes != (2 * n, 8 * n, 8 * n):
            raise ValueError(f"access hint arrays of {sizes} bytes")
        pools = struct.unpack(f"<{n}H", self.pools)
        if n and max(pools) >= len(self.pool_ids):
            raise ValueError("an access hint names no pool")
        return zip(
            map(self.pool_ids.__getitem__, pools),
            struct.unpack(f"<{n}Q", self.offsets),
            struct.unpack(f"<{n}Q", self.generations),
            strict=True,
        )


@dataclass(frozen=True)
class BlockRequest:
    """A removal MaruServer starts; the node blocks new local pins first."""

    block_id: str
    items: tuple[KeyLocation, ...]
    reason: str
