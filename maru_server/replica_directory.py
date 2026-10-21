# SPDX-License-Identifier: Apache-2.0
"""Owner-scoped CPU/CXL L1 pools, and remote CXL regions read across sessions.

CPU and mixed pools keep access within the owning worker. Remote CXL regions live in
a memory node's CXL; a session gets one region when it opens and one more each
time it asks (``expand``), as a cxl Handler does with ``request_alloc``. Any
session of the same namespace can lease and read their keys; the owner evicts
its own keys in the order of last use across all sessions.

A region stays with its session while the owner uses it. The owner gives
one back alone (``return_region``) only after the remote CXL agent deregistered
it, so no RDMA reaches it any more; the region goes back to the resource
manager at once, and leases other sessions still hold on its keys are
reported lost on release.

When the owner leaves (a drained close, or its session expires), MaruServer
takes its regions over, as it keeps a cxl Handler's regions while keys refer
to them. A region with keys is kept: its keys are still found and the region
keeper (``region_keeper.py``) holds its mapping at the remote CXL agent so
readers keep reading. A region without keys goes back. The owner never wrote
a page that a found key refers to, so a late write from it, or from an
owner that only seemed gone, lands on no key's bytes. A taken-over region
goes back to the resource manager only after the keeper released it at its
remote CXL agent: when the resource manager is full (the kept region read
longest ago first), or when its keys cannot be read any more.
"""

import logging
import time
import uuid
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from threading import Event, RLock

from maru_common.storage_policy import FixedOrderPolicy, validate_order
from maru_common.storage_types import MAX_STORAGE_BATCH, StorageLocation
from maru_remote.region_map import read_handle

logger = logging.getLogger(__name__)


class RegionTooLargeError(ValueError):
    """A remote CXL region larger than any pool can give; room never heals it."""


REMOTE_CXL = "remote_cxl"
MAX_REGIONS_PER_SESSION = 4096
# Sessions that hold space or may still use it; an expired remote CXL session
# holds nothing (MaruServer took its regions over) and does not count
MAX_SESSIONS = 1024
# Expired remote CXL sessions kept so that a late drained close or
# return_region still succeeds; the oldest record goes beyond this (a
# return_region for it is then refused, and MaruServer still gives the region
# back, having taken it over)
MAX_EXPIRED_REMOTE_SESSIONS = 4096
RELEASE_RETRY_S = 1.0
# A kept region given back to make room is waited for this long before
# another one goes for the same reason.
ROOM_WAIT_S = 30.0
# A worker refused its first region is waited for this long: meanwhile no
# session that already holds a region gets another one.
FIRST_REGION_WAIT_S = 30.0


def _bound(items: dict, limit: int) -> None:
    """Drop the oldest entries of an insertion-ordered dict beyond ``limit``."""
    while len(items) > limit:
        del items[next(iter(items))]


@dataclass
class StoragePool:
    pool_id: str
    medium: str
    capacity: int
    page_size: int
    handle: object = None
    owner_id: str = ""
    replicas: dict[str, StorageLocation] = field(default_factory=dict)
    allocations: dict[str, str] = field(default_factory=dict)
    pages: set[int] = field(default_factory=set)
    allocated_bytes: int = 0
    quarantined_bytes: int = 0
    acquired_objects: int = 0
    # Remote CXL regions only: the namespace they serve, whether their keys can be
    # found (False once the owner expired or closed), the last use of each
    # key and the read leases on each key.
    namespace: str = ""
    schema: str = ""
    visible: bool = True
    last_use: dict[str, float] = field(default_factory=dict)
    leased: dict[str, int] = field(default_factory=dict)
    # Remote CXL regions only: the boot of the memory node the owner last
    # reported for the region, and what MaruServer does with a region whose
    # owner left ("keep": keys stay found; "return": release, then free).
    boot: str = ""
    fate: str = ""


@dataclass
class StorageSession:
    token: str
    engine_id: str
    namespace: str
    node_id: str
    schema: str
    pools: dict[str, StoragePool]
    read_order: tuple[str, ...]
    write_order: tuple[str, ...]
    deadline: float
    active: bool = True
    leases: dict[str, list[StorageLocation]] = field(default_factory=dict)
    operations: dict[str, tuple[list[dict], list[str]]] = field(default_factory=dict)
    report_sequence: int = -1
    # Remote CXL regions owned by this session (pool_id -> region) and their size;
    # a remote CXL session is one with a region size, and it may hold no
    # region for a while (its only region went back after a reboot)
    regions: dict[str, StoragePool] = field(default_factory=dict)
    region_size: int = 0
    page_size: int = 0
    # lease_id -> (region, key) for leases on remote CXL regions, possibly of others
    remote_cxl_leases: dict[str, list[tuple[StoragePool, str]]] = field(
        default_factory=dict
    )
    # expand op_id -> pool_id of the region it gave, so a resent expand whose
    # reply was lost gets the same region instead of a second one
    expansions: dict[str, str] = field(default_factory=dict)
    # pool_ids of regions given back with return_region (a resend succeeds),
    # oldest first
    returned: dict[str, None] = field(default_factory=dict)


class ReplicaDirectory:
    def __init__(
        self,
        capacity_limit: int = 64 * 1024**3,
        session_ttl: float = 30,
        reserve_cxl=None,
        release_cxl=None,
        clock=None,
        largest_cxl_pool=None,
    ):
        if capacity_limit <= 0 or session_ttl <= 0:
            raise ValueError("CPU capacity limit and session TTL must be positive")
        self.server_epoch = uuid.uuid4().hex
        self.capacity_limit = capacity_limit
        self.session_ttl = session_ttl
        self._reserve_cxl = reserve_cxl
        self._release_cxl = release_cxl
        # () -> bytes of the largest pool a region may come from, or None
        self._largest_cxl_pool = largest_cxl_pool
        self._clock = clock or (lambda: time.monotonic())
        self._sessions: dict[str, StorageSession] = {}
        self._engines: dict[str, str] = {}
        self._closed_tokens: set[str] = set()
        # Expired remote CXL sessions, oldest first (see MAX_SESSIONS)
        self._expired_remote: OrderedDict[str, None] = OrderedDict()
        # (namespace, key) -> remote CXL region holding the key
        self._remote_cxl_index: dict[tuple[str, str], StoragePool] = {}
        # Regions whose release the RM has not confirmed yet
        self._releasing: dict[str, StoragePool] = {}
        # Remote CXL regions whose owner left, taken over by MaruServer
        self._departed: dict[str, StoragePool] = {}
        # Region requests the RM refused since the keeper last looked
        self._refused_reservations = 0
        # Kept regions given back to make room, not at the RM yet -> when
        self._room_pending: dict[str, float] = {}
        # Engines refused their first remote CXL region -> when last refused
        self._first_region_waits: dict[str, float] = {}
        # Set when there is work for the region keeper
        self.keeper_wakeup = Event()
        self._next_release_retry = 0.0
        self._lock = RLock()

    @property
    def supports_mixed(self):
        return self._reserve_cxl is not None and self._release_cxl is not None

    supports_remote_cxl = supports_mixed

    @staticmethod
    def _text(value, name: str) -> str:
        if not isinstance(value, str) or not value or len(value) > 4096:
            raise ValueError(f"{name} must be a nonempty string of at most 4096 chars")
        return value

    @staticmethod
    def _integer(value, name: str, minimum: int = 0) -> int:
        if type(value) is not int or value < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}")
        return value

    # ---------------------------------------------------------------- lifetime

    def _expire(self):
        now = self._clock()
        for session in list(self._sessions.values()):
            if session.active and now >= session.deadline:
                session.active = False
                if self._engines.get(session.engine_id) == session.token:
                    del self._engines[session.engine_id]
                # Timeout is not proof of unmapping: pools stay charged until a
                # drained close. Remote CXL regions are taken over (their keys
                # stay found), and the leases this session held as a reader end
                # with it.
                self._drop_reader_leases(session)
                self._take_over(session)
                if not session.pools:
                    self._expired_remote[session.token] = None
        while len(self._expired_remote) > MAX_EXPIRED_REMOTE_SESSIONS:
            token, _ = self._expired_remote.popitem(last=False)
            self._sessions.pop(token, None)
        self._retry_releases()

    def _session(self, payload: dict) -> StorageSession:
        if payload.get("server_epoch") != self.server_epoch:
            raise ValueError("Server epoch changed; restart the storage session")
        session = self._sessions.get(payload.get("session_token"))
        if session is None or not session.active:
            raise ValueError("Storage session is absent or expired")
        return session

    def execute(self, action: str, payload: dict) -> dict:
        handlers = {
            "open": self._open,
            "expand": self._expand,
            "heartbeat": self._heartbeat,
            "commit": self._commit,
            "resolve": self._resolve,
            "probe": self._probe,
            "acquire": self._acquire,
            "release": self._release,
            "victims": self._victims,
            "remove": self._remove,
            "close": self._close,
            "return_region": self._return,
            "usage": lambda _: self._usage(),
        }
        with self._lock:
            self._expire()
            try:
                if not isinstance(payload, dict) or action not in handlers:
                    raise ValueError("Unknown storage operation")
                return {"success": True, "result": handlers[action](payload)}
            except (ValueError, TypeError, KeyError) as exc:
                return {"success": False, "error": str(exc)}

    # -------------------------------------------------------------------- open

    def _open(self, p: dict) -> dict:
        if p.get("server_epoch") != self.server_epoch:
            raise ValueError("Server epoch changed")
        token = self._text(p.get("session_token"), "session_token")
        if token in self._closed_tokens:
            raise ValueError("Storage session was closed")
        if len(self._closed_tokens) >= 65536:
            raise ValueError(
                "Session tombstone limit reached; restart the metadata server"
            )
        engine = self._text(p.get("engine_id"), "engine_id")
        namespace = self._text(p.get("namespace"), "namespace")
        node = self._text(p.get("node_id"), "node_id")
        schema = self._text(p.get("schema"), "schema")
        region_size = self._integer(
            p.get("remote_cxl_region_size", 0), "remote_cxl_region_size"
        )
        capacity = self._integer(p.get("capacity"), "capacity", 0 if region_size else 1)
        page_size = self._integer(p.get("page_size"), "page_size", 1)
        cxl_capacity = self._integer(p.get("cxl_capacity", 0), "cxl_capacity")
        for size in (capacity, cxl_capacity, region_size):
            if size and (
                size < page_size or size % page_size or size // page_size > 1_000_000
            ):
                raise ValueError(
                    "Pool capacity must be page aligned and at most 1000000 pages"
                )
        if cxl_capacity and not self.supports_mixed:
            raise ValueError(
                "CXL storage is unavailable; mixed mode requires a CXL-enabled server"
            )
        if region_size and not self.supports_remote_cxl:
            raise ValueError("Remote CXL storage requires a CXL-enabled server")
        if region_size and (capacity or cxl_capacity):
            raise ValueError("A remote CXL session cannot also hold CPU or CXL pools")
        read_order = validate_order(p.get("read_order", ["cpu", "cxl"]))
        write_order = validate_order(p.get("write_order", ["cpu", "cxl"]))
        previous = self._sessions.get(token)
        if previous is not None:
            if not previous.active:
                # An expired remote CXL session has no regions left: MaruServer
                # took them over.
                raise ValueError("Session token reused with a different registration")
            expected = (
                engine,
                namespace,
                node,
                schema,
                capacity,
                page_size,
                cxl_capacity,
                region_size,
                read_order,
                write_order,
            )
            actual = (
                previous.engine_id,
                previous.namespace,
                previous.node_id,
                previous.schema,
                previous.pools["cpu"].capacity if "cpu" in previous.pools else 0,
                previous.page_size,
                previous.pools["cxl"].capacity if "cxl" in previous.pools else 0,
                previous.region_size,
                previous.read_order,
                previous.write_order,
            )
            if actual != expected:
                raise ValueError("Session token reused with a different registration")
            return self._grant(previous)
        if engine in self._engines:
            raise ValueError("Storage supports one active worker per engine_id")
        if len(self._sessions) - len(self._expired_remote) >= MAX_SESSIONS:
            raise ValueError("Storage session limit reached")
        if region_size:
            self._check_remote_cxl_peers(namespace, schema, page_size)
        granted = sum(
            s.pools["cpu"].capacity
            for s in self._sessions.values()
            if s.node_id == node and "cpu" in s.pools
        )
        if granted + capacity > self.capacity_limit:
            raise ValueError(
                "CPU host capacity limit exceeded (includes expired grants)"
            )
        pools = {}
        if capacity:
            pools["cpu"] = StoragePool(uuid.uuid4().hex, "cpu", capacity, page_size)
        if cxl_capacity:
            owner_id = "l1:" + uuid.uuid4().hex
            handle = self._reserve_cxl(owner_id, cxl_capacity)
            if handle is None:
                raise ValueError("CXL pool reservation failed")
            pools["cxl"] = StoragePool(
                uuid.uuid4().hex, "cxl", cxl_capacity, page_size, handle, owner_id
            )
        regions = {}
        if region_size:
            region = self._reserve_first_region(
                engine, namespace, schema, region_size, page_size
            )
            regions[region.pool_id] = region
        session = StorageSession(
            token,
            engine,
            namespace,
            node,
            schema,
            pools,
            read_order,
            write_order,
            self._clock() + self.session_ttl,
            regions=regions,
            region_size=region_size,
            page_size=page_size,
        )
        self._sessions[token] = session
        self._engines[engine] = token
        return self._grant(session)

    def _check_remote_cxl_peers(self, namespace: str, schema: str, page_size: int):
        """Readers of another session's region must agree on the KV layout.

        The keys a new session can find are in the regions of the active
        remote CXL sessions of its namespace (present and future ones) and in
        the kept regions of owners that left.
        """
        layouts = [
            (session.schema, session.page_size)
            for session in self._sessions.values()
            if session.active and session.region_size and session.namespace == namespace
        ] + [
            (region.schema, region.page_size)
            for region in self._departed.values()
            if region.fate == "keep" and region.namespace == namespace
        ]
        if any(layout != (schema, page_size) for layout in layouts):
            raise ValueError(
                "Remote CXL regions of this namespace use another schema or page size"
            )

    def _reserve_first_region(
        self, engine: str, namespace: str, schema: str, size: int, page_size: int
    ) -> StoragePool:
        """A session's first region; a refusal makes the engine wait first in line."""
        try:
            region = self._reserve_region(namespace, schema, size, page_size)
        except RegionTooLargeError:
            raise  # the engine does not retry, so nobody waits for it
        except ValueError:
            self._first_region_waits[engine] = self._clock()
            raise
        self._first_region_waits.pop(engine, None)
        return region

    def _waiting_for_first_region(self, engine: str) -> bool:
        """Whether another engine was refused its first region lately."""
        now = self._clock()
        for waiter, since in list(self._first_region_waits.items()):
            if now - since > FIRST_REGION_WAIT_S:
                del self._first_region_waits[waiter]
        return any(waiter != engine for waiter in self._first_region_waits)

    def _largest_pool(self) -> int | None:
        if self._largest_cxl_pool is None:
            return None
        try:
            return self._largest_cxl_pool()
        except Exception:
            return None  # unknown: the refusal stays one that room may heal

    def _reserve_region(
        self, namespace: str, schema: str, size: int, page_size: int
    ) -> StoragePool:
        owner_id = "remote_cxl:" + uuid.uuid4().hex
        handle = self._reserve_cxl(owner_id, size)
        if handle is None:
            largest = self._largest_pool()
            if largest is not None and size > largest:
                # No room will ever come: a refusal the worker does not retry.
                raise RegionTooLargeError(
                    f"Remote CXL region size {size} is larger than the largest "
                    f"pool of the resource manager ({largest} bytes); lower "
                    "maru_pool_size"
                )
            # The keeper gives a kept region back to make room for the next try.
            self._refused_reservations += 1
            self.keeper_wakeup.set()
            raise ValueError("Remote CXL region reservation failed")
        return StoragePool(
            uuid.uuid4().hex,
            REMOTE_CXL,
            size,
            page_size,
            handle,
            owner_id,
            namespace=namespace,
            schema=schema,
        )

    @staticmethod
    def _pool_info(pool: StoragePool) -> dict:
        return {
            "pool_id": pool.pool_id,
            "medium": pool.medium,
            "capacity": pool.capacity,
            **({"handle": pool.handle.to_dict()} if pool.handle is not None else {}),
        }

    @staticmethod
    def _all_pools(session: StorageSession):
        yield from session.pools.values()
        yield from session.regions.values()

    def _grant(self, session):
        # A remote CXL session may hold no region for a while.
        first = session.pools.get("cpu") or next(iter(self._all_pools(session)), None)
        return {
            "pool_id": first.pool_id if first is not None else None,
            "server_epoch": self.server_epoch,
            "session_ttl": self.session_ttl,
            "capacity": first.capacity if first is not None else 0,
            "pools": [self._pool_info(pool) for pool in self._all_pools(session)],
        }

    def _expand(self, p: dict) -> dict:
        """One more remote CXL region for the session, as ``request_alloc`` gives a
        cxl Handler one more region."""
        session = self._session(p)
        op_id = self._text(p.get("op_id"), "op_id")
        if not session.region_size:
            raise ValueError("The session has no remote CXL regions")
        given = session.expansions.get(op_id)
        if given is not None and given in session.regions:
            return {"pool": self._pool_info(session.regions[given])}
        if len(session.regions) >= MAX_REGIONS_PER_SESSION:
            raise ValueError("Remote CXL region limit of the session reached")
        if not session.regions:
            region = self._reserve_first_region(
                session.engine_id,
                session.namespace,
                session.schema,
                session.region_size,
                session.page_size,
            )
        elif self._waiting_for_first_region(session.engine_id):
            # The space that comes free goes to the worker without a region;
            # this session evicts its own keys instead.
            raise ValueError("Remote CXL space is held for a worker without a region")
        else:
            region = self._reserve_region(
                session.namespace,
                session.schema,
                session.region_size,
                session.page_size,
            )
        session.regions[region.pool_id] = region
        session.expansions[op_id] = region.pool_id
        _bound(session.expansions, MAX_REGIONS_PER_SESSION)
        return {"pool": self._pool_info(region)}

    # --------------------------------------------------------------- heartbeat

    def _heartbeat(self, p: dict) -> dict:
        session = self._session(p)
        seq = self._integer(p.get("sequence"), "sequence")
        reports = p.get("pools")
        if reports is None:
            first = session.pools.get("cpu") or next(iter(self._all_pools(session)))
            reports = [{"pool_id": first.pool_id, **p}]
        by_id = {pool.pool_id: pool for pool in self._all_pools(session)}
        # A remote CXL session may report a subset: a region can be granted while
        # the report is on its way.
        exact = not session.region_size
        if not isinstance(reports, list) or (exact and len(reports) != len(by_id)):
            raise ValueError("Invalid pool usage report")
        parsed = []
        for report in reports:
            pool = by_id.pop(report["pool_id"], None)
            if pool is None:
                # A region given back while the report was on its way.
                if report.get("pool_id") in session.returned:
                    continue
                raise ValueError("Invalid pool usage report")
            allocated = self._integer(report.get("allocated_bytes"), "allocated_bytes")
            quarantined = self._integer(
                report.get("quarantined_bytes"), "quarantined_bytes"
            )
            if allocated > pool.capacity or quarantined > allocated:
                raise ValueError("Invalid pool usage")
            boot = report.get("boot")
            parsed.append(
                (pool, allocated, quarantined, boot if isinstance(boot, str) else "")
            )
        if seq > session.report_sequence:
            session.report_sequence = seq
            for pool, allocated, quarantined, boot in parsed:
                pool.allocated_bytes = allocated
                pool.quarantined_bytes = quarantined
                if boot:
                    pool.boot = boot
        session.deadline = self._clock() + self.session_ttl
        return {}

    # ------------------------------------------------------------ store, find

    def _keys(self, p: dict) -> list[str]:
        keys = p.get("keys")
        if not isinstance(keys, list) or len(keys) > MAX_STORAGE_BATCH:
            raise ValueError("Invalid storage key batch")
        return [self._text(key, "key") for key in keys]

    def _entries(self, p: dict) -> list[tuple[str, StorageLocation]]:
        entries = p.get("entries")
        if not isinstance(entries, list) or not 0 < len(entries) <= MAX_STORAGE_BATCH:
            raise ValueError("Invalid storage entry batch")
        return [
            (self._text(entry["key"], "key"), StorageLocation(**entry["location"]))
            for entry in entries
        ]

    @staticmethod
    def _owned_pool(session: StorageSession, loc: StorageLocation):
        if loc.medium == REMOTE_CXL:
            return session.regions.get(loc.pool_id)
        pool = session.pools.get(loc.medium)
        return pool if pool is not None and pool.pool_id == loc.pool_id else None

    def _commit(self, p: dict) -> dict:
        session = self._session(p)
        op_id = self._text(p.get("op_id"), "op_id")
        entries = p.get("entries")
        if not isinstance(entries, list) or not 0 < len(entries) <= MAX_STORAGE_BATCH:
            raise ValueError("Invalid commit batch")
        previous = session.operations.get(op_id)
        if previous is not None:
            if previous[0] != entries:
                raise ValueError("op_id reused with different entries")
            return {"statuses": previous[1]}
        if len(session.operations) >= 65536:
            raise ValueError("Storage operation ledger is full; start a fresh session")
        parsed = []
        for key, loc in self._entries(p):
            pool = self._owned_pool(session, loc)
            if pool is None:
                raise ValueError("Location belongs to another pool or medium")
            self._text(loc.allocation_id, "allocation_id")
            self._integer(loc.generation, "generation", 1)
            self._integer(loc.offset, "offset")
            self._integer(loc.length, "length", 1)
            if (
                loc.offset % pool.page_size
                or loc.length > pool.page_size
                or loc.offset + pool.page_size > pool.capacity
            ):
                raise ValueError("Location exceeds its pool page")
            parsed.append((key, loc, pool))
        now = self._clock()
        statuses = []
        for key, loc, pool in parsed:
            remote_cxl = pool.medium == REMOTE_CXL
            if key in pool.replicas or (
                remote_cxl and (session.namespace, key) in self._remote_cxl_index
            ):
                # The first commit of a key wins, also across remote CXL sessions.
                status = "ALREADY_PRESENT"
            elif loc.allocation_id in pool.allocations or loc.offset in pool.pages:
                status = "REJECTED"
            else:
                pool.replicas[key] = loc
                pool.allocations[loc.allocation_id] = key
                pool.pages.add(loc.offset)
                if remote_cxl:
                    self._remote_cxl_index[(session.namespace, key)] = pool
                    pool.last_use[key] = now
                status = "CREATED"
            statuses.append(status)
        session.operations[op_id] = (entries, statuses)
        return {"statuses": statuses}

    def _resolve(self, p: dict) -> dict:
        session = self._session(p)
        result = session.operations.get(p.get("op_id"))
        # UNKNOWN is not an abort: retry the exact op_id, or drain and close.
        return {
            "state": "COMMITTED" if result else "UNKNOWN",
            "statuses": result[1] if result else [],
        }

    @staticmethod
    def _select(session, key):
        policy = FixedOrderPolicy(session.read_order)
        for medium in policy.candidates(set(session.pools)):
            loc = session.pools[medium].replicas.get(key)
            if loc is not None:
                return loc
        return None

    def _remote_cxl_lookup(self, namespace: str, key: str) -> StoragePool | None:
        pool = self._remote_cxl_index.get((namespace, key))
        if pool is None or not pool.visible or key not in pool.replicas:
            return None
        return pool

    def _probe(self, p: dict) -> dict:
        keys = self._keys(p)
        session = self._sessions.get(self._engines.get(p.get("engine_id")))
        valid = (
            session is not None
            and session.active
            and session.namespace == p.get("namespace")
        )
        namespace = p.get("namespace")
        now = self._clock()
        results = []
        # Only a remote CXL session can read another session's region: a CPU
        # or mixed session of the same namespace would be told of keys it
        # cannot load, and an engine still waiting for its first region has
        # no session to load them with.
        remote_cxl = valid and bool(session.region_size)
        for key in keys:
            hit = bool(valid and self._select(session, key))
            if not hit and remote_cxl:
                pool = self._remote_cxl_lookup(namespace, key)
                if pool is not None:
                    pool.last_use[key] = now
                    hit = True
            results.append(hit)
        first = next(iter(self._all_pools(session)), None) if valid else None
        return {
            "results": results,
            "pool_id": first.pool_id if first is not None else None,
        }

    # ------------------------------------------------------------------- read

    def _acquire(self, p: dict) -> dict:
        session = self._session(p)
        keys = self._keys(p)
        if len(session.leases) + len(session.remote_cxl_leases) >= 4096:
            raise ValueError("Storage read lease limit reached")
        # A reader may name its lease, so it can release a lease whose reply
        # it never received.
        requested = p.get("lease_id")
        if requested is not None:
            requested = self._text(requested, "lease_id")
            if requested in session.leases or requested in session.remote_cxl_leases:
                raise ValueError("lease_id is already in use")
        now = self._clock()
        entries = []
        remote_cxl_held: list[tuple[StoragePool, str]] = []
        regions = {}
        region_boots = {}
        for key in keys:
            loc = self._select(session, key)
            if loc is None:
                # Only a remote CXL session can read another session's region.
                pool = (
                    self._remote_cxl_lookup(session.namespace, key)
                    if session.region_size
                    else None
                )
                if pool is not None:
                    loc = pool.replicas[key]
                    pool.leased[key] = pool.leased.get(key, 0) + 1
                    pool.last_use[key] = now
                    pool.acquired_objects += 1
                    remote_cxl_held.append((pool, key))
                    regions[pool.pool_id] = self._region_handle(session, pool)
                    # Readers must reject a mapping made after the memory
                    # node rebooted, before the owner/keeper retires its keys.
                    region_boots[pool.pool_id] = pool.boot
            else:
                session.pools[loc.medium].acquired_objects += 1
            entries.append(loc)
        lease_id = (requested or uuid.uuid4().hex) if any(entries) else None
        if lease_id is not None:
            local = [loc for loc in entries if loc and loc.medium != REMOTE_CXL]
            if local:
                session.leases[lease_id] = local
            if remote_cxl_held:
                session.remote_cxl_leases[lease_id] = remote_cxl_held
        return {
            "entries": [asdict(loc) if loc else None for loc in entries],
            "lease_id": lease_id,
            "regions": regions,
            "region_boots": region_boots,
        }

    @staticmethod
    def _region_handle(session: StorageSession, pool: StoragePool) -> dict:
        """The handle of an own region; a read handle of anyone else's.

        Only the region's handle (with the resource manager's auth token) can
        release a region at its remote CXL agent, so it goes to the owner alone.
        """
        handle = pool.handle.to_dict()
        if session.regions.get(pool.pool_id) is pool:
            return handle
        return read_handle(handle)

    @staticmethod
    def _drop_remote_cxl_lease(held: list[tuple[StoragePool, str]]) -> None:
        for pool, key in held:
            count = pool.leased.get(key, 0) - 1
            if count > 0:
                pool.leased[key] = count
            else:
                pool.leased.pop(key, None)

    def _drop_reader_leases(self, session: StorageSession) -> None:
        for held in session.remote_cxl_leases.values():
            self._drop_remote_cxl_lease(held)
        session.remote_cxl_leases.clear()

    def _release(self, p: dict) -> dict:
        if p.get("server_epoch") != self.server_epoch:
            raise ValueError("Server epoch changed")
        session = self._sessions.get(p.get("session_token"))
        lease_id = p.get("lease_id")
        held = False
        if session is not None:
            held = session.active and (
                lease_id in session.leases or lease_id in session.remote_cxl_leases
            )
            session.leases.pop(lease_id, None)
            remote_lease = session.remote_cxl_leases.pop(lease_id, None)
            if remote_lease:
                self._drop_remote_cxl_lease(remote_lease)
        # held=False tells a reader its bytes may have been evicted or the
        # region returned mid-read, so the bytes must not be used.
        return {"held": held}

    # ---------------------------------------------------------------- evict

    def _owned_regions(self, session: StorageSession) -> dict[str, StoragePool]:
        if not session.region_size:
            raise ValueError("The session has no remote CXL regions")
        return session.regions

    def _victims(self, p: dict) -> dict:
        """Least recently used, unleased keys of the caller's own regions.

        ``pools``, if given, limits them to those regions (ids the caller
        does not own are ignored): a worker evicts only where it can write
        the freed pages.
        """
        session = self._session(p)
        count = self._integer(p.get("count"), "count", 1)
        pools = p.get("pools")
        if pools is not None and (
            not isinstance(pools, list) or not all(isinstance(x, str) for x in pools)
        ):
            raise ValueError("pools must be a list of pool ids")
        candidates = [
            (pool, key)
            for pool_id, pool in self._owned_regions(session).items()
            if pools is None or pool_id in pools
            for key in pool.replicas
            if not pool.leased.get(key)
        ]
        candidates.sort(key=lambda item: item[0].last_use.get(item[1], 0.0))
        return {
            "entries": [
                {"key": key, "location": asdict(pool.replicas[key])}
                for pool, key in candidates[: min(count, MAX_STORAGE_BATCH)]
            ]
        }

    def _remove(self, p: dict) -> dict:
        """Remove keys the caller owns, only if still at that location and
        not leased; a lease taken after ``victims`` keeps the key.

        REMOVED and ABSENT both free the page for the owner: ABSENT means no
        key refers to that page any more, which is how a retried remove
        whose first reply was lost learns that it succeeded.
        """
        session = self._session(p)
        regions = self._owned_regions(session)
        statuses = []
        for key, loc in self._entries(p):
            pool = regions.get(loc.pool_id)
            if pool is None:
                statuses.append("KEPT")
            elif pool.replicas.get(key) == loc and not pool.leased.get(key):
                self._forget(pool, key)
                statuses.append("REMOVED")
            elif (
                loc.allocation_id not in pool.allocations
                and loc.offset not in pool.pages
            ):
                statuses.append("ABSENT")
            else:
                statuses.append("KEPT")
        return {"statuses": statuses}

    def _forget(self, pool: StoragePool, key: str) -> None:
        loc = pool.replicas.pop(key)
        pool.allocations.pop(loc.allocation_id, None)
        pool.pages.discard(loc.offset)
        pool.last_use.pop(key, None)
        if self._remote_cxl_index.get((pool.namespace, key)) is pool:
            del self._remote_cxl_index[(pool.namespace, key)]

    # ------------------------------------------------------------ return

    def _hide(self, region: StoragePool) -> None:
        """Stop finding a region's keys; its bytes and leases stay."""
        region.visible = False
        for key in region.replicas:
            if self._remote_cxl_index.get((region.namespace, key)) is region:
                del self._remote_cxl_index[(region.namespace, key)]

    def _return_region(self, region: StoragePool) -> None:
        """The owner gave the region back after its remote CXL agent released it.

        No RDMA reaches the region any more, so it goes back to the resource
        manager at once. Readers that still hold a lease on its keys learn
        on release that the lease was lost and drop what they read.
        """
        self._hide(region)
        self._void_leases(region)
        region.replicas.clear()
        region.allocations.clear()
        region.pages.clear()
        region.last_use.clear()
        self._release_region(region)

    def _return(self, p: dict) -> dict:
        """Give one region back, as a cxl Handler's ``return_alloc``.

        Accepted from an expired session too, like a drained close: the
        owner sends it only after the remote CXL agent released the region.
        """
        if p.get("server_epoch") != self.server_epoch:
            raise ValueError("Server epoch changed")
        session = self._sessions.get(p.get("session_token"))
        if session is None:
            raise ValueError("Storage session is absent")
        pool_id = self._text(p.get("pool_id"), "pool_id")
        region = session.regions.pop(pool_id, None)
        if region is None:
            if pool_id in session.returned:
                return {}
            raise ValueError("Not a region of this session")
        session.returned[pool_id] = None
        _bound(session.returned, MAX_REGIONS_PER_SESSION)
        self._return_region(region)
        return {}

    def _void_leases(self, region: StoragePool) -> None:
        """End every lease on a region; their releases report held=False."""
        for session in self._sessions.values():
            for lease_id, held in list(session.remote_cxl_leases.items()):
                if any(pool is region for pool, _ in held):
                    del session.remote_cxl_leases[lease_id]
                    self._drop_remote_cxl_lease(
                        [(pool, key) for pool, key in held if pool is not region]
                    )
        region.leased.clear()

    def _release_region(self, region: StoragePool) -> None:
        try:
            released = self._release_cxl(region.owner_id, region.handle.region_id)
        except Exception:
            # The RM may not have freed it: retried as an unconfirmed release.
            logger.warning(
                "releasing remote CXL region %s at the resource manager failed",
                region.pool_id,
                exc_info=True,
            )
            released = False
        if released:
            self._releasing.pop(region.pool_id, None)
        else:
            self._releasing[region.pool_id] = region

    def _retry_releases(self) -> None:
        """Retry RM releases at most once a second (each may wait for the RM)."""
        now = self._clock()
        if not self._releasing or now < self._next_release_retry:
            return
        self._next_release_retry = now + RELEASE_RETRY_S
        for region in list(self._releasing.values()):
            self._release_region(region)

    def _close(self, p: dict) -> dict:
        if p.get("server_epoch") != self.server_epoch:
            raise ValueError("Server epoch changed")
        token = self._text(p.get("session_token"), "session_token")
        if p.get("drained") is not True:
            raise ValueError("Storage pools must be drained before close")
        if token not in self._closed_tokens and len(self._closed_tokens) >= 65536:
            raise ValueError("Storage session tombstone limit reached")
        self._closed_tokens.add(token)
        session = self._sessions.get(token)
        if session is not None:
            session.active = False
            if self._engines.get(session.engine_id) == token:
                del self._engines[session.engine_id]
            self._drop_reader_leases(session)
            self._note_boots(session, p.get("pools"))
            self._take_over(session)
            cxl = session.pools.get("cxl")
            if cxl and not self._release_cxl(cxl.owner_id, cxl.handle.region_id):
                raise ValueError("CXL pool release unconfirmed; retry drained close")
            del self._sessions[token]
            self._expired_remote.pop(token, None)
        return {}

    # ------------------------------------------------------------------ usage

    # ------------------------------------------------------------ take over

    def _take_over(self, session: StorageSession) -> None:
        """MaruServer takes the regions of a session whose owner left.

        A region with keys is kept and its keys stay found; one without keys
        goes back. Either way the region keeper deals with its remote CXL agent
        first. A resent ``return_region`` of a taken region succeeds.
        """
        for region in session.regions.values():
            if region.replicas:
                region.fate = "keep"
            else:
                region.fate = "return"
                self._hide(region)
                self._void_leases(region)
            self._departed[region.pool_id] = region
            session.returned[region.pool_id] = None
        _bound(session.returned, MAX_REGIONS_PER_SESSION)
        if session.regions:
            self.keeper_wakeup.set()
        session.regions.clear()

    @staticmethod
    def _note_boots(session: StorageSession, reports: object) -> None:
        """Record the boot ids an owner sent with its drained close."""
        if not isinstance(reports, list):
            return
        for report in reports:
            if not isinstance(report, dict):
                continue
            region = session.regions.get(report.get("pool_id"))
            boot = report.get("boot")
            if region is not None and isinstance(boot, str) and boot:
                region.boot = boot

    def departed(self) -> list[tuple[str, dict, str, str]]:
        """Regions whose owner left: ``(pool_id, handle, fate, boot)``.

        ``fate`` is ``"keep"`` (hold the mapping) or ``"return"`` (release,
        then :meth:`given_back`); ``boot`` is the boot the owner last
        reported for the region's memory node ("" if none).
        """
        with self._lock:
            self._expire()
            return [
                (pool.pool_id, pool.handle.to_dict(), pool.fate, pool.boot)
                for pool in self._departed.values()
            ]

    def give_back(self, pool_id: str, reason: str) -> None:
        """Stop finding a kept region's keys; the keeper then releases it.

        Args:
            pool_id: The region.
            reason: Why (logged).
        """
        with self._lock:
            region = self._departed.get(pool_id)
            if region is None or region.fate == "return":
                return
            logger.info(
                "giving back remote CXL region %d: %s", region.handle.region_id, reason
            )
            region.fate = "return"
            self._hide(region)
            self._void_leases(region)
            self.keeper_wakeup.set()

    def given_back(self, pool_id: str) -> None:
        """Its remote CXL agent released a region: free it at the RM.

        Args:
            pool_id: The region.
        """
        with self._lock:
            region = self._departed.pop(pool_id, None)
            if region is not None:
                self._return_region(region)

    def make_room(self) -> int:
        """Give back a kept region when the RM refused a region request.

        The kept region read longest ago goes. One goes at a time: while it
        is on its way to the RM, further refusals give back no other, since
        the space it frees may be what the next request needs.

        Returns:
            How many kept regions are given back now (0 or 1).
        """
        with self._lock:
            wanted, self._refused_reservations = self._refused_reservations, 0
            now = self._clock()
            for pool_id, since in list(self._room_pending.items()):
                if pool_id not in self._departed or now - since > ROOM_WAIT_S:
                    del self._room_pending[pool_id]
            if not wanted or self._room_pending:
                return 0
            kept = [r for r in self._departed.values() if r.fate == "keep"]
            if not kept:
                return 0
            oldest = min(kept, key=lambda r: max(r.last_use.values(), default=0.0))
            self._room_pending[oldest.pool_id] = now
        self.give_back(oldest.pool_id, "the resource manager is full")
        return 1

    def _pool_usage(self, s: StorageSession | None, pool: StoragePool) -> dict:
        leases = s.leases.values() if s is not None else ()
        return {
            "pool_id": pool.pool_id,
            "engine_id": s.engine_id if s is not None else "",
            "node_id": s.node_id if s is not None else "",
            "medium": pool.medium,
            "tier": "L1",
            "scope": "namespace" if pool.medium == REMOTE_CXL else "process",
            "active": s.active if s is not None else False,
            "owner_instance_id": pool.owner_id,
            "capacity_bytes": pool.capacity,
            "reserved_bytes": pool.handle.length if pool.handle else pool.capacity,
            "ready_bytes": sum(loc.length for loc in pool.replicas.values()),
            "ready_allocated_bytes": len(pool.pages) * pool.page_size,
            "allocated_bytes": max(
                pool.allocated_bytes, len(pool.pages) * pool.page_size
            ),
            "quarantined_bytes": pool.quarantined_bytes,
            "pinned_bytes": 0,
            "replicas": len(pool.replicas),
            "read_leases": (
                sum(pool.leased.values())
                if pool.medium == REMOTE_CXL
                else sum(
                    any(loc.pool_id == pool.pool_id for loc in lease)
                    for lease in leases
                )
            ),
            "acquired_objects": pool.acquired_objects,
            "write_order": list(s.write_order) if s is not None else [],
            "read_order": list(s.read_order) if s is not None else [],
        }

    def _usage(self, medium=None) -> dict:
        pools = [
            (s, pool) for s in self._sessions.values() for pool in self._all_pools(s)
        ] + [(None, pool) for pool in self._departed.values()]
        return {
            "server_epoch": self.server_epoch,
            "host_capacity_limit": self.capacity_limit,
            "pools": [
                self._pool_usage(s, pool)
                for s, pool in pools
                if medium is None or pool.medium == medium
            ],
        }

    def get_usage(self, medium=None) -> dict:
        with self._lock:
            self._expire()
            return self._usage(medium)
