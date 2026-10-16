# SPDX-License-Identifier: Apache-2.0
"""Owner-scoped CPU/CXL L1 pools, and remote CXL regions read across sessions.

CPU and mixed pools keep access within the owning worker. Remote CXL regions live in
a memory node's CXL; a session gets one region when it opens and one more each
time it asks (``expand``), as a cxl Handler does with ``request_alloc``. Any
session of the same namespace can lease and read their keys; the owner evicts
its own keys in the order of last use across all sessions.

A region stays with its session until the owner gives it back, one region
at a time (``return_region``) or all of them with a drained close. Expiry
only hides its keys: a timeout does not prove that the owner stopped writing.
The owner gives a region back only after the region mapper deregistered it,
so no RDMA reaches it any more; the region goes back to the resource manager
at once, and leases other sessions still hold on its keys are reported lost
on release.
"""

import time
import uuid
from dataclasses import asdict, dataclass, field
from threading import RLock

from maru_common.storage_policy import FixedOrderPolicy, validate_order
from maru_common.storage_types import MAX_STORAGE_BATCH, StorageLocation

REMOTE_CXL = "remote_cxl"
MAX_REGIONS_PER_SESSION = 4096
RELEASE_RETRY_S = 1.0


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
    # Remote CXL regions owned by this session (pool_id -> region) and their size
    regions: dict[str, StoragePool] = field(default_factory=dict)
    region_size: int = 0
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
    ):
        if capacity_limit <= 0 or session_ttl <= 0:
            raise ValueError("CPU capacity limit and session TTL must be positive")
        self.server_epoch = uuid.uuid4().hex
        self.capacity_limit = capacity_limit
        self.session_ttl = session_ttl
        self._reserve_cxl = reserve_cxl
        self._release_cxl = release_cxl
        self._clock = clock or (lambda: time.monotonic())
        self._sessions: dict[str, StorageSession] = {}
        self._engines: dict[str, str] = {}
        self._closed_tokens: set[str] = set()
        # (namespace, key) -> remote CXL region holding the key
        self._remote_cxl_index: dict[tuple[str, str], StoragePool] = {}
        # Regions whose release the RM has not confirmed yet
        self._releasing: dict[str, StoragePool] = {}
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
                # Timeout is not proof of unmapping: pools and regions stay
                # charged until a drained close. Remote CXL keys stop being found,
                # and the leases this session held as a reader end with it.
                self._drop_reader_leases(session)
                for region in session.regions.values():
                    self._hide(region)
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
            first = next(iter(self._all_pools(previous)))
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
                first.page_size,
                previous.pools["cxl"].capacity if "cxl" in previous.pools else 0,
                previous.region_size,
                previous.read_order,
                previous.write_order,
            )
            if not previous.active or actual != expected:
                raise ValueError("Session token reused with a different registration")
            return self._grant(previous)
        if engine in self._engines:
            raise ValueError("Storage supports one active worker per engine_id")
        if len(self._sessions) >= 1024:
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
            region = self._reserve_region(namespace, schema, region_size, page_size)
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
        )
        self._sessions[token] = session
        self._engines[engine] = token
        return self._grant(session)

    def _check_remote_cxl_peers(self, namespace: str, schema: str, page_size: int):
        """Readers of another session's region must agree on the KV layout."""
        for session in self._sessions.values():
            if not session.active or session.namespace != namespace:
                continue
            for region in session.regions.values():
                if region.schema != schema or region.page_size != page_size:
                    raise ValueError(
                        "Remote CXL regions of this namespace use another schema or page size"
                    )

    def _reserve_region(
        self, namespace: str, schema: str, size: int, page_size: int
    ) -> StoragePool:
        owner_id = "remote_cxl:" + uuid.uuid4().hex
        handle = self._reserve_cxl(owner_id, size)
        if handle is None:
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
        first = session.pools.get("cpu") or next(iter(self._all_pools(session)))
        return {
            "pool_id": first.pool_id,
            "server_epoch": self.server_epoch,
            "session_ttl": self.session_ttl,
            "capacity": first.capacity,
            "pools": [self._pool_info(pool) for pool in self._all_pools(session)],
        }

    def _expand(self, p: dict) -> dict:
        """One more remote CXL region for the session, as ``request_alloc`` gives a
        cxl Handler one more region."""
        session = self._session(p)
        op_id = self._text(p.get("op_id"), "op_id")
        if not session.regions:
            raise ValueError("The session has no remote CXL regions")
        given = session.expansions.get(op_id)
        if given is not None and given in session.regions:
            return {"pool": self._pool_info(session.regions[given])}
        if len(session.regions) >= MAX_REGIONS_PER_SESSION:
            raise ValueError("Remote CXL region limit of the session reached")
        first = next(iter(session.regions.values()))
        region = self._reserve_region(
            session.namespace, session.schema, session.region_size, first.page_size
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
        exact = not session.regions
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
            parsed.append((pool, allocated, quarantined))
        if seq > session.report_sequence:
            session.report_sequence = seq
            for pool, allocated, quarantined in parsed:
                pool.allocated_bytes = allocated
                pool.quarantined_bytes = quarantined
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
        for key in keys:
            hit = bool(valid and self._select(session, key))
            if not hit and isinstance(namespace, str):
                # Remote CXL residency does not depend on the prober's session.
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
        for key in keys:
            loc = self._select(session, key)
            if loc is None:
                pool = self._remote_cxl_lookup(session.namespace, key)
                if pool is not None:
                    loc = pool.replicas[key]
                    pool.leased[key] = pool.leased.get(key, 0) + 1
                    pool.last_use[key] = now
                    pool.acquired_objects += 1
                    remote_cxl_held.append((pool, key))
                    regions[pool.pool_id] = pool.handle.to_dict()
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
        }

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
        if not session.regions:
            raise ValueError("The session has no remote CXL regions")
        return session.regions

    def _victims(self, p: dict) -> dict:
        """Least recently used, unleased keys of the caller's own regions."""
        session = self._session(p)
        count = self._integer(p.get("count"), "count", 1)
        candidates = [
            (pool, key)
            for pool in self._owned_regions(session).values()
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
        """The owner gave the region back after its region mapper released it.

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
        owner sends it only after the region mapper released the region.
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
        if self._release_cxl(region.owner_id, region.handle.region_id):
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
            for region in session.regions.values():
                self._return_region(region)
            session.regions.clear()
            cxl = session.pools.get("cxl")
            if cxl and not self._release_cxl(cxl.owner_id, cxl.handle.region_id):
                raise ValueError("CXL pool release unconfirmed; retry drained close")
            del self._sessions[token]
        return {}

    # ------------------------------------------------------------------ usage

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
        ]
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
