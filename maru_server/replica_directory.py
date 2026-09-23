# SPDX-License-Identifier: Apache-2.0
"""Owner-scoped, fixed CPU/CXL L1 pools, isolated from legacy KVManager.

M2a keeps CXL access within the owning worker as well: cross-engine mapping,
replication and eviction need their own reader lifetime protocol.
"""

import time
import uuid
from dataclasses import asdict, dataclass, field
from threading import RLock

from maru_common.storage_policy import FixedOrderPolicy, validate_order
from maru_common.storage_types import MAX_STORAGE_BATCH, StorageLocation


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


class ReplicaDirectory:
    def __init__(
        self,
        capacity_limit: int = 64 * 1024**3,
        session_ttl: float = 30,
        reserve_cxl=None,
        release_cxl=None,
    ):
        if capacity_limit <= 0 or session_ttl <= 0:
            raise ValueError("CPU capacity limit and session TTL must be positive")
        self.server_epoch = uuid.uuid4().hex
        self.capacity_limit = capacity_limit
        self.session_ttl = session_ttl
        self._reserve_cxl = reserve_cxl
        self._release_cxl = release_cxl
        self._sessions: dict[str, StorageSession] = {}
        self._engines: dict[str, str] = {}
        self._closed_tokens: set[str] = set()
        self._lock = RLock()

    @property
    def supports_mixed(self):
        return self._reserve_cxl is not None and self._release_cxl is not None

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

    def _expire(self):
        now = time.monotonic()
        for session in self._sessions.values():
            if session.active and now >= session.deadline:
                session.active = False
                if self._engines.get(session.engine_id) == session.token:
                    del self._engines[session.engine_id]
                # Timeout is not proof of unmapping. Charge both pools until ACK.

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
            "heartbeat": self._heartbeat,
            "commit": self._commit,
            "resolve": self._resolve,
            "probe": self._probe,
            "acquire": self._acquire,
            "release": self._release,
            "close": self._close,
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
        capacity = self._integer(p.get("capacity"), "capacity", 1)
        page_size = self._integer(p.get("page_size"), "page_size", 1)
        cxl_capacity = self._integer(p.get("cxl_capacity", 0), "cxl_capacity")
        for size in (capacity, cxl_capacity):
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
        read_order = validate_order(p.get("read_order", ["cpu", "cxl"]))
        write_order = validate_order(p.get("write_order", ["cpu", "cxl"]))
        previous = self._sessions.get(token)
        if previous is not None:
            cpu = previous.pools["cpu"]
            cxl = previous.pools.get("cxl")
            expected = (
                engine,
                namespace,
                node,
                schema,
                capacity,
                page_size,
                cxl_capacity,
                read_order,
                write_order,
            )
            actual = (
                previous.engine_id,
                previous.namespace,
                previous.node_id,
                previous.schema,
                cpu.capacity,
                cpu.page_size,
                cxl.capacity if cxl else 0,
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
        granted = sum(
            s.pools["cpu"].capacity
            for s in self._sessions.values()
            if s.node_id == node
        )
        if granted + capacity > self.capacity_limit:
            raise ValueError(
                "CPU host capacity limit exceeded (includes expired grants)"
            )
        pools = {"cpu": StoragePool(uuid.uuid4().hex, "cpu", capacity, page_size)}
        if cxl_capacity:
            owner_id = "l1:" + uuid.uuid4().hex
            handle = self._reserve_cxl(owner_id, cxl_capacity)
            if handle is None:
                raise ValueError("CXL pool reservation failed")
            pools["cxl"] = StoragePool(
                uuid.uuid4().hex, "cxl", cxl_capacity, page_size, handle, owner_id
            )
        session = StorageSession(
            token,
            engine,
            namespace,
            node,
            schema,
            pools,
            read_order,
            write_order,
            time.monotonic() + self.session_ttl,
        )
        self._sessions[token] = session
        self._engines[engine] = token
        return self._grant(session)

    def _grant(self, session):
        cpu = session.pools["cpu"]
        return {
            "pool_id": cpu.pool_id,
            "server_epoch": self.server_epoch,
            "session_ttl": self.session_ttl,
            "capacity": cpu.capacity,
            "pools": [
                {
                    "pool_id": pool.pool_id,
                    "medium": pool.medium,
                    "capacity": pool.capacity,
                    **(
                        {"handle": pool.handle.to_dict()}
                        if pool.handle is not None
                        else {}
                    ),
                }
                for pool in session.pools.values()
            ],
        }

    def _heartbeat(self, p: dict) -> dict:
        session = self._session(p)
        seq = self._integer(p.get("sequence"), "sequence")
        reports = p.get("pools")
        if reports is None:
            reports = [{"pool_id": session.pools["cpu"].pool_id, **p}]
        if not isinstance(reports, list) or len(reports) != len(session.pools):
            raise ValueError("Invalid pool usage report")
        by_id = {pool.pool_id: pool for pool in session.pools.values()}
        parsed = []
        for report in reports:
            pool = by_id.pop(report["pool_id"])
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
        session.deadline = time.monotonic() + self.session_ttl
        return {}

    def _keys(self, p: dict) -> list[str]:
        keys = p.get("keys")
        if not isinstance(keys, list) or len(keys) > MAX_STORAGE_BATCH:
            raise ValueError("Invalid storage key batch")
        return [self._text(key, "key") for key in keys]

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
        for entry in entries:
            key = self._text(entry["key"], "key")
            loc = StorageLocation(**entry["location"])
            pool = session.pools.get(loc.medium)
            if pool is None or loc.pool_id != pool.pool_id:
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
        statuses = []
        for key, loc, pool in parsed:
            if key in pool.replicas:
                status = "ALREADY_PRESENT"
            elif loc.allocation_id in pool.allocations or loc.offset in pool.pages:
                status = "REJECTED"
            else:
                pool.replicas[key] = loc
                pool.allocations[loc.allocation_id] = key
                pool.pages.add(loc.offset)
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

    def _probe(self, p: dict) -> dict:
        keys = self._keys(p)
        session = self._sessions.get(self._engines.get(p.get("engine_id")))
        valid = (
            session is not None
            and session.active
            and session.namespace == p.get("namespace")
        )
        return {
            "results": [bool(valid and self._select(session, key)) for key in keys],
            "pool_id": session.pools["cpu"].pool_id if valid else None,
        }

    def _acquire(self, p: dict) -> dict:
        session = self._session(p)
        keys = self._keys(p)
        if len(session.leases) >= 4096:
            raise ValueError("Storage read lease limit reached")
        entries = [self._select(session, key) for key in keys]
        lease_id = uuid.uuid4().hex if any(entries) else None
        if lease_id is not None:
            session.leases[lease_id] = [loc for loc in entries if loc]
            for loc in session.leases[lease_id]:
                session.pools[loc.medium].acquired_objects += 1
        return {
            "entries": [asdict(loc) if loc else None for loc in entries],
            "lease_id": lease_id,
        }

    def _release(self, p: dict) -> dict:
        if p.get("server_epoch") != self.server_epoch:
            raise ValueError("Server epoch changed")
        session = self._sessions.get(p.get("session_token"))
        if session is not None:
            session.leases.pop(p.get("lease_id"), None)
        return {}

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
            cxl = session.pools.get("cxl")
            if cxl and not self._release_cxl(cxl.owner_id, cxl.handle.region_id):
                raise ValueError("CXL pool release unconfirmed; retry drained close")
            del self._sessions[token]
        return {}

    def _usage(self, medium=None) -> dict:
        return {
            "server_epoch": self.server_epoch,
            "host_capacity_limit": self.capacity_limit,
            "pools": [
                {
                    "pool_id": pool.pool_id,
                    "engine_id": s.engine_id,
                    "node_id": s.node_id,
                    "medium": pool.medium,
                    "tier": "L1",
                    "scope": "process",
                    "active": s.active,
                    "owner_instance_id": pool.owner_id,
                    "capacity_bytes": pool.capacity,
                    "reserved_bytes": pool.handle.length
                    if pool.handle
                    else pool.capacity,
                    "ready_bytes": sum(loc.length for loc in pool.replicas.values()),
                    "ready_allocated_bytes": len(pool.pages) * pool.page_size,
                    "allocated_bytes": max(
                        pool.allocated_bytes, len(pool.pages) * pool.page_size
                    ),
                    "quarantined_bytes": pool.quarantined_bytes,
                    "pinned_bytes": 0,
                    "replicas": len(pool.replicas),
                    "read_leases": sum(
                        any(loc.pool_id == pool.pool_id for loc in lease)
                        for lease in s.leases.values()
                    ),
                    "acquired_objects": pool.acquired_objects,
                    "write_order": list(s.write_order),
                    "read_order": list(s.read_order),
                }
                for s in self._sessions.values()
                for pool in s.pools.values()
                if medium is None or pool.medium == medium
            ],
        }

    def get_usage(self, medium=None) -> dict:
        with self._lock:
            self._expire()
            return self._usage(medium)
