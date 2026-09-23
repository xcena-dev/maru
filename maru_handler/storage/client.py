# SPDX-License-Identifier: Apache-2.0
"""Owner-scoped CPU and mixed L1 storage; only metadata crosses the RPC."""

import logging
import socket
import threading
import uuid
from dataclasses import asdict

from maru_common.storage_types import (
    MAX_STORAGE_BATCH,
    MIXED_STORAGE_CAPABILITY,
    STORAGE_CAPABILITY,
    CpuLocation,
    StorageError,
    StorageUnavailableError,
)

from .cpu import CpuAllocation, CpuPool, CpuReadLease
from .mixed import MixedPool

logger = logging.getLogger(__name__)


class CpuStorageClient:
    def __init__(self, config, rpc):
        self.config = config
        self.rpc = rpc
        self.pool: CpuPool | MixedPool | None = None
        self.connected = False
        self._credentials: dict = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._reads = 0
        self._sequence = 0
        self._keys: dict[str, CpuAllocation] = {}
        self._pending: dict[str, tuple[list[str], list[CpuAllocation]]] = {}

    def connect(self) -> bool:
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError(
                    "Create a fresh CPU handler after close/session failure"
                )
            if self.connected:
                return True
            self.rpc.connect()
            try:
                handshake = self.rpc.handshake()
                if not handshake.get("success", True):
                    raise StorageUnavailableError(
                        handshake.get("error", "Handshake failed")
                    )
                if STORAGE_CAPABILITY not in handshake.get("capabilities", []):
                    raise StorageError("MaruServer does not support cpu_storage_v1")
                mixed = self.config.storage_backend == "mixed"
                if mixed and MIXED_STORAGE_CAPABILITY not in handshake.get(
                    "capabilities", []
                ):
                    raise StorageError(
                        "Mixed storage requires mixed_storage_v1 on a CXL-enabled server"
                    )
                self._credentials = {
                    "server_epoch": handshake["server_epoch"],
                    "session_token": uuid.uuid4().hex,
                }
                if not self.config.metadata_only:
                    page_size = self.config.chunk_size_bytes
                    capacity = self.config.pool_size // page_size * page_size
                    grant = self._call(
                        "open",
                        engine_id=self.config.engine_id,
                        namespace=self.config.cache_namespace,
                        schema=self.config.storage_schema,
                        node_id=self.config.node_id or socket.gethostname(),
                        capacity=capacity,
                        page_size=page_size,
                        cxl_capacity=(
                            self.config.cxl_pool_size // page_size * page_size
                            if mixed
                            else 0
                        ),
                        read_order=list(self.config.read_order),
                        write_order=list(self.config.write_order),
                    )
                    self.pool = (
                        MixedPool(
                            grant["pools"],
                            page_size,
                            handshake.get("rm_address") or self.config.rm_address,
                            self.config.write_order,
                        )
                        if mixed
                        else CpuPool(grant["pool_id"], capacity, page_size)
                    )
                    interval = grant["session_ttl"] / 3
                    self._thread = threading.Thread(
                        target=self._heartbeat_loop,
                        args=(interval,),
                        daemon=True,
                        name="maru-cpu-heartbeat",
                    )
                self.connected = True
                if self._thread is not None:
                    self._thread.start()
                return True
            except Exception:
                # Even an open timeout may have reserved a grant.
                if self.pool is not None:
                    self.pool.close()
                try:
                    if self._credentials and not self.config.metadata_only:
                        self._call("close", drained=True)
                except Exception:
                    logger.warning("CPU open cleanup could not confirm grant release")
                self.rpc.close()
                raise

    def _call(self, action: str, **payload) -> dict:
        return self.rpc.storage(action, {**self._credentials, **payload})

    def _ensure(self, data: bool = True) -> None:
        if not self.connected or self._stop.is_set():
            raise StorageUnavailableError("CPU cache session is unavailable")
        if data and self.pool is None:
            raise RuntimeError("A metadata-only client cannot access CPU buffers")

    def _heartbeat_loop(self, interval: float) -> None:
        while not self._stop.wait(interval):
            try:
                with self._lock:
                    self._sequence += 1
                    self._call(
                        "heartbeat", sequence=self._sequence, **self.pool.usage()
                    )
                    self._resolve_pending()
            except StorageError:
                # A new epoch/expired session is not reconnectable with the
                # old allocations. Fail closed until this worker restarts.
                logger.warning(
                    "CPU cache session expired; restart worker to re-enable cache"
                )
                with self._lock:
                    self.connected = False
                    self._stop.set()
                return
            except Exception:
                # Discovery is revalidated by acquire. A network timeout is
                # not proof of a lost session, nor permission to free memory.
                logger.warning("CPU cache heartbeat failed", exc_info=True)

    def alloc(self, size: int) -> CpuAllocation:
        with self._lock:
            self._ensure()
            return self.pool.alloc(size)

    def free(self, handle: CpuAllocation) -> None:
        with self._lock:
            if self.pool is not None:
                self.pool.free(handle)

    def has_local(self, key: str) -> bool:
        with self._lock:
            return self.connected and not self._stop.is_set() and key in self._keys

    def batch_store(self, keys: list[str], handles: list[CpuAllocation]) -> list[bool]:
        if len(keys) != len(handles):
            raise ValueError("keys and handles must have the same length")
        if len(keys) > MAX_STORAGE_BATCH:
            out = []
            for i in range(0, len(keys), MAX_STORAGE_BATCH):
                out.extend(
                    self.batch_store(
                        keys[i : i + MAX_STORAGE_BATCH],
                        handles[i : i + MAX_STORAGE_BATCH],
                    )
                )
            return out
        if not keys:
            return []
        with self._lock:
            self._ensure()
            if len({id(h) for h in handles}) != len(handles):
                raise ValueError(
                    "A CPU allocation can only be committed once per batch"
                )
            for key, handle in zip(keys, handles, strict=True):
                if not isinstance(key, str) or not key or len(key) > 4096:
                    raise ValueError("Invalid CPU key")
                self.pool.validate(handle)
                if handle.state != "writing":
                    raise ValueError("CPU allocation was already submitted")
            op_id = uuid.uuid4().hex
            self._pending[op_id] = (keys[:], handles[:])
            for handle in handles:
                handle.state = "quarantined"
            entries = [
                {"key": key, "location": asdict(h.location)}
                for key, h in zip(keys, handles, strict=True)
            ]
            try:
                result = self._call("commit", op_id=op_id, entries=entries)
            except StorageError:
                # Explicit rejection happens before mutation. The server's
                # per-entry rejections are represented in statuses instead.
                self._finish_commit(op_id, ["REJECTED"] * len(keys))
                return [False] * len(keys)
            except Exception:
                logger.warning("CPU commit outcome unknown; retaining allocations")
                return [False] * len(keys)
            return self._finish_commit(op_id, result.get("statuses", []))

    def _finish_commit(self, op_id: str, statuses: list[str]) -> list[bool]:
        keys, handles = self._pending[op_id]
        if len(statuses) != len(keys) or any(
            s not in {"CREATED", "ALREADY_PRESENT", "REJECTED"} for s in statuses
        ):
            return [False] * len(keys)  # retain uncertain ownership
        for key, handle, status in zip(keys, handles, statuses, strict=True):
            if status == "CREATED":
                handle.state = "ready"
                self._keys[key] = handle
            else:
                handle.state = "writing"
                self.pool.free(handle)
        del self._pending[op_id]
        return [s != "REJECTED" for s in statuses]

    def _resolve_pending(self) -> None:
        for op_id in list(self._pending)[:32]:
            result = self._call("resolve", op_id=op_id)
            if result.get("state") == "COMMITTED":
                self._finish_commit(op_id, result["statuses"])
            elif result.get("state") == "UNKNOWN":
                keys, handles = self._pending[op_id]
                entries = [
                    {"key": key, "location": asdict(handle.location)}
                    for key, handle in zip(keys, handles, strict=True)
                ]
                result = self._call("commit", op_id=op_id, entries=entries)
                self._finish_commit(op_id, result.get("statuses", []))

    def batch_exists(self, keys: list[str]) -> list[bool]:
        self._ensure(data=False)
        out = []
        for i in range(0, len(keys), MAX_STORAGE_BATCH):
            batch = keys[i : i + MAX_STORAGE_BATCH]
            try:
                result = self._call(
                    "probe",
                    engine_id=self.config.engine_id,
                    namespace=self.config.cache_namespace,
                    keys=batch,
                )
                values = result.get("results", [])
                out.extend(
                    values if len(values) == len(batch) else [False] * len(batch)
                )
            except Exception:
                out.extend([False] * len(batch))
        return out

    def batch_retrieve(self, keys: list[str]) -> list[CpuReadLease | None]:
        if len(keys) > MAX_STORAGE_BATCH:
            out = []
            try:
                for i in range(0, len(keys), MAX_STORAGE_BATCH):
                    out.extend(self.batch_retrieve(keys[i : i + MAX_STORAGE_BATCH]))
            except Exception:
                for info in out:
                    if info is not None:
                        info.release()
                raise
            return out
        with self._lock:
            self._ensure()
            result = self._call("acquire", keys=keys)
            lease_id = result.get("lease_id")
            views = []
            try:
                entries = result["entries"]
                if len(entries) != len(keys):
                    raise StorageUnavailableError("Malformed CPU acquire response")
                for entry in entries:
                    location = CpuLocation(**entry) if entry else None
                    views.append(
                        (self.pool.resolve(location), location) if location else None
                    )
            except Exception:
                for view in views:
                    if view:
                        view[0].release()
                if lease_id:
                    self._release_server_lease(lease_id)
                raise
            remaining = sum(v is not None for v in views)
            self._reads += remaining

            def release():
                nonlocal remaining
                with self._lock:
                    self._reads -= 1
                    remaining -= 1
                    if remaining == 0 and lease_id:
                        self._release_server_lease(lease_id)

            return [
                CpuReadLease(view[0], view[1], release) if view else None
                for view in views
            ]

    def _release_server_lease(self, lease_id: str) -> None:
        try:
            self._call("release", lease_id=lease_id)
        except Exception:
            # No eviction in M1. A later drained close acknowledges all leases.
            logger.warning("CPU read lease release could not be confirmed")

    def close(self) -> None:
        with self._lock:
            if self._reads:
                raise RuntimeError("Release CPU read leases before closing the handler")
            self._stop.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None
        with self._lock:
            if self.pool is not None:
                self.pool.close()
                try:
                    self._call("close", drained=True)
                except Exception:
                    logger.warning(
                        "CPU pool retired; server grant release is unconfirmed"
                    )
                self.pool = None
            self._keys.clear()
            self._pending.clear()
            self.connected = False
            self.rpc.close()
