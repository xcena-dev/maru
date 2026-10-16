# SPDX-License-Identifier: Apache-2.0
"""Several remote CXL pools behind one worker, one MaruServer each."""

from __future__ import annotations

import hashlib
import logging
import threading
from collections.abc import Callable
from typing import Any

from maru_common.storage_types import StorageError, StorageUnavailableError

from ..cpu import CpuReadLease
from .client import RemoteCxlStorageClient
from .datapath import RdmaDataPath
from .region import RemoteCxlAllocation

logger = logging.getLogger(__name__)

POOL_RETRY_S = 5.0


def _route_score(pool: str, route_key: str) -> bytes:
    return hashlib.sha256(f"{pool}|{route_key}".encode()).digest()


class PoolRouter:
    """Several remote CXL pools, one MaruServer each; a request stays in one pool.

    The connector only uses chunks that hit contiguously from the start of a
    prompt, so every chunk of a request is routed by one key (the request's
    first chunk key) with rendezvous hashing over the pools' server URLs:
    adding or removing a pool moves only the prefixes that hash to it.

    A pool whose MaruServer cannot be reached at start does not stop the
    worker: the prefixes routed to it miss, and a background thread connects
    it every :data:`POOL_RETRY_S` seconds until it answers.
    """

    def __init__(
        self,
        config: Any,
        rpcs: list,
        urls: list[str],
        *,
        transport_factory: Callable[[str], Any] | None = None,
        mapper_factory: Callable[[str], Any] | None = None,
    ) -> None:
        """Create one client per pool sharing one data path.

        Args:
            config: ``MaruConfig`` of the worker.
            rpcs: One MaruServer RPC client per pool.
            urls: The pools' MaruServer URLs (the routing identities).
            transport_factory: NIXL transport factory.
            mapper_factory: Region mapper client factory.

        Raises:
            ValueError: if ``rpcs`` and ``urls`` do not pair up one to one.
        """
        if len(rpcs) != len(urls) or len(set(urls)) != len(urls):
            raise ValueError("One RPC client per distinct MaruServer URL is required")
        self.config = config
        self.urls = urls
        self.data = RdmaDataPath(config, transport_factory, mapper_factory)
        self.clients = [
            RemoteCxlStorageClient(config, rpc, data=self.data) for rpc in rpcs
        ]
        self._stop = threading.Event()
        self._retry: threading.Thread | None = None
        self._connect_lock = threading.Lock()

    @property
    def connected(self) -> bool:
        """Whether at least one pool is connected."""
        return any(c.connected for c in self.clients)

    def connect(self) -> bool:
        """Connect every pool that answers; at least one must.

        Raises:
            StorageError: if a MaruServer refused the configuration.
            Exception: the first failure if no pool could be connected.
        """
        if not self.config.metadata_only:
            self.data.start()
        failures = []
        for c in self.clients:
            try:
                c.connect()
            except StorageError:
                self._close_clients()
                self.data.close()
                raise
            except Exception as exc:
                failures.append(exc)
                logger.warning(
                    "Remote CXL pool %s is unavailable: %s", c.config.server_url, exc
                )
        if len(failures) == len(self.clients):
            self.data.close()
            raise failures[0]
        if failures:
            self._retry = threading.Thread(
                target=self._retry_loop, daemon=True, name="maru-remote-cxl-pool-retry"
            )
            self._retry.start()
        return True

    def _ensure(self, data: bool = False) -> None:
        """Raise unless some pool is connected (``MaruHandler`` calls this)."""
        if not self.connected:
            raise StorageUnavailableError("No remote CXL pool is available")

    def _retry_loop(self) -> None:
        """Connect the pools that never connected until each one answers."""
        while not self._stop.wait(POOL_RETRY_S):
            waiting = [c for c in self.clients if not c.ever_connected]
            if not waiting:
                return
            for c in waiting:
                with self._connect_lock:
                    if self._stop.is_set():
                        return
                    try:
                        c.connect()
                        logger.info(
                            "Remote CXL pool %s is available", c.config.server_url
                        )
                    except Exception as exc:
                        logger.debug("Remote CXL pool still unavailable: %s", exc)

    def _pick(self, route_key: str | None) -> RemoteCxlStorageClient:
        if not route_key:
            raise ValueError("Several remote CXL pools need a route key per request")
        best = max(
            range(len(self.urls)), key=lambda i: _route_score(self.urls[i], route_key)
        )
        return self.clients[best]

    def _owner(self, handle: RemoteCxlAllocation) -> RemoteCxlStorageClient:
        for c in self.clients:
            if c.regions.get(handle.region.pool_id) is handle.region:
                return c
        raise ValueError("Allocation does not belong to a current remote CXL region")

    def alloc(self, size: int, route_key: str | None = None) -> RemoteCxlAllocation:
        """Allocate in the pool the route key picks.

        Raises:
            MemoryError: if that pool is unavailable or full.
        """
        c = self._pick(route_key)
        if not c.connected:
            raise MemoryError("Remote CXL pool of this prefix is unavailable")
        return c.alloc(size)

    def free(self, handle: RemoteCxlAllocation) -> None:
        """Give back an allocation that was not stored."""
        # A handle of a closed session only gives back its slot in the shared
        # staging buffer, which any client can do.
        for c in self.clients:
            if c.regions.get(handle.region.pool_id) is handle.region:
                c.free(handle)
                return
        self.clients[0].free(handle)

    def retrieve_capacity(self) -> int | None:
        """How many keys the next batch_retrieve should ask for at most (the
        pools share one staging buffer)."""
        staging = self.data.staging
        return None if staging is None else staging.load_capacity()

    def storage_gpu_accessible(self) -> bool:
        """Whether the shared staging buffer is page-locked for GPU kernels."""
        staging = self.data.staging
        return staging is not None and staging.cuda_registered

    def has_local(self, key: str) -> bool:
        """Whether a connected pool committed ``key`` for this worker."""
        return any(c.has_local(key) for c in self.clients if c.connected)

    def batch_store(
        self, keys: list[str], handles: list[RemoteCxlAllocation]
    ) -> list[bool]:
        """Store each allocation in the pool it was allocated from."""
        if len(keys) != len(handles):
            raise ValueError("keys and handles must have the same length")
        groups: dict[int, list[int]] = {}
        for i, handle in enumerate(handles):
            groups.setdefault(id(self._owner(handle)), []).append(i)
        by_id = {id(c): c for c in self.clients}
        out = [False] * len(keys)
        for cid, index in groups.items():
            results = by_id[cid].batch_store(
                [keys[i] for i in index], [handles[i] for i in index]
            )
            for i, ok in zip(index, results, strict=True):
                out[i] = ok
        return out

    def batch_exists(self, keys: list[str], route_key: str | None = None) -> list[bool]:
        """Probe the pool the route key (or the first key) picks."""
        if not keys:
            return []
        c = self._pick(route_key or keys[0])
        return c.batch_exists(keys) if c.connected else [False] * len(keys)

    def batch_retrieve(
        self, keys: list[str], route_key: str | None = None
    ) -> list[CpuReadLease | None]:
        """Read from the pool the route key (or the first key) picks."""
        if not keys:
            return []
        c = self._pick(route_key or keys[0])
        return c.batch_retrieve(keys) if c.connected else [None] * len(keys)

    def _close_clients(self) -> None:
        for c in self.clients:
            if c.connected or c.regions:
                c.close()

    def close(self) -> None:
        """Stop retrying, close every pool, then the shared data path."""
        self._stop.set()
        with self._connect_lock:
            pass  # a connect in progress finishes before the clients close
        if self._retry is not None:
            self._retry.join(timeout=5)
        self._close_clients()
        if any(c.has_unconfirmed_transfers() for c in self.clients):
            logger.warning(
                "RDMA transfers still own staging memory; leaving it registered"
            )
            return
        self.data.close()
