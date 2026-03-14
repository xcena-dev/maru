# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruHandler - Main interface for Maru shared memory KV cache client.

This module provides the primary entry point for clients to interact with
the Maru shared memory KV cache system.

Example:
    from maru import MaruConfig, MaruHandler

    config = MaruConfig(server_url="tcp://localhost:5555")
    with MaruHandler(config) as handler:
        # Zero-copy store: alloc → write to buf → store
        handle = handler.alloc(size=len(data))
        handle.buf[:] = data
        handler.store(key=12345, handle=handle)

        result = handler.retrieve(key=12345)  # returns MemoryInfo
"""

import ctypes
import logging
import threading

import numpy as np

from maru_common import MaruConfig
from maru_shm import MaruHandle

from .memory import (
    AllocHandle,
    DaxMapper,
    MemoryInfo,
    OwnedRegionManager,
    PagedMemoryAllocator,
)
from .rpc_client import RpcClient

logger = logging.getLogger(__name__)


def _gil_free_memcpy(dst: memoryview, src: memoryview | bytes, nbytes: int) -> None:
    """Copy *nbytes* from *src* into *dst*, releasing the GIL during copy.

    Uses ``ctypes.memmove`` which releases the GIL (all ctypes foreign-function
    calls do) for the actual memcpy, allowing other Python threads to run
    concurrently.
    """
    dst_c = (ctypes.c_char * nbytes).from_buffer(dst)
    if isinstance(src, memoryview) and not src.readonly:
        src_c = (ctypes.c_char * nbytes).from_buffer(src)
    elif isinstance(src, memoryview):
        # read-only memoryview — zero-copy view via numpy to get raw pointer
        arr = np.frombuffer(src[:nbytes], dtype=np.uint8)
        src_c = arr.ctypes.data
    else:
        # bytes — ctypes.memmove accepts bytes directly
        src_c = src
    ctypes.memmove(dst_c, src_c, nbytes)


class MaruHandler:
    """Main interface for Maru shared memory KV cache operations.

    This class handles:
    - Connection management to MaruServer
    - Memory mapping via DaxMapper
    - KV store/retrieve operations

    Thread-safety:
    - Read operations (exists, retrieve, batch_exists, batch_retrieve) are
      lock-free — they rely on RpcAsyncClient (already thread-safe) and
      DaxMapper's internal lock for lazy mapping.
    - Write operations (store, batch_store, delete) are serialized by
      ``_write_lock`` to guarantee atomicity of allocate-write-register.
    - ``close()`` sets ``_closing`` event to reject new operations, then
      acquires ``_write_lock`` to wait for in-flight writes before teardown.

    Architecture::

        MaruHandler
            ├── RpcClient (server communication, sole RPC owner)
            ├── DaxMapper (memory mapping via MaruShmClient, owns all mmap/munmap)
            ├── OwnedRegionManager (owned regions + allocation, no RPC)
            │   ├── OwnedRegion 1 (PagedMemoryAllocator)
            │   ├── OwnedRegion 2 (PagedMemoryAllocator)
            │   └── ...
            └── _key_to_location (key -> (region_id, page_index))
    """

    def __init__(self, config: MaruConfig | None = None):
        """Initialize MaruHandler.

        Args:
            config: Configuration object. If None, uses defaults.
        """
        self._config = config or MaruConfig()
        if self._config.use_async_rpc:
            from .rpc_async_client import RpcAsyncClient

            self._rpc = RpcAsyncClient(
                self._config.server_url,
                timeout_ms=self._config.timeout_ms,
                max_inflight=self._config.max_inflight,
            )
        else:
            self._rpc = RpcClient(
                self._config.server_url,
                timeout_ms=self._config.timeout_ms,
            )
        self._mapper = DaxMapper()

        # Managers (initialized on connect)
        self._owned: OwnedRegionManager | None = None

        # Thread-safety
        self._write_lock = threading.Lock()
        self._closing = threading.Event()

        # Connection state
        self._key_to_location: dict[str, tuple[int, int]] = {}
        self._connected = False

        logger.debug("Created MaruHandler with config: %s", self._config)

    # =========================================================================
    # Connection Management
    # =========================================================================

    def connect(self) -> bool:
        """Connect to the server and request a memory allocation.

        Returns:
            True if successful
        """
        if self._connected:
            return True

        try:
            # 1. Connect RPC client
            self._rpc.connect()

            # 2. Initialize managers
            self._owned = OwnedRegionManager(
                mapper=self._mapper,
                chunk_size=self._config.chunk_size_bytes,
            )

            # 3. Request initial owned region via RPC
            response = self._rpc.request_alloc(
                instance_id=self._config.instance_id,
                size=self._config.pool_size,
            )
            if not response.success or response.handle is None:
                logger.error(
                    "Failed to request initial allocation: %s",
                    getattr(response, "error", "unknown"),
                )
                self._owned = None
                self._rpc.close()
                return False

            # 4. Add region to OwnedRegionManager (mmap + allocator)
            try:
                self._owned.add_region(response.handle)
            except Exception:
                logger.error("Failed to init initial region", exc_info=True)
                try:
                    self._rpc.return_alloc(
                        self._config.instance_id,
                        response.handle.region_id,
                    )
                except Exception:
                    logger.debug(
                        "Failed to return allocation during cleanup", exc_info=True
                    )
                self._owned = None
                self._rpc.close()
                return False

            self._connected = True

            # 5. Pre-map shared regions (eager mapping)
            if self._config.eager_map:
                self._premap_shared_regions()

            logger.info(
                "Connected: chunk_size=%d",
                self._config.chunk_size_bytes,
            )
            return True

        except Exception:
            logger.error("Failed to connect", exc_info=True)
            return False

    def close(self) -> None:
        """Close the connection and return all allocations.

        Sets ``_closing`` event to reject new operations, then acquires
        ``_write_lock`` to wait for in-flight writes before teardown.
        """
        if not self._connected:
            return

        self._closing.set()  # reject new operations immediately

        try:
            with self._write_lock:
                # 1. Close owned regions (allocator cleanup only) → get region_ids
                owned_region_ids: list[int] = []
                if self._owned is not None:
                    owned_region_ids = self._owned.close()

                # 2. Return allocations to server via RPC
                for rid in owned_region_ids:
                    try:
                        self._rpc.return_alloc(self._config.instance_id, rid)
                    except Exception:
                        logger.error("Failed to return region %d", rid, exc_info=True)

                # 3. Unmap all regions (owned + shared) via DaxMapper
                self._mapper.close()

                # 4. Close RPC connection
                self._rpc.close()

        except Exception:
            logger.error("Error during close", exc_info=True)

        finally:
            self._connected = False
            self._owned = None
            self._key_to_location.clear()

    # =========================================================================
    # KV Operations
    # =========================================================================

    def alloc(self, size: int) -> AllocHandle:
        """Allocate a page and return a handle with a writable mmap memoryview.

        The caller writes directly to ``handle.buf``, then passes the handle
        to ``store(key, handle=handle)`` to register without copying.

        Args:
            size: Required bytes (must be <= chunk_size)

        Returns:
            AllocHandle with writable memoryview

        Raises:
            RuntimeError: If not connected or closing
            ValueError: If size exceeds chunk_size or allocation fails
        """
        self._ensure_connected()

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            chunk_size = self._owned.get_chunk_size()
            if size > chunk_size:
                raise ValueError(
                    f"Requested size {size} exceeds chunk_size {chunk_size}"
                )

            result = self._owned.allocate()
            if result is None:
                if not self._expand_region():
                    raise ValueError("Cannot allocate page: pool exhausted")
                result = self._owned.allocate()
                if result is None:
                    raise ValueError("Cannot allocate page after expansion")

            region_id, page_index = result

            buf = self._mapper.get_buffer_view(
                region_id,
                page_index * chunk_size,
                size,
            )
            if buf is None:
                self._owned.free(region_id, page_index)
                raise ValueError(f"Failed to get buffer view for region {region_id}")

            handle = AllocHandle(
                buf=buf,
                _region_id=region_id,
                _page_index=page_index,
                _size=size,
            )
            logger.debug(
                "alloc: size=%d, region=%d, page=%d",
                size,
                region_id,
                page_index,
            )
            return handle

    def free(self, handle: AllocHandle) -> None:
        """Free a page previously obtained via alloc().

        Can be called before store() (discard) or after (eviction).

        Args:
            handle: AllocHandle from alloc()

        Raises:
            ValueError: If handle is not tracked (already freed or invalid)
        """
        self._ensure_connected()

        with self._write_lock:
            region_id = handle._region_id
            page_index = handle._page_index

            # Find and remove the key mapping if stored
            key_to_remove = None
            for key, loc in self._key_to_location.items():
                if loc == (region_id, page_index):
                    key_to_remove = key
                    break

            if key_to_remove is not None:
                del self._key_to_location[key_to_remove]

            self._owned.free(region_id, page_index)

            logger.debug(
                "free: region=%d, page=%d, key=%s",
                region_id,
                page_index,
                key_to_remove,
            )

    def store(
        self,
        key: str,
        info: MemoryInfo | memoryview | None = None,
        prefix: bytes | None = None,
        *,
        data: memoryview | None = None,
        handle: AllocHandle | None = None,
    ) -> bool:
        """Store data to the KV cache.

        If ``handle`` is provided (zero-copy path), data is already written
        to the mmap region via alloc() and only register_kv is performed.
        Otherwise, allocate + memcpy + register are performed in one call.

        Args:
            key: The chunk key string
            info: MemoryInfo or memoryview with data
            prefix: Optional bytes to prepend (e.g., serialized metadata header)
            data: memoryview with data (preferred, keyword-only)
            handle: AllocHandle from alloc() for zero-copy store

        Returns:
            True if successful
        """
        self._ensure_connected()

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            # Duplicate skip: check if key already exists (common to both paths)
            if key in self._key_to_location:
                if handle is not None:
                    self._owned.free(handle._region_id, handle._page_index)
                logger.debug("store: key=%s already in local map, skipping", key)
                return True
            elif self._rpc.exists_kv(key):
                if handle is not None:
                    self._owned.free(handle._region_id, handle._page_index)
                logger.debug("store: key=%s already exists on server, skipping", key)
                return True

            if handle is not None:
                # ── Zero-copy path ──
                if data is not None or info is not None:
                    raise ValueError("Cannot specify both handle and data/info")

                region_id = handle._region_id
                page_index = handle._page_index
                offset = page_index * self._owned.get_chunk_size()
                total_size = handle._size

                is_new = self._rpc.register_kv(
                    key=key,
                    region_id=region_id,
                    kv_offset=offset,
                    kv_length=total_size,
                )

                if not is_new:
                    self._owned.free(region_id, page_index)
                    logger.debug(
                        "store: key=%s lost register race, freed page "
                        "(region=%d, page=%d)",
                        key,
                        region_id,
                        page_index,
                    )
                    return True

                self._key_to_location[key] = (region_id, page_index)

                logger.debug(
                    "Stored (zero-copy) key=%s: region=%d, page=%d, offset=%d, size=%d",
                    key,
                    region_id,
                    page_index,
                    offset,
                    total_size,
                )
                return True

            # ── Allocate + memcpy + register ──
            # Resolve source memoryview from either parameter
            if data is not None:
                src = data
            elif isinstance(info, memoryview):
                src = info
            elif isinstance(info, MemoryInfo):
                src = info.view
            else:
                raise TypeError(
                    "Must provide data (memoryview) or info (MemoryInfo | memoryview)"
                )

            # Normalize to 1D unsigned-byte view for mmap slice assignment
            if src.format != "B":
                src = src.cast("B")

            data_size = len(src)
            prefix_len = len(prefix) if prefix else 0
            total_size = prefix_len + data_size

            logger.debug(
                "store: key=%s, data=%d bytes, prefix=%d bytes, "
                "total=%d bytes, readonly=%s",
                key,
                data_size,
                prefix_len,
                total_size,
                src.readonly,
            )

            if total_size > self._owned.get_chunk_size():
                logger.error(
                    "Total size %d exceeds chunk_size %d",
                    total_size,
                    self._owned.get_chunk_size(),
                )
                return False

            # Allocate page + CXL write + register (new or overwrite only)
            result = self._owned.allocate()
            if result is None:
                if not self._expand_region():
                    logger.error("Cannot allocate page for key %s", key)
                    return False
                result = self._owned.allocate()
                if result is None:
                    return False

            region_id, page_index = result

            # 2. Get writable memoryview slice for the page
            buf = self._mapper.get_buffer_view(
                region_id,
                page_index * self._owned.get_chunk_size(),
                total_size,
            )
            if buf is None:
                self._owned.free(region_id, page_index)
                return False

            # 3. Write prefix + data via GIL-free memcpy
            offset = 0
            if prefix:
                _gil_free_memcpy(buf[offset:], prefix, prefix_len)
                offset += prefix_len
            _gil_free_memcpy(buf[offset:], src, data_size)

            # 4. Register KV with server
            offset = page_index * self._owned.get_chunk_size()
            is_new = self._rpc.register_kv(
                key=key,
                region_id=region_id,
                kv_offset=offset,
                kv_length=total_size,
            )

            if not is_new:
                # Race condition: another instance registered the same key
                # between our exists_kv check and register_kv call.
                # Free the page we just wrote — the data is identical anyway.
                self._owned.free(region_id, page_index)
                logger.debug(
                    "store: key=%s lost register race, freed page (region=%d, page=%d)",
                    key,
                    region_id,
                    page_index,
                )
                return True

            # 5. Track
            self._key_to_location[key] = (region_id, page_index)

            logger.debug(
                "Stored key=%s: region=%d, page=%d, offset=%d, size=%d",
                key,
                region_id,
                page_index,
                offset,
                total_size,
            )
            return True

    def retrieve(self, key: str) -> MemoryInfo | None:
        """Retrieve a zero-copy MemoryInfo from the KV cache.

        Returns a MemoryInfo with a memoryview slice of the mmap region.
        Works for both owned (RW) and shared (RO) regions.

        WARNING: The returned memoryview is only valid while the region
        remains mapped. Do not use after calling close().

        Args:
            key: The chunk key string

        Returns:
            MemoryInfo with memoryview, or None if not found
        """
        self._ensure_connected()

        result = self._rpc.lookup_kv(key)
        if not result.found or result.handle is None:
            logger.debug("Key %s not found", key)
            return None

        handle = result.handle
        region_id = handle.region_id

        # Shared region: on-demand mapping
        if not self._owned.is_owned(region_id):
            if self._mapper.get_region(region_id) is None:
                try:
                    self._mapper.map_region(handle)
                except Exception:
                    logger.error(
                        "Failed to map shared region %d", region_id, exc_info=True
                    )
                    return None

        buf = self._mapper.get_buffer_view(
            region_id, result.kv_offset, result.kv_length
        )
        if buf is None:
            logger.error("Region %d: get_buffer_view returned None", region_id)
            return None

        logger.debug(
            "retrieve: key=%s, region=%d, page=%d, offset=%d, size=%d, "
            "readonly=%s, owned=%s",
            key,
            region_id,
            result.kv_offset // self._owned.get_chunk_size(),
            result.kv_offset,
            result.kv_length,
            buf.readonly,
            self._owned.is_owned(region_id),
        )
        return MemoryInfo(view=buf)

    def exists(self, key: str) -> bool:
        """Check if a key exists.

        Args:
            key: The chunk key string

        Returns:
            True if exists
        """
        self._ensure_connected()
        return self._rpc.exists_kv(key)

    def delete(self, key: str) -> bool:
        """Delete a key and free the corresponding page.

        Args:
            key: The chunk key string

        Returns:
            True if deleted
        """
        self._ensure_connected()

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            # RPC first, then local free — prevents inconsistency on RPC failure
            result = self._rpc.delete_kv(key)
            if result:
                location = self._key_to_location.pop(key, None)
                if location is not None:
                    region_id, page_index = location
                    self._owned.free(region_id, page_index)
                logger.debug("Deleted key=%s", key)
            else:
                logger.debug("Delete key=%s: not found on server", key)

            return result

    def healthcheck(self) -> bool:
        """Check if the handler and MaruServer are healthy.

        Verifies local connection state and sends a heartbeat RPC
        to confirm the MaruServer is responsive.

        Returns:
            True if connected and server responded to heartbeat
        """
        if not self._connected or self._closing.is_set():
            return False

        try:
            return self._rpc.heartbeat()
        except Exception as e:
            logger.warning("Healthcheck failed: %s", e)
            return False

    def get_stats(self) -> dict:
        """Get server statistics."""
        self._ensure_connected()

        stats = self._rpc.get_stats()
        result = {
            "kv_manager": {
                "total_entries": stats.kv_manager.total_entries,
                "total_size": stats.kv_manager.total_size,
            },
            "allocation_manager": {
                "num_allocations": stats.allocation_manager.num_allocations,
                "total_allocated": stats.allocation_manager.total_allocated,
                "active_clients": stats.allocation_manager.active_clients,
            },
        }

        if self._owned is not None:
            store_stats = self._owned.get_stats()
            result["store_regions"] = store_stats
            # Backward compat: first region stats as "allocator"
            regions_list = store_stats.get("regions", [])
            if regions_list:
                result["allocator"] = regions_list[0]

        return result

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def batch_retrieve(self, keys: list[str]) -> list[MemoryInfo | None]:
        """Retrieve multiple values as MemoryInfo in batch.

        Uses a single batch RPC call for lookup, returns zero-copy
        memoryview slices for both owned (RW) and shared (RO) regions.

        WARNING: Returned memoryviews are only valid while regions remain mapped.

        On RPC failure, allocated pages are freed but data already written to
        those pages is not zeroed. This is safe because the pages are never
        registered with the server and will be overwritten on reuse.

        Args:
            keys: List of chunk key strings

        Returns:
            List of MemoryInfo (None for keys not found)
        """
        self._ensure_connected()

        try:
            batch_resp = self._rpc.batch_lookup_kv(keys)
        except Exception:
            logger.error("batch_retrieve RPC failed", exc_info=True)
            return [None] * len(keys)

        results: list[MemoryInfo | None] = []
        for i, entry in enumerate(batch_resp.entries):
            if not entry.found or entry.handle is None:
                results.append(None)
                continue

            handle = entry.handle
            region_id = handle.region_id

            # Ensure region is mapped
            if not self._owned.is_owned(region_id):
                if self._mapper.get_region(region_id) is None:
                    try:
                        self._mapper.map_region(handle)
                    except Exception:
                        logger.error(
                            "Failed to map shared region %d",
                            region_id,
                            exc_info=True,
                        )
                        results.append(None)
                        continue

            buf = self._mapper.get_buffer_view(
                region_id, entry.kv_offset, entry.kv_length
            )
            if buf is None:
                logger.error("Region %d: get_buffer_view returned None", region_id)
                results.append(None)
                continue

            logger.debug(
                "batch_retrieve: key=%s, region=%d, page=%d, "
                "offset=%d, size=%d, readonly=%s",
                keys[i],
                region_id,
                entry.kv_offset // self._owned.get_chunk_size(),
                entry.kv_offset,
                entry.kv_length,
                buf.readonly,
            )
            results.append(MemoryInfo(view=buf))

        hits = sum(1 for r in results if r is not None)
        ro_count = sum(1 for r in results if r is not None and r.view.readonly)
        logger.debug(
            "batch_retrieve: %d/%d hits, %d readonly (shared), %d writable (owned)",
            hits,
            len(keys),
            ro_count,
            hits - ro_count,
        )
        return results

    def batch_store(
        self,
        keys: list[str],
        infos: list[MemoryInfo | memoryview],
        prefixes: list[bytes | None] | None = None,
    ) -> list[bool]:
        """Store multiple key-value pairs in batch.

        Uses a single batch RPC call for registration.

        Args:
            keys: List of chunk key strings
            infos: List of MemoryInfo or memoryview with data
            prefixes: Optional list of prefix bytes per entry

        Returns:
            List of booleans indicating success for each key
        """
        self._ensure_connected()

        if len(keys) != len(infos):
            raise ValueError("keys and infos must have the same length")
        if prefixes is not None and len(prefixes) != len(keys):
            raise ValueError("prefixes must have the same length as keys")

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            chunk_size = self._owned.get_chunk_size()
            results = [True] * len(keys)
            register_entries = []
            allocations: dict[int, tuple[int, int]] = {}

            # Phase 1: Batch check which keys already exist (avoid CXL write waste)
            try:
                exists_resp = self._rpc.batch_exists_kv(keys)
                exists_results = exists_resp.results
            except Exception:
                logger.error(
                    "batch_exists RPC failed, proceeding without check", exc_info=True
                )
                exists_results = [False] * len(keys)

            skipped = sum(exists_results)
            if skipped > 0:
                logger.debug(
                    "batch_store: %d/%d keys already exist, skipping CXL write",
                    skipped,
                    len(keys),
                )
            logger.info(
                "batch_store precheck: server_exists=%d/%d, first_5=%s",
                skipped,
                len(keys),
                list(zip(keys[:5], exists_results[:5], strict=False)),
            )

            # Phase 2: Only process new keys (skip duplicates)
            for i, (key, info) in enumerate(zip(keys, infos, strict=True)):
                is_local = key in self._key_to_location
                if is_local:
                    # Same instance already stored — same key = same content, skip
                    logger.debug(
                        "batch_store: key=%s already in local map, skipping", key
                    )
                    continue  # results[i] stays True (idempotent)
                if exists_results[i]:
                    # Another instance already registered — skip CXL write
                    logger.debug(
                        "batch_store: key=%s already exists on server, skipping", key
                    )
                    continue  # results[i] stays True (idempotent)

                prefix = prefixes[i] if prefixes else None
                prefix_len = len(prefix) if prefix else 0
                # Normalize to 1D unsigned-byte view for mmap slice assignment
                src = info if isinstance(info, memoryview) else info.view
                if src.format != "B":
                    src = src.cast("B")
                data_size = len(src)
                total_size = prefix_len + data_size

                if total_size > chunk_size:
                    logger.error(
                        "Total size %d exceeds chunk_size %d for key %s",
                        total_size,
                        chunk_size,
                        key,
                    )
                    results[i] = False
                    continue

                # Allocate page (expand if needed)
                alloc_result = self._owned.allocate()
                if alloc_result is None:
                    if not self._expand_region():
                        logger.error("Cannot allocate page for key %s", key)
                        results[i] = False
                        continue
                    alloc_result = self._owned.allocate()
                    if alloc_result is None:
                        results[i] = False
                        continue

                region_id, page_index = alloc_result
                allocations[i] = (region_id, page_index)

                # Write to page via GIL-free memcpy
                buf = self._mapper.get_buffer_view(
                    region_id, page_index * chunk_size, total_size
                )
                if buf is None:
                    self._owned.free(region_id, page_index)
                    results[i] = False
                    continue

                mv_offset = 0
                if prefix:
                    _gil_free_memcpy(buf[mv_offset:], prefix, prefix_len)
                    mv_offset += prefix_len
                _gil_free_memcpy(buf[mv_offset:], src, data_size)

                offset = page_index * chunk_size
                register_entries.append((key, region_id, offset, total_size))

            # Batch register
            if register_entries:
                logger.info(
                    "batch_store register request: %d entries, first_5=%s",
                    len(register_entries),
                    [entry[0] for entry in register_entries[:5]],
                )
                try:
                    batch_resp = self._rpc.batch_register_kv(register_entries)
                except Exception:
                    logger.error("Batch register RPC failed", exc_info=True)
                    for _idx, (rid, pidx) in allocations.items():
                        self._owned.free(rid, pidx)
                    return [False] * len(keys)

                if not batch_resp.success:
                    logger.error("Batch register RPC failed")
                    for _idx, (rid, pidx) in allocations.items():
                        self._owned.free(rid, pidx)
                    return [False] * len(keys)

                logger.info(
                    "batch_store register response: new=%d/%d, first_5=%s",
                    sum(batch_resp.results),
                    len(batch_resp.results),
                    list(
                        zip(
                            [entry[0] for entry in register_entries[:5]],
                            batch_resp.results[:5],
                            strict=False,
                        )
                    ),
                )

                batch_idx = 0
                for i in range(len(keys)):
                    if results[i] and i in allocations:
                        if batch_idx < len(batch_resp.results):
                            results[i] = batch_resp.results[batch_idx]
                        batch_idx += 1

            # Track
            for i, key in enumerate(keys):
                if results[i] and i in allocations:
                    self._key_to_location[key] = allocations[i]

            total_bytes = sum(
                (
                    infos[i].nbytes
                    if isinstance(infos[i], memoryview)
                    else infos[i].view.nbytes
                )
                for i in range(len(keys))
                if results[i]
            )
            logger.debug(
                "batch_store: %d/%d succeeded, total_data=%d bytes",
                sum(results),
                len(keys),
                total_bytes,
            )
            return results

    def batch_exists(self, keys: list[str]) -> list[bool]:
        """Check if multiple keys exist.

        Uses a single batch RPC call instead of N individual calls.

        Args:
            keys: List of chunk key strings

        Returns:
            List of booleans indicating existence for each key
        """
        self._ensure_connected()

        try:
            batch_resp = self._rpc.batch_exists_kv(keys)
        except Exception:
            logger.error("batch_exists RPC failed", exc_info=True)
            return [False] * len(keys)
        logger.info(
            "handler.batch_exists response: hits=%d/%d, first_5=%s",
            sum(batch_resp.results),
            len(keys),
            list(zip(keys[:5], batch_resp.results[:5], strict=False)),
        )
        return batch_resp.results

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def pool_handle(self) -> MaruHandle | None:
        """Get initial pool handle (backward compat)."""
        if self._owned is None:
            return None
        first_rid = self._owned.get_first_region_id()
        if first_rid is None:
            return None
        mapped = self._mapper.get_region(first_rid)
        return mapped.handle if mapped else None

    @property
    def allocator(self) -> PagedMemoryAllocator | None:
        """Get the first region's allocator (backward compat)."""
        if self._owned is None:
            return None
        return self._owned.get_first_allocator()

    @property
    def owned_region_manager(self) -> OwnedRegionManager | None:
        """Get the owned region manager."""
        return self._owned

    @property
    def instance_id(self) -> str:
        """Get instance ID."""
        return self._config.instance_id

    @property
    def connected(self) -> bool:
        """Check if connected."""
        return self._connected

    # =========================================================================
    # Helpers
    # =========================================================================

    def _expand_region(self) -> bool:
        """Request a new store region from the server and add it.

        Returns:
            True if expansion succeeded.
        """
        try:
            response = self._rpc.request_alloc(
                instance_id=self._config.instance_id,
                size=self._config.pool_size,
            )
        except Exception:
            logger.error("RPC request_alloc failed during expand", exc_info=True)
            return False

        if not response.success or response.handle is None:
            logger.error(
                "Server refused region expansion: %s",
                getattr(response, "error", "unknown"),
            )
            return False

        handle = response.handle
        try:
            self._owned.add_region(handle)
            logger.info("Expanded: new store region %d", handle.region_id)
            return True
        except Exception:
            logger.error("Failed to init expanded region", exc_info=True)
            try:
                self._rpc.return_alloc(self._config.instance_id, handle.region_id)
            except Exception:
                logger.debug(
                    "Failed to return allocation during expansion cleanup",
                    exc_info=True,
                )
            return False

    def _premap_shared_regions(self) -> None:
        """Pre-map all existing shared regions from other instances.

        Called during connect() to eliminate mmap from the retrieve hot path.
        Failures are logged but do not block connection — lazy fallback
        remains as safety net in retrieve().
        """
        try:
            response = self._rpc.list_allocations(
                exclude_instance_id=self._config.instance_id
            )
        except Exception as e:
            logger.warning("Failed to list allocations for pre-map: %s", e)
            return

        if not response.success:
            logger.warning(
                "list_allocations failed: %s",
                response.error or "unknown",
            )
            return

        # NOTE: Race window exists between list_allocations() and map_region().
        # A region owner may disconnect between these calls, making the handle
        # stale. This is safe — map_region() failure is caught below and lazy
        # fallback in retrieve() handles it.
        mapped_count = 0
        for handle in response.allocations:
            if self._mapper.get_region(handle.region_id) is not None:
                continue  # already mapped (own region)
            try:
                self._mapper.map_region(handle, prefault=False)
                mapped_count += 1
            except Exception as e:
                logger.warning(
                    "Failed to pre-map shared region %d: %s",
                    handle.region_id,
                    e,
                )

        logger.info(
            "Pre-mapped %d shared regions (%d total from server)",
            mapped_count,
            len(response.allocations),
        )

    def _ensure_connected(self) -> None:
        """Ensure connected, raise if not or if closing."""
        if self._closing.is_set():
            raise RuntimeError("Handler is closing")
        if not self._connected or self._owned is None:
            raise RuntimeError("Not connected. Call connect() first.")

    # =========================================================================
    # Context Manager
    # =========================================================================

    def __enter__(self) -> "MaruHandler":
        """Context manager entry."""
        if self._config.auto_connect:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
