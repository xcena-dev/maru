"""MaruHandlerFs - marufs-based KV cache handler for Maru (marufs/fs mode).

Replaces ZeroMQ RPC + UDS with VFS syscalls + ioctl on marufs kernel filesystem.
No maru_resourced daemon, no MaruMetaServer — all operations go via MarufsClient.

Design:
- Owned regions are marufs files created by this instance (RW mmap).
- Shared regions are marufs files created by other instances (RO mmap).
- Each KV entry is stored at a page-aligned offset in an owned region.
- marufs global index stores name-ref entries: key_name → (region, byte_offset).
  name_offset(region_fd, key, offset) registers in the global index.
  find_name(any_fd, key) returns (region_name, offset) via global search.
- Local cache (_key_to_location) provides fast path for own keys.
Example:
    from maru_common import MaruConfig
    from maru_handler.handler_fs import MaruHandlerFs
    from maru_handler.memory import MemoryInfo

    config = MaruConfig(mount_path="/mnt/marufs")
    with MaruHandlerFs(config) as h:
        info = MemoryInfo(view=memoryview(b"hello world"))
        h.store(key="model@1@0@abc123@bf16", info=info)
        result = h.retrieve(key="model@1@0@abc123@bf16")
"""

import ctypes
import hashlib
import logging
import os
import threading

from maru_common import MaruConfig
from marufs import MarufsClient
from marufs.ioctl import MARUFS_NAME_MAX

from .handler import MaruHandler
from .memory import AllocHandle, MemoryInfo, OwnedRegionManager, PagedMemoryAllocator
from .memory.marufs_mapper import MarufsMapper

logger = logging.getLogger(__name__)


def _key_to_name(key: str) -> str:
    """Encode a key string to an marufs global index name.

    Uses the key directly if it fits within MARUFS_NAME_MAX.
    For longer keys, appends a SHA-256 hash suffix to avoid collision.
    """
    encoded = key.encode("utf-8")
    if len(encoded) <= MARUFS_NAME_MAX:
        return key
    # Hash the full key and append truncated hash to avoid collisions.
    # 64-bit hash → collision probability ~1% at ~600M keys (birthday paradox).
    digest = hashlib.sha256(encoded).hexdigest()[:16]
    # Reserve space: 63 - 1 (separator '#') - 16 (hash) = 30 bytes for prefix
    prefix_max = MARUFS_NAME_MAX - 1 - 16
    prefix = encoded[:prefix_max].decode("utf-8", errors="ignore")
    truncated = f"{prefix}#{digest}"
    logger.warning(
        "_key_to_name: key exceeds MARUFS_NAME_MAX (%d bytes > %d), truncated to '%s'",
        len(encoded),
        MARUFS_NAME_MAX,
        truncated,
    )
    return truncated


def _parse_chunk_hash(key: str) -> int:
    """Extract chunk_hash from key and apply bit spreading for shard selection.

    Key format: ``{model}@{ws}@{wid}@{chunk_hash_hex}@{dtype}``
    Kernel uses upper 16 bits for shard selection, so we spread bits
    to ensure non-zero upper bits.

    Returns 0 if parsing fails.
    """
    parts = key.split("@")
    if len(parts) < 4:
        return 0
    try:
        h = int(parts[3], 16)
    except (ValueError, IndexError):
        return 0
    return ((h << 16) | (h >> 48)) & 0xFFFFFFFFFFFFFFFF


def _region_name_for_instance(instance_id: str, index: int) -> str:
    """Generate a deterministic region filename for this instance.

    Format: ``maru_{instance_id_short}_{index:04d}``
    Keeps total length under MARUFS_NAME_MAX.
    """
    short = instance_id.replace("-", "")[:16]
    return f"maru_{short}_{index:04d}"


def _gil_free_memcpy_at(
    dst: memoryview, dst_offset: int, src: bytes | memoryview, length: int
) -> None:
    """Copy bytes without holding the GIL (via ctypes.memmove)."""
    dst_addr = ctypes.addressof(ctypes.c_char.from_buffer(dst, dst_offset))
    if isinstance(src, memoryview):
        if src.readonly:
            # Read-only memoryview: use numpy to get raw pointer
            import numpy as np

            arr = np.frombuffer(src[:length], dtype=np.uint8)
            src_addr = arr.ctypes.data
        else:
            src_addr = ctypes.addressof(ctypes.c_char.from_buffer(src))
    else:
        src_addr = ctypes.cast(ctypes.c_char_p(src), ctypes.c_void_p).value
    ctypes.memmove(dst_addr, src_addr, length)


class MaruHandlerFs(MaruHandler):
    """marufs-based KV cache handler (marufs/fs mode).

    Provides the same store/retrieve/exists/delete API as MaruHandler (RPC mode)
    but uses marufs filesystem operations instead of ZeroMQ RPC.

    Global key lookup uses marufs's partitioned global index:
    - name_offset(region_fd, key, offset) registers a name-ref entry
    - find_name(dir_fd, key) searches all name-refs and returns
      (region_name, offset)
    - No dedicated index region or slot mapping needed

    Thread-safety:
    - Read operations (exists, retrieve) are lock-free after connect.
    - Write operations (store, delete) are serialized by ``_write_lock``.
    - ``close()`` sets ``_closing`` to reject new operations, then waits
      for in-flight writes before teardown.

    Architecture:
        MaruHandlerFs
            ├── MarufsClient (marufs VFS + ioctl)
            ├── MarufsMapper (mmap owned + shared regions)
            ├── OwnedRegionManager (page allocator over owned marufs files)
            └── _key_to_location (key → (region_name, page_index, key_hash))
    """

    def __init__(self, config: MaruConfig | None = None):
        """Initialize MaruHandlerFs.

        Args:
            config: MaruConfig with ``mount_path`` set.
        """
        if hasattr(self, "_initialized"):
            return
        self._initialized = True
        self._config = config or MaruConfig()
        if not self._config.is_marufs_mode:
            raise ValueError(
                "MaruHandlerFs requires mount_path to be set in MaruConfig. "
                "Set config.mount_path to the marufs mount point (e.g. '/mnt/marufs')."
            )

        self._marufs: MarufsClient = MarufsClient(self._config.mount_path)
        self._mapper: MarufsMapper = MarufsMapper(self._marufs)
        self._owned: OwnedRegionManager | None = None

        # Thread-safety
        self._write_lock = threading.Lock()
        self._closing = threading.Event()

        # Local cache: key (str) → (region_name, page_index, key_hash)
        self._key_to_location: dict[str, tuple[str, int, int]] = {}

        self._connected = False
        self._region_index = 0  # monotonic counter for region filenames

        # Key layout cache (calibrated lazily on first key)
        # Key format: {model}@{ws}@{wid}@{chunk_hash_hex}@{dtype}
        # Within one instance, only chunk_hash_hex varies.
        self._key_hash_start: int = (
            -1
        )  # char offset of chunk_hash_hex; -1 = not yet calibrated
        self._key_suffix_len: int = 0  # len("@{dtype}")
        self._key_is_ascii: bool = False

        logger.debug(
            "Created MaruHandlerFs with mount_path=%s instance_id=%s",
            self._config.mount_path,
            self._config.instance_id,
        )

    # =========================================================================
    # Connection Management
    # =========================================================================

    def connect(self) -> bool:
        """Initialize owned region(s) on marufs.

        Creates the first owned data region and opens a directory fd
        for global name lookups.

        Returns:
            True if successful.
        """
        if self._connected:
            return True

        try:
            # 1. Open directory fd for find_name ioctl
            self._marufs.get_dir_fd()

            # 2. Initialize owned region manager
            self._owned = OwnedRegionManager(
                chunk_size=self._config.chunk_size_bytes,
            )

            # 3. Create initial owned data region
            region_name = self._next_region_name()
            self._mapper.map_owned_region(region_name, self._config.pool_size)
            self._owned.add_region(region_name, self._config.pool_size)

            self._connected = True

            logger.info(
                "MaruHandlerFs connected: mount=%s instance=%s "
                "initial_region=%s chunk_size=%d pool_size=%d",
                self._config.mount_path,
                self._config.instance_id,
                region_name,
                self._config.chunk_size_bytes,
                self._config.pool_size,
            )
            return True

        except Exception as e:
            logger.error("MaruHandlerFs connect failed: %s", e)
            self._owned = None
            return False

    def close(self) -> None:
        """Close all mappings and marufs file descriptors.

        Clears own KV entries from the global index and deletes owned
        data region files.
        """
        if not self._connected:
            return

        self._closing.set()

        try:
            with self._write_lock:
                # 1. Clear own KV entries from the global index
                for key, (region_name, _page_idx, key_hash) in self._key_to_location.items():
                    if self._owned is not None and self._owned.is_owned(region_name):
                        key_name = self._get_name(key)
                        fd = self._mapper.get_fd(region_name)
                        if fd is not None:
                            try:
                                self._marufs.clear_name(fd, key_name, key_hash)
                            except OSError:
                                pass

                # 2. Close owned region manager (allocator cleanup)
                owned_names: list[str] = []
                if self._owned is not None:
                    owned_names = self._owned.close()
                    self._owned = None

                # 3. Close all mmap'd regions via MarufsMapper
                self._mapper.close()

                # 4. Delete owned data region files
                for name in owned_names:
                    try:
                        self._marufs.delete_region(name)
                        logger.debug("Deleted owned region file: %s", name)
                    except OSError as e:
                        logger.warning(
                            "Could not delete region %s on close: %s", name, e
                        )

                # 5. Close remaining fds (dir fd, shared regions)
                self._marufs.close()

        except Exception as e:
            logger.error("Error during MaruHandlerFs.close(): %s", e)
        finally:
            self._connected = False
            self._key_to_location.clear()

    # =========================================================================
    # KV Operations
    # =========================================================================

    def store(
        self,
        key: str,
        info: MemoryInfo | memoryview | None = None,
        prefix: bytes | None = None,
        *,
        data: memoryview | None = None,
        handle: AllocHandle | None = None,
    ) -> bool:
        """Store data in the marufs KV cache.

        Allocates a page in an owned region, writes data via mmap, and
        registers a name-ref in the marufs global index.

        Note: The zero-copy ``handle`` path (alloc + store) is not supported
        in marufs mode. If ``handle`` is provided, a NotImplementedError is raised.
        Use ``info`` or ``data`` instead.

        Args:
            key: CacheEngineKey string (e.g. "model@ws@wid@hash@dtype").
            info: MemoryInfo or memoryview with source data.
            prefix: Optional bytes to prepend before the data.
            data: Alternative to info — raw memoryview of data.
            handle: Not supported in marufs mode.

        Returns:
            True if successful.
        """
        if handle is not None:
            raise NotImplementedError(
                "Zero-copy alloc/store(handle=) is not supported in marufs mode. "
                "Use info= or data= instead."
            )
        if info is None and data is not None:
            info = MemoryInfo(view=data)
        self._ensure_connected()

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            # Normalize to 1D byte view
            src = info.view
            if src.format != "B":
                src = src.cast("B")

            data_size = len(src)
            prefix_len = len(prefix) if prefix else 0
            total_size = prefix_len + data_size

            chunk_size = self._owned.get_chunk_size()
            if total_size > chunk_size:
                logger.error(
                    "store: total_size=%d exceeds chunk_size=%d for key=%s",
                    total_size,
                    chunk_size,
                    key,
                )
                return False

            # Overwrite: free old page and clear old entries
            old_location = self._key_to_location.get(key)
            if old_location is not None:
                old_region_name, old_page_index, _ = old_location
                self._clear_key_from_region(old_region_name, old_page_index, key)
                self._key_to_location.pop(key)

            # Allocate a page (expand if needed)
            result = self._owned.allocate()
            if result is None:
                if not self._expand_region():
                    logger.error("store: cannot allocate page for key=%s", key)
                    return False
                result = self._owned.allocate()
                if result is None:
                    logger.error("store: still no page after expand for key=%s", key)
                    return False

            region_name, page_index = result
            byte_offset = page_index * chunk_size

            # Write to mmap via memoryview slice assignment (zero-copy)
            region = self._mapper.get_region(region_name)
            if region is None:
                self._owned.free(region_name, page_index)
                logger.error("store: region not found: %s", region_name)
                return False
            buf = region.get_buffer_view(byte_offset, total_size)
            if buf is None:
                self._owned.free(region_name, page_index)
                logger.error(
                    "store: get_buffer_view failed for region=%s offset=%d size=%d",
                    region_name,
                    byte_offset,
                    total_size,
                )
                return False

            write_offset = 0
            if prefix:
                _gil_free_memcpy_at(buf, write_offset, prefix, prefix_len)
                write_offset += prefix_len
            _gil_free_memcpy_at(buf, write_offset, src, data_size)

            # Register name-ref in marufs global index
            key_name = self._get_name(key)
            key_hash = self._get_hash(key)
            fd = region.fd

            try:
                self._marufs.name_offset(fd, key_name, byte_offset, key_hash)
            except OSError as e:
                self._owned.free(region_name, page_index)
                logger.error(
                    "store: name_offset failed for key=%s region=%s: %s",
                    key,
                    region_name,
                    e,
                )
                return False

            # Track locally
            self._key_to_location[key] = (region_name, page_index, key_hash)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "store: key=%s region=%s page=%d offset=%d size=%d",
                    key,
                    region_name,
                    page_index,
                    byte_offset,
                    total_size,
                )
            return True

    def retrieve(self, key: str) -> MemoryInfo | None:
        """Retrieve a zero-copy MemoryInfo for the given key.

        Fast path: local cache. Slow path: marufs global index lookup.

        Args:
            key: CacheEngineKey string.

        Returns:
            MemoryInfo with memoryview slice, or None if not found.
        """
        self._ensure_connected()

        owned = self._owned  # snapshot to avoid race with close()
        if owned is None:
            return None

        location = self._key_to_location.get(key)
        if location is None:
            location = self._lookup_global(key)

        if location is None:
            logger.debug("retrieve: key=%s not found", key)
            return None

        region_name, page_index, _kh = location
        chunk_size = owned.get_chunk_size()
        byte_offset = page_index * chunk_size

        # Ensure region is mapped (may be shared/remote)
        if not self._mapper.is_mapped(region_name):
            try:
                self._mapper.map_shared_region(region_name)
            except Exception as e:
                logger.error("retrieve: failed to map region=%s: %s", region_name, e)
                return None

        # Full chunk returned; caller uses prefix to determine actual data boundary
        buf = self._mapper.get_buffer_view(region_name, byte_offset, chunk_size)
        if buf is None:
            logger.error(
                "retrieve: get_buffer_view returned None for region=%s offset=%d",
                region_name,
                byte_offset,
            )
            return None

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "retrieve: key=%s region=%s page=%d offset=%d readonly=%s owned=%s",
                key,
                region_name,
                page_index,
                byte_offset,
                buf.readonly,
                owned.is_owned(region_name),
            )
        return MemoryInfo(view=buf)

    def exists(self, key: str) -> bool:
        """Check if a key exists in the KV cache.

        Args:
            key: CacheEngineKey string.

        Returns:
            True if the key is present.
        """
        self._ensure_connected()

        owned = self._owned  # snapshot to avoid race with close()
        if owned is None:
            return False

        if key in self._key_to_location:
            return True
        return self._lookup_global(key) is not None

    def delete(self, key: str) -> bool:
        """Delete a key and free the corresponding page.

        Removes the name-ref from the marufs global index.

        Args:
            key: CacheEngineKey string.

        Returns:
            True if the key was found and deleted.
        """
        self._ensure_connected()

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            location = self._key_to_location.get(key)
            if location is None:
                logger.debug("delete: key=%s not found in local cache", key)
                return False

            region_name, page_index, _kh = location
            self._clear_key_from_region(region_name, page_index, key)
            self._key_to_location.pop(key, None)
            return True

    def healthcheck(self) -> bool:
        """Check if the handler is operational.

        Verifies connection state and that the marufs mount is accessible.

        Returns:
            True if healthy.
        """
        if not self._connected or self._closing.is_set():
            return False
        try:
            # Check marufs mount is accessible
            os.listdir(self._config.mount_path)
            return True
        except OSError as e:
            logger.warning("healthcheck: marufs mount inaccessible: %s", e)
            return False

    def get_stats(self) -> dict:
        """Get statistics for owned regions."""
        self._ensure_connected()
        result: dict = {}
        if self._owned is not None:
            result["store_regions"] = self._owned.get_stats()
        result["key_count"] = len(self._key_to_location)
        return result

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def batch_store(
        self,
        keys: list[str],
        infos: list[MemoryInfo],
        prefixes: list[bytes | None] | None = None,
    ) -> list[bool]:
        """Store multiple key-value pairs in batch.

        Checks existence first (batch ioctl) to skip duplicates,
        then writes only new keys under a single lock.

        Args:
            keys: List of CacheEngineKey strings.
            infos: List of MemoryInfo with data.
            prefixes: Optional per-entry prefix bytes.

        Returns:
            List of booleans indicating success for each key.
        """
        self._ensure_connected()

        if len(keys) != len(infos):
            raise ValueError("keys and infos must have the same length")
        if prefixes is not None and len(prefixes) != len(keys):
            raise ValueError("prefixes must have the same length as keys")

        # Phase 1: Separate local-hit vs miss, batch ioctl for misses.
        # Note: global index check runs outside _write_lock (benign TOCTOU race).
        # If another instance deletes a key between Phase 1 and Phase 2, we skip
        # re-storing it. The caller sees success and the key will be re-stored on
        # the next retrieval miss.
        local_hits: set[int] = set()
        miss_indices: list[int] = []
        miss_key_names: list[str] = []
        for i, key in enumerate(keys):
            if key in self._key_to_location:
                local_hits.add(i)
            else:
                miss_indices.append(i)
                miss_key_names.append(self._get_name(key))

        # Batch ioctl for misses only
        global_hits: set[int] = set()
        miss_hashes: list[int] = []
        if miss_key_names:
            miss_hashes = [self._get_hash(keys[i]) for i in miss_indices]
            batch_results = self._marufs.batch_find_name(
                self._marufs.get_dir_fd(), miss_key_names, miss_hashes
            )
            for j, idx in enumerate(miss_indices):
                if batch_results[j] is not None:
                    global_hits.add(idx)

        # Cache name/hash from Phase 1 for reuse in Phase 2
        name_cache = dict(zip(miss_indices, miss_key_names, strict=True))
        hash_cache = dict(zip(miss_indices, miss_hashes, strict=True))

        skip_set = local_hits | global_hits

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            # Re-check local cache under lock to prevent TOCTOU race
            # (Phase 1 ran outside lock; another thread may have stored these keys)
            for i in range(len(keys)):
                if i not in skip_set and keys[i] in self._key_to_location:
                    skip_set.add(i)

            chunk_size = self._owned.get_chunk_size()
            results = [True] * len(keys)

            # Phase 2: Allocate + mmap write for new keys only
            # Group by region for per-region batch ioctl.
            pending: dict[str, list[tuple[int, str, int, int]]] = {}
            # pending[region_name] = [(index, key_name, byte_offset, key_hash), ...]
            allocations: dict[int, tuple[str, int]] = {}

            for i, (key, info) in enumerate(zip(keys, infos, strict=True)):
                if i in skip_set:
                    continue  # already stored locally or in global index

                prefix = prefixes[i] if prefixes else None
                prefix_len = len(prefix) if prefix else 0

                src = info.view
                if src.format != "B":
                    src = src.cast("B")
                data_size = len(src)
                total_size = prefix_len + data_size

                if total_size > chunk_size:
                    logger.error(
                        "batch_store: total_size=%d exceeds chunk_size=%d for key=%s",
                        total_size,
                        chunk_size,
                        key,
                    )
                    results[i] = False
                    continue

                # Allocate page (expand if needed)
                alloc = self._owned.allocate()
                if alloc is None:
                    if not self._expand_region():
                        logger.error(
                            "batch_store: cannot allocate page for key=%s", key
                        )
                        results[i] = False
                        continue
                    alloc = self._owned.allocate()
                    if alloc is None:
                        results[i] = False
                        continue

                region_name, page_index = alloc
                byte_offset = page_index * chunk_size
                allocations[i] = (region_name, page_index)

                # Write to mmap (before registration — avoids read-before-write race)
                region = self._mapper.get_region(region_name)
                if region is None:
                    self._owned.free(region_name, page_index)
                    del allocations[i]
                    results[i] = False
                    continue
                buf = region.get_buffer_view(byte_offset, total_size)
                if buf is None:
                    self._owned.free(region_name, page_index)
                    del allocations[i]
                    results[i] = False
                    continue

                write_offset = 0
                if prefix:
                    _gil_free_memcpy_at(buf, write_offset, prefix, prefix_len)
                    write_offset += prefix_len
                _gil_free_memcpy_at(buf, write_offset, src, data_size)

                # Collect for batch registration (reuse Phase 1 encoding)
                key_name = name_cache[i]
                key_hash = hash_cache[i]
                pending.setdefault(region_name, []).append(
                    (i, key_name, byte_offset, key_hash)
                )

            # Phase 3: Batch register per region (1 ioctl per region)
            for region_name, entries in pending.items():
                region = self._mapper.get_region(region_name)
                if region is None:
                    for idx, _, _, _ in entries:
                        self._owned.free(*allocations.pop(idx))
                        results[idx] = False
                    continue

                names = [e[1] for e in entries]
                offsets = [e[2] for e in entries]
                hashes = [e[3] for e in entries]

                try:
                    reg_results = self._marufs.batch_name_offset(
                        region.fd, names, offsets, hashes
                    )
                except OSError as e:
                    logger.error("batch_store: batch_name_offset failed: %s", e)
                    for idx, _, _, _ in entries:
                        self._owned.free(*allocations.pop(idx))
                        results[idx] = False
                    continue

                for j, (idx, _, _, kh) in enumerate(entries):
                    if reg_results[j]:
                        rn, pi = allocations[idx]
                        self._key_to_location[keys[idx]] = (rn, pi, kh)
                    else:
                        self._owned.free(*allocations.pop(idx))
                        results[idx] = False

        return results

    def batch_retrieve(self, keys: list[str]) -> list[MemoryInfo | None]:
        """Retrieve multiple values using batch ioctl.

        Uses a single MARUFS_IOC_BATCH_FIND_NAME ioctl call (per 512 keys)
        instead of N individual find_name calls.

        Args:
            keys: List of CacheEngineKey strings.

        Returns:
            List of MemoryInfo (None for missing keys).
        """
        self._ensure_connected()

        owned = self._owned
        if owned is None:
            return [None] * len(keys)

        chunk_size = owned.get_chunk_size()

        # 1. Separate cache hits vs misses
        locations: list[tuple[str, int] | None] = [None] * len(keys)
        miss_indices: list[int] = []
        miss_key_names: list[str] = []

        for i, key in enumerate(keys):
            loc = self._key_to_location.get(key)
            if loc is not None:
                locations[i] = (loc[0], loc[1])
            else:
                miss_indices.append(i)
                miss_key_names.append(self._get_name(key))

        # 2. Batch ioctl for all cache misses (1 syscall per 512 keys)
        if miss_key_names:
            miss_hashes = [self._get_hash(keys[i]) for i in miss_indices]
            batch_results = self._marufs.batch_find_name(
                self._marufs.get_dir_fd(), miss_key_names, miss_hashes
            )

            for j, idx in enumerate(miss_indices):
                result = batch_results[j]
                if result is None:
                    continue
                region_name, byte_offset = result
                if not region_name or "/" in region_name or "\x00" in region_name:
                    continue
                page_index = byte_offset // chunk_size
                locations[idx] = (region_name, page_index)

        # 2b. Cache only owned-region keys — shared-region entries may be
        # evicted by the remote owner without notification.
        with self._write_lock:
            if not self._closing.is_set():
                for j, idx in enumerate(miss_indices):
                    if locations[idx] is not None:
                        rn, pi = locations[idx]
                        if owned.is_owned(rn):
                            self._key_to_location[keys[idx]] = (
                                rn,
                                pi,
                                miss_hashes[j],
                            )

        # 3. Pre-map all needed regions (deduplicated)
        regions_to_map: set[str] = set()
        for loc in locations:
            if loc is not None:
                rn, _ = loc
                if not self._mapper.is_mapped(rn):
                    regions_to_map.add(rn)

        failed_regions: set[str] = set()
        for rn in regions_to_map:
            try:
                self._mapper.map_shared_region(rn)
            except Exception as e:
                logger.error("batch_retrieve: failed to map region=%s: %s", rn, e)
                failed_regions.add(rn)

        # 4. Build results (no per-key map_shared_region)
        results: list[MemoryInfo | None] = []
        for i, _key in enumerate(keys):
            loc = locations[i]
            if loc is None:
                results.append(None)
                continue

            region_name, page_index = loc
            if region_name in failed_regions:
                results.append(None)
                continue

            byte_offset = page_index * chunk_size

            buf = self._mapper.get_buffer_view(region_name, byte_offset, chunk_size)

            if buf is None:
                results.append(None)
            else:
                results.append(MemoryInfo(view=buf))

        return results

    def batch_exists(self, keys: list[str]) -> list[bool]:
        """Check existence of multiple keys using batch ioctl.

        Uses a single MARUFS_IOC_BATCH_FIND_NAME ioctl call (per 512 keys)
        instead of N individual find_name calls.

        Args:
            keys: List of CacheEngineKey strings.

        Returns:
            List of booleans indicating existence.
        """
        self._ensure_connected()

        owned = self._owned
        if owned is None:
            return [False] * len(keys)

        chunk_size = owned.get_chunk_size()

        # Separate cache hits vs misses
        results: list[bool] = [False] * len(keys)
        miss_indices: list[int] = []
        miss_key_names: list[str] = []

        for i, key in enumerate(keys):
            if key in self._key_to_location:
                results[i] = True
            else:
                miss_indices.append(i)
                miss_key_names.append(self._get_name(key))

        # Batch ioctl for misses
        if miss_key_names:
            miss_hashes = [self._get_hash(keys[i]) for i in miss_indices]
            batch_results = self._marufs.batch_find_name(
                self._marufs.get_dir_fd(), miss_key_names, miss_hashes
            )

            cache_updates: list[tuple[int, str, int, int]] = []
            for j, idx in enumerate(miss_indices):
                result = batch_results[j]
                if result is None:
                    continue
                region_name, byte_offset = result
                if not region_name or "/" in region_name or "\x00" in region_name:
                    continue
                page_index = byte_offset // chunk_size
                cache_updates.append((idx, region_name, page_index, miss_hashes[j]))
                results[idx] = True

            # Cache only owned-region keys — shared-region entries may be
            # evicted by the remote owner without notification.
            with self._write_lock:
                if not self._closing.is_set():
                    for idx, rn, pi, h in cache_updates:
                        if owned.is_owned(rn):
                            self._key_to_location[keys[idx]] = (rn, pi, h)

            if logger.isEnabledFor(logging.DEBUG):
                for idx, rn, pi, _h in cache_updates:
                    logger.debug(
                        "batch_exists: found key=%s in region=%s page=%d (owned=%s)",
                        keys[idx],
                        rn,
                        pi,
                        owned.is_owned(rn),
                    )

        return results

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def instance_id(self) -> str:
        """Get instance ID."""
        return self._config.instance_id

    @property
    def connected(self) -> bool:
        """Check if connected."""
        return self._connected

    @property
    def allocator(self) -> PagedMemoryAllocator | None:
        """Get the first region's allocator (compat with RPC handler API)."""
        if self._owned is None:
            return None
        return self._owned.get_first_allocator()

    @property
    def owned_region_manager(self) -> OwnedRegionManager | None:
        """Get the owned region manager."""
        return self._owned

    # =========================================================================
    # Global Index Lookup
    # =========================================================================

    def _lookup_global(self, key: str) -> tuple[str, int, int] | None:
        """Look up a key via the marufs global index.

        Args:
            key: CacheEngineKey string.

        Returns:
            (region_name, page_index, key_hash) if found, None otherwise.
        """
        key_name = self._get_name(key)
        key_hash = self._get_hash(key)
        try:
            result = self._marufs.find_name(
                self._marufs.get_dir_fd(), key_name, key_hash
            )
        except OSError:
            return None
        if result is None:
            return None

        region_name, byte_offset = result
        # Validate region_name from ioctl response
        if not region_name or "/" in region_name or "\x00" in region_name:
            logger.warning(
                "_lookup_global: invalid region_name from ioctl: %r", region_name
            )
            return None

        owned = self._owned  # snapshot to avoid race with close()
        if owned is None:
            return None
        chunk_size = owned.get_chunk_size()
        page_index = byte_offset // chunk_size

        # Cache only owned-region keys — shared-region entries may be evicted
        # by the remote owner without notification, leading to stale cache.
        if owned.is_owned(region_name):
            with self._write_lock:
                if not self._closing.is_set():
                    self._key_to_location[key] = (region_name, page_index, key_hash)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "_lookup_global: key=%s → region=%s page=%d",
                key,
                region_name,
                page_index,
            )
        return (region_name, page_index, key_hash)

    # =========================================================================
    # Key Layout (cached fast-path)
    # =========================================================================

    def _calibrate_key_layout(self, key: str) -> None:
        """Detect fixed prefix/suffix offsets from the first CacheEngineKey.

        Key format: ``{model}@{ws}@{wid}@{chunk_hash_hex}@{dtype}``
        Within one instance, only ``chunk_hash_hex`` varies.
        Calibrates once on the first key; subsequent calls are no-ops.
        """
        # Walk to 3rd '@' separator (end of model@ws@wid)
        idx = 0
        for _ in range(3):
            pos = key.find("@", idx)
            if pos < 0:
                return  # non-standard format — keep slow path
            idx = pos + 1

        # Find 4th '@' separator (before dtype)
        hash_end = key.find("@", idx)
        if hash_end < 0:
            return

        # Set _key_hash_start LAST — it is the "ready" flag checked in fast path.
        # CPython GIL serializes these assignments, so no torn reads occur.
        # On non-CPython runtimes, a threading.Lock would be needed here.
        self._key_suffix_len = len(key) - hash_end
        self._key_is_ascii = key.isascii()
        self._key_hash_start = idx  # must be last (acts as ready flag)

    def _get_name(self, key: str) -> str:
        """Convert key to global index name (fast path when calibrated)."""
        if self._key_hash_start < 0:
            self._calibrate_key_layout(key)
        # ASCII keys: len(key) == len(key.encode("utf-8"))
        if self._key_is_ascii and len(key) <= MARUFS_NAME_MAX:
            return key
        return _key_to_name(key)

    def _get_hash(self, key: str) -> int:
        """Extract chunk_hash with cached offsets (fast path when calibrated)."""
        if self._key_hash_start < 0:
            self._calibrate_key_layout(key)
        if self._key_hash_start >= 0:
            try:
                if self._key_suffix_len > 0:
                    h = int(key[self._key_hash_start : -self._key_suffix_len], 16)
                else:
                    h = int(key[self._key_hash_start :], 16)
            except (ValueError, IndexError):
                return 0
            return ((h << 16) | (h >> 48)) & 0xFFFFFFFFFFFFFFFF
        return _parse_chunk_hash(key)

    # =========================================================================
    # Helpers
    # =========================================================================

    def _next_region_name(self) -> str:
        """Generate the next unique region filename for this instance."""
        name = _region_name_for_instance(self._config.instance_id, self._region_index)
        self._region_index += 1
        return name

    def _expand_region(self) -> bool:
        """Create a new owned region when the current one is full.

        Returns:
            True if expansion succeeded.
        """
        try:
            region_name = self._next_region_name()
            self._mapper.map_owned_region(region_name, self._config.pool_size)
            self._owned.add_region(region_name, self._config.pool_size)
            logger.info("Expanded: created new owned region %s", region_name)
            return True
        except Exception as e:
            logger.error("_expand_region failed: %s", e)
            return False

    def _clear_key_from_region(
        self, region_name: str, page_index: int, key: str
    ) -> None:
        """Remove key from global index and free page.

        Args:
            region_name: Name of the owning region.
            page_index: Page index within the region.
            key: Key string to remove.
        """
        key_name = self._get_name(key)
        key_hash = self._get_hash(key)

        # Clear from global index
        region = self._mapper.get_region(region_name)
        if region is not None:
            try:
                self._marufs.clear_name(region.fd, key_name, key_hash)
            except OSError as e:
                logger.warning(
                    "_clear_key: clear_name failed for key=%s region=%s: %s",
                    key,
                    region_name,
                    e,
                )

        # Free page in owned region
        if self._owned is not None and self._owned.is_owned(region_name):
            try:
                self._owned.free(region_name, page_index)
            except KeyError:
                logger.warning(
                    "_clear_key: free failed for region=%s page=%d",
                    region_name,
                    page_index,
                )

    def _ensure_connected(self) -> None:
        """Raise if not connected or closing."""
        if self._closing.is_set():
            raise RuntimeError("Handler is closing")
        if not self._connected or self._owned is None:
            raise RuntimeError("Not connected. Call connect() first.")

    # =========================================================================
    # Context Manager
    # =========================================================================

    def __enter__(self) -> "MaruHandlerFs":
        """Context manager entry."""
        if self._config.auto_connect:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
