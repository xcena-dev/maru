# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""CxlMemoryAllocator — Pool-based LMCache MemoryAllocatorInterface over MaruHandler.

Pre-creates MemoryObj per page at region mapping time. Allocate/get return
pooled objects by (region_id, page_index) lookup. Address encoding uses
bit-packing: (region_id << 32) | page_index.
"""

import threading

import torch
from lmcache.logging import init_logger
from lmcache.v1.memory_management import (
    MemoryAllocatorInterface,
    MemoryFormat,
    MemoryObj,
    MemoryObjMetadata,
    TensorMemoryObj,
)

from maru_handler import MaruHandler
from maru_handler.memory import AllocHandle

logger = init_logger(__name__)


class CxlMemoryAllocator(MemoryAllocatorInterface):
    """LMCache MemoryAllocatorInterface backed by Maru CXL shared memory.

    Pool-based design: MemoryObjs are pre-created per page when a region
    is mapped. allocate/get return pooled objects by (region_id, page_index).

    Address encoding: (region_id << 32) | page_index — stateless O(1)
    bidirectional conversion, no cumulative offset table needed.
    """

    def __init__(
        self,
        handler: MaruHandler,
        shapes: list[torch.Size],
        dtypes: list[torch.dtype],
        fmt: MemoryFormat,
        chunk_size: int,
    ):
        self._handler = handler
        self._lock = threading.Lock()

        # LMCache metadata for MemoryObj construction
        self._shapes = shapes
        self._dtypes = dtypes
        self._fmt = fmt
        self._chunk_size = chunk_size

        # Pre-created MemoryObj pool: region_id -> [MemoryObj per page]
        self._pool: dict[int, list[TensorMemoryObj]] = {}

        # Build pool for initially owned regions
        self._build_owned_pools()

    # =========================================================================
    # Address Encoding
    # =========================================================================

    @staticmethod
    def encode_address(region_id: int, page_index: int) -> int:
        """Encode (region_id, page_index) into a single integer."""
        return (region_id << 32) | page_index

    @staticmethod
    def decode_address(address: int) -> tuple[int, int]:
        """Decode a single integer into (region_id, page_index)."""
        return (address >> 32, address & 0xFFFFFFFF)

    # =========================================================================
    # Pool Management
    # =========================================================================

    def _build_owned_pools(self) -> None:
        """Build pool for all currently owned regions."""
        orm = self._handler.owned_region_manager
        if orm is None:
            return
        for rid in list(orm.get_region_ids()):
            region = orm.get_owned_region(rid)
            if region is not None:
                self._build_region_pool(rid, region.allocator.page_count)

    def _build_region_pool(self, region_id: int, page_count: int) -> None:
        """Pre-create MemoryObjs for all pages in a region.

        Called when a region is mapped (owned or shared).

        Args:
            region_id: The region ID.
            page_count: Number of pages in the region.
        """
        chunk_size = self._chunk_size
        mapper = self._handler.mapper
        objs: list[TensorMemoryObj] = []

        for pid in range(page_count):
            offset = pid * chunk_size
            buf = mapper.get_buffer_view(region_id, offset, chunk_size)
            if buf is None:
                logger.error(
                    "[Maru] buffer view failed region=%d page=%d",
                    region_id,
                    pid,
                )
                continue

            flat_dtype = self._dtypes[0]
            tensor = torch.frombuffer(buf, dtype=flat_dtype)

            metadata = MemoryObjMetadata(
                shape=self._shapes[0],
                dtype=flat_dtype,
                address=self.encode_address(region_id, pid),
                phy_size=chunk_size,
                ref_count=1,
                fmt=self._fmt,
                shapes=self._shapes if len(self._shapes) > 1 else None,
                dtypes=self._dtypes if len(self._dtypes) > 1 else None,
            )
            objs.append(TensorMemoryObj(tensor, metadata, parent_allocator=None))

        with self._lock:
            self._pool[region_id] = objs

        logger.debug("[Maru] pool built region=%d pages=%d", region_id, len(objs))

    def ensure_region_pool(self, region_id: int) -> bool:
        """Ensure pool exists for a region (on-demand for shared regions).

        Args:
            region_id: The region ID.

        Returns:
            True if pool exists or was successfully created.
        """
        with self._lock:
            if region_id in self._pool:
                return True

        # Shared region: compute page_count from mapped region size
        mapped = self._handler.mapper.get_region(region_id)
        if mapped is None:
            return False

        page_count = mapped.size // self._chunk_size
        self._build_region_pool(region_id, page_count)
        return region_id in self._pool

    # =========================================================================
    # MemoryAllocatorInterface
    # =========================================================================

    def allocate(
        self,
        shapes: torch.Size | list[torch.Size],
        dtypes: torch.dtype | list[torch.dtype],
        fmt: MemoryFormat = MemoryFormat.UNDEFINED,
        allocator_type: str | None = None,
    ) -> MemoryObj | None:
        """Allocate a CXL page and return the pooled MemoryObj.

        Pool objects are pre-created with the canonical shapes/dtypes/fmt
        from __init__. The shapes/dtypes/fmt arguments are accepted for
        interface compatibility but the pool's metadata is used.

        Args:
            shapes: Tensor shape(s) (for size computation only).
            dtypes: Tensor dtype(s) (for size computation only).
            fmt: Memory format (unused, pool has canonical fmt).
            allocator_type: Unused, for interface compatibility.

        Returns:
            TensorMemoryObj from the pool, or None on failure.
        """
        shapes_list, dtypes_list = self._adapt_shapes_and_dtypes(shapes, dtypes)

        size = 0
        for shape, dtype in zip(shapes_list, dtypes_list, strict=True):
            size += shape.numel() * dtype.itemsize

        if size == 0:
            return None

        try:
            handle = self._handler.alloc(size=size)
        except (ValueError, RuntimeError) as e:
            logger.debug("[Maru] alloc failed: %s", e)
            return None

        rid, pid = handle.region_id, handle.page_index

        # Ensure pool exists (may need rebuild after region expansion)
        with self._lock:
            region_pool = self._pool.get(rid)

        if region_pool is None:
            orm = self._handler.owned_region_manager
            region = orm.get_owned_region(rid) if orm else None
            if region is not None:
                self._build_region_pool(rid, region.allocator.page_count)
            with self._lock:
                region_pool = self._pool.get(rid)

        if region_pool is None or pid >= len(region_pool):
            logger.error("[Maru] pool miss region=%d page=%d", rid, pid)
            self._handler.free(handle)
            return None

        obj = region_pool[pid]
        logger.debug("[Maru] allocate rid=%d pid=%d size=%d", rid, pid, size)
        return obj

    def batched_allocate(
        self,
        shapes: torch.Size | list[torch.Size],
        dtypes: torch.dtype | list[torch.dtype],
        batch_size: int,
        fmt: MemoryFormat = MemoryFormat.UNDEFINED,
        allocator_type: str | None = None,
    ) -> list[MemoryObj] | None:
        """Allocate multiple CXL-backed MemoryObjs.

        Args:
            shapes: Tensor shape(s) (same for each allocation).
            dtypes: Tensor dtype(s) (same for each allocation).
            batch_size: Number of allocations.
            fmt: Memory format.
            allocator_type: Unused, for interface compatibility.

        Returns:
            List of TensorMemoryObj, or None if any allocation fails.
        """
        results = []
        for _ in range(batch_size):
            obj = self.allocate(shapes, dtypes, fmt, allocator_type)
            if obj is None:
                for allocated in results:
                    self.free(allocated)
                return None
            results.append(obj)
        return results

    def free(
        self,
        memory_obj: MemoryObj,
        allocator_type: str | None = None,
    ) -> None:
        """No-op. CXL pages are managed by MaruBackend.remove(), not here.

        Pool objects persist across store/retrieve cycles. Actual page
        deallocation happens when MaruBackend.remove() is called.
        """
        pass

    def batched_free(
        self,
        memory_objs: list[MemoryObj],
        allocator_type: str | None = None,
        update_stats: bool = True,
    ) -> None:
        """No-op. See free()."""
        pass

    def close(self) -> None:
        """Clean up allocator state."""
        with self._lock:
            self._pool.clear()

    # =========================================================================
    # Store / Retrieve Helpers
    # =========================================================================

    def create_store_handle(self, memory_obj: MemoryObj) -> AllocHandle:
        """Create an AllocHandle from MemoryObj for handler.store().

        Extracts (region_id, page_index) from metadata.address via
        bit decoding. The returned handle has an empty buf — data is
        already in CXL memory.

        Args:
            memory_obj: MemoryObj with address set by this allocator.

        Returns:
            AllocHandle for MaruHandler.store().
        """
        rid, pid = self.decode_address(memory_obj.metadata.address)
        return AllocHandle(
            buf=memoryview(b""),
            _region_id=rid,
            _page_index=pid,
            _size=memory_obj.metadata.phy_size,
        )

    def get_by_location(
        self,
        region_id: int,
        page_index: int,
        actual_size: int,
        single_token_size: int,
    ) -> MemoryObj | None:
        """Look up a pooled MemoryObj by (region_id, page_index).

        For shared regions, builds the pool on-demand if not yet created.

        Args:
            region_id: The region ID from retrieve response.
            page_index: The page index from retrieve response.
            actual_size: Actual data size in bytes.
            single_token_size: Bytes per single token (for partial chunk).

        Returns:
            MemoryObj from the pool, or None if not found.
        """
        with self._lock:
            region_pool = self._pool.get(region_id)

        if region_pool is None:
            if not self.ensure_region_pool(region_id):
                return None
            with self._lock:
                region_pool = self._pool.get(region_id)
            if region_pool is None:
                return None

        if page_index >= len(region_pool):
            logger.error(
                "Page index %d out of range for region %d (pool size=%d)",
                page_index,
                region_id,
                len(region_pool),
            )
            return None

        source = region_pool[page_index]

        if actual_size == self._chunk_size:
            logger.debug(
                "[Maru] get_by_location rid=%d pid=%d full", region_id, page_index
            )
            return source

        # Partial chunk: create a view without mutating the pool object
        logger.debug(
            "[Maru] get_by_location rid=%d pid=%d partial=%d/%d",
            region_id,
            page_index,
            actual_size,
            self._chunk_size,
        )
        return self._create_partial_view(source, actual_size, single_token_size)

    def _create_partial_view(
        self,
        source: TensorMemoryObj,
        actual_size: int,
        single_token_size: int,
    ) -> TensorMemoryObj:
        """Create a partial-chunk view from a pooled MemoryObj.

        The pool object is not mutated. Returns a new TensorMemoryObj
        with a sliced tensor and adjusted shape.

        Args:
            source: The full-chunk pooled MemoryObj.
            actual_size: Actual data size in bytes.
            single_token_size: Bytes per single token.

        Returns:
            New TensorMemoryObj with sliced data and adjusted shape.
        """
        # Slice the flat raw_data tensor to actual_size elements
        dtype_size = source.metadata.dtype.itemsize
        sliced_tensor = source.raw_data[: actual_size // dtype_size]

        shape_list = list(source.metadata.shape)
        shape_list[2] = actual_size // single_token_size

        metadata = MemoryObjMetadata(
            shape=torch.Size(shape_list),
            dtype=source.metadata.dtype,
            address=source.metadata.address,
            phy_size=actual_size,
            ref_count=1,
            fmt=source.metadata.fmt,
            shapes=source.metadata.shapes,
            dtypes=source.metadata.dtypes,
        )
        return TensorMemoryObj(sliced_tensor, metadata, parent_allocator=None)
