# SPDX-License-Identifier: Apache-2.0
"""MaruHostTensorAllocator — CXL-backed tensor allocator for SGLang HostKVCache.

TODO(CXL-as-L2): Place kv_buffer directly in CXL for L2=L3 zero-copy.
Blocker: HiCache slot lifecycle (protect freed slots from remote reads).

Configuration is read from environment variables:
    MARU_SERVER_URL  — MaruServer address (default: tcp://localhost:5555)
    MARU_INSTANCE_ID — Instance identifier (optional, auto-generated if unset)
    MARU_TIMEOUT_MS  — RPC timeout in milliseconds (default: 5000)
    MARU_EAGER_MAP   — Pre-map shared regions on connect (default: 1)

The pool_size is determined automatically from the tensor dimensions
requested by HostKVCache.allocate().
"""

import ctypes
import logging
import os

import torch

from maru import MaruConfig, MaruHandler

logger = logging.getLogger(__name__)

# Lazy import: HostTensorAllocator lives in sglang.srt which may not be
# importable in all environments (unit tests, dev tooling).
try:
    from sglang.srt.mem_cache.memory_pool_host import HostTensorAllocator
except ImportError:

    class HostTensorAllocator:  # type: ignore[no-redef]
        """Minimal stub for environments without sglang.srt."""

        def __init__(self):
            self.dtype = None
            self.dims = None

        def allocate(self, dims, dtype, device="cpu"):
            self.dtype = dtype
            self.dims = dims
            return torch.empty(dims, dtype=dtype, device=device)


class MaruHostTensorAllocator(HostTensorAllocator):
    """CXL shared-memory tensor allocator for SGLang HostKVCache.

    Allocates the kv_buffer directly from Maru's CXL shared memory region,
    allowing MaruStorage standalone mode to skip data copies on store.
    """

    def __init__(self):
        super().__init__()
        self._handler: MaruHandler | None = None
        self._base_ptr: int = 0  # base address of the CXL region

    @property
    def handler(self) -> MaruHandler | None:
        """The connected MaruHandler (available after allocate())."""
        return self._handler

    @property
    def base_ptr(self) -> int:
        """Base address of the CXL mmap region backing the kv_buffer."""
        return self._base_ptr

    def allocate(
        self, dims: tuple, dtype: torch.dtype, device: str = "cpu"
    ) -> torch.Tensor:
        """Allocate a tensor backed by CXL shared memory.

        Creates a MaruHandler, connects to MaruServer, requests a CXL region
        large enough for the tensor, and returns a torch.Tensor backed by the
        mmap'd CXL memory.

        Args:
            dims: Tensor dimensions requested by HostKVCache.
            dtype: Tensor dtype.
            device: Ignored (always CPU-mapped CXL memory).

        Returns:
            torch.Tensor backed by CXL shared memory.
        """
        self.dims = dims
        self.dtype = dtype

        # Compute required bytes
        numel = 1
        for d in dims:
            numel *= d
        element_size = torch.tensor([], dtype=dtype).element_size()
        total_bytes = numel * element_size

        # Read config from environment variables
        server_url = os.environ.get("MARU_SERVER_URL", "tcp://localhost:5555")
        instance_id = os.environ.get("MARU_INSTANCE_ID", None)
        timeout_ms = int(os.environ.get("MARU_TIMEOUT_MS", "5000"))
        eager_map = os.environ.get("MARU_EAGER_MAP", "1") != "0"

        # Use chunk_size = total_bytes so the entire pool is one chunk.
        # This simplifies the mapping: region offset 0, length = total_bytes.
        config = MaruConfig(
            server_url=server_url,
            instance_id=instance_id,
            pool_size=total_bytes,
            chunk_size_bytes=total_bytes,
            auto_connect=False,
            timeout_ms=timeout_ms,
            use_async_rpc=True,
            eager_map=eager_map,
        )

        handler = MaruHandler(config)
        if not handler.connect():
            raise RuntimeError(
                f"MaruHostTensorAllocator: failed to connect to {server_url}"
            )

        # Get the mapped region's base address.
        # After connect(), the handler has one owned region.
        owned = handler.owned_region_manager
        region_id = owned.get_first_region_id()
        mapper = handler._mapper

        buf = mapper.get_buffer_view(region_id, 0, total_bytes)
        if buf is None:
            handler.close()
            raise RuntimeError("MaruHostTensorAllocator: failed to get CXL buffer view")

        self._handler = handler
        self._base_ptr = ctypes.addressof(ctypes.c_char.from_buffer(buf))

        # Create torch.Tensor backed by the CXL memory
        c_array = (ctypes.c_byte * total_bytes).from_buffer(buf)
        tensor = torch.frombuffer(c_array, dtype=torch.uint8, count=total_bytes)

        if dtype != torch.uint8:
            assert total_bytes % element_size == 0, (
                "Total bytes must be divisible by element size"
            )
            tensor = tensor.view(dtype)

        tensor = tensor.view(dims)

        logger.info(
            "MaruHostTensorAllocator: allocated %d MB CXL tensor "
            "(dims=%s, dtype=%s, region=%d, base_ptr=0x%x)",
            total_bytes >> 20,
            dims,
            dtype,
            region_id,
            self._base_ptr,
        )
        return tensor
