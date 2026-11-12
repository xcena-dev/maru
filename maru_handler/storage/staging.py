# SPDX-License-Identifier: Apache-2.0
"""Page-sized host slots between GPU memory and RDMA transfers."""

from __future__ import annotations

import logging
import mmap
import threading
from collections import deque

logger = logging.getLogger(__name__)


class StagingBuffer:
    """Fixed-size slots carved from one anonymous mapping."""

    def __init__(self, capacity: int, slot_bytes: int):
        count = capacity // slot_bytes
        if count < 1:
            raise ValueError("remote staging capacity must hold at least one slot")
        self.slot_bytes = slot_bytes
        self.count = count
        self.nbytes = count * slot_bytes
        self._mapping = mmap.mmap(-1, self.nbytes)
        self._view = memoryview(self._mapping)
        from maru_remote.transport import buffer_address

        self.address = buffer_address(self._view)
        self._free: deque[int] = deque(range(count))
        self._free_lock = threading.Lock()  # engine, loader and store threads
        self.cuda_registered = False
        # Slots that stores leave free so that a concurrent load finds room
        self.load_reserve = 0

    def cuda_register(self) -> bool:
        """Page-lock the buffer for CUDA so GPU copies and kernels can read it.

        Best effort, as for mapped CXL regions: on failure the buffer stays
        pageable and callers use their pageable-copy path.

        Returns:
            Whether the buffer is now registered with CUDA.
        """
        if self.cuda_registered:
            return True
        try:
            import torch

            if not torch.cuda.is_available():
                return False
            from maru_handler.memory.mapper import _clear_cuda_sticky_error, _cuda_rc

            rc = _cuda_rc(
                torch.cuda.cudart().cudaHostRegister(self.address, self.nbytes, 0)
            )
            if rc != 0:
                _clear_cuda_sticky_error()
                logger.warning(
                    "cudaHostRegister of the remote staging buffer failed: rc=%d", rc
                )
                return False
        except (ImportError, RuntimeError, OSError) as exc:
            logger.warning(
                "cudaHostRegister of the remote staging buffer failed: %s", exc
            )
            return False
        self.cuda_registered = True
        return True

    def take(self, n: int = 1, keep: int = 0) -> list[int] | None:
        """Take ``n`` free slots, leaving at least ``keep`` free.

        Returns None (and takes nothing) if that is not possible.
        """
        with self._free_lock:
            if len(self._free) - n < keep:
                return None
            return [self._free.popleft() for _ in range(n)]

    def give(self, slot: int) -> None:
        """Return one slot."""
        with self._free_lock:
            self._free.append(slot)

    def free_count(self) -> int:
        """Number of free slots."""
        with self._free_lock:
            return len(self._free)

    def load_capacity(self) -> int:
        """Slots the next load batch may take: the free slots, at most the
        load reserve when there is one."""
        with self._free_lock:
            free = len(self._free)
        return min(free, self.load_reserve) if self.load_reserve else free

    def view(self, slot: int) -> memoryview:
        """Writable view of one whole slot."""
        start = slot * self.slot_bytes
        return self._view[start : start + self.slot_bytes]

    def addr(self, slot: int) -> int:
        """Address of the first byte of a slot."""
        return self.address + slot * self.slot_bytes

    def close(self) -> None:
        """Unmap the buffer unless a caller still holds an exported view."""
        if self.cuda_registered:
            try:
                import torch

                from maru_handler.memory.mapper import (
                    _clear_cuda_sticky_error,
                    _cuda_rc,
                )

                if _cuda_rc(torch.cuda.cudart().cudaHostUnregister(self.address)) != 0:
                    _clear_cuda_sticky_error()
            except (ImportError, RuntimeError, OSError) as exc:
                logger.warning(
                    "cudaHostUnregister of the remote staging buffer failed: %s", exc
                )
            self.cuda_registered = False
        try:
            self._view.release()
            self._mapping.close()
        except BufferError:
            logger.warning("remote staging buffer still exported; leaving it mapped")
