# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory backend protocol for MaruShmClient / MarufsClient substitution.

Both MaruShmClient and MarufsClient satisfy this protocol structurally
(no inheritance required). Use for type annotations in DaxMapper and
AllocationManager to catch interface drift via mypy.
"""

from __future__ import annotations

import mmap
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maru_common.types import MaruHandle


@runtime_checkable
class MemoryBackend(Protocol):
    """Structural protocol for CXL memory backends."""

    def alloc(self, size: int, pool_id: int = 0) -> MaruHandle: ...

    def free(self, handle: MaruHandle) -> None: ...

    def mmap(self, handle: MaruHandle, prot: int, flags: int = 0) -> mmap.mmap: ...

    def munmap(self, handle: MaruHandle) -> None: ...

    def close(self) -> None: ...
