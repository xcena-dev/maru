# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""ReadLease: what a retrieve hands back on a node or the cpu backend, whatever
medium the bytes are on (DRAM or CXL pool)."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class ReadLease(Protocol):
    """Bytes that stay valid until ``release()``; releasing twice does
    nothing. The caller releases after its last read or GPU copy."""

    @property
    def view(self) -> memoryview: ...

    def release(self) -> None: ...
