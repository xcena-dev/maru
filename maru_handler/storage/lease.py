# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Explicit ownership of node read buffers."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class ReadLease(Protocol):
    """Bytes that stay valid until ``release()``; releasing twice does
    nothing. The caller releases after its last read or GPU copy."""

    @property
    def view(self) -> memoryview: ...

    def release(self) -> None: ...
