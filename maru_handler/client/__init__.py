# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruClient: the worker side of the multi-storage structure.

A worker keeps two connections: ServerConn to MaruServer (session, lookup and
pin, unpin) and StorageConn to the StorageService of its node (attach, credit,
write_done, fetch). MaruClient hides both behind the MaruHandler methods, so
the vLLM connector does not change with them.
"""

from .maru_client import CreditUnavailableError, MaruClient, probe_node
from .storage_conn import AttachRefusedError

__all__ = ["AttachRefusedError", "CreditUnavailableError", "MaruClient", "probe_node"]
