# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Re-export from maru_common.ipc for backward compatibility."""

from maru_common.ipc import *  # noqa: F401,F403
from maru_common.ipc import (  # noqa: F401 — explicit re-exports for type checkers
    HEADER_SIZE,
    AllocReq,
    AllocResp,
    ChownReq,
    ErrorResp,
    FreeReq,
    FreeResp,
    GetFdReq,
    GetFdResp,
    MsgHeader,
    MsgType,
    PermGrantReq,
    PermResp,
    PermRevokeReq,
    PermSetDefaultReq,
    StatsReq,
    StatsResp,
)
