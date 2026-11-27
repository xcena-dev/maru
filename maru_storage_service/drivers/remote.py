# SPDX-License-Identifier: Apache-2.0
"""GPU-node remote control configuration; data lives in worker staging."""

import os

from ..config_fields import (
    NodeConfigError,
    TierContext,
    TierSpec,
    _bool,
    _number,
    _str,
    parse_size,
)
from ..tiers import Level, Tier


def parse(raw: dict, ctx: TierContext) -> dict:
    url = _str(raw, "url", ctx.what)
    host, sep, port = url.removeprefix("tcp://").rpartition(":")
    if not sep or not host or not port.isdigit() or not 0 < int(port) < 65536:
        raise NodeConfigError(f"{ctx.what}.url: expected host:port")
    device = raw.get("rdma_device", "")
    if not isinstance(device, str):
        raise NodeConfigError(f"{ctx.what}.rdma_device: expected a string")
    timeout = _number(raw, "timeout_s", ctx.what, 30)
    if timeout <= 0:
        raise NodeConfigError(f"{ctx.what}.timeout_s: must be positive")
    write_hints = _bool(raw, "write_hints", ctx.what, False)
    wait_ms = raw.get("write_wait_ms", 50)
    if type(wait_ms) is not int or not 0 <= wait_ms <= 10_000:
        raise NodeConfigError(
            f"{ctx.what}.write_wait_ms: expected an integer from 0 to 10000"
        )
    page = ctx.page_size
    # Default: max(1 GiB, 64 pages), rounded up to whole pages
    default_staging = max(-(-(1 << 30) // page), 64) * page
    staging = parse_size(
        raw.get("staging_size", default_staging), f"{ctx.what}.staging_size"
    )
    total = parse_size(raw.get("staging_max_bytes", staging), "staging_max_bytes")
    kept = parse_size(raw.get("quarantine_max", staging // 2), "quarantine_max")
    if staging < 2 * ctx.page_size or staging % ctx.page_size:
        raise NodeConfigError("staging_size: need two or more whole pages")
    if total < staging or not ctx.page_size <= kept <= staging:
        raise NodeConfigError(
            "need staging_max_bytes >= staging_size >= quarantine_max >= page_size"
        )
    return {
        "options": {
            "url": f"{host}:{port}",
            "rdma_device": device,
            "key_file": os.path.expanduser(_str(raw, "key_file", ctx.what)),
            "timeout_s": timeout,
            "write_hints": write_hints,
            "write_wait_ms": wait_ms,
            "staging_size": staging,
            "staging_max_bytes": total,
            "quarantine_max": kept,
        }
    }


class RemoteTier(Tier):
    name = "remote"
    level = Level.L2

    def pool_infos(self):
        return []

    def describe(self, adopted):
        return "worker direct RDMA"


SPEC = TierSpec(
    store="remote",
    level=Level.L2,
    local=False,
    kind=None,
    keys=frozenset(
        {
            "store",
            "url",
            "rdma_device",
            "key_file",
            "timeout_s",
            "write_hints",
            "write_wait_ms",
            "staging_size",
            "staging_max_bytes",
            "quarantine_max",
        }
    ),
    common=False,
    gpu_only=True,
    parse=parse,
    make=lambda config, env: RemoteTier(),
)
