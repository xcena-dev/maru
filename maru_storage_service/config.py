# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService node config: JSON, checked before anything starts.

Unknown keys are errors so a typo never falls back to a default.
"""

import grp
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from maru_common.node_protocol import DEFAULT_NODE_SOCKET, Budgets, TransferDefaults
from maru_common.storage_types import PoolKind

from .config_fields import NodeConfigError as NodeConfigError
from .config_fields import TierConfig as TierConfig
from .config_fields import TierContext
from .config_fields import _bool as _bool
from .config_fields import _number as _number
from .config_fields import _require_keys as _require_keys
from .config_fields import _str as _str
from .config_fields import parse_size as parse_size
from .drivers.cxl import DEFAULT_REGION_SIZE as DEFAULT_REGION_SIZE
from .drivers.cxl import DEFAULT_RM as DEFAULT_RM
from .drivers.registry import TIERS
from .tiers import Level

# node_id goes into pool ids such as dram:{node_id}:{node_epoch}
_NODE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
ROLES = ("gpu", "memory")
DEFAULT_STATE_DIR = "/run/maru"

DEFAULT_HIGH_WATERMARK = 0.9
DEFAULT_LOW_WATERMARK = 0.8
# Budget defaults (all provisional until measured)
CREDIT_MAX_PAGES = 16
# A worker's credit cap is at least this much: one vLLM step of an 8B model
# stores about 512 MiB, which small pages must not split into credit waits
CREDIT_MAX_MIN_BYTES = 512 << 20
# Node credit: the credit tiers' share that may be out being written at once.
# Above one step (2 GiB for an 8B model at 16384 tokens) on a 16 GiB DRAM;
# a step asks only for what it stores, so the rest stays cache
CREDIT_TOTAL_FRACTION = 0.25
CREDIT_REFILL_LOW_FRACTION = 0.25
TRANSFER_RESERVE_FRACTION = 0.05

_TOP_KEYS = {
    "node_id",
    "state_dir",
    "server",
    "page_size",
    "tiers",
    "budgets",
}
GPU_KEYS = {"socket", "socket_group", "local_pin"}
MEMORY_KEYS = {"remote_server"}
WORKER_BUDGET_KEYS = {
    "credit_max_bytes",
    "credit_refill_low",
    "credit_refill_high",
}
_BUDGET_KEYS = {
    "credit_max_bytes",
    "credit_total_max_bytes",
    "credit_refill_low",
    "credit_refill_high",
    "commit_pending_max_bytes",
    "transfer_reserve_bytes",
}
_REMOTE_SERVER_KEYS = {
    "listen",
    "key_file",
    "rdma_device",
    "reserve_ttl_s",
    "peer_quarantine_max",
    "quarantine_max",
}
# A memory node's peer side (the GPU nodes' remote tiers)
DEFAULT_RESERVE_TTL_S = 60.0


@dataclass(frozen=True)
class TierTraits:
    """What the StorageService does with a tier's pools, instead of asking
    which medium they are."""

    name: str  # the store name
    level: Level
    kind: PoolKind | None  # the wire kind of its pools
    credit: bool  # workers get credit from it (the top tier)
    evicts: bool  # pressure retires least recently used pages


@dataclass(frozen=True)
class RemoteServerConfig:
    """A memory node's endpoint for GPU nodes' remote tiers."""

    listen: str  # host:port
    key_file: str
    # Quarantined bytes at which a peer node (all peers) gets no more room
    peer_quarantine_max: int
    quarantine_max: int | None = None  # None: 1/8 of the registered L1 bytes
    rdma_device: str = ""  # UCX device of the NIC; empty for UCX's choice
    # A credited page whose WRITE_DONE has not come by then is quarantined
    reserve_ttl_s: float = DEFAULT_RESERVE_TTL_S


@dataclass(frozen=True)
class NodeConfig:
    node_id: str
    role: str
    socket: str
    server: str
    page_size: int
    local_pin: bool
    tiers: tuple[TierConfig, ...]
    budgets: Budgets
    state_dir: str = DEFAULT_STATE_DIR
    remote_server: RemoteServerConfig | None = None
    # Group the socket is given to (mode 0660); None keeps the service's group
    socket_group: str | None = None
    source: str = field(default="", compare=False)

    @property
    def top(self) -> TierConfig:
        return self.tiers[0]

    def tier(self, store: str) -> TierConfig | None:
        return next((t for t in self.tiers if t.store == store), None)

    def traits(self) -> dict[str, TierTraits]:
        """GPU stores keep their entries; memory stores evict under pressure."""
        t = self.top
        spec = TIERS[t.store]
        return {
            t.store: TierTraits(
                t.store, spec.level, spec.kind, True, self.role == "memory"
            )
        }

    def transfer_defaults(self) -> TransferDefaults:
        """Workers copy directly to and from the store."""
        return TransferDefaults(async_load=False, async_store=False)


def _parse_tier(
    raw: Any, index: int, page_size: int, last: bool, role: str
) -> TierConfig:
    what = f"tiers[{index}]"
    if not isinstance(raw, dict):
        raise NodeConfigError(f"{what}: expected an object, got {type(raw).__name__}")
    store = raw.get("store")
    if isinstance(store, str):
        if store == "cxl":
            raise NodeConfigError(
                f'{what}.store: "cxl" names the shared CXL backend; a node\'s own CXL store is "local_cxl"'
            )
        if store == "cpu":
            raise NodeConfigError(
                f'{what}.store: "cpu" names the worker backend; a node\'s DRAM store is "dram"'
            )
    if not isinstance(store, str) or store not in TIERS:
        raise NodeConfigError(f"{what}.store: one of {list(TIERS)}, got {store!r}")
    spec = TIERS[store]
    what = f"{what} ({store})"
    keys = (
        spec.keys
        if role == "memory"
        else spec.keys - {"high_watermark", "low_watermark"}
    )
    _require_keys(raw, keys, what)

    tier: dict[str, Any] = {"store": store}
    size = None
    if spec.common:
        size = raw.get("size")
        if size is None:
            if not spec.size_optional:
                raise NodeConfigError(f"{what}.size: required")
        else:
            size = parse_size(size, f"{what}.size")
            if size < page_size:
                raise NodeConfigError(
                    f"{what}.size: {size} is smaller than page_size {page_size}"
                )

        high = _number(raw, "high_watermark", what, DEFAULT_HIGH_WATERMARK)
        low = _number(raw, "low_watermark", what, DEFAULT_LOW_WATERMARK)
        if not 0 < low < high <= 1:
            raise NodeConfigError(
                f"{what}: need 0 < low_watermark < high_watermark <= 1, got {low} and {high}"
            )
        tier.update(
            size=size,
            high_watermark=high if role == "memory" else None,
            low_watermark=low if role == "memory" else None,
        )

    ctx = TierContext(what=what, index=index, page_size=page_size, last=last, size=size)
    tier.update(spec.parse(raw, ctx))
    return TierConfig(**tier)


def _parse_budgets(
    raw: Any, page_size: int, tiers: tuple[TierConfig, ...], role: str
) -> Budgets:
    keys = _BUDGET_KEYS if role == "gpu" else _BUDGET_KEYS - WORKER_BUDGET_KEYS
    raw = _require_keys(raw if raw is not None else {}, keys, "budgets")
    top = tiers[0]

    def size_or(key: str, default: int | None, why: str = "") -> int:
        if key in raw:
            return parse_size(raw[key], f"budgets.{key}")
        if default is None:
            raise NodeConfigError(f"budgets.{key}: required {why}".rstrip())
        return default

    def share(tier: TierConfig, fraction: float) -> int | None:
        return None if tier.size is None else int(tier.size * fraction)

    no_size = f"because tiers[0] ({top.store}) has no size"
    credit_tiers = [top]
    sized = [t for t in credit_tiers if t.size is not None]
    credit_total = size_or(
        "credit_total_max_bytes",
        None
        if top.size is None
        else int(sum(t.size or 0 for t in sized) * CREDIT_TOTAL_FRACTION),
        no_size,
    )
    # A worker may hold up to the node total: it asks for each step's stores
    # when the step starts. What it keeps at hand between steps is 16 pages
    # or 512 MiB, whichever is more, in whole pages and within its cap
    credit_max = size_or(
        "credit_max_bytes", max(page_size, credit_total // page_size * page_size)
    )
    refill_high = size_or(
        "credit_refill_high",
        max(
            page_size,
            min(max(CREDIT_MAX_PAGES * page_size, CREDIT_MAX_MIN_BYTES), credit_max)
            // page_size
            * page_size,
        ),
    )
    refill_low = size_or(
        "credit_refill_low", int(refill_high * CREDIT_REFILL_LOW_FRACTION)
    )
    # Writes waiting for their commit come from credit: as much may wait
    commit_pending = size_or("commit_pending_max_bytes", credit_total)

    memory_tiers = [t for t in tiers if t.is_memory]
    reserve_raw = raw.get("transfer_reserve_bytes")
    reserve: dict[str, int] = {}
    for t in memory_tiers:
        if isinstance(reserve_raw, dict):
            unknown = sorted(set(reserve_raw) - {m.store for m in memory_tiers})
            if unknown:
                raise NodeConfigError(
                    f"budgets.transfer_reserve_bytes: no memory tier named {unknown}"
                )
            value = reserve_raw.get(t.store)
            if value is not None:
                reserve[t.store] = parse_size(
                    value, f"budgets.transfer_reserve_bytes.{t.store}"
                )
                continue
        elif reserve_raw is not None:
            reserve[t.store] = parse_size(reserve_raw, "budgets.transfer_reserve_bytes")
            continue
        default = share(t, TRANSFER_RESERVE_FRACTION)
        if default is None:
            raise NodeConfigError(
                f"budgets.transfer_reserve_bytes: required for the {t.store} tier because it has no size"
            )
        reserve[t.store] = default

    if credit_max < page_size:
        raise NodeConfigError(
            f"budgets.credit_max_bytes: {credit_max} is smaller than page_size {page_size}"
        )
    if not refill_low <= refill_high <= credit_max <= credit_total:
        raise NodeConfigError(
            "budgets: need credit_refill_low <= credit_refill_high <= credit_max_bytes "
            f"<= credit_total_max_bytes, got {refill_low}, {refill_high}, {credit_max}, "
            f"{credit_total} (values not set in budgets are defaults; a top tier this "
            "small cannot give one page of credit)"
        )
    if top.size is not None:
        # Credit and pending writes leave the transfer reserve free
        capacity = sum(t.size or 0 for t in sized)
        used = (
            credit_total + commit_pending + sum(reserve.get(t.store, 0) for t in sized)
        )
        if used > capacity:
            names = " + ".join(t.store for t in sized)
            raise NodeConfigError(
                f"budgets: credit_total_max_bytes + commit_pending_max_bytes + "
                f"transfer_reserve_bytes ({used}) exceed the tiers credit comes "
                f"from ({names}: {capacity})"
            )
    return Budgets(
        credit_max_bytes=credit_max,
        credit_total_max_bytes=credit_total,
        credit_refill_low=refill_low,
        credit_refill_high=refill_high,
        commit_pending_max_bytes=commit_pending,
        transfer_reserve_bytes=reserve,
    )


def _parse_remote_server(raw: Any, budgets: Budgets) -> RemoteServerConfig:
    what = "config.remote_server"
    rs = _require_keys(raw, _REMOTE_SERVER_KEYS, what)
    listen = _str(rs, "listen", what)
    host, sep, port = listen.removeprefix("tcp://").rpartition(":")
    if not sep or not host or not port.isdigit() or not 0 < int(port) < 65536:
        raise NodeConfigError(f"{what}.listen: expected host:port, got {listen!r}")
    device = rs.get("rdma_device", "")
    if not isinstance(device, str):
        raise NodeConfigError(f"{what}.rdma_device: expected a string, got {device!r}")
    ttl = _number(rs, "reserve_ttl_s", what, DEFAULT_RESERVE_TTL_S)
    if ttl <= 0:
        raise NodeConfigError(f"{what}.reserve_ttl_s: must be positive, got {ttl}")

    def cap(key: str, default: int | None) -> int | None:
        if key not in rs:
            return default
        value = parse_size(rs[key], f"{what}.{key}")
        if value == 0:
            raise NodeConfigError(f"{what}.{key}: must be positive")
        return value

    peer_max = cap("peer_quarantine_max", 2 * budgets.credit_max_bytes)
    assert peer_max is not None
    return RemoteServerConfig(
        listen=f"{host}:{port}",
        key_file=os.path.expanduser(_str(rs, "key_file", what)),
        rdma_device=device,
        reserve_ttl_s=float(ttl),
        peer_quarantine_max=peer_max,
        quarantine_max=cap("quarantine_max", None),
    )


def parse_node_config(raw: Any, source: str = "", *, role: str = "gpu") -> NodeConfig:
    """Check a decoded node config and fill its defaults."""
    if role not in ROLES:
        raise ValueError(f"unknown node role: {role!r}")
    if isinstance(raw, dict) and "role" in raw:
        raise NodeConfigError(
            "config.role: run maru-storage for a GPU node or maru-memory-node for a memory node"
        )
    keys = _TOP_KEYS | (GPU_KEYS if role == "gpu" else MEMORY_KEYS)
    raw = _require_keys(raw, keys, "config")
    node_id = _str(raw, "node_id", "config")
    if not _NODE_ID_RE.match(node_id):
        raise NodeConfigError(
            f"config.node_id: letters, digits, '.', '_' and '-' only, got {node_id!r}"
        )
    server = _str(raw, "server", "config")
    state_dir = os.path.expanduser(_str(raw, "state_dir", "config", DEFAULT_STATE_DIR))
    socket_path = _str(raw, "socket", "config", DEFAULT_NODE_SOCKET)
    socket_group = None
    if raw.get("socket_group") is not None:
        socket_group = _str(raw, "socket_group", "config")
        try:
            grp.getgrnam(socket_group)
        except KeyError:
            raise NodeConfigError(
                f"config.socket_group: no group named {socket_group!r}"
            ) from None
    if "page_size" not in raw:
        raise NodeConfigError("config.page_size: required")
    page_size = parse_size(raw["page_size"], "config.page_size")
    if page_size <= 0:
        raise NodeConfigError("config.page_size: must be positive")
    local_pin = _bool(raw, "local_pin", "config", False)
    if local_pin:
        raise NodeConfigError("config.local_pin: local pins are not supported")

    raw_tiers = raw.get("tiers")
    if not isinstance(raw_tiers, list):
        raise NodeConfigError("config.tiers: required, a list")
    if len(raw_tiers) != 1:
        raise NodeConfigError(
            f"tiers: a node serves one store (dram, local_cxl or remote), got {len(raw_tiers)}"
        )
    tiers = (_parse_tier(raw_tiers[0], 0, page_size, last=True, role=role),)
    spec = TIERS[tiers[0].store]
    if spec.gpu_only and role == "memory":
        raise NodeConfigError(
            f"config.tiers: a memory node cannot have a {tiers[0].store} tier"
        )

    budgets = _parse_budgets(raw.get("budgets"), page_size, tiers, role)
    remote_server = None
    if raw.get("remote_server") is not None:
        remote_server = _parse_remote_server(raw["remote_server"], budgets)
    return NodeConfig(
        node_id=node_id,
        role=role,
        socket=socket_path,
        socket_group=socket_group,
        server=server,
        page_size=page_size,
        local_pin=local_pin,
        tiers=tiers,
        budgets=budgets,
        state_dir=state_dir,
        remote_server=remote_server,
        source=source,
    )


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict:
    keys = [k for k, _ in pairs]
    duplicated = sorted({k for k in keys if keys.count(k) > 1})
    if duplicated:
        raise NodeConfigError(f"a key appears twice in one object: {duplicated}")
    return dict(pairs)


def _reject_constant(name: str) -> Any:
    raise NodeConfigError(f"{name} is not a valid number here")


def load_node_config(path: str | Path, *, role: str = "gpu") -> NodeConfig:
    """Read and check the node config file."""
    path = Path(path)
    try:
        text = path.read_bytes().decode("utf-8")
        raw = json.loads(
            text,
            object_pairs_hook=_no_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except OSError as e:
        raise NodeConfigError(f"{path}: cannot read: {e}") from e
    except UnicodeDecodeError as e:
        raise NodeConfigError(f"{path}: not UTF-8 text: {e}") from e
    except json.JSONDecodeError as e:
        raise NodeConfigError(f"{path}: not valid JSON: {e}") from e
    except NodeConfigError as e:
        raise NodeConfigError(f"{path}: {e}") from None
    try:
        return parse_node_config(raw, source=str(path), role=role)
    except NodeConfigError as e:
        raise NodeConfigError(f"{path}: {e}") from None
