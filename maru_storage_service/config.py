# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService node config: JSON, checked before anything starts.

Unknown keys are errors so a typo never falls back to a default.
"""

import grp
import itertools
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
from .drivers.peer import DEFAULT_RDMA_TIMEOUT_S as DEFAULT_RDMA_TIMEOUT_S
from .drivers.registry import TIERS
from .tiers import Level

# Old names read as the current store (the session backends called DRAM "cpu")
STORE_ALIASES = {"cpu": "dram"}
# node_id goes into pool ids such as dram:{node_id}:{node_epoch}
_NODE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
DEMOTIONS = ("back", "through")
ROLES = ("gpu", "memory")

DEFAULT_HIGH_WATERMARK = 0.9
DEFAULT_LOW_WATERMARK = 0.8
# Write-through copies in flight to the tier below, per tier
DEFAULT_THROUGH_MAX_INFLIGHT = 256 << 20

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
DEFAULT_FETCH_REQUEST_MAX = 1 << 30
DEFAULT_FETCH_MAX_INFLIGHT = 4 << 30

_TOP_KEYS = {
    "node_id",
    "role",
    "socket",
    "socket_group",
    "server",
    "page_size",
    "local_pin",
    "tiers",
    "budgets",
    "remote_server",
}
_BUDGET_KEYS = {
    "credit_max_bytes",
    "credit_total_max_bytes",
    "credit_refill_low",
    "credit_refill_high",
    "commit_pending_max_bytes",
    "transfer_reserve_bytes",
    "fetch_request_max_bytes",
    "fetch_max_inflight_bytes",
}
_REMOTE_SERVER_KEYS = {
    "listen",
    "key_file",
    "rdma_device",
    "reserve_ttl_s",
    "peer_quarantine_max",
    "quarantine_max",
    "stage_workers",
    "stage_queue",
}
# A memory node's peer side (the GPU nodes' remote tiers)
DEFAULT_RESERVE_TTL_S = 2 * DEFAULT_RDMA_TIMEOUT_S
DEFAULT_STAGE_WORKERS = 4
DEFAULT_STAGE_QUEUE = 64


@dataclass(frozen=True)
class TierTraits:
    """What the StorageService does with a tier's pools, instead of asking
    which medium they are."""

    name: str  # the store name
    level: Level
    kind: PoolKind | None  # the wire kind of its pools
    credit: bool  # workers get credit from it (the top tier)
    evicts: bool  # pressure makes room: by demotion if it demotes, else by retiring
    # Demotion back and through: pressure moves its pages to these tiers, in
    # order: each takes what the ones before cannot
    demotes_to: tuple[str, ...] = ()
    # Demotion through: pages that become READY here are copied at once, to
    # the first of these tiers that takes them
    copies_to: tuple[str, ...] = ()
    # Workers get the credit the tiers above lack from it
    credit_fallback: bool = False
    # Bytes of those copies in flight at once; past it a page is skipped
    through_max_inflight: int = 0

    @property
    def io_only(self) -> bool:
        """Only the tier's I/O moves its pages; workers never map them."""
        return self.level is Level.L2


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
    stage_workers: int = DEFAULT_STAGE_WORKERS
    stage_queue: int = DEFAULT_STAGE_QUEUE


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
        """Traits of each tier, by store name in config order. The top tier
        gives credit, and a tier with credit_fallback the credit the tiers
        above lack. A tier with demotion "back" evicts by moving its pages
        down: an L1 tier over L2 tiers to those, in order (remote, then ssd),
        any other tier to the tier below. A tier that gets new pages (credit
        or demotion) and demotes nowhere evicts by dropping. Another node's
        tier (remote) takes the pages demoted to it and never evicts: its
        owner does."""
        out: dict[str, TierTraits] = {}
        receives = {self.tiers[0].store}  # the top tier gets the writes
        for i, t in enumerate(self.tiers):
            spec = TIERS[t.store]
            if not spec.local:
                out[t.store] = TierTraits(
                    name=t.store,
                    level=spec.level,
                    kind=None,
                    credit=False,
                    evicts=False,
                )
                continue
            below = self.tiers[i + 1 :]
            targets: tuple[str, ...] = ()
            if t.demotion in DEMOTIONS and below:
                l2 = itertools.takewhile(lambda b: not b.is_memory, below)
                if spec.level is Level.L1:
                    targets = tuple(b.store for b in l2)
                targets = targets or (below[0].store,)
            fallback = i > 0 and t.credit_fallback
            out[t.store] = TierTraits(
                name=t.store,
                level=spec.level,
                kind=spec.kind,
                credit=i == 0,
                evicts=t.store in receives or fallback,
                demotes_to=targets,
                copies_to=targets if t.demotion == "through" else (),
                credit_fallback=fallback,
                through_max_inflight=t.through_max_inflight or 0,
            )
            receives.update(targets)
        return out

    def transfer_defaults(self) -> TransferDefaults:
        """Workers park loads when a tier can only be fetched from; a store
        ends in a tier workers map, so it needs no write-behind."""
        fetched = any(not t.is_memory for t in self.tiers)
        return TransferDefaults(async_load=fetched, async_store=False)


def _parse_tier(raw: Any, index: int, page_size: int, last: bool) -> TierConfig:
    what = f"tiers[{index}]"
    if not isinstance(raw, dict):
        raise NodeConfigError(f"{what}: expected an object, got {type(raw).__name__}")
    store = raw.get("store")
    if isinstance(store, str):
        store = STORE_ALIASES.get(store, store)
    if not isinstance(store, str) or store not in TIERS:
        raise NodeConfigError(f"{what}.store: one of {list(TIERS)}, got {store!r}")
    spec = TIERS[store]
    what = f"{what} ({store})"
    _require_keys(raw, spec.keys, what)

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

        if last:
            if "demotion" in raw:
                raise NodeConfigError(
                    f"{what}.demotion: the last tier has nothing below it"
                )
            demotion = None
        else:
            demotion = raw.get("demotion", "back")
            if demotion not in DEMOTIONS:
                raise NodeConfigError(
                    f"{what}.demotion: one of {list(DEMOTIONS)}, got {demotion!r}"
                )
        through_max = None
        if "through_max_inflight" in raw:
            if demotion != "through":
                raise NodeConfigError(
                    f"{what}.through_max_inflight: only with demotion through"
                )
            through_max = parse_size(
                raw["through_max_inflight"], f"{what}.through_max_inflight"
            )
            if through_max < page_size:
                raise NodeConfigError(
                    f"{what}.through_max_inflight: {through_max} is smaller than "
                    f"page_size {page_size}"
                )
        elif demotion == "through":
            through_max = DEFAULT_THROUGH_MAX_INFLIGHT

        high = _number(raw, "high_watermark", what, DEFAULT_HIGH_WATERMARK)
        low = _number(raw, "low_watermark", what, DEFAULT_LOW_WATERMARK)
        if not 0 < low < high <= 1:
            raise NodeConfigError(
                f"{what}: need 0 < low_watermark < high_watermark <= 1, got {low} and {high}"
            )
        tier.update(
            size=size,
            demotion=demotion,
            high_watermark=high,
            low_watermark=low,
            through_max_inflight=through_max,
        )

    ctx = TierContext(what=what, index=index, page_size=page_size, last=last, size=size)
    tier.update(spec.parse(raw, ctx))
    return TierConfig(**tier)


def _parse_budgets(raw: Any, page_size: int, tiers: tuple[TierConfig, ...]) -> Budgets:
    raw = _require_keys(raw if raw is not None else {}, _BUDGET_KEYS, "budgets")
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
    credit_tiers = [t for i, t in enumerate(tiers) if i == 0 or t.credit_fallback]
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
    # A tier only reached by I/O (SSD, remote): its pages are fetched into the
    # top tier, which keeps a page of its transfer reserve for that
    fetches = any(not t.is_memory for t in tiers)
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
        if fetches and t is top:
            default = max(default, page_size)
        reserve[t.store] = default

    fetch_request = size_or("fetch_request_max_bytes", DEFAULT_FETCH_REQUEST_MAX)
    fetch_inflight = size_or("fetch_max_inflight_bytes", DEFAULT_FETCH_MAX_INFLIGHT)

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
    # The fetch caps bind only a node that fetches
    if fetches and not page_size <= fetch_request <= fetch_inflight:
        raise NodeConfigError(
            "budgets: need page_size <= fetch_request_max_bytes <= "
            f"fetch_max_inflight_bytes, got {page_size}, {fetch_request}, {fetch_inflight}"
        )
    if fetches and fetch_inflight < 2 * page_size:
        raise NodeConfigError(
            f"budgets.fetch_max_inflight_bytes: {fetch_inflight} cannot hold one "
            f"page and its bounce buffer (2 x page_size = {2 * page_size})"
        )
    if fetches and reserve.get(top.store, 0) < page_size:
        raise NodeConfigError(
            f"budgets.transfer_reserve_bytes: the {top.store} tier keeps "
            f"{reserve.get(top.store, 0)}, less than one page ({page_size}) to "
            "fetch into"
        )
    if top.size is not None:
        # Credit comes from the top tier and the fallback tiers, each less
        # its transfer reserve
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
        fetch_request_max_bytes=fetch_request,
        fetch_max_inflight_bytes=fetch_inflight,
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
    counts = {}
    for key, default in (
        ("stage_workers", DEFAULT_STAGE_WORKERS),
        ("stage_queue", DEFAULT_STAGE_QUEUE),
    ):
        value = rs.get(key, default)
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise NodeConfigError(
                f"{what}.{key}: expected an integer >= 1, got {value!r}"
            )
        counts[key] = value
    if counts["stage_queue"] < counts["stage_workers"]:
        raise NodeConfigError(
            f"{what}: stage_queue ({counts['stage_queue']}) is smaller than "
            f"stage_workers ({counts['stage_workers']})"
        )
    return RemoteServerConfig(
        listen=f"{host}:{port}",
        key_file=os.path.expanduser(_str(rs, "key_file", what)),
        rdma_device=device,
        reserve_ttl_s=float(ttl),
        peer_quarantine_max=peer_max,
        quarantine_max=cap("quarantine_max", None),
        stage_workers=counts["stage_workers"],
        stage_queue=counts["stage_queue"],
    )


def parse_node_config(raw: Any, source: str = "") -> NodeConfig:
    """Check a decoded node config and fill its defaults."""
    raw = _require_keys(raw, _TOP_KEYS, "config")
    node_id = _str(raw, "node_id", "config")
    if not _NODE_ID_RE.match(node_id):
        raise NodeConfigError(
            f"config.node_id: letters, digits, '.', '_' and '-' only, got {node_id!r}"
        )
    server = _str(raw, "server", "config")
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
    role = raw.get("role", "gpu")
    if role not in ROLES:
        raise NodeConfigError(f"config.role: one of {list(ROLES)}, got {role!r}")
    if "page_size" not in raw:
        raise NodeConfigError("config.page_size: required")
    page_size = parse_size(raw["page_size"], "config.page_size")
    if page_size <= 0:
        raise NodeConfigError("config.page_size: must be positive")
    local_pin = _bool(raw, "local_pin", "config", False)
    if local_pin:
        raise NodeConfigError(
            "config.local_pin: the local pin path is not supported yet"
        )

    raw_tiers = raw.get("tiers")
    if not isinstance(raw_tiers, list) or not raw_tiers:
        raise NodeConfigError("config.tiers: required, a non-empty list")
    tiers = tuple(
        _parse_tier(t, i, page_size, last=(i == len(raw_tiers) - 1))
        for i, t in enumerate(raw_tiers)
    )
    stores = [t.store for t in tiers]
    duplicated = sorted({s for s in stores if stores.count(s) > 1})
    if duplicated:
        raise NodeConfigError(f"config.tiers: a store appears twice: {duplicated}")
    # The wire names a node's tier by the kind of its pools
    kinds = [TIERS[s].kind for s in stores if TIERS[s].local]
    shared = sorted({str(k) for k in kinds if k is not None and kinds.count(k) > 1})
    if shared:
        raise NodeConfigError(
            f"config.tiers: two local tiers have pools of one kind: {shared}"
        )
    if not tiers[0].is_memory:
        memory = " or ".join(s.store for s in TIERS.values() if s.level is Level.L1)
        raise NodeConfigError(
            f"config.tiers[0]: the first tier must be {memory} (workers copy to it from the GPU), got {tiers[0].store}"
        )
    for t in tiers:
        spec = TIERS[t.store]
        if spec.gpu_only and role == "memory":
            raise NodeConfigError(
                f"config.tiers: a memory node cannot have a {t.store} tier"
            )
        if spec.last_only and t is not tiers[-1]:
            raise NodeConfigError(f"config.tiers: {t.store} must be the last tier")

    budgets = _parse_budgets(raw.get("budgets"), page_size, tiers)
    remote_server = None
    if raw.get("remote_server") is not None:
        if role != "memory":
            raise NodeConfigError(
                "config.remote_server: only a memory node serves other nodes"
            )
        remote_server = _parse_remote_server(raw["remote_server"], budgets)
    if role == "memory" and remote_server is None:
        raise NodeConfigError(
            "config.remote_server: required on a memory node (other nodes reach it there)"
        )
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


def load_node_config(path: str | Path) -> NodeConfig:
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
        return parse_node_config(raw, source=str(path))
    except NodeConfigError as e:
        raise NodeConfigError(f"{path}: {e}") from None
