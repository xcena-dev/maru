# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Registered stores and their leased lifetimes."""

import logging
import mmap
import threading

import pytest

from maru_common.storage_types import (
    PoolDescriptor,
    PoolInfo,
    PoolKind,
    RmDomain,
    Visibility,
)
from maru_storage_service import node_run as service_module
from maru_storage_service.config import parse_node_config
from maru_storage_service.config_fields import TierSpec
from maru_storage_service.drivers.registry import TIERS
from maru_storage_service.service import Node, StorageService
from maru_storage_service.tiers import L1Tier, Leased, Level
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import PAGE as NODE_PAGE
from tests.unit.ms_node_helpers import remove_tree, short_tmpdir, start_node

PAGE = mmap.PAGESIZE
WAIT = 5
POOL = "fake:n:pool"


def allow_store(monkeypatch, name):
    monkeypatch.setattr(
        service_module, "SERVED_TIERS", {"gpu": ([name],), "memory": ([name],)}
    )


def test_a_registered_store_is_started_by_the_service(monkeypatch, caplog):
    from maru_storage_service.drivers.dram import DramTier

    made = []

    def make(tier, env):
        made.append(
            DramTier(
                tier,
                node_id=env.node_id,
                node_epoch=env.node_epoch,
                page_size=env.page_size,
            )
        )
        return made[-1]

    spec = TierSpec(
        "fake",
        Level.L1,
        True,
        PoolKind.DRAM,
        frozenset({"store", "size", "hugepage"}),
        parse=lambda raw, ctx: {"hugepage": False},
        make=make,
    )
    monkeypatch.setitem(TIERS, "fake", spec)
    allow_store(monkeypatch, "fake")
    server, sockdir = FakeMaruServer(heartbeat_s=0.1), short_tmpdir()
    try:
        config = parse_node_config(
            {
                "node_id": "gpu-test",
                "state_dir": sockdir,
                "socket": f"{sockdir}/node.sock",
                "server": server.url,
                "page_size": NODE_PAGE,
                "tiers": [{"store": "fake", "size": 16 * NODE_PAGE}],
            }
        )
        service = start_node(config)
        try:
            [tier] = made
            pool_id = tier.pool.pool_id
            assert pool_id in server.nodes["gpu-test"].pools
            assert service.pools.tier_of(pool_id) == "fake"
            assert service.pools.creditable(pool_id)
        finally:
            service.stop()
        assert tier.pool.worker_fd() is None
    finally:
        server.close()
        remove_tree(sockdir)


def test_a_spec_that_misstates_leased_fails_the_start(monkeypatch):
    from maru_storage_service.drivers.dram import DramTier

    spec = TierSpec(
        "fake",
        Level.L1,
        True,
        PoolKind.DRAM,
        frozenset({"store", "size"}),
        make=lambda t, e: DramTier(
            t, node_id=e.node_id, node_epoch=e.node_epoch, page_size=e.page_size
        ),
        leased=True,
    )
    monkeypatch.setitem(TIERS, "fake", spec)
    allow_store(monkeypatch, "fake")
    config = parse_node_config(
        {
            "node_id": "gpu-test",
            "socket": "/tmp/maru-plugin.sock",
            "state_dir": "/tmp/maru-plugin-state",
            "server": "tcp://localhost:1",
            "page_size": NODE_PAGE,
            "tiers": [{"store": "fake", "size": 16 * NODE_PAGE}],
        }
    )
    with pytest.raises(TypeError, match="leased=True, but it made a DramTier"):
        start_node(config)


LESSOR = "lessor-1"


class LeasedPool:
    """A pool of FakeLeased."""

    def __init__(self, info: PoolInfo):
        self._info = info

    @property
    def pool_id(self) -> str:
        return self._info.pool_id

    @property
    def capacity(self) -> int:
        return self._info.capacity

    @property
    def pool_generation(self) -> int:
        return self._info.pool_generation

    def info(self) -> PoolInfo:
        return self._info

    def descriptor(self, memfd: int | None = None) -> PoolDescriptor:
        return PoolDescriptor(pool=self._info)

    def worker_fd(self) -> int | None:
        return None


class FakeLeased(L1Tier, Leased):
    """A leased L1 store: one pool of the cxl wire kind from a fake lessor,
    its lifetime calls recorded. ``refuse``: the claim fails, the node
    claimed by another run."""

    name = "lease"

    def __init__(self, tier, env, refuse: str | None = None):
        self.env = env
        self.size = tier.size
        self.refuse = refuse
        self.calls: list[str] = []
        self.pools: list[LeasedPool] = []
        self.claimed_elsewhere = False

    def open(self) -> None:
        self.calls.append("open")

    def claim(self) -> None:
        self.calls.append("claim")
        if self.refuse is not None:
            self.claimed_elsewhere = True
            raise RuntimeError(self.refuse)
        info = PoolInfo(
            f"cxl:{LESSOR}:1",
            PoolKind.CXL,
            self.env.node_id,
            1,
            self.size,
            self.env.page_size,
            rm_domain=LESSOR,
            region_id=1,
            visibility=Visibility.COHERENT,
        )
        self.pools = [LeasedPool(info)]

    def start_renewing(self) -> None:
        self.calls.append("start_renewing")

    def lose(self, reason: str, claimed_elsewhere: bool) -> threading.Thread:
        """What its renew thread does once the lessor takes the pools back."""
        self.claimed_elsewhere = claimed_elsewhere
        thread = threading.Thread(target=self.env.on_lost, args=(reason,))
        thread.start()
        return thread

    def taken_over(self, pool_id: str) -> bool:
        return False

    def rm_domains(self) -> list[RmDomain]:
        return [RmDomain(LESSOR)]

    def earlier_holds(self) -> dict[str, set[str]]:
        return {}

    def watch_holds(self, on_clear) -> None:
        self.calls.append("watch_holds")

    def worker_settled(self, incarnation: str) -> None:
        self.calls.append(f"worker_settled {incarnation}")

    def after_transfers_closed(self) -> None:
        self.calls.append("after_transfers_closed")

    def release(self) -> None:
        self.calls.append("release")

    def time_left(self) -> float:
        return 60.0

    def map_for_transfers(self, pool) -> mmap.mmap:
        self.calls.append("map")
        return mmap.mmap(-1, pool.capacity)

    def stop(self) -> None:
        self.calls.append("stop")

    def close(self) -> None:
        self.calls.append("close")

    def describe(self, adopted: set[str]) -> str:
        return f"{len(self.pools)} leased pools"


def register_leased(monkeypatch, refuse: str | None = None) -> list[FakeLeased]:
    made: list[FakeLeased] = []

    def make(tier, env):
        made.append(FakeLeased(tier, env, refuse))
        return made[-1]

    keys = frozenset({"store", "size", "high_watermark", "low_watermark"})
    spec = TierSpec("lease", Level.L1, True, PoolKind.CXL, keys, make=make, leased=True)
    monkeypatch.setitem(TIERS, "lease", spec)
    allow_store(monkeypatch, "lease")
    return made


def leased_config(server_url: str, sockdir: str):
    """A single leased store."""
    return parse_node_config(
        {
            "node_id": "gpu-test",
            "socket": f"{sockdir}/node.sock",
            "state_dir": sockdir,
            "server": server_url,
            "page_size": NODE_PAGE,
            "tiers": [
                {"store": "lease", "size": 8 * NODE_PAGE},
            ],
        }
    )


@pytest.mark.parametrize("claimed", [False, True])
def test_a_registered_leased_store_lives_through_a_lost_lease(
    monkeypatch, caplog, claimed
):
    made = register_leased(monkeypatch)
    server, sockdir = FakeMaruServer(heartbeat_s=0.1), short_tmpdir()
    caplog.set_level(logging.INFO, logger="maru_storage_service.service")
    try:
        service = start_node(leased_config(server.url, sockdir))
        try:
            [tier] = made
            [pool] = tier.pools
            assert pool.pool_id in server.nodes["gpu-test"].pools  # announced
            assert service.pools.tier_of(pool.pool_id) == "lease"
            started = ["open", "claim", "start_renewing", "watch_holds"]
            assert tier.calls == started
            service._worker_settled("w1")  # what the StorageManager calls
            tier.lose("the lessor took it back", claimed).join(WAIT)
            assert service.wait_finished(WAIT)
        finally:
            service.stop()
        # The run ends with leave as it does for the cxl store
        assert service.teardown_failed is None
        assert service.ended == "the lessor took it back"
        assert service.new_run is not claimed
        assert service.claimed_elsewhere is claimed
        assert tier.calls == started + [
            "worker_settled w1",
            "stop",
            "release",
            "close",
        ]
        # Its pool is no longer offered at ATTACH
        assert [d.pool.pool_id for d, _ in service.local.pools] == []
        assert "ends (leave): the lessor took it back" in caplog.text
        assert "node session closed (leave)" in caplog.text
    finally:
        remove_tree(sockdir)
        server.close()


def test_a_leased_store_claimed_by_another_run_ends_the_node(monkeypatch):
    made = register_leased(monkeypatch, refuse="taken by run e9")
    server, sockdir = FakeMaruServer(heartbeat_s=0.1), short_tmpdir()
    try:
        node = Node(
            leased_config(server.url, sockdir),
            lambda c: StorageService(c, rpc_timeout_s=0.3),
            pause_s=0.01,
        )
        assert node.run() is False
        assert node.ended == "another run claimed the node (taken by run e9)"
        assert node.runs == 1 and node.failed is None
        [tier] = made
        assert tier.calls == ["open", "claim", "stop", "close"]
    finally:
        remove_tree(sockdir)
        server.close()
