# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""L2 I/O contracts and registered stores with leased lifetimes."""

import collections
import logging
import mmap
import threading
import time
from dataclasses import dataclass, field

import pytest

from maru_common.storage_types import (
    Location,
    PoolDescriptor,
    PoolInfo,
    PoolKind,
    RmDomain,
    Visibility,
)
from maru_storage_service import service as service_module
from maru_storage_service.config import parse_node_config
from maru_storage_service.config_fields import TierSpec
from maru_storage_service.drivers.registry import TIERS
from maru_storage_service.page_table import PageRef
from maru_storage_service.service import Node, StorageService
from maru_storage_service.tiers import IOResult, L1Tier, L2Tier, Leased, Level
from maru_storage_service.transfer_engine import Copy, TransferEngine
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import PAGE as NODE_PAGE
from tests.unit.ms_node_helpers import remove_tree, short_tmpdir, start_node

PAGE = mmap.PAGESIZE
WAIT = 5
POOL = "fake:n:pool"


@dataclass(eq=False)
class FakeResult:
    run: str
    src: PageRef
    dst: PageRef
    ok: bool = False
    late: bool = False
    cancelled: bool = False
    error: Exception | None = None
    kept: bool = False
    done: threading.Event = field(default_factory=threading.Event)


class FakeL2(L2Tier):
    """Pages in a dict, moved on the tier's own thread."""

    name = "fake"

    def __init__(self, run: str):
        self.run = run
        self.pages: dict[int, bytes] = {}
        self.memory: dict[str, mmap.mmap] = {}
        self.events: list[str] = []
        self.entered = threading.Event()
        self.gate = threading.Event()
        self.gate.set()
        self._cv = threading.Condition()
        self._queue: collections.deque = collections.deque()
        self._stopping = False
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def pool_infos(self) -> list[PoolInfo]:
        return [PoolInfo(POOL, PoolKind.DRAM, "n", 1, 4 * PAGE, PAGE)]

    def describe(self, adopted: set[str]) -> str:
        return "fake"

    def io_size(self, nbytes: int) -> int:
        return nbytes

    def attach(self, pool_id: str, memory: mmap.mmap) -> None:
        self.memory[pool_id] = memory

    def detach(self, pool_id: str) -> None:
        self.events.append(f"detach {pool_id}")
        del self.memory[pool_id]

    def submit_store(self, src, dst, *, refs, callback=None, deadline=None):
        def work() -> None:
            view = self.memory[src.pool_id]
            self.pages[dst.offset] = bytes(view[src.offset : src.offset + src.nbytes])

        return self._submit(work, refs, callback, deadline)

    def submit_load(self, src, dst, *, refs, callback=None, deadline=None):
        def work() -> None:
            data = self.pages[src.offset][: dst.nbytes]
            self.memory[dst.pool_id][dst.offset : dst.offset + len(data)] = data

        return self._submit(work, refs, callback, deadline)

    def drain(self) -> None:
        with self._cv:
            self._stopping = True
            queued = list(self._queue)
            self._queue.clear()
            self._cv.notify_all()
        for _, result, callback, _ in queued:
            result.cancelled = True
            self._finish(result, callback)
        self._thread.join(WAIT)
        self.events.append("drained")

    def _submit(self, work, refs, callback, deadline) -> IOResult:
        result = FakeResult(self.run, *refs)
        with self._cv:
            if self._stopping:
                raise RuntimeError("drained")
            self._queue.append((work, result, callback, deadline))
            self._cv.notify_all()
        return result

    def _loop(self) -> None:
        while True:
            with self._cv:
                while not self._queue and not self._stopping:
                    self._cv.wait()
                if not self._queue:
                    return
                work, result, callback, deadline = self._queue.popleft()
            self.entered.set()
            assert self.gate.wait(WAIT)
            try:
                work()
                result.ok = True
                result.late = deadline is not None and time.monotonic() > deadline
            except Exception as e:
                result.error = e
            self._finish(result, callback)

    def _finish(self, result: FakeResult, callback) -> None:
        if callback is not None:
            callback(result)
        self.events.append("callback")
        result.done.set()


def loc(pool_id: str, offset: int, kind: PoolKind = PoolKind.DRAM) -> Location:
    return Location(pool_id, "a", 1, offset, PAGE, kind)


@pytest.fixture
def engine():
    engine = TransferEngine(1 << 40)
    memory = mmap.mmap(-1, 4 * PAGE)
    tier = FakeL2("run-1")
    engine.add_pool("mem", memory)
    engine.add_l2(tier)
    yield engine, tier, memory
    engine.close()


def test_a_store_and_a_load_go_through_the_tier(engine):
    engine, tier, memory = engine
    assert tier.level is Level.L2 and tier.memory == {"mem": memory}
    assert engine.has_pool(POOL)
    memory[PAGE : PAGE + 5] = b"bytes"
    mem_ref, page_ref = PageRef("mem", 1, 1), PageRef(POOL, 2, 1)
    seen = []
    store = engine.submit_io(
        Copy(loc("mem", PAGE), loc(POOL, 2 * PAGE, PoolKind.DRAM), 5),
        mem_ref,
        page_ref,
        callback=seen.append,
    )
    assert store.done.wait(WAIT) and store.ok and not store.late
    assert seen == [store] and (store.src, store.dst) == (mem_ref, page_ref)
    assert tier.pages[2 * PAGE] == b"bytes"
    # past its deadline but every byte moved: ok and late, the caller decides
    load = engine.submit_io(
        Copy(loc(POOL, 2 * PAGE, PoolKind.DRAM), loc("mem", 3 * PAGE), 5),
        page_ref,
        PageRef("mem", 3, 1),
        deadline=time.monotonic() - 1,
    )
    assert load.done.wait(WAIT) and load.ok and load.late
    assert load.run == "run-1" and memory[3 * PAGE : 3 * PAGE + 5] == b"bytes"
    with pytest.raises(ValueError, match="no file I/O"):
        engine.submit_io(Copy(loc("mem", 0), loc("mem", PAGE), 5), mem_ref, mem_ref)


def test_close_drains_the_tier_before_it_detaches_and_unmaps(engine):
    engine, tier, memory = engine
    tier.gate.clear()
    page_ref = PageRef(POOL, 0, 1)
    running = engine.submit_io(
        Copy(loc("mem", 0), loc(POOL, 0, PoolKind.DRAM), 5),
        PageRef("mem", 0, 1),
        page_ref,
    )
    assert tier.entered.wait(WAIT)
    queued = engine.submit_io(
        Copy(loc("mem", PAGE), loc(POOL, PAGE, PoolKind.DRAM), 5),
        PageRef("mem", 1, 1),
        PageRef(POOL, 1, 1),
    )
    closer = threading.Thread(target=engine.close)
    closer.start()
    try:
        assert not running.done.wait(0.05)
        assert not memory.closed and "detach mem" not in tier.events
    finally:
        tier.gate.set()
        closer.join(WAIT)
    assert running.ok and queued.cancelled and not queued.ok
    assert tier.events == ["callback", "callback", "drained", "detach mem"]
    assert memory.closed and not engine.has_pool(POOL)
    with pytest.raises(ValueError):
        engine.submit_io(
            Copy(loc("mem", 0), loc(POOL, 0, PoolKind.DRAM), 5),
            PageRef("mem", 0, 1),
            page_ref,
        )


class StuckL2(FakeL2):
    """A tier that cannot let go of one mapping."""

    def detach(self, pool_id: str) -> None:
        if pool_id == "stuck":
            raise OSError("still registered")
        super().detach(pool_id)


def test_a_mapping_a_tier_cannot_detach_stays_mapped_and_is_reported():
    engine = TransferEngine(1 << 40)
    tier = StuckL2("run")
    stuck, free = mmap.mmap(-1, PAGE), mmap.mmap(-1, PAGE)
    engine.add_pool("stuck", stuck)
    engine.add_pool("free", free)
    engine.add_l2(tier)
    assert engine.close() == {"stuck"}
    assert not stuck.closed
    assert free.closed
    stuck.close()


@dataclass(eq=True)
class Unhashable:
    """A dataclass with eq=True and no frozen: not hashable."""

    value: int = 0


class UnhashableL2(FakeL2, Unhashable):
    __hash__ = None  # type: ignore[assignment]


def test_tiers_need_not_be_hashable():
    engine = TransferEngine(1 << 40)
    tier = UnhashableL2("run")
    memory = mmap.mmap(-1, PAGE)
    engine.add_l2(tier)
    engine.add_pool("m", memory)
    assert engine.close() == set()
    assert "drained" in tier.events
    assert memory.closed


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
            assert service.transfers is None
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
            assert service.transfers is None
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
