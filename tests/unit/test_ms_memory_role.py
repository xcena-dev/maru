# SPDX-License-Identifier: Apache-2.0
"""A memory node run: its config, its RemoteServer's place in the start and
the teardown, whole-pool NIC registration before NODE_OPEN, and the
capability MaruServer must accept."""

import copy
import os

import pytest

from maru_common.protocol import MessageType
from maru_common.remote_protocol import RemotePool
from maru_common.storage_types import MEMORY_CAPABILITY, CloseMode, PoolKind
from maru_remote.exporter import NixlExporter
from maru_remote.transport import buffer_address
from maru_storage_service.config import (
    DEFAULT_RESERVE_TTL_S,
    NodeConfigError,
    parse_node_config,
)
from maru_storage_service.memory_node import RemoteParts, RunOps, agent_name
from maru_storage_service.remote_server import RemoteServer
from maru_storage_service.service import (
    CapabilityRefusedError,
    Node,
    NotServableError,
    StorageService,
)
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import remove_tree, short_tmpdir, start_node
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.remote_transport_fakes import FakeTransport
from tests.unit.test_ms_cxl_tier import FakeRm, anon_region

PAGE = 1 << 20
LISTEN = "127.0.0.1:9871"


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def fake():
    server = FakeMaruServer()
    yield server
    server.close()


@pytest.fixture
def sockdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


def memory_raw(url: str, sockdir: str, *, cxl: bool = False, ssd: bool = False):
    tiers = [{"store": "dram", "size": 8 * PAGE, "hugepage": False}]
    if cxl:
        tiers.append({"store": "cxl", "size": 4 * PAGE, "region_size": 2 * PAGE})
    if ssd:
        tiers.append({"store": "ssd", "size": 16 * PAGE, "path": f"{sockdir}/ssd"})
    return {
        "node_id": "mem1",
        "role": "memory",
        "socket": f"{sockdir}/node.sock",
        "server": url,
        "page_size": PAGE,
        "remote_server": {"listen": LISTEN, "key_file": f"{sockdir}/key"},
        "tiers": tiers,
        "budgets": {
            "credit_max_bytes": 2 * PAGE,
            "credit_total_max_bytes": 2 * PAGE,
            "credit_refill_low": PAGE,
            "credit_refill_high": 2 * PAGE,
            "commit_pending_max_bytes": 2 * PAGE,
            "transfer_reserve_bytes": PAGE,
        },
    }


class RecordingServer(RemoteServer):
    """A RemoteServer that records its lifetime; ``taken`` fails start."""

    def __init__(self, log: list, taken: bool = False):
        self.log = log
        self.taken = taken
        self.ready_with: tuple[str, list[RemotePool]] | None = None

    def start(self) -> None:
        if self.taken:
            raise OSError(98, "address already in use")
        self.log.append("remote start")

    def ready(self, server_epoch: str, pools: list[RemotePool]) -> None:
        self.ready_with = (server_epoch, pools)
        self.log.append("remote ready")

    def stop(self) -> None:
        self.log.append("remote stop")


class LoggedExporter(NixlExporter):
    """A NixlExporter over a fake NIC that logs, and refuses or cannot
    deregister the pools of the given stores."""

    def __init__(self, transport, log: list, refused=(), stuck=()):
        super().__init__(transport)
        self.log = log
        self.refused = set(refused)
        self.stuck = set(stuck)

    def attach(self, pool_id, memory):
        if pool_id.split(":")[0] in self.refused:
            raise RuntimeError(f"the NIC refused {pool_id}")
        super().attach(pool_id, memory)
        self.log.append(f"attach {pool_id.split(':')[0]}")

    def detach(self, pool_id):
        if pool_id.split(":")[0] in self.stuck:
            raise RuntimeError(f"cannot deregister {pool_id}")
        if self.export(pool_id) is not None:
            self.log.append(f"detach {pool_id.split(':')[0]}")
        super().detach(pool_id)


class Parts:
    """Makes each run's RemoteParts and keeps the last one."""

    def __init__(self, log: list, *, refused=(), stuck=(), taken=False):
        self.log = log
        self.refused, self.stuck, self.taken = refused, stuck, taken
        self.made: list[RemoteParts] = []

    def __call__(self, service: StorageService) -> RemoteParts:
        transport = FakeTransport(
            agent_name(service.config.node_id, service.node_epoch)
        )
        parts = RemoteParts(
            LoggedExporter(transport, self.log, self.refused, self.stuck),
            RecordingServer(self.log, self.taken),
            close=lambda: (transport.close(), self.log.append("agent close")),
        )
        self.made.append(parts)
        return parts

    @property
    def last(self) -> RemoteParts:
        return self.made[-1]


def memory_node(fake, sockdir, parts, *, cxl=False, ssd=False, **kwargs):
    config = parse_node_config(memory_raw(fake.url, sockdir, cxl=cxl, ssd=ssd))
    if cxl:
        kwargs.setdefault("region_source", FakeRm())
        kwargs.setdefault("map_region", anon_region)
        kwargs.setdefault("cxl_renew_s", 0.02)
    return start_node(config, remote_parts=parts, **kwargs)


def announced(fake) -> set[str]:
    return {p.split(":")[0] for p in fake.nodes["mem1"].pools}


# ------------------------------------------------------------------ config


def test_remote_server_defaults():
    c = parse_node_config(memory_raw("tcp://ms:5555", "/tmp/x"))
    rs = c.remote_server
    assert rs.listen == LISTEN
    assert rs.rdma_device == ""
    assert rs.reserve_ttl_s == DEFAULT_RESERVE_TTL_S == 60.0
    assert rs.peer_quarantine_max == 2 * c.budgets.credit_max_bytes
    assert rs.quarantine_max is None
    assert (rs.stage_workers, rs.stage_queue) == (4, 64)


def test_remote_server_keys():
    raw = memory_raw("tcp://ms:5555", "/tmp/x")
    raw["remote_server"].update(
        listen="tcp://192.0.2.22:9871",
        rdma_device="rocep193s0",
        reserve_ttl_s=30,
        peer_quarantine_max="1G",
        quarantine_max="8G",
        stage_workers=2,
        stage_queue=8,
    )
    rs = parse_node_config(raw).remote_server
    assert rs.listen == "192.0.2.22:9871"
    assert rs.rdma_device == "rocep193s0"
    assert rs.reserve_ttl_s == 30.0
    assert (rs.peer_quarantine_max, rs.quarantine_max) == (1 << 30, 8 << 30)
    assert (rs.stage_workers, rs.stage_queue) == (2, 8)


@pytest.mark.parametrize(
    ("key", "value", "match"),
    [
        ("listen", "192.0.2.22", "host:port"),
        ("listen", ":9871", "host:port"),
        ("listen", "h:0", "host:port"),
        ("listen", "h:70000", "host:port"),
        ("rdma_device", 3, "rdma_device"),
        ("reserve_ttl_s", 0, "reserve_ttl_s"),
        ("peer_quarantine_max", 0, "peer_quarantine_max: must be positive"),
        ("quarantine_max", "0", "quarantine_max: must be positive"),
        ("stage_workers", 0, "stage_workers"),
        ("stage_workers", True, "stage_workers"),
        ("stage_queue", 2.5, "stage_queue"),
        ("stage_queue", 2, "smaller than stage_workers"),
        ("unknown", 1, "unknown keys"),
    ],
)
def test_remote_server_rejects(key, value, match):
    raw = memory_raw("tcp://ms:5555", "/tmp/x")
    raw["remote_server"][key] = value
    with pytest.raises(NodeConfigError, match=match):
        parse_node_config(raw)


def test_only_a_memory_node_has_a_remote_server():
    raw = memory_raw("tcp://ms:5555", "/tmp/x")
    del raw["role"]
    with pytest.raises(NodeConfigError, match="only a memory node"):
        parse_node_config(raw)


def test_a_memory_node_is_servable_write_through_too():
    raw = memory_raw("tcp://ms:5555", "/tmp/x", ssd=True)
    Node(parse_node_config(raw))  # check_servable passes
    raw = copy.deepcopy(raw)
    raw["tiers"][0]["demotion"] = "through"
    traits = Node(parse_node_config(raw)).config.traits()
    assert traits["dram"].copies_to == traits["dram"].demotes_to == ("ssd",)
    raw["tiers"].insert(0, {"store": "ssd", "size": 16 * PAGE, "path": "/x"})
    with pytest.raises((NotServableError, NodeConfigError)):
        Node(parse_node_config(raw))


# ------------------------------------------------------------------- start


def test_the_run_binds_registers_announces_then_serves(fake, sockdir):
    log: list = []
    parts = Parts(log)
    node = memory_node(fake, sockdir, parts)
    try:
        assert log == ["remote start", "attach dram", "remote ready"]
        assert node.local is None and not os.path.exists(f"{sockdir}/node.sock")
        [opened] = fake.requests(MessageType.NODE_OPEN)
        assert opened.capabilities == [MEMORY_CAPABILITY]
        assert fake.nodes["mem1"].capabilities == [MEMORY_CAPABILITY]
        dram = node.tier("dram").pool
        assert set(fake.nodes["mem1"].pools) == {dram.pool_id}
        epoch, pools = parts.last.server.ready_with
        assert epoch == fake.server_epoch
        export = parts.last.exporter.export(dram.pool_id)
        info = dram.info()
        assert pools == [
            RemotePool(
                dram.pool_id,
                PoolKind.DRAM,
                info.pool_generation,
                info.capacity,
                export.base,
                export.metadata,
            )
        ]
        assert export.nbytes == info.capacity
        assert node.transfers is not None  # always, for the registrations
        assert node.manager.fetches is None  # no workers to fetch for
        assert node.remote_ops.target is node.peer_ops
        assert node.peer_ops.quarantine_max == info.capacity // 8
    finally:
        node.stop()
    assert log[3] == "remote stop"  # first
    assert log[-2:] == ["detach dram", "agent close"]
    assert node.teardown_failed is None
    [closed] = fake.requests(MessageType.NODE_CLOSE)
    assert closed.mode == CloseMode.RESTART


def test_the_ssd_tier_is_announced_but_not_registered(fake, sockdir):
    log: list = []
    node = memory_node(fake, sockdir, Parts(log), ssd=True)
    try:
        assert announced(fake) == {"dram", "ssd"}
        assert log == ["remote start", "attach dram", "remote ready"]
        assert node.transfers.has_pool(node.tier("ssd").pool.pool_id)
    finally:
        node.stop()


def test_cxl_regions_are_registered_whole(fake, sockdir):
    log: list = []
    parts = Parts(log)
    node = memory_node(fake, sockdir, parts, cxl=True)
    try:
        regions = node.tier("cxl").pools
        assert regions and announced(fake) == {"dram", "cxl"}
        for pool in regions:
            export = parts.last.exporter.export(pool.pool_id)
            assert export.nbytes == pool.info().capacity
        _, pools = parts.last.server.ready_with
        assert {p.pool_id for p in pools} == {
            node.tier("dram").pool.pool_id,
            *(p.pool_id for p in regions),
        }
    finally:
        node.stop()
    assert log[-1] == "agent close"
    assert log.count("detach cxl") == len(regions)


def test_a_pool_the_nic_refuses_is_left_out_of_the_run(fake, sockdir):
    log: list = []
    parts = Parts(log, refused={"cxl"})
    node = memory_node(fake, sockdir, parts, cxl=True)
    try:
        assert announced(fake) == {"dram"}
        _, pools = parts.last.server.ready_with
        assert [p.kind for p in pools] == [PoolKind.DRAM]
        assert all(
            node.pools.tier_of(p.pool_id) is None for p in node.tier("cxl").pools
        )
    finally:
        node.stop()
    assert node.teardown_failed is None and log[-1] == "agent close"


def test_a_top_pool_the_nic_refuses_fails_the_start(fake, sockdir):
    log: list = []
    with pytest.raises(RuntimeError, match="cannot register the dram pool"):
        memory_node(fake, sockdir, Parts(log, refused={"dram"}))
    assert fake.requests(MessageType.NODE_OPEN) == []
    assert log == ["remote start", "remote stop", "agent close"]


def test_an_address_in_use_fails_the_start_before_node_open(fake, sockdir):
    log: list = []
    with pytest.raises(OSError, match="in use"):
        memory_node(fake, sockdir, Parts(log, taken=True))
    assert fake.requests(MessageType.NODE_OPEN) == []
    assert "attach dram" not in log


def test_a_server_that_does_not_accept_memory_nodes(sockdir):
    old = FakeMaruServer(capabilities=frozenset())
    log: list = []
    try:
        with pytest.raises(CapabilityRefusedError, match="memory_v1"):
            memory_node(old, sockdir, Parts(log))
        [closed] = old.requests(MessageType.NODE_CLOSE)
        assert closed.mode == CloseMode.LEAVE
        assert "remote ready" not in log and log[-1] == "agent close"
    finally:
        old.close()


def test_a_registration_that_cannot_end_fails_the_teardown(fake, sockdir):
    log: list = []
    parts = Parts(log, stuck={"dram"})
    node = memory_node(fake, sockdir, parts)
    dram = node.tier("dram").pool.pool_id
    node.stop()
    assert "stay registered with the NIC" in node.teardown_failed
    assert "agent close" not in log
    assert parts.last.exporter.export(dram) is not None
    assert dram in node.transfers._kept  # never unmapped


def test_node_runs_no_new_run_after_a_failed_teardown(fake, sockdir):
    log: list = []
    config = parse_node_config(memory_raw(fake.url, sockdir))
    parts = Parts(log, stuck={"dram"})

    def make(cfg):
        return StorageService(cfg, rpc_timeout_s=0.3, remote_parts=parts)

    node = Node(config, make_service=make, pause_s=0.01)
    import threading

    ran: list = []
    thread = threading.Thread(target=lambda: ran.append(node.run()))
    thread.start()
    try:
        deadline = 100
        while "remote ready" not in log and deadline:
            thread.join(0.05)
            deadline -= 1
        assert "remote ready" in log
        node.service._end_run("test", CloseMode.RESTART, new_run=True)
        thread.join(10)
        assert ran == [False] and node.failed is not None
        assert len(parts.made) == 1
    finally:
        node.stop()
        thread.join(10)


def test_run_ops_forward_to_the_manager_once_set():
    ops = RunOps()
    with pytest.raises(RuntimeError, match="not ready"):
        _ = ops.reserve
    ops.target = type("M", (), {"reserve": "manager's"})()
    assert ops.reserve == "manager's"


def test_buffer_of_the_registered_dram_pool_is_the_engines_mapping(fake, sockdir):
    log: list = []
    parts = Parts(log)
    node = memory_node(fake, sockdir, parts)
    try:
        dram = node.tier("dram").pool.pool_id
        mapping = node.transfers._maps[dram]
        assert parts.last.exporter.export(dram).base == buffer_address(mapping)
    finally:
        node.stop()


def test_the_default_parts_limit_ucx_before_the_agent(fake, sockdir, monkeypatch):
    """The real factory: UCX limited (os.environ), the run's agent, its
    exporter and a TcpRemoteServer; no NIC here, so the agent is fake."""
    import maru_storage_service.memory_node as memory_node
    from maru_storage_service.remote_server import TcpRemoteServer
    from tests.unit.remote_fakes import key_file

    monkeypatch.delenv("UCX_TLS", raising=False)
    monkeypatch.delenv("UCX_RCACHE_ENABLE", raising=False)
    made = []

    def transport(name, *, ucx_device=""):
        assert os.environ["UCX_TLS"] and os.environ["UCX_RCACHE_ENABLE"] == "n"
        made.append(FakeTransport(name, ucx_device=ucx_device))
        return made[-1]

    monkeypatch.setattr(memory_node, "NixlTransport", transport)
    raw = memory_raw(fake.url, sockdir)
    raw["remote_server"].update(listen="127.0.0.1:1", key_file=key_file())
    config = parse_node_config(raw)
    service = StorageService(config, rpc_timeout_s=0.3)
    parts = memory_node.make_remote_parts(service)
    assert isinstance(parts.server, TcpRemoteServer)
    assert [t.agent.name for t in made] == [agent_name("mem1", service.node_epoch)]
    parts.close()
