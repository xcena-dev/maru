# SPDX-License-Identifier: Apache-2.0
"""Region-extension contracts on memory-node CXL mappings."""

import dataclasses
import threading

import pytest

from maru_common.remote_protocol import RemoteHintRequest, ReserveRequest
from maru_common.storage_types import HintAction, ItemResult, Key, Status
from maru_remote.transport import TransferState
from maru_storage_service import region_extension
from maru_storage_service.config import (
    NodeConfigError,
    RegionExtensionConfig,
    parse_node_config,
)
from maru_storage_service.region_extension import RegionExtension, load_region_extension
from tests.unit.ms_fakes import FakeNode, FakeWorker
from tests.unit.ms_node_helpers import (
    PAGE,
    FakeRm,
    MetadataServer,
    anon_region,
    node_config,
    wait_until,
)
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.remote_transport_fakes import FakeTransport, mapped
from tests.unit.test_ms_memory_role import DOMAIN, MemoryEnv


class FakeExtension:
    def __init__(self):
        self.hints = []
        self.mapped_regions = []
        self.fresh = {}
        self.refuse_map = False
        self.fail_mapped = False
        self.hold = False
        self.closed = False
        self.closed_conns = []
        self.retired = []
        self.factory_args = []

    def validate_access(self, handle, access):
        assert set(handle) == {"region_id", "offset", "length", "auth_token"}
        assert access.dax_path and access.device_uuid
        if self.refuse_map:
            raise ValueError("not this device")

    def mapped(self, handle, access, *, fresh):
        if self.fail_mapped:
            raise RuntimeError("device command outcome unknown")
        self.mapped_regions.append(handle["region_id"])
        self.fresh[handle["region_id"]] = fresh

    def hint(self, conn, message, ranges):
        self.hints.append((conn, dict(message), list(ranges)))

    def retire(self, handle):
        self.retired.append(handle["region_id"])
        return not self.hold

    def close(self):
        self.closed = True

    def connection_closed(self, conn):
        self.closed_conns.append(conn)


class ExtensionEnv(MemoryEnv):
    def __init__(self, monkeypatch, extension, **options):
        self.extension, self.rm = extension, FakeRm()
        self.metadata = MetadataServer()
        super().__init__()
        self.node.stop()
        config = node_config(
            self.metadata.address,
            f"{self.directory}/unused.sock",
            role="memory",
            node_id=self.config.node_id,
            cxl_pages=16,
            region_pages=16,
        )
        self.config = dataclasses.replace(
            config,
            remote_server=self.config.remote_server,
            region_extension=RegionExtensionConfig(
                "fake", "opaque", generation_poll_s=100, **options
            ),
        )

        def load(name, epoch, config):
            extension.factory_args.append((name, epoch, config))
            return extension

        monkeypatch.setattr(region_extension, "load_region_extension", load)
        self.reader_node = FakeNode(self.metadata.wire(), "gpu-reader")
        from maru_common.storage_types import REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY

        self.reader_node.capabilities = [REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY]
        assert (
            self.reader_node.open(pools=[self.reader_node.dram()], pid_ns=101).status
            == Status.OK
        )
        self.reader = FakeWorker(self.reader_node.wire, self.reader_node)
        assert self.reader.open().status == Status.OK

    def start_cxl(self, **options):
        self.node = self.start(region_source=self.rm, map_region=anon_region, **options)
        self.parts, self.ops = self.node.role.parts, self.node.role.ops
        return self.node

    def lookup(self, key="k0"):
        return self.reader.lookup(Key("ns", key)).items[0]

    def close(self):
        self.extension.hold = False
        for node in self.nodes:
            if node.teardown_failed is not None:
                node.role._fence_failed = None
                node.role.close_exports()
                for lease in node._leases():
                    lease.after_transfers_closed()
                if node.server is not None:
                    node.server.close()
                for tier in node._tiers.values():
                    tier.close()
                node._ownership.close()
        super().close()
        self.metadata.close()


@pytest.fixture
def extension_env(monkeypatch):
    reset_fake_agents()
    envs = []

    def make(extension=None, start=True, **options):
        env = ExtensionEnv(monkeypatch, extension or FakeExtension(), **options)
        envs.append(env)
        if start:
            env.start_cxl()
        return env

    yield make
    for env in reversed(envs):
        env.close()
    reset_fake_agents()


class _EntryPoint:
    def __init__(self, name, target):
        self.name, self._target = name, target

    def load(self):
        return self._target


def test_an_extension_is_loaded_by_name_only(monkeypatch):
    ext = FakeExtension()
    seen = []
    eps = [
        _EntryPoint(
            "fake", lambda epoch, config: (seen.append((epoch, config)), ext)[1]
        ),
        _EntryPoint("other", lambda *args: object()),
    ]
    monkeypatch.setattr(
        region_extension.importlib.metadata,
        "entry_points",
        lambda *, group: eps if group == "maru.region_extensions" else [],
    )
    assert load_region_extension("fake", "e1", "config") is ext
    assert seen == [("e1", "config")] and isinstance(ext, RegionExtension)
    with pytest.raises(ValueError, match="no package registers"):
        load_region_extension("missing", "e1")
    with pytest.raises(ValueError, match="does not implement"):
        load_region_extension("other", "e1")


def test_the_data_path_is_unchanged_and_hello_advertises_hints(extension_env):
    env = extension_env()
    _, hello = env.connect()
    assert hello.hint_actions == ["read_hint", "read_hint_end"]
    offered = env.reserve().credits
    memory = env.parts._maps[offered[0].pool_id]
    memory[offered[0].offset : offered[0].offset + PAGE] = b"x" * PAGE
    assert env.write(offered).items[0].result == ItemResult.ADDED
    assert env.lookup().result == ItemResult.HIT
    worker = FakeTransport("direct-reader")
    target, address = mapped(PAGE)
    worker.register(address, PAGE)
    peer = worker.add_peer(hello.pools[0].metadata)
    try:
        assert (
            worker.post(
                "READ", peer, [(address, hello.pools[0].base + offered[0].offset, PAGE)]
            ).poll_state()
            == TransferState.DONE
        )
        assert target[:] == b"x" * PAGE
        assert env.extension.mapped_regions and not env.extension.hints
    finally:
        worker.close()
        target.close()


def test_only_a_newly_allocated_region_is_fresh(extension_env):
    env = extension_env()
    ext = env.extension
    rid = ext.mapped_regions[0]
    assert ext.fresh[rid] is True
    env.write(env.reserve().credits)
    env.node.stop()
    replacement = env.start_cxl()
    assert rid in ext.mapped_regions and ext.fresh[rid] is False
    assert replacement.adopted and env.lookup().result == ItemResult.HIT


def test_a_shared_read_does_not_repeat_the_fresh_hook(extension_env):
    env = extension_env()
    env.write(env.reserve().credits)
    env.lookup()
    env.lookup()
    assert len(env.extension.mapped_regions) == 1
    assert env.extension.factory_args == [("fake", env.node.node_epoch, "opaque")]


def test_a_region_stays_until_the_extension_lets_it_go(extension_env, monkeypatch):
    env = extension_env()
    ext = env.extension
    ext.hold = True
    monkeypatch.setattr("maru_storage_service.memory.service.EXTENSION_DRAIN_S", 0.0)
    parts = env.parts
    from maru_common.storage_types import CloseMode

    env.node.stop(CloseMode.LEAVE)
    assert "still holds memory" in env.node.teardown_failed
    assert not parts.exporter.exports()
    assert parts._maps and all(not memory.closed for memory in parts._maps.values())
    assert env.rm.released == [] and ext.closed
    ext.hold = False
    assert env.node.role.sweep_regions()
    assert ext.retired and not env.node.role._regions


def test_a_region_the_extension_refuses_is_not_mapped(extension_env):
    ext = FakeExtension()
    ext.refuse_map = True
    env = extension_env(ext)
    assert not ext.mapped_regions and not env.parts._maps
    assert env.reserve().credits == []


@pytest.mark.parametrize("hold", [False, True])
def test_a_failed_mapped_hook_keeps_the_region_out_of_use(
    extension_env, monkeypatch, hold
):
    ext = FakeExtension()
    ext.fail_mapped, ext.hold = True, hold
    env = extension_env(ext, start=False)
    monkeypatch.setattr("maru_storage_service.memory.service.EXTENSION_DRAIN_S", 0.0)
    with pytest.raises(RuntimeError, match="command outcome unknown"):
        env.start_cxl()
    failed = env.nodes[-1]
    assert not failed.role.parts.exporter.exports()
    assert (failed.teardown_failed is not None) is hold
    assert env.config.node_id not in env.metadata.server.ms_service.registry.nodes
    assert not ext.mapped_regions


def test_closing_the_memory_node_closes_the_extension(extension_env, monkeypatch):
    env = extension_env()
    env.extension.hold = True
    monkeypatch.setattr("maru_storage_service.memory.service.EXTENSION_DRAIN_S", 0.0)
    env.node.stop()
    assert env.extension.closed
    assert env.extension.retired
    assert env.node.teardown_failed and env.parts._maps


def test_the_extension_hears_each_closed_connection(extension_env):
    env = extension_env()
    a, _ = env.connect()
    a.close()
    wait_until(lambda: len(env.extension.closed_conns) == 1)
    b, _ = env.connect()
    b.close()
    wait_until(lambda: len(env.extension.closed_conns) == 2)
    assert len(set(env.extension.closed_conns)) == 2


def raw_config():
    return {
        "node_id": "mem",
        "server": "tcp://127.0.0.1:1",
        "page_size": PAGE,
        "tiers": [{"store": "local_cxl", "size": 16 * PAGE, "region_size": 16 * PAGE}],
        "region_extension": {"name": "fake"},
    }


def test_missing_extension_refuses_start_and_releases_the_node_lock(
    extension_env, monkeypatch
):
    env = extension_env(start=False)

    def refuse(*args):
        raise ValueError("no package registers region extension")

    monkeypatch.setattr(region_extension, "load_region_extension", refuse)
    from maru_storage_service.cli import EXIT_START, serve
    from maru_storage_service.ownership import FileLock

    assert serve(env.config) == EXIT_START
    lock = FileLock(f"{env.directory}/{env.config.node_id}.lock")
    lock.claim()
    lock.close()
    assert env.rm.calls == []


@pytest.mark.parametrize(
    "key", ["hint_requests_max", "generation_poll_s", "generation_grace_s"]
)
@pytest.mark.parametrize("value", [0, -1, True, "1", float("inf")])
def test_extension_config_refuses_invalid_limits(key, value):
    raw = raw_config()
    raw["region_extension"][key] = value
    with pytest.raises(NodeConfigError, match=key):
        parse_node_config(raw, role="memory")


def test_startup_failure_closes_the_loaded_extension(extension_env):
    class Broken(FakeExtension):
        def memory_generation(self):
            raise RuntimeError("generation unavailable")

    ext = Broken()
    env = extension_env(ext, start=False)
    from maru_storage_service.cli import EXIT_START, serve

    assert serve(env.config) == EXIT_START
    assert ext.closed and env.rm.calls == []


def test_extension_config_is_memory_cxl_only_and_round_trips():
    from maru_storage_service.cli import config_to_dict

    raw = raw_config()
    config = parse_node_config(raw, role="memory")
    assert parse_node_config(config_to_dict(config), role="memory") == config
    with pytest.raises(NodeConfigError, match="unknown keys"):
        parse_node_config(raw, role="gpu")
    raw["tiers"] = [{"store": "dram", "size": 16 * PAGE}]
    with pytest.raises(NodeConfigError, match="requires local_cxl"):
        parse_node_config(raw, role="memory")


def test_read_hints_check_page_generation_and_connection_limit(extension_env):
    env = extension_env(hint_requests_max=1)
    credit = env.reserve().credits[0]
    env.write([credit])
    result = []
    request = RemoteHintRequest(HintAction.READ_HINT, "r", [credit])
    env.node.role.hint(4, request, result.append)
    assert result.pop().status == Status.OK
    ranges = env.extension.hints[-1][2]
    assert ranges == [
        {
            "handle": env.node.tier("local_cxl").pools[0].region.handle.to_dict(),
            "offset": credit.offset,
            "length": credit.length,
        }
    ]
    env.node.role.hint(4, dataclasses.replace(request, request="r2"), result.append)
    assert result.pop().status == Status.REJECTED
    env.node.role.hint(
        5,
        dataclasses.replace(
            request, ranges=[dataclasses.replace(credit, generation=0)]
        ),
        result.append,
    )
    assert result.pop().status == Status.REJECTED
    env.node.role.connection_closed(4)
    assert 4 not in env.node.role._hint_requests


@pytest.mark.parametrize("action", [HintAction.WRITE_HINT, HintAction.WRITE_WAIT])
def test_reserve_hints_are_advice_and_run_without_the_page_lock(extension_env, action):
    ext = FakeExtension()
    ext.hint_actions = (action,)
    env = extension_env(ext)

    def hint(conn, message, ranges):
        assert env.ops._lock.acquire(blocking=False)
        env.ops._lock.release()
        assert conn > 0 and message["action"] == action and ranges
        raise RuntimeError("hint failed")

    ext.hint = hint
    from maru_common.remote_protocol import RemoteMessageType

    connection, _ = env.connect()
    response = env.send(
        connection,
        RemoteMessageType.RESERVE,
        ReserveRequest(env.op(), 1, hint=action, wait_ms=10, domain=DOMAIN),
    )
    assert response.credits and response.status == Status.OK


@pytest.mark.parametrize("where", ["mapping", "registration"])
def test_partial_mapping_failure_still_retires_the_region(
    extension_env, monkeypatch, where
):
    env = extension_env(start=False)
    if where == "mapping":
        monkeypatch.setattr(
            "maru_storage_service.drivers.cxl.CxlTier.map_for_transfers",
            lambda *args: None,
        )
    else:

        def refuse(*args, **kwargs):
            raise RuntimeError("registration failed")

        monkeypatch.setattr(FakeTransport, "register", refuse)
    with pytest.raises(RuntimeError):
        env.start_cxl()
    assert env.extension.retired and env.extension.closed
    assert env.nodes[-1].teardown_failed is None


def test_authenticated_tcp_hints_reach_the_extension_outside_server_lock(extension_env):
    from maru_common.remote_protocol import RemoteMessageType

    env = extension_env()
    credit = env.reserve().credits[0]
    env.write([credit])
    conn, _ = env.connect()
    original = env.extension.hint
    observed = []

    def hint(*args):
        def probe():
            with env.parts.server._cv:
                observed.append(True)

        thread = threading.Thread(target=probe)
        thread.start()
        thread.join(1)
        assert not thread.is_alive()
        original(*args)

    env.extension.hint = hint
    answer = conn.call(
        RemoteMessageType.HINT,
        RemoteHintRequest(HintAction.READ_HINT, "r", [credit]),
        3,
    )
    assert answer.status == Status.OK and observed == [True]
    assert env.extension.hints[-1][0] > 0
    assert env.extension.hints[-1][1]["epoch"] == env.node.node_epoch


def test_closed_connection_hints_are_ignored(extension_env):
    env = extension_env()
    env.node.role.hint(
        5,
        RemoteHintRequest(HintAction.READ_HINT, "r"),
        lambda _: pytest.fail("closed response"),
        lambda: True,
    )
    assert not env.extension.hints and not env.node.role._hint_requests


def test_write_wait_does_not_block_another_connection(extension_env):
    from maru_common.remote_protocol import PingRequest, RemoteMessageType

    ext = FakeExtension()
    ext.hint_actions = ("write_wait",)
    env = extension_env(ext)
    first, _ = env.connect()
    other, _ = env.connect(node_id="gpu-b")
    entered, release = threading.Event(), threading.Event()

    def hint(conn, message, ranges):
        entered.set()
        assert release.wait(1)

    ext.hint = hint
    answers = []
    thread = threading.Thread(
        target=lambda: answers.append(
            env.send(
                first,
                RemoteMessageType.RESERVE,
                ReserveRequest(
                    env.op(), 1, hint=HintAction.WRITE_WAIT, wait_ms=1000, domain=DOMAIN
                ),
            )
        )
    )
    thread.start()
    try:
        assert entered.wait(3)
        assert (
            other.call(RemoteMessageType.PING, PingRequest(), 0.5).status == Status.OK
        )
    finally:
        release.set()
        thread.join(3)
    assert not thread.is_alive() and answers[0].credits
