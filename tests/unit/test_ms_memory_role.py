# SPDX-License-Identifier: Apache-2.0
"""Memory-node lifetime, registration and worker-direct data access."""

import dataclasses
import socket

import pytest

from maru_common.remote_protocol import (
    REMOTE_WIRE_VERSION,
    AgentMetadata,
    AgentsRequest,
    HelloRequest,
    RemoteWriteDoneRequest,
    ReserveRequest,
    TransferEnd,
    Written,
    peer_origin,
)
from maru_common.remote_protocol import (
    RemoteMessageType as Code,
)
from maru_common.serializer import op_digest
from maru_common.storage_types import (
    MEMORY_CAPABILITY,
    REMOTE_CAPABILITY,
    WORKER_RDMA_CAPABILITY,
    Key,
    OpRef,
    Status,
    WriteDomain,
)
from maru_remote.auth import load_key, write_key
from maru_remote.transport import TransferState
from maru_storage_service.config import RemoteServerConfig
from maru_storage_service.remote_client import RemoteConnection
from maru_storage_service.service import StorageService
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    free_port,
    node_config,
    remove_tree,
    short_tmpdir,
)
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.remote_transport_fakes import FakeTransport, mapped

A = peer_origin("gpu-a", "run-a")
DOMAIN = WriteDomain("gpu-a", "run-a", "instance", 1)


def call(fn, origin, req):
    import threading

    result, event = [], threading.Event()

    def done(answer):
        result.append(answer)
        event.set()

    fn(origin, req, done)
    assert event.wait(5)
    assert len(result) == 1
    return result[0]


class MemoryEnv:
    def __init__(self):
        self.directory = short_tmpdir()
        self.fake = FakeMaruServer()
        self.key_path = f"{self.directory}/maru.key"
        write_key(self.key_path)
        config = node_config(
            self.fake.url, f"{self.directory}/unused.sock", role="memory"
        )
        self.config = dataclasses.replace(
            config,
            remote_server=RemoteServerConfig(
                f"127.0.0.1:{free_port()}", self.key_path, 2 * PAGE, 8 * PAGE
            ),
        )
        self.nodes, self.clients = [], []
        self.seq = 0
        self.node = self.start()
        self.ops = self.node.role.ops
        self.parts = self.node.role.parts

    def start(self, **options):
        node = StorageService(
            self.config,
            make_transport=lambda name, device: FakeTransport(name),
            rpc_timeout_s=0.15,
            evict_interval_s=100,
            **options,
        )
        self.nodes.append(node)
        node.start()
        return node

    def op(self, origin=A):
        self.seq += 1
        return OpRef(origin, self.seq, b"", 0)

    def reserve(self, pages=1, domain=DOMAIN):
        origin = peer_origin(domain.node_id, domain.node_epoch)
        return call(
            self.ops.reserve,
            origin,
            ReserveRequest(self.op(origin), pages, domain=domain),
        )

    def write(self, credits, domain=DOMAIN, end=TransferEnd.DONE, key="k"):
        origin = peer_origin(domain.node_id, domain.node_epoch)
        return call(
            self.ops.write_done,
            origin,
            RemoteWriteDoneRequest(
                self.op(origin),
                [
                    Written(Key("ns", f"{key}{i}"), c, PAGE, end)
                    for i, c in enumerate(credits)
                ],
                domain,
            ),
        )

    def connect(self, **changes):
        req = HelloRequest(
            REMOTE_WIRE_VERSION,
            "gpu-a",
            "run-a",
            "boot",
            123,
            self.node.server.server_epoch,
            "",
            b"",
            PAGE,
            1,
            [REMOTE_CAPABILITY, WORKER_RDMA_CAPABILITY],
        )
        req = dataclasses.replace(req, **changes)
        conn, answer = RemoteConnection.open(
            self.parts.server.address, load_key(self.key_path), req, 3
        )
        self.clients.append(conn)
        return conn, answer

    def send(self, conn, code, req):
        req = dataclasses.replace(
            req, op=dataclasses.replace(req.op, digest=op_digest(req))
        )
        return conn.call(code, req, 3)

    def close(self):
        for conn in self.clients:
            conn.close()
        for node in reversed(self.nodes):
            node.stop()
        self.fake.close()
        remove_tree(self.directory)


@pytest.fixture
def memory():
    reset_fake_agents()
    env = MemoryEnv()
    yield env
    env.close()
    reset_fake_agents()


def test_memory_run_exports_before_serving_and_uses_no_data_mover(memory):
    node, parts = memory.node, memory.parts
    assert node.local is None
    conn, hello = memory.connect()
    assert hello.status == Status.OK and hello.capabilities == [MEMORY_CAPABILITY]
    assert hello.boot == node.server.host_boot_id
    assert hello.hint_actions == []
    assert hello.agent_name == parts.name
    (pool,) = hello.pools
    assert pool.pool_registration == node.pool_registrations[pool.pool_id] > 0
    assert pool.base and pool.metadata
    with socket.socket(socket.AF_UNIX) as probe:
        assert probe.connect_ex(f"{memory.directory}/unused.sock") != 0
    assert not conn.closed


def test_workers_write_and_read_directly_through_separate_registrations(memory):
    conn, hello = memory.connect()
    gpu = FakeTransport("worker")
    source, addr = mapped(PAGE, 7)
    gpu.register(addr, PAGE)
    assert (
        conn.call(
            Code.AGENTS, AgentsRequest(add=[AgentMetadata("worker", gpu.metadata())]), 3
        ).status
        == Status.OK
    )
    reserved = memory.send(
        conn, Code.RESERVE, ReserveRequest(memory.op(), 1, domain=DOMAIN)
    )
    assert reserved.status == Status.OK
    (credit,) = reserved.credits
    writer = reserved.write_agent
    assert writer.domain == DOMAIN and writer.agent_name != hello.agent_name
    peer = gpu.add_peer(writer.agent_metadata)
    (pool,) = writer.pools
    assert pool.pool_registration > 0
    assert (
        gpu.post("WRITE", peer, [(addr, pool.base + credit.offset, PAGE)]).poll_state()
        == TransferState.DONE
    )
    result = memory.send(
        conn,
        Code.WRITE_DONE,
        RemoteWriteDoneRequest(
            memory.op(),
            [Written(Key("ns", "data"), credit, PAGE, TransferEnd.DONE)],
            DOMAIN,
        ),
    )
    assert result.items[0].result == "ADDED"
    reader = gpu.add_peer(hello.pools[0].metadata)
    source[:] = bytes(PAGE)
    assert (
        gpu.post(
            "READ", reader, [(addr, hello.pools[0].base + credit.offset, PAGE)]
        ).poll_state()
        == TransferState.DONE
    )
    assert source[:] == bytes([7]) * PAGE
    gpu.close()
    source.close()


def test_unchecked_transport_is_refused_before_an_agent_is_created(memory, monkeypatch):
    from maru_remote.checked import UncheckedTransportError

    memory.node.stop()
    monkeypatch.setenv("UCX_TLS", "tcp")
    node = StorageService(memory.config)
    with pytest.raises(UncheckedTransportError):
        node.start()
    assert node.wait_finished(0) and node.teardown_failed is None


def test_listener_failure_does_not_open_a_catalog_session(memory):
    other = StorageService(
        dataclasses.replace(memory.config, node_id="other"),
        make_transport=lambda name, device: FakeTransport(name),
    )
    with pytest.raises(OSError):
        other.start()
    assert "other" not in memory.fake.nodes
    assert other.wait_finished(0) and other.teardown_failed is None


def test_a_failed_revocation_keeps_all_mappings_until_process_restart(
    memory, monkeypatch
):
    memory.reserve()
    transport = memory.parts._writers[DOMAIN].transport
    with monkeypatch.context() as patch:
        patch.setattr(
            transport,
            "deregister",
            lambda _: (_ for _ in ()).throw(RuntimeError("busy")),
        )
        memory.node.stop()
        assert memory.node.teardown_failed is not None
        assert memory.parts._maps and all(
            not m.closed for m in memory.parts._maps.values()
        )
        assert transport.agent.regs
    memory.parts.close()
    for tier in memory.node._tiers.values():
        tier.close()
    memory.node._ownership.close()


def test_agents_are_idempotent_bounded_per_gpu_node_and_connect_each_registration(
    memory,
):
    parts = memory.parts
    parts.agents_max = 2
    workers = [FakeTransport(f"worker{i}") for i in range(3)]
    adds = [AgentMetadata(w.agent.name, w.metadata()) for w in workers]
    req = AgentsRequest(add=adds[:2])
    assert call(memory.ops.agents, A, req).status == Status.OK
    assert call(memory.ops.agents, A, req).status == Status.OK
    assert (
        call(memory.ops.agents, A, AgentsRequest(add=adds[2:])).status
        == Status.REJECTED
    )
    next_run = peer_origin(DOMAIN.node_id, "run-b")
    assert (
        call(memory.ops.agents, next_run, AgentsRequest(add=adds[2:])).status
        == Status.REJECTED
    )
    memory.reserve()
    writer = parts._writers[DOMAIN]
    assert set(parts._agents) <= parts.transport.agent.peers
    assert set(parts._agents) <= writer.transport.agent.peers
    assert set(parts._agents) <= writer.progress.expecting()
    assert (
        call(memory.ops.agents, A, AgentsRequest(remove=[adds[0].agent_name])).status
        == Status.OK
    )
    assert (
        call(memory.ops.agents, next_run, AgentsRequest(add=adds[2:])).status
        == Status.OK
    )
    for worker in workers:
        worker.close()


@pytest.mark.parametrize("field", ["owner", "name", "metadata", "size"])
def test_invalid_agent_updates_are_rejected_before_metadata_is_exposed(memory, field):
    worker = FakeTransport("worker")
    record = AgentMetadata("worker", worker.metadata())
    assert call(memory.ops.agents, A, AgentsRequest(add=[record])).status == Status.OK
    origin = A
    if field == "owner":
        origin = peer_origin("other", "run")
    elif field == "name":
        record = dataclasses.replace(record, agent_name="")
    elif field == "metadata":
        record = dataclasses.replace(record, agent_metadata=b"changed")
    else:
        record = dataclasses.replace(record, agent_metadata=b"x" * ((1 << 20) + 1))
    assert (
        call(memory.ops.agents, origin, AgentsRequest(add=[record])).status
        == Status.REJECTED
    )
    assert memory.parts._agents == {"worker": (A, worker.metadata())}
    worker.close()


def test_write_domain_agent_limit_counts_failed_registrations(memory, monkeypatch):
    from tests.unit.test_ms_peer_ops import fail_fence, fence

    parts = memory.parts
    parts.domains_max = 1
    memory.reserve()
    with monkeypatch.context() as patch:
        fail_fence(memory, patch)
        assert fence(memory).result == "failure"
        other = dataclasses.replace(DOMAIN, number=2)
        full = memory.reserve(domain=other)
        assert full.status == Status.OK and not full.credits
        assert len(parts._writers) == 1
    assert fence(memory).result == "success"
    assert memory.reserve(domain=other).credits


def test_hint_is_explicitly_unsupported_and_old_capabilities_are_refused(memory):
    from maru_common.remote_protocol import RemoteHintRequest
    from maru_common.storage_types import HintAction

    conn, response = memory.connect()
    assert response.status == Status.OK
    assert (
        conn.call(Code.HINT, RemoteHintRequest(HintAction.READ_HINT, "r"), 3).status
        == Status.REJECTED
    )
    _, refused = memory.connect(capabilities=[REMOTE_CAPABILITY, "stage_v1"])
    assert refused.status == Status.REJECTED and refused.pools == []


def test_memory_command_reports_key_errors_without_creating_a_transport(
    memory, monkeypatch, capsys
):
    from maru_storage_service import cli

    memory.node.stop()
    missing = dataclasses.replace(
        memory.config,
        remote_server=dataclasses.replace(
            memory.config.remote_server, key_file=f"{memory.directory}/missing"
        ),
    )
    assert cli.serve(missing) == cli.EXIT_START
    assert "cannot start" in capsys.readouterr().err


def test_uncertain_agent_update_blocks_further_resource_growth_until_restart(memory):
    worker = FakeTransport("actual-name")
    request = AgentsRequest(add=[AgentMetadata("declared-name", worker.metadata())])
    assert call(memory.ops.agents, A, request).status == Status.REJECTED
    assert memory.parts._agents_failed
    assert memory.parts.transport.agent.peers == {"actual-name"}
    assert (
        call(memory.ops.agents, A, AgentsRequest(remove=["declared-name"])).status
        == Status.REJECTED
    )
    assert memory.reserve().status == Status.REJECTED
    assert memory.parts.transport.agent.peers == {"actual-name"}
    worker.close()
