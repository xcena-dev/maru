# SPDX-License-Identifier: Apache-2.0
"""NixlExporter: whole-pool NIC registration per run, and the TransferEngine
deregistering a pool before it unmaps it."""

import pytest

from maru_remote.exporter import Export, NixlExporter
from maru_remote.transport import TransferState
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.remote_transport_fakes import FakeTransport, mapped

N = 1 << 16
PAGE = 4096


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def mem():
    return FakeTransport("mem")


@pytest.fixture
def exporter(mem):
    return NixlExporter(mem)


def test_attach_registers_the_whole_mapping(mem, exporter):
    pool, addr = mapped(N)
    exporter.attach("dram:m:1", pool)
    export = exporter.export("dram:m:1")
    assert export == Export("dram:m:1", addr, N, export.metadata)
    assert mem.agent.registered == [(addr, N)]
    assert export.metadata == mem.agent.get_partial_agent_metadata(
        [(addr, N, mem.agent.key_of(addr, N))], inc_conn_info=True
    )
    assert exporter.exports() == [export]


def test_a_peer_reaches_any_page_by_base_and_metadata(mem, exporter):
    pool, _ = mapped(N, 9)
    exporter.attach("dram:m:1", pool)
    export = exporter.export("dram:m:1")
    gpu = FakeTransport("gpu")
    local, local_addr = mapped(PAGE, 7)
    gpu.register(local_addr, PAGE)
    peer = gpu.add_peer(export.metadata)
    offset = 3 * PAGE
    write = gpu.post("WRITE", peer, [(local_addr, export.base + offset, PAGE)])
    assert write.poll_state() is TransferState.DONE
    assert bytes(pool[offset : offset + PAGE]) == bytes([7]) * PAGE
    assert bytes(pool[:offset]) == bytes([9]) * offset


def test_detach_fences_late_rdma(mem, exporter):
    pool, _ = mapped(N, 9)
    exporter.attach("dram:m:1", pool)
    export = exporter.export("dram:m:1")
    gpu = FakeTransport("gpu")
    local, local_addr = mapped(PAGE, 7)
    gpu.register(local_addr, PAGE)
    peer = gpu.add_peer(export.metadata)
    gpu.stall()
    pending = gpu.post("WRITE", peer, [(local_addr, export.base, PAGE)])
    assert gpu.fail_stalled(late=True) == 1
    assert pending.poll_state() is TransferState.ERR
    exporter.detach("dram:m:1")
    assert mem.agent.registered == []
    assert gpu.deliver_late() == 0
    assert bytes(pool) == bytes([9]) * N
    assert exporter.export("dram:m:1") is None


def test_the_same_pool_twice_is_refused(mem, exporter):
    pool, _ = mapped(N)
    exporter.attach("dram:m:1", pool)
    with pytest.raises(ValueError, match="already registered"):
        exporter.attach("dram:m:1", pool)
    assert len(mem.agent.registered) == 1


def test_a_refused_registration_leaves_nothing(mem, exporter):
    mem.agent.close()  # the NIC refuses every registration
    pool, _ = mapped(N)
    with pytest.raises(RuntimeError):
        exporter.attach("dram:m:1", pool)
    assert exporter.exports() == []


def test_failed_metadata_undoes_the_registration(mem, exporter, monkeypatch):
    def refuse(registration):
        raise RuntimeError("no metadata")

    monkeypatch.setattr(mem, "partial_metadata", refuse)
    pool, _ = mapped(N)
    with pytest.raises(RuntimeError, match="no metadata"):
        exporter.attach("dram:m:1", pool)
    assert mem.agent.registered == []
    assert exporter.exports() == []


def test_detach_of_an_unregistered_pool_is_a_no_op(mem, exporter):
    exporter.detach("dram:m:1")
    assert exporter.exports() == []


def test_a_failed_detach_keeps_the_registration(mem, exporter, monkeypatch):
    pool, addr = mapped(N)
    exporter.attach("dram:m:1", pool)

    def refuse(handle):
        raise RuntimeError("NIC busy")

    monkeypatch.setattr(mem, "deregister", refuse)
    with pytest.raises(RuntimeError, match="NIC busy"):
        exporter.detach("dram:m:1")
    assert exporter.export("dram:m:1") is None  # never told to peers again
    assert mem.agent.registered == [(addr, N)]
    with pytest.raises(ValueError, match="already registered"):
        exporter.attach("dram:m:1", pool)
    monkeypatch.undo()
    exporter.detach("dram:m:1")
    assert mem.agent.registered == []


def test_a_registration_it_could_not_undo_stays_held(mem, exporter, monkeypatch):
    def refuse(_):
        raise RuntimeError("refused")

    monkeypatch.setattr(mem, "partial_metadata", refuse)
    monkeypatch.setattr(mem, "deregister", refuse)
    pool, addr = mapped(N)
    with pytest.raises(RuntimeError, match="refused"):
        exporter.attach("dram:m:1", pool)
    assert mem.agent.registered == [(addr, N)]
    assert exporter.export("dram:m:1") is None and exporter.exports() == []
    monkeypatch.undo()
    exporter.detach("dram:m:1")  # ends it
    assert mem.agent.registered == []


def test_exports_keep_the_order_registered(exporter):
    pools = [mapped(N) for _ in range(3)]
    for i, (pool, _) in enumerate(pools):
        exporter.attach(f"p{2 - i}", pool)
    assert [e.pool_id for e in exporter.exports()] == ["p2", "p1", "p0"]
