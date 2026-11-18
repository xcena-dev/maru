# SPDX-License-Identifier: Apache-2.0
"""S7 fake integration gate: real Catalog, two StorageServices and fake DMA."""

import dataclasses
import threading
import time

import pytest

from maru_common.ms_rpc import RpcUnknownError
from maru_common.protocol import CommitItem, MessageType
from maru_common.remote_protocol import (
    HelloRefusal,
    RoomLimit,
    TransferEnd,
)
from maru_common.remote_protocol import (
    RemoteMessageType as R,
)
from maru_common.storage_types import (
    MEMORY_CAPABILITY,
    REMOTE_CAPABILITY,
    ItemResult,
    Key,
    KeyLocation,
    MissReason,
    PoolKind,
)
from maru_server.node_rpc import NodeRpcServer
from maru_server.node_server import NodeServer
from maru_storage_service.page_table import PageState
from tests.unit.ms_node_helpers import PAGE, wait_until
from tests.unit.ms_remote_helpers import NS, Gate, sentinel, state
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.test_ms_fetch import pins, settled
from tests.unit.test_ms_remote_demotion import workers as workers
from tests.unit.test_ms_remote_tier import free_port


@pytest.fixture
def ms():
    server = NodeServer(session_ttl_s=6, heartbeat_s=0.3)
    port = free_port()
    rpc = NodeRpcServer(server, host="127.0.0.1", port=port)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    wait_until(lambda: rpc._running, what="MaruServer listening")
    try:
        yield server, f"tcp://127.0.0.1:{port}"
    finally:
        rpc.stop()
        thread.join(5)
        assert not thread.is_alive()
        server.close()


@pytest.fixture
def gate(ms, workers, monkeypatch, ssd_dir):
    import maru_storage_service.drivers.peer as peer

    reset_fake_agents()
    monkeypatch.setattr(peer, "PING_S", 0.05)
    monkeypatch.setattr(peer, "RECONNECT_S", 0.05)
    monkeypatch.setattr(peer, "RPC_TIMEOUT_S", 0.4)
    env = Gate(*ms, workers, monkeypatch, ssd_dir)
    try:
        yield env
        env.check()
    finally:
        env.close()
        reset_fake_agents()


def observe_posts(gate, remote, method):
    entered, tickets = threading.Event(), []
    original = getattr(remote, method)

    def post(*args, **kwargs):
        ticket = original(*args, **kwargs)
        tickets.append(ticket)
        entered.set()
        return ticket

    gate.monkeypatch.setattr(remote, method, post)
    return entered, tickets


def wait_state(node, location, expected):
    wait_until(lambda: state(node, location) == expected, what=f"page {expected}")


def test_memory_protocol_exact_drop_and_generation_reuse(gate):
    memory = gate.memory()
    peer = gate.peer(memory)
    location = peer.write("generation", 1)
    for wrong in (
        dataclasses.replace(location, generation=location.generation + 1),
        dataclasses.replace(location, allocation_id="another"),
        dataclasses.replace(location, offset=location.offset + PAGE),
        dataclasses.replace(location, length=location.length - 1),
    ):
        assert peer.drop("generation", wrong).outcome.result != ItemResult.RETIRED
        assert gate.locations("generation") == [location]
    assert peer.drop("other-key", location).outcome.result != ItemResult.RETIRED
    (unused,) = peer.reserve().credits
    assert peer.cancel([unused]).freed == 1
    assert state(memory, unused) is None
    assert peer.drop("generation", location).outcome.result == ItemResult.RETIRED
    wait_until(lambda: not gate.locations("generation"), what="exact DROP")
    fresh = peer.write("generation", 2)
    assert fresh.generation != location.generation
    assert peer.drop("generation", location).outcome.result != ItemResult.RETIRED
    assert gate.locations("generation") == [fresh]


@pytest.mark.parametrize("fault", ["ttl", "disconnect", "unknown"])
def test_memory_uncertain_credit_survives_reconnect_and_pressure(gate, fault):
    memory = gate.memory()
    peer = gate.peer(memory)
    (credit,) = peer.reserve().credits
    if fault == "ttl":
        memory.peer_ops._clock = lambda: time.monotonic() + 1000
        assert memory.peer_ops.sweep() == 1
    elif fault == "disconnect":
        peer.conn.close()
    else:
        assert (
            peer.done(credit, "uncertain", TransferEnd.UNKNOWN).items[0].result
            == ItemResult.OK
        )
    wait_state(memory, credit, PageState.QUARANTINED)
    gate.check()
    peer.conn.close()
    peer.connect()
    others = peer.reserve(4).credits
    assert all((c.pool_id, c.offset) != (credit.pool_id, credit.offset) for c in others)
    peer.cancel(others)
    gate.pressure(memory, "dram")
    assert state(memory, credit) == PageState.QUARANTINED
    assert memory.peer_ops.quarantined[peer.node_id] == PAGE


def test_memory_pressure_moves_ingress_through_cxl_ssd_then_drops(gate):
    memory = gate.memory(("dram", "cxl", "ssd"))
    peer = gate.peer(memory)
    peer.write("cascade")
    for tier, expected in (("dram", PoolKind.CXL), ("cxl", PoolKind.SSD)):
        gate.pressure(memory, tier)
        wait_until(
            lambda expected=expected: (
                [location.kind for location in gate.locations("cascade")] == [expected]
            ),
            what=f"memory {expected}",
        )
        gate.check()
    gate.pressure(memory, "ssd")
    wait_until(lambda: not gate.locations("cascade"), what="memory last tier dropped")


@pytest.mark.parametrize("tiers", [("dram",), ("cxl",), ("dram", "cxl")])
def test_remote_move_uses_last_local_tier_and_empty_finalizer(gate, tiers):
    memory = gate.memory()
    gpu = gate.gpu(memory, tiers)
    gate.put(gpu, "move")
    calls = []
    finalize = gpu.server.finalize_move

    def record(items):
        calls.extend(items)
        return finalize(items)

    gate.monkeypatch.setattr(gpu.server, "finalize_move", record)
    for tier in tiers:
        gate.pressure(gpu, tier)
        if tier != tiers[-1]:
            wait_until(
                lambda: (
                    [location.kind for location in gate.locations("move", gpu)]
                    == [PoolKind.CXL]
                ),
                what="local CXL demotion",
            )
    gate.moved(gpu, "move")
    assert len(gate.locations("move", memory)) == 1
    assert calls[-1].finalizer_id == ""
    assert gpu.manager.move_results["COMMIT:ADDED"] == len(tiers)
    assert gpu.manager.move_results["RETIRED"] >= 1


@pytest.mark.parametrize("race", ["drop", "memory-demotes", "pinned", "foreign"])
def test_finalize_races_keep_sources_and_converge_without_extra_copy(gate, race):
    memory = gate.memory(("dram", "cxl"))
    gpu = gate.gpu(memory)
    writer = gate.put(gpu, "race")
    (source,) = gate.locations("race", gpu)
    ref = gpu.pools.find(source)
    observer = gate.loader(gpu)
    held = (
        observer.session.lookup(Key(NS, "race")).items[0] if race == "pinned" else None
    )
    remote = gpu.tier("remote")
    _, writes = observe_posts(gate, remote, "submit_store")
    finalize = gpu.server.finalize_move
    entered, results, destinations = threading.Event(), [], []

    def interfere(items):
        if not entered.is_set():
            destinations.append(items[0].dest)
            if race == "drop":
                gate.peer(memory).drop("race", items[0].dest)
            elif race == "memory-demotes":
                gate.pressure(memory, "dram")
                wait_until(
                    lambda: (
                        [location.kind for location in gate.locations("race", memory)]
                        == [PoolKind.CXL]
                    ),
                    what="destination moved",
                )
            elif race == "foreign":
                assert (
                    gpu.server.retire([KeyLocation(Key(NS, "race"), source)])
                    .items[0]
                    .result
                    == ItemResult.RETIRED
                )
                occupant = dataclasses.replace(
                    source, allocation_id="foreign", generation=source.generation + 1
                )
                value = sentinel("occupant")
                gate.values[Key(NS, "occupant")] = value
                gpu.transfers._maps[source.pool_id][
                    source.offset : source.offset + len(value)
                ] = value
                assert (
                    gpu.server.commit(
                        [CommitItem(Key(NS, "occupant"), occupant, len(value))]
                    )
                    .items[0]
                    .result
                    == ItemResult.ADDED
                )
            answer = finalize(items)
            results.extend(answer.items)
            entered.set()
            return answer
        return finalize(items)

    gate.monkeypatch.setattr(gpu.server, "finalize_move", interfere)
    gate.pressure(gpu, "dram")
    assert entered.wait(5)
    if race == "foreign":
        wait_until(lambda: not gate.locations("race", memory), what="foreign copy DROP")
        assert results[0].cause.value == "MOVED"
        wait_until(
            lambda: (
                gpu.pools.page(gpu.pools.find(gate.locations("occupant")[0])).key
                == Key(NS, "occupant")
            ),
            what="reconciled occupant",
        )
    else:
        assert gate.locations("race", gpu) == [source]
        assert gpu.pools.page(ref).state == PageState.READY
        if race == "pinned":
            assert results[0].cause.value == "PINNED"
            observer.session.unpin(held.pin_id)
            wait_until(
                lambda: ref in gpu.manager._remote_copies, what="remembered copy"
            )
        else:
            assert results[0].result == ItemResult.REJECTED
            wait_until(
                lambda: gpu.manager.move_results["REJECTED"] == 1,
                what="rejection applied",
            )
        gate.check()
        gate.pressure(gpu, "dram")
        gate.moved(gpu, "race")
        assert len(writes) == (1 if race == "pinned" else 2)
        if race == "drop":
            assert gpu.manager.move_results["COMMIT:ADDED"] == 2
        elif race == "memory-demotes":
            assert (
                gpu.manager.move_results["COMMIT:DUPLICATE"]
                + gpu.manager.move_results["COMMIT:ADDED"]
                == 2
            )
    writer.close()


@pytest.mark.parametrize(
    "gpu_cap,memory_cap", [(True, True), (False, True), (True, False), (False, False)]
)
def test_catalog_excludes_gpu_dram_and_requires_both_capabilities(
    gate, gpu_cap, memory_cap
):
    memory = gate.memory()
    gpu = gate.gpu(memory)
    other = gate.gpu(memory, node_id="gpu2")
    gate.put(other, "gpu-owned")
    gate.peer(memory).write("memory-owned")
    reader = gate.loader(gpu)
    service = gate.server.ms_service
    with service.lock:
        service.registry.nodes["gpu1"].session.capabilities = (
            (REMOTE_CAPABILITY,) if gpu_cap else ()
        )
        service.registry.nodes["mem1"].session.capabilities = (
            (MEMORY_CAPABILITY,) if memory_cap else ()
        )
    assert (
        reader.session.lookup(Key(NS, "gpu-owned")).items[0].result == ItemResult.MISS
    )
    found = reader.session.lookup(Key(NS, "memory-owned")).items[0]
    assert found.result == (
        ItemResult.FETCH if gpu_cap and memory_cap else ItemResult.MISS
    )
    if found.result == ItemResult.FETCH:
        assert found.plan.source_node == "mem1"


@pytest.mark.parametrize(
    "kind",
    [
        "dram",
        "cxl",
        "ssd",
    ],
)
def test_fetch_bytes_match_memory_sources(gate, kind):
    tiers = (kind,) if kind != "ssd" else ("dram", "ssd")
    memory = gate.memory(tiers)
    peer = gate.peer(memory)
    peer.write("fetch")
    if kind == "ssd":
        gate.pressure(memory, "dram")
        wait_until(
            lambda: (
                [location.kind for location in gate.locations("fetch")]
                == [PoolKind.SSD]
            ),
            what="SSD source",
        )
    gpu = gate.gpu(memory)
    reader = gate.loader(gpu)
    answer = reader.fetch("fetch").items[0]
    assert answer.result == ItemResult.HIT
    assert reader.read(answer.location) == sentinel("fetch")
    reader.unpin(answer.pin_id)
    wait_until(lambda: settled(gpu, gate.server), what="fetch settled")


def test_not_ready_is_immediate_miss_without_post_or_source_pin(gate):
    memory = gate.memory()
    gate.peer(memory).write("offline")
    gpu = gate.gpu(memory)
    reader = gate.loader(gpu)
    items = reader.items("offline")
    remote = gpu.tier("remote")
    gate.monkeypatch.setattr(remote, "ready", lambda _: False)
    entered, tickets = observe_posts(gate, remote, "submit_load")
    started = time.monotonic()
    answer = reader.send(reader.request(items)).result(1).items[0]
    assert answer.result == ItemResult.MISS and answer.reason == MissReason.OFFLINE
    assert time.monotonic() - started < 0.8
    assert not entered.is_set() and not tickets
    assert pins(gate.server, "offline", gate.locations("offline")[0]) == 0


def test_late_done_before_timeout_keeps_pin_and_publishes_cache(gate):
    memory = gate.memory()
    source = gate.peer(memory).write("late")
    gpu = gate.gpu(memory)
    reader = gate.loader(gpu)
    remote = gpu.tier("remote")
    entered, tickets = observe_posts(gate, remote, "submit_load")
    remote.transport.stall()
    reply = reader.send(reader.request(reader.items("late"), deadline_ms=80))
    assert entered.wait(2)
    assert reply.result(2).items[0].reason == MissReason.TIMEOUT
    assert pins(gate.server, "late", source) == 1
    assert gpu.pools.page(tickets[0].dst).state == PageState.TRANSFER
    remote.transport.stall(False)
    assert remote.transport.finish_stalled() == 1
    wait_until(lambda: settled(gpu, gate.server), what="late cache published")
    (copy,) = gate.locations("late", gpu)
    assert reader.read(copy) == sentinel("late")
    assert reader.session.lookup(Key(NS, "late")).items[0].result == ItemResult.HIT


@pytest.mark.parametrize("op", ["write", "read"])
def test_err_releases_handle_but_late_packet_only_hits_quarantine(gate, op):
    memory = gate.memory()
    gpu = gate.gpu(memory)
    remote = gpu.tier("remote")
    method = "submit_store" if op == "write" else "submit_load"
    entered, tickets = observe_posts(gate, remote, method)
    if op == "write":
        gate.put(gpu, "err")
        (source,) = gate.locations("err", gpu)
        source_ref = gpu.pools.find(source)
    else:
        gate.peer(memory).write("err")
        reader = gate.loader(gpu)
    remote.transport.stall()
    if op == "write":
        gate.pressure(gpu, "dram")
    else:
        reply = reader.send(reader.request(reader.items("err")))
    assert entered.wait(2)
    ticket = tickets[0]
    if op == "write":
        with remote._lock:
            transfer = next(t for t in remote._transfers if t is ticket)
            handle = transfer.pending
        (credit,) = memory.peer_ops._reserves[remote.origin][1]
        target_node, target = memory, credit
    else:
        target_node = gpu
        target = gpu.pools.transfer_location(ticket.dst)
    assert remote.transport.fail_stalled(late=True) == 1
    remote.transport.stall(False)
    wait_until(
        lambda: ticket.kept and ticket.state.released, what="ERR handle released"
    )
    if op == "write":
        wait_state(memory, credit, PageState.QUARANTINED)
        assert gpu.pools.page(source_ref).state == PageState.READY
        assert gpu.pools.io_refs(source_ref) == 1
        assert handle.finished
    else:
        assert reply.result(2).items[0].result == ItemResult.MISS
        wait_state(gpu, target, PageState.QUARANTINED)
    gate.check()
    remote.transport.stall(False)
    wait_until(lambda: remote.ready("mem1"), what="reconnected after ERR")
    before = remote._nodes[0].conn_gen
    old = remote._nodes[0].conn
    old.close()
    wait_until(
        lambda: remote._nodes[0].conn_gen > before and remote.ready("mem1"),
        what="new HELLO",
    )
    peer = gate.peer(memory, "pressure")
    credits = peer.reserve(4).credits
    if op == "write":
        assert all(c.offset != credit.offset for c in credits)
    else:
        pool_id = gpu.tier("dram").pool.pool_id
        refs = gpu.pools.reserve_transfer(pool_id, 32)
        assert ticket.dst not in refs
        for ref in refs:
            gpu.pools.transfer_failed(ref)
    peer.cancel(credits)
    assert state(target_node, target) == PageState.QUARANTINED
    mapping = target_node.transfers._maps[target.pool_id]
    assert bytes(
        mapping[target.offset : target.offset + len(sentinel("err"))]
    ) != sentinel("err")
    assert remote.transport.deliver_late() == 1
    assert bytes(
        mapping[target.offset : target.offset + len(sentinel("err"))]
    ) == sentinel("err")
    assert state(target_node, target) == PageState.QUARANTINED
    assert remote.kept_bytes == PAGE


def test_tcp_cut_replays_write_done_without_overlapping_peer_credits(gate):
    memory = gate.memory()
    gpu = gate.gpu(memory)
    gate.put(gpu, "tcp")
    remote = gpu.tier("remote")
    entered, tickets = observe_posts(gate, remote, "submit_store")
    remote.transport.stall()
    gate.pressure(gpu, "dram")
    assert entered.wait(2)
    node = remote._nodes[0]
    (credit,) = memory.peer_ops._reserves[remote.origin][1]
    before = node.conn_gen
    allow = threading.Event()
    connect = remote._connect

    def reconnect(n):
        if not allow.is_set():
            raise ConnectionError("TCP partition")
        return connect(n)

    gate.monkeypatch.setattr(remote, "_connect", reconnect)
    node.conn.close()
    wait_state(memory, credit, PageState.QUARANTINED)
    other = gate.peer(memory, "other")
    credits = other.reserve(4).credits
    assert credits and all(c.offset != credit.offset for c in credits)
    remote.transport.stall(False)
    gate.permit(memory, credit, "WRITE DONE")
    assert remote.transport.finish_stalled() == 1
    wait_until(lambda: node.unanswered, what="WRITE_DONE queued for replay")
    assert tickets[0].ok and gate.locations("tcp", gpu)
    gate.check()
    allow.set()
    wait_until(
        lambda: node.conn_gen > before and remote.ready("mem1") and not node.unanswered,
        what="replay acknowledged",
    )
    wait_until(lambda: gate.locations("tcp", memory), what="replayed COMMIT")
    if gate.locations("tcp", gpu):
        gate.pressure(gpu, "dram")
    gate.moved(gpu, "tcp")
    assert len(gate.locations("tcp", memory)) == 1
    other.cancel(credits)


def give_up(gate, node):
    node.server.stop()
    service = gate.server.ms_service
    with service.lock:
        registry = service.registry
        session = registry.nodes[node.config.node_id].session
        session.deadline = time.monotonic() - registry.node_give_up_s - 1
        assert registry.give_up(time.monotonic(), 1) == 1
        registry.maintain(time.monotonic(), 10000)


@pytest.mark.parametrize("lost", ["reader", "owner"])
def test_maru_server_partition_rejects_late_read_and_reuses_only_after_done(gate, lost):
    memory = gate.memory()
    peer = gate.peer(memory)
    source = peer.write("partition")
    gpu = gate.gpu(memory)
    reader = gate.loader(gpu)
    remote = gpu.tier("remote")
    entered, tickets = observe_posts(gate, remote, "submit_load")
    remote.transport.stall()
    reply = reader.send(reader.request(reader.items("partition"), deadline_ms=80))
    assert entered.wait(2)
    assert reply.result(2).items[0].reason == MissReason.TIMEOUT
    assert pins(gate.server, "partition", source) == 1
    give_up(gate, gpu if lost == "reader" else memory)
    assert pins(gate.server, "partition", source) == 0
    dest = tickets[0].dst
    assert gpu.pools.page(dest).state == PageState.TRANSFER
    if lost == "reader":
        assert peer.drop("partition", source).outcome.result == ItemResult.RETIRED
        wait_until(lambda: memory.pools.find(source) is None, what="source reusable")
        # Reuse the physical source before the READ finishes; these bytes must not publish.
        table = memory.pools._tables[source.pool_id]
        with memory.pools._lock:
            table._free.remove(source.offset // PAGE)
            table._free.appendleft(source.offset // PAGE)
        fresh = peer.write("replacement")
        assert fresh.offset == source.offset and fresh.generation != source.generation
    remote.transport.stall(False)
    assert remote.transport.finish_stalled() == 1
    wait_until(lambda: gpu.manager.fetches.flights() == 0, what="late copy discarded")
    assert tickets[0].ok
    assert not gate.locations("partition", gpu)
    assert gpu.pools.page(dest).state in (PageState.FREE, PageState.PENDING)


def test_lost_memory_commit_reply_ends_run_and_keeps_gpu_source(gate):
    memory = gate.memory()
    gpu = gate.gpu(memory)
    gate.put(gpu, "commit-loss")
    source = gate.locations("commit-loss", gpu)[0]
    rpc = memory.server._rpc
    call = rpc.call
    committed, heal = threading.Event(), threading.Event()

    def partition(code, request, *args, **kwargs):
        if heal.is_set():
            return call(code, request, *args, **kwargs)
        if code == MessageType.COMMIT:
            if not committed.is_set():
                answer = call(code, request, *args, **kwargs)
                assert answer.items[0].result == ItemResult.ADDED
                committed.set()
            raise RpcUnknownError("COMMIT answer lost")
        if committed.is_set():
            raise RpcUnknownError("memory to MaruServer partition")
        return call(code, request, *args, **kwargs)

    gate.monkeypatch.setattr(rpc, "call", partition)
    gate.pressure(gpu, "dram")
    assert committed.wait(3)
    service = gate.server.ms_service
    with service.lock:
        registry = service.registry
        registry.nodes["mem1"].session.deadline = (
            time.monotonic() - registry.node_give_up_s - 1
        )
        assert registry.give_up(time.monotonic(), 1) == 1
        registry.maintain(time.monotonic(), 10000)
    heal.set()
    wait_until(
        lambda: memory.manager.ended is not None,
        timeout=10,
        what="memory run ends after partition",
    )
    assert gate.locations("commit-loss", gpu) == [source]
    assert state(gpu, source) == PageState.READY
    gate.monkeypatch.setattr(rpc, "call", call)


@pytest.mark.parametrize("kind", ["dram", "cxl"])
def test_memory_kill_restart_fences_old_dma_and_resets_remote_run(gate, kind):
    from tests.unit.test_ms_cxl_tier import FakeRm

    rm = FakeRm()
    memory = gate.memory((kind,), rm=rm, map_region=gate.regions.node_map)
    gpu = gate.gpu(memory)
    gate.put(gpu, "killed")
    remote = gpu.tier("remote")
    entered, tickets = observe_posts(gate, remote, "submit_store")
    remote.transport.stall()
    gate.pressure(gpu, "dram")
    assert entered.wait(2)
    node = remote._nodes[0]
    old_epoch, old_ops, old_peer = node.node_epoch, node.ops, node.peer
    (credit,) = memory.peer_ops._reserves[remote.origin][1]
    old_mapping = memory.transfers._maps[credit.pool_id]
    disconnected = threading.Event()
    check_state = remote.transport.check_state

    def checked(handle):
        try:
            return check_state(handle)
        except RuntimeError as error:
            if "REMOTE_DISCONNECT" in str(error):
                disconnected.set()
            raise

    gate.monkeypatch.setattr(remote.transport, "check_state", checked)
    memory.remote.exporter.transport.kill()
    assert disconnected.wait(2)
    wait_until(
        lambda: tickets[0].kept and tickets[0].state.released,
        what="REMOTE_DISCONNECT terminal ERR",
    )
    assert not tickets[0].ok and not tickets[0].pending.completed
    assert gate.locations("killed", gpu)
    assert remote.kept_bytes == PAGE
    gate.check()
    for table in memory.pools._tables.values():
        for page in table.pages:
            if page.state == PageState.QUARANTINED:
                gate.evidence[(id(table), page.index, page.generation)] = "agent killed"
    memory.stop()
    remote.transport.stall(False)
    fresh = gate.memory(
        (kind,),
        port=int(memory.config.remote_server.listen.rsplit(":", 1)[1]),
        rm=rm,
        map_region=gate.regions.node_map,
    )
    wait_until(
        lambda: node.node_epoch != old_epoch and remote.ready("mem1"),
        what="fresh memory HELLO",
    )
    assert node.ops is not old_ops and not node.unanswered and not node.lost_reserves
    assert not remote.transport.has_peer(old_peer)
    peer = gate.peer(fresh)
    replacement = peer.write("fresh-run")
    if kind == "cxl":
        assert (
            replacement.pool_id == credit.pool_id
            and replacement.offset == credit.offset
        )
        assert replacement.generation != credit.generation
    else:
        assert replacement.pool_id != credit.pool_id
    assert old_mapping.closed
    assert remote.transport.finish_stalled() == 0
    assert remote.kept_bytes == PAGE


def test_new_gpu_epoch_retires_live_old_process_without_freeing_credits(gate):
    memory = gate.memory()
    old = gate.peer(memory, "same-gpu", "old")
    (credit,) = old.reserve().credits
    new = gate.peer(memory, "same-gpu", "new")
    wait_until(lambda: old.conn.closed, what="old epoch connection closed")
    assert not old.transport.agent.closed
    wait_state(memory, credit, PageState.QUARANTINED)
    with pytest.raises(AssertionError):
        old.connect()
    assert old.hello.refusal == HelloRefusal.RETIRED
    (fresh,) = new.reserve().credits
    assert fresh.offset != credit.offset
    new.cancel([fresh])
    assert state(memory, credit) == PageState.QUARANTINED


def test_reconnects_and_gpu_epochs_reach_peer_then_node_quarantine_caps(gate):
    memory = gate.memory()
    memory.peer_ops.peer_quarantine_max = 2 * PAGE
    memory.peer_ops.quarantine_max = 3 * PAGE
    a = gate.peer(memory, "capped", "a1")
    (first,) = a.reserve().credits
    a.conn.close()
    wait_state(memory, first, PageState.QUARANTINED)
    a.connect()
    (second,) = a.reserve().credits
    b = gate.peer(memory, "capped", "a2")
    wait_state(memory, second, PageState.QUARANTINED)
    assert (b.ping().room, b.ping().limit) == (0, RoomLimit.QUARANTINE)
    assert b.reserve().credits == []
    other = gate.peer(memory, "healthy")
    (third,) = other.reserve().credits
    assert len({first.offset, second.offset, third.offset}) == 3
    other.conn.close()
    wait_state(memory, third, PageState.QUARANTINED)
    other.connect()
    assert other.ping().room == b.ping().room == 0
    assert other.reserve().credits == []
    assert memory.peer_ops.quarantined == {"capped": 2 * PAGE, "healthy": PAGE}
    gate.check()
    for table in memory.pools._tables.values():
        for page in table.pages:
            if page.state == PageState.QUARANTINED:
                gate.evidence[(id(table), page.index, page.generation)] = (
                    "registration ended"
                )
    memory.stop()
    fresh = gate.memory(port=int(memory.config.remote_server.listen.rsplit(":", 1)[1]))
    restored = gate.peer(fresh, "capped", "new-memory-run")
    assert restored.ping().room > 0 and restored.reserve().credits


def test_gpu_quarantine_cap_stops_new_remote_posts(gate):
    memory = gate.memory()
    gpu = gate.gpu(memory)
    remote = gpu.tier("remote")
    remote.quarantine_max = PAGE
    gate.put(gpu, "cap")
    entered, tickets = observe_posts(gate, remote, "submit_store")
    remote.transport.stall()
    gate.pressure(gpu, "dram")
    assert entered.wait(2)
    assert remote.transport.fail_stalled(late=True) == 1
    remote.transport.stall(False)
    wait_until(lambda: remote.kept_bytes == PAGE, what="GPU cap reached")
    assert not remote.ready("mem1")
    assert remote.reserve([Key(NS, "blocked")]) == [None]
    ticket = tickets[0]
    from maru_storage_service.tiers import Span

    credit = next(iter(memory.peer_ops._reserves[remote.origin].values()))[0]
    with pytest.raises(RuntimeError, match="cap"):
        remote.submit_store(
            Span(ticket.src.pool_id, ticket.src.index * PAGE, 3000),
            credit,
            refs=(ticket.src, None),
        )
    assert len(tickets) == 1
    remote.transport.stall(False)
    assert remote.transport.deliver_late() == 1
    wait_state(memory, credit, PageState.QUARANTINED)


@pytest.mark.parametrize("refused", ["memory-cxl", "gpu-l1"])
def test_partial_registration_is_not_announced_or_selected(gate, refused):
    def transport(name, device):
        from tests.unit.test_ms_remote_tier import fake_transport

        made = fake_transport(name, device)
        register = made.register

        def refuse(*args, **kwargs):
            tag = kwargs.get("tag", "")
            if (
                refused == "memory-cxl"
                and name.startswith("maru-mem-")
                and tag.startswith("cxl:")
            ) or (
                refused == "gpu-l1"
                and name.startswith("maru-gpu-")
                and tag.startswith("dram:")
            ):
                raise RuntimeError("injected NIC registration refusal")
            return register(*args, **kwargs)

        made.register = refuse
        return made

    memory = gate.memory(("dram", "cxl"), make_transport=transport)
    gpu = gate.gpu(memory, make_transport=transport)
    service = gate.server.ms_service
    with service.lock:
        announced = service.catalog.pools_of("mem1")
        caps = service.registry.capabilities_of("gpu1")
    if refused == "memory-cxl":
        assert [p.kind for p in announced] == [PoolKind.DRAM]
        assert all(
            p.kind == PoolKind.DRAM for p in gpu.tier("remote")._nodes[0].pools.values()
        )
        gate.peer(memory).write("registered")
        reader = gate.loader(gpu)
        result = reader.fetch("registered").items[0]
        assert result.result == ItemResult.HIT
        assert reader.read(result.location) == sentinel("registered")
        reader.unpin(result.pin_id)
    else:
        assert REMOTE_CAPABILITY not in caps
        assert not gpu.tier("remote").ready("mem1")
        assert gpu.tier("remote")._threads == []
        gate.put(gpu, "local-only")
        assert gate.locations("local-only", gpu)


def test_deregistration_failure_keeps_cxl_mapping_and_rm_hold(gate):
    def held_mapping(source, pool, holder):
        source.node_hold(pool.node_id, pool.node_epoch, pool.region.handle, holder)
        return gate.regions.node_map(pool)

    gate.monkeypatch.setattr(
        "maru_storage_service.drivers.cxl.map_region", held_mapping
    )
    memory = gate.memory(("cxl",), map_region=None)
    peer = gate.peer(memory)
    peer.write("held")
    exporter = memory.remote.exporter
    pool = memory.tier("cxl").pools[0]
    region_id = pool.region.handle.region_id
    rm = memory.tier("cxl").source
    detach = exporter.detach

    def stuck(_pool_id):
        raise RuntimeError("injected NIC deregistration failure")

    gate.monkeypatch.setattr(exporter, "detach", stuck)
    memory.stop()
    assert (
        memory.teardown_failed is not None
        and "stay registered" in memory.teardown_failed
    )
    assert not memory.transfers._kept[pool.pool_id].closed
    assert pool.pool_id in memory.transfers._kept
    assert rm.holds[region_id]
    assert not exporter.transport.agent.closed
    gate.check()

    def cleanup():
        detach(pool.pool_id)
        memory.transfers._kept[pool.pool_id].close()
        memory.tier("cxl").after_transfers_closed()
        exporter.transport.close()

    gate.cleanups.append(cleanup)


def test_duplicate_commit_finalizes_onto_the_existing_exact_location(gate):
    memory = gate.memory()
    existing = gate.peer(memory).write("duplicate")
    gpu = gate.gpu(memory)
    gate.put(gpu, "duplicate")
    gate.pressure(gpu, "dram")
    gate.moved(gpu, "duplicate")
    assert gate.locations("duplicate", memory) == [existing]
    assert gpu.manager.move_results["COMMIT:DUPLICATE"] == 1


def test_real_gpu_restarts_accumulate_quarantine_without_blocking_other_nodes(gate):
    memory = gate.memory()
    memory.peer_ops.peer_quarantine_max = 2 * PAGE
    for run in range(2):
        gpu = gate.gpu(memory)
        remote = gpu.tier("remote")
        writer = gate.put(gpu, f"restart-{run}")
        entered, tickets = observe_posts(gate, remote, "submit_store")
        remote.transport.stall()
        gate.pressure(gpu, "dram")
        assert entered.wait(2)
        assert remote.transport.fail_stalled(late=True) == 1
        remote.transport.stall(False)
        wait_until(lambda tickets=tickets: tickets[0].kept, what="failed write kept")
        wait_until(
            lambda run=run: memory.peer_ops.quarantined["gpu1"] == (run + 1) * PAGE,
            what="peer quarantine accumulated",
        )
        assert remote.transport.deliver_late() == 1
        gate.check()
        writer.close()
        gpu.stop()
        assert gpu.teardown_failed is None
    fresh = gate.gpu(memory)
    assert fresh.tier("remote").reserve([Key(NS, "capped")]) == [None]
    other = gate.gpu(memory, node_id="gpu2")
    gate.put(other, "unblocked")
    gate.pressure(other, "dram")
    gate.moved(other, "unblocked")
    assert gate.locations("unblocked", memory)


def test_lost_write_done_reply_replays_same_seq_and_commits_once(gate):
    from maru_storage_service.remote_client import RemoteConnection

    gate.monkeypatch.setattr("maru_storage_service.drivers.peer.RPC_TIMEOUT_S", 2)
    memory = gate.memory()
    gpu = gate.gpu(memory)
    gate.put(gpu, "ledger")
    remote = gpu.tier("remote")
    lost, allow = threading.Event(), threading.Event()
    requests, commits = [], []
    submit = RemoteConnection.submit
    commit = memory.server.commit
    connect = remote._connect

    def counted(items):
        commits.extend(items)
        return commit(items)

    def reconnect(node):
        if lost.is_set() and not allow.is_set():
            raise ConnectionError("reply lost, TCP partition")
        return connect(node)

    def send(conn, code, request, callback):
        if code != R.WRITE_DONE:
            return submit(conn, code, request, callback)
        requests.append((request.op.seq, request.op.digest))
        if len(requests) > 1:
            return submit(conn, code, request, callback)

        def discard(answer):
            if isinstance(answer, ConnectionError):
                callback(answer)
                return
            assert answer.items[0].result == ItemResult.ADDED
            lost.set()
            callback(ConnectionError("WRITE_DONE reply lost"))
            conn.close()

        return submit(conn, code, request, discard)

    gate.monkeypatch.setattr(memory.server, "commit", counted)
    gate.monkeypatch.setattr(remote, "_connect", reconnect)
    gate.monkeypatch.setattr(RemoteConnection, "submit", send)
    gate.pressure(gpu, "dram")
    assert lost.wait(2)
    assert gate.locations("ledger", gpu) and gate.locations("ledger", memory)
    gate.check()
    allow.set()
    wait_until(
        lambda: len(requests) == 2 and not remote._nodes[0].unanswered,
        what="ledger replay answered",
    )
    gate.moved(gpu, "ledger")
    assert requests[0] == requests[1] and len(commits) == 1


@pytest.mark.parametrize("kind", ["dram", "cxl"])
def test_memory_kill_ends_read_as_err_and_keeps_local_destination(gate, kind):
    memory = gate.memory((kind,))
    source = gate.peer(memory).write("read-kill")
    gpu = gate.gpu(memory)
    reader = gate.loader(gpu)
    remote = gpu.tier("remote")
    entered, tickets = observe_posts(gate, remote, "submit_load")
    remote.transport.stall()
    reply = reader.send(reader.request(reader.items("read-kill")))
    assert entered.wait(2)
    memory.remote.exporter.transport.kill()
    assert reply.result(2).items[0].result == ItemResult.MISS
    wait_until(
        lambda: tickets[0].kept and tickets[0].state.released,
        what="READ REMOTE_DISCONNECT released",
    )
    assert remote.kept_bytes == PAGE and not tickets[0].pending.completed
    assert gpu.pools.page(tickets[0].dst).state == PageState.QUARANTINED
    wait_until(
        lambda: pins(gate.server, "read-kill", source) == 0,
        what="failed READ pin released",
    )
    assert not gate.locations("read-kill", gpu)
    remote.transport.stall(False)
    assert remote.transport.finish_stalled() == 0


def test_gpu_kill_then_new_epoch_keeps_old_write_target_quarantined(gate):
    memory = gate.memory()
    gpu = gate.gpu(memory)
    writer = gate.put(gpu, "gpu-kill")
    remote = gpu.tier("remote")
    entered, tickets = observe_posts(gate, remote, "submit_store")
    remote.transport.stall()
    gate.pressure(gpu, "dram")
    assert entered.wait(2)
    (credit,) = memory.peer_ops._reserves[remote.origin][1]
    assert remote.transport.fail_stalled(late=True) == 1
    remote.transport.stall(False)
    wait_until(lambda: tickets[0].kept, what="old WRITE packet still in flight")
    wait_state(memory, credit, PageState.QUARANTINED)
    gate.check()
    remote.transport.kill()
    writer.close()
    gpu.stop()
    assert gpu.teardown_failed is None
    fresh = gate.gpu(memory)
    assert fresh.node_epoch != gpu.node_epoch
    assert remote.origin in memory.peer_ops._retired
    assert state(memory, credit) == PageState.QUARANTINED
    assert remote.transport.deliver_late() == 1
    mapping = memory.transfers._maps[credit.pool_id]
    assert bytes(mapping[credit.offset : credit.offset + 3000]) == sentinel("gpu-kill")
    gate.put(fresh, "new-gpu-run")
    gate.pressure(fresh, "dram")
    gate.moved(fresh, "new-gpu-run")
    (location,) = gate.locations("new-gpu-run", memory)
    assert location.offset != credit.offset
    assert state(memory, credit) == PageState.QUARANTINED
