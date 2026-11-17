# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Run boundaries over real metadata RPCs and the private, file-backed test RM.

Only device paths in access replies are redirected; ownership, holds, fencing,
LOOKUP, COMMIT, RETIRE and replay decisions use the production implementations.
Every race is ordered by an Event, queue, or completed RPC, never a sleep.
"""

import dataclasses
import multiprocessing
import queue
import threading
from contextlib import contextmanager
from pathlib import Path

import pytest

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP, MsRpcClient
from maru_common.protocol import (
    MESSAGE_CLASSES,
    MS_MESSAGE_CLASSES,
    CommitItem,
    HandshakeRequest,
    MessageType,
    RetireRequest,
)
from maru_common.serializer import op_digest
from maru_common.storage_types import (
    ItemResult,
    Key,
    KeyLocation,
    OpRef,
    PoolKind,
    Status,
    generation_epoch_seq,
    run_holder,
)
from maru_handler import MaruHandler
from maru_handler.client import pool_maps
from maru_server.node_rpc import NodeRpcServer
from maru_server.node_server import NodeServer
from maru_server.pin_manager import HolderKind
from maru_shm.client import MaruShmClient
from maru_shm.types import NodeLeaseExpiredError, StaleNodeEpochError
from maru_storage_service import service as service_module
from maru_storage_service.cli import EXIT_LOST, serve
from maru_storage_service.drivers.cxl import CxlTier
from maru_storage_service.pool_manager import PageState
from maru_storage_service.service import Node, StorageService
from tests.integration.test_ms_s2_real_server import free_port
from tests.integration.test_rm_node_ownership import ALIGN
from tests.integration.test_rm_node_ownership import rm_address as private_rm
from tests.unit.ms_node_helpers import PAGE, node_config, remove_tree, short_tmpdir

pytestmark = pytest.mark.integration
WAIT = 15
rm_address = private_rm


class FileRm:
    """Real RM calls, with only the fake daemon's device path translated."""

    def __init__(self, address, path):
        self.client = MaruShmClient(address=address)
        self.path = str(path)
        self.calls = []
        self.polls = None
        self.permits = queue.Queue()

    def __getattr__(self, name):
        method = getattr(self.client, name)

        def call(*args, **kwargs):
            self.calls.append(name)
            return method(*args, **kwargs)

        return call

    def node_hold(self, *args, **kwargs):
        access = self.client.node_hold(*args, **kwargs)
        assert access.dax_path == "/dev/dax0.0"
        return dataclasses.replace(access, dax_path=self.path)

    def get_access(self, *args, **kwargs):
        access = self.client.get_access(*args, **kwargs)
        assert access.dax_path == "/dev/dax0.0"
        return dataclasses.replace(access, dax_path=self.path)

    def node_holds(self, *args):
        if (
            self.polls is not None
            and threading.current_thread().name == "maru-cxl-holds"
        ):
            self.polls.put(True)  # entering the next poll also fences the last one
            assert self.permits.get(timeout=WAIT)
        return self.client.node_holds(*args)


@pytest.fixture
def workdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def metadata():
    """Production MaruServer and RPC dispatch, with no host DAX scanning."""
    servers = []

    def make(port=None):
        server = NodeServer(session_ttl_s=60, heartbeat_s=0.1)
        rpc = NodeRpcServer(server, port=port or free_port())
        thread = threading.Thread(target=rpc.start, daemon=True)
        thread.start()
        servers.append((server, rpc, thread))
        client = MsRpcClient(
            rpc.address,
            timeout_s=WAIT,
            message_classes=MESSAGE_CLASSES,
            strict_codes=MS_MESSAGE_CLASSES,
        )
        try:
            # ZMQ queues the handshake until bind; the answer is the ready barrier.
            assert client.call(MessageType.HANDSHAKE, HandshakeRequest()).success
        finally:
            client.close()
        return server.ms_service, rpc.address

    def stop(url):
        record = next(r for r in servers if r[1].address == url)
        servers.remove(record)
        server, rpc, thread = record
        rpc.stop()
        thread.join(WAIT)
        assert not thread.is_alive()
        server.close()

    make.stop = stop
    yield make
    for _, rpc, _ in list(reversed(servers)):
        stop(rpc.address)


@pytest.fixture
def real_rm(rm_address, tmp_path, monkeypatch):
    # rm_address's daemon owns this sparse file, never the host's /dev/dax*.
    path = tmp_path / "dev/dax0.0"
    assert path.is_file() and path.is_relative_to(tmp_path)
    rm = FileRm(rm_address, path)
    holds = pool_maps.RegionHolds()
    monkeypatch.setattr(pool_maps, "_HOLDS", holds)
    monkeypatch.setattr(pool_maps, "_rm_client", lambda address: rm)
    yield rm
    rm.permits.put(True)  # let a failed test's watcher leave
    assert holds.unholds.flush(WAIT)
    rm.client.close()


def config(url, socket, address, *, regions=1, dram=False):
    cfg = node_config(
        url,
        socket,
        pages=8,
        high=1.0,
        low=0.9,
        cxl_pages=regions * ALIGN // PAGE,
        region_pages=ALIGN // PAGE,
    )
    cxl = dataclasses.replace(
        cfg.tiers[1], rm=address, high_watermark=1.0, low_watermark=0.9
    )
    return dataclasses.replace(cfg, tiers=(cfg.tiers[0], cxl) if dram else (cxl,))


def publish(service, pool_id, key, value):
    """Seed an exact pool through its real byte mapping and COMMIT RPC."""
    (ref,) = service.pools.reserve_transfer(pool_id, 1)
    location = dataclasses.replace(
        service.pools.transfer_location(ref), length=len(value)
    )
    service.transfers._maps[pool_id][location.offset : location.offset + len(value)] = (
        value
    )
    service.pools.transfer_done(ref, Key("ns", key), len(value))
    response = service.server.commit([CommitItem(Key("ns", key), location, len(value))])
    assert response.status == Status.OK and response.items[0].result == ItemResult.ADDED
    service.pools.commit_added(ref)
    service.manager.placement.on_ready(ref)
    return ref, location


@contextmanager
def worker(socket):
    h = MaruHandler(
        MaruConfig(
            maru_node=socket,
            engine_id="reader",
            cache_namespace="ns",
            chunk_size_bytes=PAGE,
            pool_size=PAGE,
            pin_pools=False,
            load_only=True,
            auto_connect=False,
            timeout_ms=1000,
        )
    )
    h.connect()
    try:
        yield h
    finally:
        h.release_retrieved(list(h._cpu._reads))
        h.close()


@contextmanager
def running_node(cfg, rm, *, next_run=None):
    started, outcome = queue.Queue(), queue.Queue()

    class ObservedService(StorageService):
        def _start(self):
            if self.later_run and next_run is not None:
                assert next_run.wait(WAIT)
            super()._start()
            started.put(self)

    node = Node(
        cfg,
        lambda c: ObservedService(
            c, region_source=rm, cxl_renew_s=0.1, rpc_timeout_s=1, evict_interval_s=60
        ),
        pause_s=0,
    )

    def run():
        try:
            outcome.put(node.run())
        except BaseException as exc:
            outcome.put(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        first = started.get(timeout=WAIT)
        yield node, first, started
    finally:
        if next_run is not None:
            next_run.set()
        node.stop()
        rm.permits.put(True)
        thread.join(WAIT)
        assert not thread.is_alive()
        result = outcome.get(timeout=WAIT)
        assert result is True, result


def reattached(client, monkeypatch):
    event = threading.Event()
    take = client._take_attach

    def observed(conn, response, fds, first):
        take(conn, response, fds, first)
        if not first:
            event.set()  # mappings installed AND ServerConn.move_to completed

    monkeypatch.setattr(client, "_take_attach", observed)
    return event


def holders(rm, service, region):
    regions = rm.client.node_holds(service.config.node_id, service.node_epoch)
    return {
        h.holder: h.node_epoch
        for r in regions
        if r.handle == region.handle
        for h in r.holders
    }


def earlier_service(cfg, address, path, pipe, unholding, release):
    """A different live process: its ss@epoch hold cannot be reaped as ours."""
    rm = FileRm(address, path)
    unhold = rm.client.unhold

    def gated(handle, holder, instance):
        if holder.startswith("ss@"):
            unholding.set()
            assert release.wait(60)
        return unhold(handle, holder, instance)

    rm.unhold = gated
    service = StorageService(
        cfg, region_source=rm, cxl_renew_s=0.1, rpc_timeout_s=1, evict_interval_s=60
    )
    try:
        service.start()
        pool = service.tier("cxl").pools[0]
        _, location = publish(service, pool.pool_id, "ready", b"old-ready")
        pipe.send((service.node_epoch, pool, location))
        assert pipe.recv() == "stop"
        service.stop()
        pipe.send("stopped")
    finally:
        release.set()
        service.stop()
        rm.client.close()
        pipe.close()


@pytest.mark.parametrize("first_release", ["worker", "ss"])
def test_real_rm_quarantine_waits_for_both_earlier_holds(
    real_rm, metadata, workdir, monkeypatch, first_release
):
    ms, url = metadata()
    address = real_rm.client._address
    cfg = config(url, f"{workdir}/node.sock", address, dram=True)
    ctx = multiprocessing.get_context("spawn")
    parent, child = ctx.Pipe()
    unholding, release = ctx.Event(), ctx.Event()
    # pytest's importlib mode creates a tests namespace in-process. Recreate
    # its search path in the spawned interpreter (some venvs install tests/).
    bootstrap = (
        "import tests\n"
        "tests.__path__ = [test_root]\n"
        "from tests.integration.test_ms_s5c_run_safety import earlier_service\n"
        "earlier_service(*fixture_args)\n"
    )
    proc = ctx.Process(
        target=exec,
        args=(
            bootstrap,
            {
                "test_root": str(Path(__file__).resolve().parents[1]),
                "fixture_args": (cfg, address, real_rm.path, child, unholding, release),
            },
        ),
    )
    proc.start()
    child.close()
    try:
        assert parent.poll(WAIT)
        epoch, pool, location = parent.recv()
        with worker(cfg.socket) as h:
            client = h._cpu
            read = h.retrieve("ready")
            assert read is not None and bytes(read.view) == b"old-ready"
            old_map = client.pools.get(pool.pool_id)
            attached = reattached(client, monkeypatch)
            parent.send("stop")
            assert unholding.wait(WAIT)  # old service unmapped, unhold not sent yet
            real_rm.polls = queue.Queue()
            watch = CxlTier.watch_holds
            cleared = threading.Event()

            def watch_now(self, callback, interval_s=1):
                def clear(pool_id):
                    callback(pool_id)
                    real_rm.polls.put("cleared")
                    cleared.set()

                return watch(self, clear, interval_s=0)

            monkeypatch.setattr(CxlTier, "watch_holds", watch_now)
            with running_node(cfg, real_rm) as (node, second, _):
                assert attached.wait(WAIT)
                # Earlier worker pins are released in bounded maintenance steps.
                # Run that step synchronously instead of racing its timer.
                ms.maintain(give_up=False)
                assert real_rm.polls.get(timeout=WAIT) is True
                old_worker = run_holder(client.identity.incarnation, epoch)
                old_ss = f"ss@{epoch}"
                new_worker = run_holder(client.identity.incarnation, second.node_epoch)
                names = holders(real_rm, second, pool.region)
                assert names == {
                    old_worker: epoch,
                    old_ss: epoch,
                    new_worker: second.node_epoch,
                    second.own_holder: second.node_epoch,
                }
                assert second.tier("cxl").earlier_holds() == {
                    pool.pool_id: {old_worker, old_ss}
                }
                assert second.pools.pool_quarantined(pool.pool_id)
                assert second.pools.bytes_in(PageState.READY, pool.pool_id) == PAGE
                assert not second.pools.creditable(pool.pool_id)
                # READY remains readable via a real LOOKUP while free space is fenced.
                ready = h.retrieve("ready")
                assert ready is not None and bytes(ready.view) == b"old-ready"
                h.release_retrieved([ready])
                assert client.server_conn._flush_unpins()
                ((ref, key, loc),) = second.pools.ready_pages()
                assert loc == location
                second.manager.retire([ref], [KeyLocation(key, loc)])
                assert second.manager.retire_results["RETIRED"] == 1, (
                    second.manager.retire_results,
                    list(ms.pins._pins.values()),
                    second.manager.ended,
                    second.server.epoch_lost,
                )
                assert h.retrieve("ready") is None
                assert (
                    second.pools.bytes_in(PageState.QUARANTINED, pool.pool_id) == ALIGN
                )

                def drop(which):
                    if which == "worker":
                        h.release_retrieved([read])
                        assert client.pools.flush(WAIT)
                        assert pool_maps._HOLDS.unholds.flush(WAIT)
                        assert old_map.mapping._pool._closed
                    else:
                        release.set()
                        assert parent.poll(WAIT) and parent.recv() == "stopped"

                drop(first_release)
                real_rm.permits.put(True)
                # A new poll fences the previous snapshot; an early clear fails
                # here immediately instead of merely timing out on a dead watcher.
                assert real_rm.polls.get(timeout=WAIT) is True
                remaining = old_ss if first_release == "worker" else old_worker
                assert second.tier("cxl").earlier_holds() == {pool.pool_id: {remaining}}
                assert not cleared.is_set()
                assert second.pools.pool_quarantined(pool.pool_id)
                assert second.pools.bytes_in(PageState.FREE, pool.pool_id) == 0
                assert second.manager._reserve_below(PoolKind.CXL, 1) == []
                drop("ss" if first_release == "worker" else "worker")
                real_rm.permits.put(True)
                assert cleared.wait(WAIT)
                assert not second.pools.pool_quarantined(pool.pool_id)
                assert second.pools.bytes_in(PageState.FREE, pool.pool_id) == ALIGN
                assert second.pools.creditable(pool.pool_id)
                assert holders(real_rm, second, pool.region) == {
                    new_worker: second.node_epoch,
                    second.own_holder: second.node_epoch,
                }
                assert real_rm.calls.count("node_claim") == 1
                assert (
                    "node_alloc" not in real_rm.calls
                    and "node_release" not in real_rm.calls
                )
                assert node.runs == 1
                second.pools.check()
    finally:
        release.set()
        if proc.is_alive():
            proc.join(WAIT)
        if proc.is_alive():
            proc.terminate()
            proc.join(WAIT)
        parent.close()
    assert proc.exitcode == 0


def test_delayed_real_lookup_cannot_read_a_reused_region_mapping(
    real_rm, metadata, workdir, monkeypatch
):
    _, old_url = metadata()
    cfg = config(
        old_url, f"{workdir}/node.sock", real_rm.client._address, regions=2, dram=True
    )
    first = StorageService(
        cfg,
        region_source=real_rm,
        cxl_renew_s=0.1,
        rpc_timeout_s=1,
        evict_interval_s=60,
    )
    first.start()
    entered, deliver = threading.Event(), threading.Event()
    result, answers = queue.Queue(), []
    thread = None
    try:
        a, b = first.tier("cxl").pools
        publish(first, a.pool_id, "held", b"kept-old")
        _, old_location = publish(first, b.pool_id, "lookup", b"old-data")
        with worker(cfg.socket) as h:
            client = h._cpu
            held = h.retrieve("held")
            assert held is not None and bytes(held.view) == b"kept-old"
            old_map = client.pools.get(a.pool_id)
            attached = reattached(client, monkeypatch)
            disconnected = threading.Event()
            on_lost = client._storage._on_lost

            def lost(conn):
                on_lost(conn)
                disconnected.set()

            monkeypatch.setattr(client._storage, "_on_lost", lost)
            rpc = client.server_conn._rpc
            call = rpc.call

            def delayed(code, request, *args, **kwargs):
                response = call(code, request, *args, **kwargs)
                if code == MessageType.LOOKUP_AND_PIN and request.items == [
                    Key("ns", "lookup")
                ]:
                    assert response.items[0].result == ItemResult.HIT
                    answers.append((request, response))
                    entered.set()
                    assert deliver.wait(WAIT)
                return response

            monkeypatch.setattr(rpc, "call", delayed)

            def retrieve():
                try:
                    result.put(h.retrieve("lookup"))
                except BaseException as exc:
                    result.put(exc)

            thread = threading.Thread(target=retrieve, daemon=True)
            thread.start()
            assert entered.wait(WAIT)  # a real pinned answer has crossed the wire
            first.stop()
            assert disconnected.wait(WAIT)
            assert client.pools.flush(WAIT) and pool_maps._HOLDS.unholds.flush(WAIT)
            metadata.stop(old_url)
            _, new_url = metadata(port=int(old_url.rsplit(":", 1)[1]))
            assert new_url == old_url  # actual server restart at the same address
            with running_node(cfg, real_rm) as (_, second, _):
                assert attached.wait(WAIT)
                assert client.pools.flush(WAIT) and pool_maps._HOLDS.unholds.flush(WAIT)
                assert second.tier("cxl").pools[1].region.handle == b.region.handle
                assert not second.pools.pool_quarantined(b.pool_id)
                (unused,) = second.pools.reserve_transfer(
                    second.tier("dram").pool.pool_id, 1
                )
                second.pools.transfer_failed(unused)
                _, reused = publish(second, b.pool_id, "different", b"new-data")
                assert reused.pool_id == old_location.pool_id
                assert reused.offset == old_location.offset
                # Both metadata services start epoch_seq at 1; use the same allocation
                # counter too, so generation cannot accidentally hide a bad run check.
                assert generation_epoch_seq(reused.generation) == 1
                assert generation_epoch_seq(old_location.generation) == 1
                assert reused.generation == old_location.generation
                assert bytes(held.view) == b"kept-old"
                old_holder = run_holder(client.identity.incarnation, first.node_epoch)
                new_holder = run_holder(client.identity.incarnation, second.node_epoch)
                assert set(holders(real_rm, second, a.region)) == {
                    old_holder,
                    new_holder,
                    second.own_holder,
                }
                assert old_holder not in holders(real_rm, second, b.region)
                mapping = client.pools.get(b.pool_id).mapping
                reads = []
                view = mapping.view

                def observed(offset, length):
                    reads.append((offset, length))
                    return view(offset, length)

                monkeypatch.setattr(mapping, "view", observed)
                unpinned = []
                unpin = client.server_conn.unpin

                def observed_unpin(pins):
                    unpinned.extend(pins)
                    unpin(pins)

                monkeypatch.setattr(client.server_conn, "unpin", observed_unpin)
                deliver.set()
                thread.join(WAIT)
                assert not thread.is_alive()
                assert result.get(timeout=WAIT) is None
                assert reads == [], "old LOOKUP touched bytes in the new run's mapping"
                assert answers[0][1].items[0].pin_id in unpinned
                assert bytes(held.view) == b"kept-old"
                assert old_holder in holders(real_rm, second, a.region)
                h.release_retrieved([held])
                assert client.pools.flush(WAIT) and pool_maps._HOLDS.unholds.flush(WAIT)
                assert old_map.mapping._pool._closed
                assert holders(real_rm, second, a.region) == {
                    new_holder: second.node_epoch,
                    second.own_holder: second.node_epoch,
                }
                fresh = h.retrieve("different")
                assert fresh is not None and bytes(fresh.view) == b"new-data"
                h.release_retrieved([fresh])
    finally:
        deliver.set()
        if thread is not None:
            thread.join(WAIT)
        first.stop()


def test_unknown_finalize_next_node_run_adopts_copies_and_drops_finalizers(
    real_rm, metadata, workdir, monkeypatch
):
    ms, url = metadata()
    cfg = config(url, f"{workdir}/node.sock", real_rm.client._address, dram=True)
    allow_next = threading.Event()
    with running_node(cfg, real_rm, next_run=allow_next) as (node, first, started):
        ref, source = publish(
            first, first.tier("dram").pool.pool_id, "copied", b"durable-copy"
        )
        call = first.server._rpc.call
        unknown = []

        def past_replay_window(code, request, *args, **kwargs):
            if code == MessageType.FINALIZE_MOVE:
                # A real higher-sequence ACK prunes the original operation's
                # replay window BEFORE it arrives, leaving its finalizer live.
                fence = RetireRequest(
                    session=request.session,
                    server_epoch=request.server_epoch,
                    op=PLACEHOLDER_OP,
                    items=[],
                )
                fence = dataclasses.replace(
                    fence,
                    op=OpRef(
                        origin=request.op.origin,
                        seq=request.op.seq + 100,
                        digest=op_digest(fence),
                        ack=request.op.seq,
                    ),
                )
                assert call(MessageType.RETIRE, fence).status == Status.OK
                response = call(code, request, *args, **kwargs)
                assert [item.result for item in response.items] == [ItemResult.STALE]
                unknown.append(request)
                return response
            return call(code, request, *args, **kwargs)

        monkeypatch.setattr(first.server._rpc, "call", past_replay_window)
        first.manager._demote(
            PoolKind.DRAM,
            [first.tier("dram").pool.pool_id],
            (PoolKind.CXL,),
            [ref],
            [KeyLocation(Key("ns", "copied"), source)],
        )
        assert first.wait_finished(WAIT)
        assert unknown and first.new_run and "FINALIZE_MOVE" in first.ended
        (move,) = unknown[0].items
        with ms.lock:
            pin = ms.pins.get(move.finalizer_id)
            assert pin is not None and pin.holder_kind == HolderKind.FINALIZER
            assert {entry.location for entry in ms.catalog.locations(move.key)} == {
                source,
                move.dest,
            }
        allow_next.set()
        second = started.get(timeout=WAIT)
        assert node.runs == 2 and second.node_epoch != first.node_epoch
        assert move.dest.pool_id in second.adopted
        ((adopted_ref, key, location),) = second.pools.ready_pages()
        assert key == move.key and location == move.dest
        assert second.pools.page(adopted_ref).state == PageState.READY
        ms.maintain(give_up=False)  # finish bounded removal of the old DRAM pool
        with ms.lock:
            assert ms.pins.get(move.finalizer_id) is None
            assert not ms.pins.held_by(first.server.session)
            assert [entry.location for entry in ms.catalog.locations(move.key)] == [
                move.dest
            ]
        with worker(cfg.socket) as h:
            read = h.retrieve("copied")
            assert read is not None and bytes(read.view) == b"durable-copy"
            h.release_retrieved([read])
            assert h._cpu.server_conn._flush_unpins()
            second.manager.retire([adopted_ref], [KeyLocation(key, location)])
            assert second.manager.retire_results["RETIRED"] == 1
        second.pools.check()


def test_real_rm_claim_during_lease_end_exits_lost(
    real_rm, metadata, workdir, monkeypatch
):
    _, url = metadata()
    cfg = config(url, f"{workdir}/node.sock", real_rm.client._address)
    ending, finish, done = threading.Event(), threading.Event(), threading.Event()
    made, faults = [], queue.Queue()
    other = MaruShmClient(address=real_rm.client._address)
    real_node = Node
    rm_claim = real_rm.node_claim
    claims = []

    def claim_once(*args):
        claims.append(args)
        assert len(claims) == 1, "the losing Node tried to claim the competitor's run"
        return rm_claim(*args)

    monkeypatch.setattr(real_rm, "node_claim", claim_once)

    class EndingService(StorageService):
        def _start(self):
            super()._start()
            # End the actual RM lease without relying on its wall-clock reaper.
            # Renew now gets the same NODE_LEASE_EXPIRED wire response as expiry.
            assert real_rm.node_release(cfg.node_id, self.node_epoch) == 1
            with pytest.raises(NodeLeaseExpiredError):
                real_rm.node_renew(cfg.node_id, self.node_epoch)

        def _teardown(self, mode):
            if self.ended is not None:
                ending.set()
                assert finish.wait(WAIT)
            super()._teardown(mode)

    def make_node(c):
        node = real_node(
            c,
            lambda config: EndingService(
                config,
                region_source=real_rm,
                cxl_renew_s=0.1,
                rpc_timeout_s=1,
                evict_interval_s=60,
            ),
            pause_s=0,
        )
        made.append(node)
        return node

    monkeypatch.setattr(service_module, "Node", make_node)

    def claim():
        try:
            assert ending.wait(WAIT)
            node = made[0]
            assert "lease ended" in node.service.ended
            other.node_claim(cfg.node_id, "competitor")
            # A delayed in-flight renewal during teardown discovers the takeover.
            assert "another run claimed" in node.service.tier("cxl").renew_once()
        except BaseException as exc:
            faults.put(exc)
        finally:
            finish.set()
            done.set()

    thread = threading.Thread(target=claim, daemon=True)
    thread.start()
    try:
        assert serve(cfg) == EXIT_LOST
        assert done.wait(WAIT)
        assert faults.empty(), list(faults.queue)
        node = made[0]
        assert node.runs == 1 and node.service.tier("cxl").claimed_elsewhere
        assert node.failed is None and node.service.wait_finished(0)
        assert real_rm.calls.count("node_claim") == 1  # never takes it back
        assert other.node_renew(cfg.node_id, "competitor") > 0
        with pytest.raises(StaleNodeEpochError):
            real_rm.node_renew(cfg.node_id, node.service.node_epoch)
    finally:
        finish.set()
        if made:
            made[0].stop()
        thread.join(WAIT)
        other.close()
