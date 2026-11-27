# SPDX-License-Identifier: Apache-2.0
"""Orphan monitoring preserves pins without process-exit evidence."""

import json
import select
import subprocess
import sys
import threading
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from maru_common.node_protocol import process_identity
from maru_common.protocol import MessageType
from maru_common.storage_types import (
    ItemResult,
    KeptCause,
    Key,
    KeyLocation,
    OrphanWorker,
)
from maru_storage_service.gpu.orphans import OrphanWatch
from maru_storage_service.gpu.worker_registry import process_key
from maru_storage_service.server_client import ServerClient
from tests.unit.ms_node_helpers import (
    Agent,
    FakeRm,
    MetadataServer,
    exit_before_orphan_drain,
    node_config,
    publish,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)


@pytest.mark.parametrize("evidence", [True, False, None])
def test_orphan_uses_three_way_process_evidence(evidence):
    boot, pid, start, ns = process_identity()
    reports = []
    drains = []
    workers = SimpleNamespace(
        watch_process=lambda *args: evidence,
        on_process_exit=lambda key: set(),
        after_gpu_drain=lambda fn, orphan: drains.append((fn, orphan)),
    )
    watch = OrphanWatch(
        SimpleNamespace(_queue_exit=reports.append, workers=workers), boot
    )
    watch.start([OrphanWorker(inc, pid, start, boot, ns) for inc in ("a", "b")])
    if evidence is None:
        assert watch.unknown == {"a", "b"}
        assert not reports
        workers.watch_process = lambda *args: False
        watch.retry()
    elif evidence is True:
        assert not reports
        watch.on_process_exit(process_key(pid, start + 1))
        assert not reports
        watch.on_process_exit(process_key(pid, start))
        watch.on_process_exit(process_key(pid, start))
    assert not reports
    for fn, orphan in drains:
        fn(orphan)
    assert sorted(r.incarnation for r in reports) == ["a", "b"]


def test_incomparable_namespace_keeps_the_orphan_and_old_boot_proves_exit():
    boot, pid, start, ns = process_identity()
    reports = []
    workers = SimpleNamespace(on_process_exit=lambda key: set())
    watch = OrphanWatch(
        SimpleNamespace(_queue_exit=reports.append, workers=workers), boot
    )
    watch.start(
        [
            OrphanWorker("namespace", pid, start, boot, ns + 1),
            OrphanWorker("old-boot", pid, start, "previous-boot", ns),
            OrphanWorker("unknown-boot", pid, start, "", ns),
        ]
    )
    watch.on_process_exit(process_key(pid, start))
    assert [r.incarnation for r in reports] == ["old-boot"]
    assert watch.unwatchable == {"namespace", "unknown-boot"}
    assert len(watch.pending) == 2


def test_node_open_orphans_are_read_through_every_cursor():
    client = ServerClient.__new__(ServerClient)
    requests = []

    def call(code, request):
        requests.append(request)
        return SimpleNamespace(status="OK", orphans=["second"], next_cursor=None)

    client._rpc = SimpleNamespace(call=call)
    client._ids = lambda: ("session", "epoch")
    opened = SimpleNamespace(orphans=["first"], next_cursor="cursor")
    assert client.orphans(opened) == ["first", "second"]
    assert requests[0].cursor == "cursor"
    assert requests[0].session == "session"


@pytest.mark.parametrize("attach_probe", [False, True])
def test_local_cxl_orphan_pin_is_kept_through_drain(
    attach_probe, monkeypatch, tmp_path
):
    directory = short_tmpdir()
    catalog = MetadataServer()
    rm = FakeRm()
    config = node_config(
        catalog.address, f"{directory}/node.sock", cxl_pages=16, region_pages=16
    )
    nodes = []
    child = None
    script = """
import json, sys
sys.path.insert(0, sys.argv[2])
from ms_node_helpers import PAGE, RawWorker
from ms_worker_agent import _map_cxl_from
from maru_common.config import MaruConfig
from maru_common.node_protocol import DetachRequest, NodeMessageType
from maru_common.storage_types import SessionRole
from maru_handler import MaruHandler
_map_cxl_from(json.loads(sys.argv[3]))
handler = MaruHandler(MaruConfig(
    maru_node=sys.argv[1], engine_id="raw", cache_namespace="ns",
    chunk_size_bytes=PAGE, pin_pools=False, load_only=True, auto_connect=False,
))
handler.connect()
hit = handler.retrieve("kv")
assert hit is not None
incarnation = handler._cpu.identity.incarnation
handler.stop()
print(json.dumps({"pin": hit.pin_id, "incarnation": incarnation}), flush=True)
if sys.stdin.readline().strip() == "probe":
    probe = RawWorker(sys.argv[1], incarnation)
    assert probe.attach(role=SessionRole.PROBE).status.value == "OK"
    idle = RawWorker(sys.argv[1])
    assert idle.attach().status.value == "OK"
    assert idle.request(NodeMessageType.DETACH, DetachRequest()).status.value == "OK"
    print(idle.incarnation, flush=True)
sys.stdin.read()
"""
    try:
        first = start_node(config, region_source=rm)
        nodes.append(first)
        assert publish(first, "kv").result == ItemResult.ADDED
        (region,) = rm.regions[config.node_id]
        backing = tmp_path / "region"
        with backing.open("wb") as f:
            f.truncate(region.handle.length)
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                config.socket,
                str(Path(__file__).resolve().parent),
                json.dumps({region.handle.region_id: str(backing)}),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert select.select([child.stdout], [], [], 10)[0]
        info = json.loads(child.stdout.readline())
        service = catalog.server.ms_service
        assert service.pins.get(info["pin"]) is not None
        first.stop()
        second = start_node(config, region_source=rm)
        nodes.append(second)
        assert info["incarnation"] in second.role.orphans.pending
        assert not second.role.orphans.unwatchable
        if attach_probe:
            child.stdin.write("probe\n")
            child.stdin.flush()
            assert select.select([child.stdout], [], [], 5)[0]
            idle = child.stdout.readline().strip()
            assert idle
            wait_until(lambda: second.store.workers.get(idle) is None)
            assert second.store.workers.probes() == [info["incarnation"]]
        assert service.pins.get(info["pin"]) is not None
        location = service.catalog.locations(Key("ns", "kv"))[0].location
        item = KeyLocation(Key("ns", "kv"), location)
        assert second.server.retire([item]).items[0].result == ItemResult.KEPT
        resume = exit_before_orphan_drain(
            second, child, info["incarnation"], monkeypatch
        )
        assert service.pins.get(info["pin"]) is not None
        assert second.server.retire([item]).items[0].result == ItemResult.KEPT
        resume()
        wait_until(lambda: service.pins.get(info["pin"]) is None)
        assert second.server.retire([item]).items[0].result == ItemResult.RETIRED
    finally:
        if child is not None:
            if child.poll() is None:
                child.kill()
                child.wait(5)
            child.stdin.close()
            child.stdout.close()
        for node in reversed(nodes):
            node.stop()
        catalog.close()
        remove_tree(directory)


@pytest.mark.parametrize("tier", ["local_cxl", "dram"])
def test_reattached_local_worker_keeps_pins_through_copy_and_drain(
    monkeypatch, tmp_path, tier
):
    with ExitStack() as stack:
        directory = short_tmpdir()
        stack.callback(remove_tree, directory)
        catalog = MetadataServer()
        stack.callback(catalog.close)
        rm = FakeRm()
        config = node_config(
            catalog.address,
            f"{directory}/node.sock",
            cxl_pages=16 if tier == "local_cxl" else None,
            region_pages=16,
        )
        first = start_node(config, region_source=rm)
        stack.callback(first.stop)
        paths = {}
        if tier == "local_cxl":
            (region,) = rm.regions[config.node_id]
            backing = tmp_path / "region"
            with backing.open("wb") as f:
                f.truncate(region.handle.length)
            paths[region.handle.region_id] = str(backing)
        child = Agent(config.socket, read_only=True, cxl_paths=paths)
        stack.callback(child.stop)
        child.connect()
        assert publish(first, "old").result == ItemResult.ADDED
        old_bytes = child.call("retrieve", key="old")
        assert old_bytes is not None
        service = catalog.server.ms_service
        old_key = Key("ns", "old")
        old = KeyLocation(old_key, service.catalog.locations(old_key)[0].location)
        assert service.pins.count(old.key, old.location) == 1

        first.stop()
        second = start_node(config, region_source=rm)
        stack.callback(second.stop)
        wait_until(lambda: child.call("state")["node_epoch"] == second.node_epoch)
        orphans = second.role.orphans
        assert second.store.workers.get(child.incarnation) is not None
        assert publish(second, "current").result == ItemResult.ADDED
        probe = Mock(wraps=service.probe)
        monkeypatch.setitem(catalog.rpc._ms_handlers, MessageType.PROBE, probe)
        if tier == "local_cxl":
            assert child.incarnation in orphans.pending
            service.maintain()
            retired = second.server.retire([old]).items[0]
            assert (retired.result, retired.cause) == (
                ItemResult.KEPT,
                KeptCause.PINNED,
            )
            assert service.pins.count(old.key, old.location) == 1
            assert child.call("retrieve", key="current", timeout=2) is None
            assert child.call("exists", keys=["current"]) == [False]
            probe.assert_not_called()
            assert child.call("state")["session_node_epoch"] == first.node_epoch
            assert child.call("held", key="old") == old_bytes
            child.call("release", key="old")
        else:
            # The new run has its own DRAM memfd.
            wait_until(lambda: service.pins.count(old.key, old.location) == 0)
        wait_until(
            lambda: child.call("state")["session_node_epoch"] == second.node_epoch
        )
        if tier == "local_cxl":
            wait_until(lambda: service.pins.count(old.key, old.location) == 0)
            assert second.server.retire([old]).items[0].result == ItemResult.RETIRED
        assert child.call("retrieve", key="current") is not None
        assert child.call("exists", keys=["current"]) == [True]
        probe.assert_called_once()
        if tier == "dram":
            assert child.call("held", key="old") == old_bytes
            child.call("release", key="old")
        key = Key("ns", "current")
        location = service.catalog.locations(key)[0].location
        assert service.pins.count(key, location) == 1

        draining, resume = threading.Event(), threading.Event()
        workers = second.store.workers
        grace_over = workers._grace_over

        def hold_drain(*args):
            draining.set()
            assert resume.wait(10)
            grace_over(*args)

        monkeypatch.setattr(workers, "_grace_over", hold_drain)
        exits = []
        worker_exit = service.registry.worker_exit

        def record(session, refs):
            result = worker_exit(session, refs)
            exits.extend((ref.incarnation, ref.engine_id) for ref in refs)
            return result

        monkeypatch.setattr(service.registry, "worker_exit", record)
        item = KeyLocation(key, location)
        child.kill()
        try:
            assert draining.wait(5)
            wait_until(lambda: child.incarnation not in orphans.pending)
            assert not exits
            assert service.pins.count(key, location) == 1
            assert second.server.retire([item]).items[0].result == ItemResult.KEPT
        finally:
            resume.set()
        wait_until(lambda: (child.incarnation, "eng") in exits)
        assert exits == [(child.incarnation, "eng")]
        assert service.pins.count(key, location) == 0
        assert second.server.retire([item]).items[0].result == ItemResult.RETIRED
