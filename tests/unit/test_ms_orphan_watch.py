# SPDX-License-Identifier: Apache-2.0
"""Orphan monitoring preserves pins without process-exit evidence."""

import json
import select
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from maru_common.node_protocol import process_identity
from maru_common.storage_types import ItemResult, Key, KeyLocation, OrphanWorker
from maru_storage_service.gpu.orphans import OrphanWatch
from maru_storage_service.gpu.worker_registry import process_key
from maru_storage_service.server_client import ServerClient
from tests.unit.ms_node_helpers import (
    FakeRm,
    MetadataServer,
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
    workers = SimpleNamespace(
        watch_process=lambda *args: evidence, on_process_exit=lambda key: set()
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


def test_local_cxl_orphan_pin_is_kept_until_the_old_worker_exits():
    directory = short_tmpdir()
    catalog = MetadataServer()
    rm = FakeRm()
    config = node_config(catalog.address, f"{directory}/node.sock", cxl_pages=16)
    nodes = []
    child = None
    script = """
import json, sys
sys.path.insert(0, sys.argv[2])
from ms_node_helpers import RawWorker
from maru_common.storage_types import Key
from maru_handler.client.server_conn import ServerConn, WorkerIdentity
w = RawWorker(sys.argv[1])
a = w.attach()
assert a.status.value == "OK"
s = ServerConn(a.server, WorkerIdentity("raw", 0, 0, w.incarnation, "ns"), node_id=a.node_id, node_epoch=a.node_epoch, pid=a.pid, pid_start=a.pid_start, timeout_s=1)
s.open()
hit = s.lookup_and_pin([Key("ns", "kv")])[0][0]
assert hit.result.value == "HIT"
print(json.dumps({"pin": hit.pin_id, "incarnation": w.incarnation}), flush=True)
sys.stdin.read()
"""
    try:
        first = start_node(config, region_source=rm)
        nodes.append(first)
        assert publish(first, "kv").result == ItemResult.ADDED
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                config.socket,
                str(Path(__file__).resolve().parent),
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
        assert service.pins.get(info["pin"]) is not None
        location = service.catalog.locations(Key("ns", "kv"))[0].location
        item = KeyLocation(Key("ns", "kv"), location)
        assert second.server.retire([item]).items[0].result == ItemResult.KEPT
        child.kill()
        child.wait(5)
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
