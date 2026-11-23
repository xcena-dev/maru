# SPDX-License-Identifier: Apache-2.0
"""Orphan monitoring preserves pins without process-exit evidence."""

from types import SimpleNamespace

import pytest

from maru_common.node_protocol import process_identity
from maru_common.storage_types import OrphanWorker
from maru_storage_service.gpu.orphans import OrphanWatch
from maru_storage_service.server_client import ServerClient


@pytest.mark.parametrize("evidence", [True, False, None])
def test_orphan_uses_three_way_process_evidence(monkeypatch, evidence):
    boot, pid, start, ns = process_identity()
    reports = []
    watch = OrphanWatch(SimpleNamespace(_queue_exit=reports.append), boot)
    monkeypatch.setattr(watch.watcher, "watch", lambda *args: evidence)
    try:
        watch.start([OrphanWorker("worker", pid, start, boot, ns)])
        assert bool(reports) == (evidence is False)
        if evidence is None:
            assert watch.unknown == {"worker"}
            monkeypatch.setattr(watch.watcher, "watch", lambda *args: False)
            watch.retry()
            assert len(reports) == 1
        if evidence is True:
            watch._exited("worker")
            watch._exited("worker")
            assert len(reports) == 1
    finally:
        watch.stop()


def test_incomparable_namespace_keeps_the_orphan_and_old_boot_proves_exit():
    boot, pid, start, ns = process_identity()
    reports = []
    watch = OrphanWatch(SimpleNamespace(_queue_exit=reports.append), boot)
    try:
        watch.start(
            [
                OrphanWorker("namespace", pid, start, boot, ns + 1),
                OrphanWorker("old-boot", pid, start, "previous-boot", ns),
                OrphanWorker("unknown-boot", pid, start, "", ns),
            ]
        )
        assert [r.incarnation for r in reports] == ["old-boot"]
        assert watch.unwatchable == {"namespace", "unknown-boot"}
        assert len(watch.pending) == 2
    finally:
        watch.stop()


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
