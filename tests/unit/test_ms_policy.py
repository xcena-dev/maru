# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Preservation from the MaruServer policy file: load, reload, bad files."""

import json
import os

import pytest

from maru_common.storage_types import KeptCause, Status
from maru_server import ms_service
from maru_server.ms_service import PolicyError, load_policy
from tests.unit.ms_fakes import FakeClock, FakeNode, Wire, key, make_service


def write(path, data, bump=1):
    path.write_text(data if isinstance(data, str) else json.dumps(data))
    stat = os.stat(path)
    # a new mtime even within one clock tick
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + bump * 1_000_000))


def preserve(*names):
    return {"preserve": [{"namespace": "ns", "key": n} for n in names]}


def test_load_policy(tmp_path):
    path = tmp_path / "policy.json"
    write(path, preserve("a", "b"))
    assert load_policy(str(path)) == {key("a"), key("b")}
    write(path, {})
    assert load_policy(str(path)) == frozenset()


@pytest.mark.parametrize(
    "content",
    [
        "not json",
        "[]",
        '{"preserve": [], "other": 1}',
        '{"preserve": {}}',
        '{"preserve": [{"namespace": "ns"}]}',
        '{"preserve": [{"namespace": "ns", "key": 1}]}',
        '{"preserve": [{"namespace": "ns", "key": "k", "x": 1}]}',
        '{"preserve": [], "preserve": []}',
    ],
)
def test_bad_policy_files(tmp_path, content):
    path = tmp_path / "policy.json"
    path.write_text(content)
    with pytest.raises(PolicyError):
        load_policy(str(path))
    with pytest.raises(PolicyError):
        make_service(policy_file=str(path))


def test_missing_policy_file_refuses_to_start(tmp_path):
    with pytest.raises(OSError):
        make_service(policy_file=str(tmp_path / "absent.json"))


def test_preserved_keys_are_kept_and_reloads_bump_the_version(tmp_path, monkeypatch):
    path = tmp_path / "policy.json"
    write(path, preserve("k"))
    service = make_service(FakeClock(), policy_file=str(path))
    wire = Wire(service)
    node = FakeNode(wire, "n1")
    node.open()
    k = key("k")
    loc = node.loc(node.dram(), 1)
    node.commit((k, loc))
    version = node.heartbeat().policy_version
    assert node.retire((k, loc)).items[0].cause == KeptCause.PINNED
    # unchanged file: nothing happens
    service.check_policy()
    assert node.heartbeat().policy_version == version
    # an invalid edit keeps the policy in force, and is logged
    errors = []
    monkeypatch.setattr(
        ms_service.logger, "error", lambda msg, *args: errors.append(msg % args)
    )
    write(path, "{broken", bump=2)
    service.check_policy()
    assert len(errors) == 1 and "keeping the policy" in errors[0]
    assert node.retire((k, loc)).items[0].cause == KeptCause.PINNED
    assert node.heartbeat().policy_version == version
    # a valid edit applies and bumps the version
    write(path, preserve(), bump=3)
    service.check_policy()
    assert node.heartbeat().policy_version == version + 1
    assert node.retire((k, loc)).items[0].result == "RETIRED"


def test_a_removed_policy_file_keeps_the_policy(tmp_path):
    path = tmp_path / "policy.json"
    write(path, preserve("k"))
    service = make_service(FakeClock(), policy_file=str(path))
    mtime = path.stat().st_mtime_ns
    path.unlink()
    service.check_policy()
    assert service.catalog.is_preserved(key("k"))
    write(path, preserve())
    # Deletion/recreation within one filesystem clock tick can reuse mtime.
    os.utime(path, ns=(mtime, mtime))
    service.check_policy()
    assert not service.catalog.is_preserved(key("k"))
    assert service.registry.policy_version == 1


def test_no_public_set_preserved():
    service = make_service()
    assert not hasattr(service, "set_preserved")
    assert not hasattr(service, "delete")
    node = FakeNode(Wire(service), "n1")
    assert node.open().status == Status.OK


def test_the_maintenance_thread_reloads_the_file(tmp_path):
    import time

    path = tmp_path / "policy.json"
    write(path, preserve())
    service = make_service(policy_file=str(path), maintenance_interval_s=0.02)
    service.ensure_started()
    try:
        write(path, preserve("k"), bump=5)
        deadline = time.monotonic() + 5
        while (
            not service.catalog.is_preserved(key("k")) and time.monotonic() < deadline
        ):
            time.sleep(0.02)
        assert service.catalog.is_preserved(key("k"))
    finally:
        service.stop()


def test_a_file_caught_mid_write_is_retried(tmp_path, monkeypatch):
    path = tmp_path / "policy.json"
    write(path, preserve())
    service = make_service(FakeClock(), policy_file=str(path))
    errors = []
    monkeypatch.setattr(
        ms_service.logger, "error", lambda msg, *args: errors.append(msg % args)
    )
    write(path, '{"preserve": [', bump=2)  # half written
    mtime = os.stat(path).st_mtime_ns
    service.check_policy()
    service.check_policy()
    assert len(errors) == 1  # logged once, retried each pass
    assert not service.catalog.is_preserved(key("k"))
    # the writer finishes within the same mtime
    path.write_text(json.dumps(preserve("k")))
    os.utime(path, ns=(mtime, mtime))
    service.check_policy()
    assert service.catalog.is_preserved(key("k"))
