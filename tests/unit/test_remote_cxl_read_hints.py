# SPDX-License-Identifier: Apache-2.0
"""Read hints: a scheduler tells the access policy which bytes a load will read.

The directory, the remote CXL agent and both clients (a worker and its
metadata-only scheduler) are real; a fake policy and a fake extension stand in
for a device package.
"""

import threading
import time

import pytest

import maru_handler.storage.remote_cxl.client as client_module
import maru_handler.storage.remote_cxl.hints as hints_module
from maru_remote.region_map import read_handle
from maru_remote_cxl_agent.service import MapDenied, NotMapped
from tests.unit.test_remote_cxl_access_policy import FakeExtension, FakePolicy
from tests.unit.test_remote_cxl_storage import PAGE, World, put, read


class HintingPolicy(FakePolicy):
    """A fake policy that also takes read hints."""

    def __init__(self):
        super().__init__()
        self.hints = []
        self.ends = []

    def read_hint(self, channel, request_id, ranges):
        reply = channel.request("ahead", ranges, request=request_id)
        self.hints.append((request_id, [r.location.offset for r in ranges], reply))

    def read_hint_end(self, channel, request_id):
        channel.request("forget", request=request_id)
        self.ends.append(request_id)


class RecordingExtension(FakeExtension):
    def request(self, conn, message, ranges):
        self.messages = [*getattr(self, "messages", []), (conn, dict(message))]
        return super().request(conn, message, ranges)


def _until(condition, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)


@pytest.fixture
def setup(monkeypatch):
    ext = RecordingExtension()
    w = World(extension=ext)
    policy = HintingPolicy()
    monkeypatch.setattr(client_module, "load_access_policy", lambda n, c: policy)
    yield w, ext, policy
    w.close()


def test_a_scheduler_hints_the_leading_hits_and_the_load_names_its_request(setup):
    w, ext, policy = setup
    worker = w.client("a", access_policy="fake")
    for i in range(3):
        assert put(worker, f"k{i}", bytes([i + 1]) * PAGE)[0]
    scheduler = w.client("a", metadata_only=True, access_policy="fake")
    # Keys are in prompt order: a miss ends what the load can read.
    scheduler.hint_read("r1", ["k0", "k2", "missing", "k1"])
    _until(lambda: policy.hints)
    request_id, offsets, reply = policy.hints[0]
    assert request_id == "r1" and len(offsets) == 2 and reply == {"seen": 2}
    conn, message = ext.messages[-1]
    assert message["hint"] is True and message["write"] is False
    assert message["action"] == "ahead"
    # The scheduler holds no region: the agent took the hint as advice.
    assert conn not in next(iter(w.node.service.holders().values()))

    worker.set_read_request("r1")
    assert read(worker, ["k0", "k2"]) == [b"\x01" * PAGE, b"\x03" * PAGE]
    worker.set_read_request("")
    assert policy.read_requests[-1] == "r1"
    assert read(worker, ["k1"]) == [b"\x02" * PAGE]
    assert policy.read_requests[-1] == ""  # the name was cleared

    scheduler.end_read_hint("r1")
    _until(lambda: policy.ends)
    assert policy.ends == ["r1"]
    assert ext.messages[-1][1]["action"] == "forget"


def test_a_request_is_hinted_once_and_a_miss_hints_nothing(setup):
    w, ext, policy = setup
    worker = w.client("a", access_policy="fake")
    assert put(worker, "k0", b"x" * PAGE)[0]
    scheduler = w.client("a", metadata_only=True, access_policy="fake")
    scheduler.hint_read("r1", ["k0"])
    scheduler.hint_read("r1", ["k0"])
    scheduler.hint_read("r2", ["missing", "k0"])
    scheduler.end_read_hint("r2")  # never reached an agent: nothing to end
    scheduler.end_read_hint("r1")
    _until(lambda: policy.ends)
    assert [h[0] for h in policy.hints] == ["r1"]
    assert policy.ends == ["r1"]


def test_without_a_hint_hook_nothing_runs(monkeypatch):
    w = World(extension=RecordingExtension())
    try:
        policy = FakePolicy()  # no read_hint
        monkeypatch.setattr(client_module, "load_access_policy", lambda n, c: policy)
        scheduler = w.client("a", metadata_only=True, access_policy="fake")
        scheduler.hint_read("r1", ["k0"])
        assert scheduler._hints._thread is None
        plain = w.client("b", metadata_only=True)
        plain.hint_read("r1", ["k0"])  # no policy: ignored
        assert plain._hints is None
    finally:
        w.close()


def test_the_agent_takes_a_hint_only_for_a_mapped_region_and_never_a_write(setup):
    w, ext, policy = setup
    worker = w.client("a", access_policy="fake")
    assert put(worker, "k", b"x" * PAGE)[0]
    service = w.node.service
    region = next(iter(worker.regions.values()))
    stranger = 10_000  # a connection that holds no region

    def message(handle, *, hint=True, write=False):
        return {
            "action": "ahead",
            "hint": hint,
            "write": write,
            "epoch": service.epoch,
            "ranges": [{"handle": handle, "offset": 0, "length": PAGE}],
        }

    assert service.extension_request(stranger, message(read_handle(region.handle)))
    with pytest.raises(NotMapped):
        service.extension_request(
            stranger, message(read_handle(region.handle), hint=False)
        )
    with pytest.raises(MapDenied):
        service.extension_request(stranger, message(region.handle, write=True))
    other = dict(region.handle, region_id=region.handle["region_id"] + 99)
    with pytest.raises(NotMapped):
        service.extension_request(stranger, message(read_handle(other)))


def test_a_probe_says_where_hits_are_only_when_asked(setup):
    w, ext, policy = setup
    worker = w.client("a", access_policy="fake")
    assert put(worker, "k0", b"x" * PAGE)[0]
    plain = w.directory.execute(
        "probe", {"engine_id": "a", "namespace": "ns", "keys": ["k0"]}
    )["result"]
    assert "entries" not in plain
    found = w.directory.execute(
        "probe",
        {
            "engine_id": "a",
            "namespace": "ns",
            "keys": ["k0", "nope"],
            "locations": True,
        },
    )["result"]
    entry, missing = found["entries"]
    assert missing is None and entry["length"] == PAGE
    (handle,) = found["regions"].values()
    region = next(iter(worker.regions.values()))
    assert handle == read_handle(region.handle)  # never the owner's handle


def test_a_hint_needs_a_region_some_connection_holds(setup):
    w, ext, policy = setup
    worker = w.client("a", access_policy="fake")
    assert put(worker, "k", b"x" * PAGE)[0]
    service = w.node.service
    region = next(iter(worker.regions.values()))
    m = service._maps[region.handle["region_id"]]
    message = {
        "action": "ahead",
        "hint": True,
        "write": False,
        "epoch": service.epoch,
        "ranges": [{"handle": read_handle(region.handle), "offset": 0, "length": 1}],
    }
    held = set(m.holders)
    m.holders.clear()  # the last holder left; the sweep has not released it yet
    try:
        with pytest.raises(NotMapped):
            service.extension_request(10_000, message)
    finally:
        m.holders.update(held)


def test_a_policy_without_hint_hooks_reads_with_three_arguments(monkeypatch):
    class ThreeArgumentPolicy(FakePolicy):
        def read(self, channel, ranges, transfer):
            super().read(channel, ranges, transfer)

    w = World(extension=RecordingExtension())
    try:
        policy = ThreeArgumentPolicy()
        monkeypatch.setattr(client_module, "load_access_policy", lambda n, c: policy)
        worker = w.client("a", access_policy="fake")
        assert put(worker, "k", b"\x07" * PAGE)[0]
        worker.set_read_request("r1")
        assert read(worker, ["k"]) == [b"\x07" * PAGE]
        assert not worker.read_hints_enabled
    finally:
        w.close()


class _BlockedPolicy:
    """A hinting policy whose first hint blocks until released."""

    def __init__(self):
        self.release = threading.Event()
        self.hints = []
        self.ends = []

    def read_hint(self, channel, request_id, ranges):
        self.release.wait(5)
        self.hints.append(request_id)

    def read_hint_end(self, channel, request_id):
        self.ends.append(request_id)


def test_ends_are_kept_when_too_many_hints_wait(monkeypatch):
    monkeypatch.setattr(hints_module, "MAX_PENDING_HINTS", 2)
    warnings = []
    monkeypatch.setattr(
        hints_module.logger, "warning", lambda msg, *a: warnings.append(msg % a)
    )
    policy = _BlockedPolicy()
    hints = hints_module.ReadHints(
        policy, lambda keys: [("agent", [])], lambda a, m: {}
    )
    hints.hint("r0", ["k"])  # the thread takes it and blocks
    _until(lambda: hints._pending == 0)
    hints.hint("r1", ["k"])
    hints.hint("r2", ["k"])
    hints.hint("r3", ["k"])  # dropped
    assert len(warnings) == 1 and "read hints dropped" in warnings[0]
    for rid in ("r0", "r1", "r2", "r3"):
        hints.end(rid)
    policy.release.set()
    _until(lambda: len(policy.ends) == 3)
    assert policy.hints == ["r0", "r1", "r2"]
    assert policy.ends == ["r0", "r1", "r2"]  # r3 never reached the policy
    assert hints.close()


def test_hints_after_close_are_ignored():
    policy = _BlockedPolicy()
    policy.release.set()
    hints = hints_module.ReadHints(
        policy, lambda keys: [("agent", [])], lambda a, m: {}
    )
    hints.hint("r1", ["k"])
    _until(lambda: policy.hints)
    assert hints.close()
    hints.hint("r2", ["k"])
    hints.end("r1")
    assert hints._thread is None and policy.hints == ["r1"] and not policy.ends
