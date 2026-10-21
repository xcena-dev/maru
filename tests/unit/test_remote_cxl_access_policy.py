# SPDX-License-Identifier: Apache-2.0
"""Access policies and region extensions: what a device-specific package may
control in the remote CXL path, and what Maru keeps for itself.

A fake policy on the worker and a fake extension on the remote CXL agent stand
in for a device package; the directory, the agent and the client are real.
"""

import pytest

import maru_handler.storage.remote_cxl.client as client_module
from maru_common.config import MaruConfig
from maru_handler.storage.access_policy import (
    AccessPolicy,
    load_access_policy,
)
from maru_remote.region_map import read_handle
from maru_remote_cxl_agent.extension import RegionExtension, load_region_extension
from maru_remote_cxl_agent.service import MapDenied, NotMapped
from tests.unit.test_remote_cxl_storage import PAGE, World, put, read


class FakeExtension:
    """Region extension that records what it sees and can hold regions."""

    def __init__(self):
        self.requests = []
        self.mapped_regions = []
        self.refuse_map = False
        self.hold = False
        self.closed = False

    def validate_access(self, handle, access):
        if self.refuse_map:
            raise ValueError("not this device")

    def mapped(self, handle, access):
        self.mapped_regions.append(handle["region_id"])

    def request(self, conn, message, ranges):
        self.requests.append((message["action"], message["write"], len(ranges)))
        return {"seen": len(ranges)}

    def retire(self, handle):
        return not self.hold

    def close(self):
        self.closed = True


class FakePolicy:
    """Access policy that reads in steps and records its calls."""

    def __init__(self):
        self.step = 1
        self.limit = 8
        self.fail = set()
        self.held = set()
        self.calls = []

    def read(self, channel, ranges, transfer):
        if "read" in self.fail:
            raise RuntimeError("policy refused")
        assert channel.request("plan", ranges)["seen"] == len(ranges)
        if "disorder" in self.fail:
            transfer(1, len(ranges))
            return
        stop = len(ranges) - (1 if "short" in self.fail else 0)
        for i in range(0, stop, self.step):
            transfer(i, min(i + self.step, stop))
            self.calls.append(("transfer", i))

    def write_batch_limit(self, channel):
        return self.limit

    def prepare_write(self, channel, ranges):
        if "prepare" in self.fail:
            raise RuntimeError("no room to prepare")
        channel.request("prepare", ranges, write=True)
        self.calls.append(("prepare", len(ranges)))
        return len(ranges)

    def written(self, channel, token):
        self.calls.append(("written", token))

    def completed_write(self, locations):
        self.calls.append(("late", len(locations)))

    def reusable(self, location):
        return location.offset not in self.held


class _EntryPoint:
    def __init__(self, name, target):
        self.name = name
        self._target = target

    def load(self):
        return self._target


@pytest.fixture
def setup(monkeypatch):
    ext = FakeExtension()
    w = World(extension=ext)
    policy = FakePolicy()
    monkeypatch.setattr(client_module, "load_access_policy", lambda n, c: policy)
    yield w, ext, policy
    ext.hold = False
    w.close()


def test_policy_and_extension_are_loaded_by_name_only():
    policy = FakePolicy()
    found = [
        _EntryPoint("other", lambda c: object()),
        _EntryPoint("fake", lambda c: policy),
    ]
    assert load_access_policy("fake", "", entry_points=found) is policy
    assert isinstance(policy, AccessPolicy)
    with pytest.raises(ValueError, match="no package registers"):
        load_access_policy("missing", "", entry_points=found)
    with pytest.raises(ValueError, match="does not implement"):
        load_access_policy("other", "", entry_points=found)
    ext = FakeExtension()
    eps = [_EntryPoint("fake", lambda epoch, config: ext)]
    assert load_region_extension("fake", "e1", "", entry_points=eps) is ext
    assert isinstance(ext, RegionExtension)
    with pytest.raises(ValueError, match="access_policy"):
        MaruConfig(
            storage_backend="cpu",
            engine_id="e",
            cache_namespace="ns",
            access_policy="fake",
        )


def test_store_and_load_run_through_the_policy_and_its_extension(setup):
    w, ext, policy = setup
    policy.limit = 2
    c = w.client("a", access_policy="fake")
    handles = []
    for i in range(3):
        h = c.alloc(PAGE)
        h.buf[:PAGE] = bytes([i + 1]) * PAGE
        handles.append(h)
    assert c.batch_store(["k0", "k1", "k2"], handles) == [True] * 3
    assert [x for x in policy.calls if x[0] == "prepare"] == [
        ("prepare", 2),
        ("prepare", 1),
    ]
    assert [x for x in policy.calls if x[0] == "written"] == [
        ("written", 2),
        ("written", 1),
    ]
    assert ("prepare", True, 2) in ext.requests
    assert read(c, ["k0", "k1", "k2"]) == [bytes([i + 1]) * PAGE for i in range(3)]
    assert ("plan", False, 3) in ext.requests
    assert [x for x in policy.calls if x[0] == "transfer"] == [
        ("transfer", 0),
        ("transfer", 1),
        ("transfer", 2),
    ]
    assert ext.mapped_regions


@pytest.mark.parametrize("failure", ["read", "disorder", "short"])
def test_a_read_the_policy_does_not_complete_loads_as_a_miss(setup, failure):
    w, ext, policy = setup
    c = w.client("a", access_policy="fake")
    assert put(c, "k", b"x" * PAGE)[0]
    policy.fail.add(failure)
    assert read(c, ["k"]) == [None]
    policy.fail.clear()
    assert read(c, ["k"]) == [b"x" * PAGE]  # the connection and slots are intact


def test_a_write_the_policy_does_not_prepare_stores_nothing(setup):
    w, ext, policy = setup
    c = w.client("a", access_policy="fake")
    policy.fail.add("prepare")
    assert put(c, "k", b"x" * PAGE)[0] is False
    policy.fail.clear()
    assert read(c, ["k"]) == [None]
    assert put(c, "k", b"y" * PAGE)[0]
    assert read(c, ["k"]) == [b"y" * PAGE]


def test_a_page_given_back_waits_until_the_policy_lets_it_go(setup):
    w, ext, policy = setup
    c = w.client("a", access_policy="fake")
    region = next(iter(c.regions.values()))
    before = region.free_count()
    handle = c.alloc(PAGE)
    policy.held.add(handle.location.offset)
    policy.fail.add("prepare")
    handle.buf[:PAGE] = b"x" * PAGE
    assert c.batch_store(["k"], [handle]) == [False]
    assert region.free_count() == before - 1  # given back, not reusable yet
    policy.held.clear()
    assert region.free_count() == before


def test_the_agent_checks_every_range_before_the_extension_sees_it(setup):
    w, ext, policy = setup
    c = w.client("a", access_policy="fake")
    assert put(c, "k", b"x" * PAGE)[0]
    service = w.node.service
    region = next(iter(c.regions.values()))
    rid = region.handle["region_id"]
    conn = next(iter(service.holders()[rid]))

    def message(handle, write=False, offset=0, epoch=None):
        return {
            "action": "probe",
            "write": write,
            "epoch": service.epoch if epoch is None else epoch,
            "ranges": [{"handle": handle, "offset": offset, "length": PAGE}],
        }

    seen = len(ext.requests)
    with pytest.raises(MapDenied):
        service.extension_request(conn, message(region.handle, epoch="old"))
    with pytest.raises(NotMapped):
        service.extension_request(conn + 1000, message(region.handle))
    with pytest.raises(MapDenied):
        service.extension_request(conn, message(read_handle(region.handle), write=True))
    with pytest.raises(ValueError):
        service.extension_request(conn, message(region.handle, offset=region.capacity))
    assert len(ext.requests) == seen  # none of these reached the extension
    assert service.extension_request(conn, message(read_handle(region.handle)))
    assert ext.requests[-1] == ("probe", False, 1)


def test_a_region_stays_until_the_extension_lets_it_go(setup):
    w, ext, policy = setup
    c = w.client("a", access_policy="fake")
    assert put(c, "k", b"x" * PAGE)[0]
    service = w.node.service
    region = next(iter(c.regions.values()))
    rid = region.handle["region_id"]
    conn = next(iter(service.holders()[rid]))
    ext.hold = True
    with pytest.raises(RuntimeError, match="still held"):
        service.release(region.handle)
    assert rid not in service.holders()  # out of use
    with pytest.raises(NotMapped):
        service.map(conn, region.handle)
    service.sweep()
    assert rid in service._maps  # noqa: SLF001 (the mapping is still there)
    ext.hold = False
    service.sweep()
    assert rid not in service._maps  # noqa: SLF001
    service.release(region.handle)  # already released: succeeds


def test_a_region_the_extension_refuses_is_not_mapped(monkeypatch):
    ext = FakeExtension()
    ext.refuse_map = True
    w = World(extension=ext)
    monkeypatch.setattr(client_module, "load_access_policy", lambda n, c: FakePolicy())
    try:
        c = w.client("a", access_policy="fake")
        # The agent refused every region: no page can be taken for a store.
        with pytest.raises(MemoryError, match="No remote CXL region is reachable"):
            c.alloc(PAGE)
        assert not ext.mapped_regions
    finally:
        w.close()


def test_closing_the_agent_closes_the_extension(setup):
    w, ext, policy = setup
    c = w.client("a", access_policy="fake")
    assert put(c, "k", b"x" * PAGE)[0]
    ext.hold = True
    w.node.service.close(extension_drain_s=0.05)
    assert ext.closed
