# SPDX-License-Identifier: Apache-2.0
"""Region extensions: what out-of-tree code next to the memory may see and hold
in the remote CXL path, while the data path stays Maru's own.

A fake extension on the remote CXL agent stands in for a device package; the
directory, the agent and the client are real.
"""

import time

import pytest

import maru_handler.storage.remote_cxl.datapath as datapath
from maru_remote.region_map import HINT_ACTIONS, RegionMapClient, hint_actions
from maru_remote_cxl_agent.extension import RegionExtension, load_region_extension
from maru_remote_cxl_agent.service import NotMapped
from tests.unit.remote_fakes import shared_key
from tests.unit.test_remote_cxl_storage import PAGE, World, put, read


class FakeExtension:
    """Region extension that records what it sees and can hold regions."""

    def __init__(self):
        self.hints = []
        self.mapped_regions = []
        self.fresh = {}
        self.refuse_map = False
        self.fail_mapped = False
        self.hold = False
        self.closed = False
        self.closed_conns = []

    def validate_access(self, handle, access):
        if self.refuse_map:
            raise ValueError("not this device")

    def mapped(self, handle, access, *, fresh):
        if self.fail_mapped:
            raise RuntimeError("device command outcome unknown")
        self.mapped_regions.append(handle["region_id"])
        self.fresh[handle["region_id"]] = fresh

    def hint(self, conn, message, ranges):
        self.hints.append((conn, dict(message), list(ranges)))

    def retire(self, handle):
        return not self.hold

    def close(self):
        self.closed = True

    def connection_closed(self, conn):
        self.closed_conns.append(conn)


class _EntryPoint:
    def __init__(self, name, target):
        self.name = name
        self._target = target

    def load(self):
        return self._target


@pytest.fixture
def setup():
    ext = FakeExtension()
    w = World(extension=ext)
    yield w, ext
    ext.hold = False
    w.close()


def test_an_extension_is_loaded_by_name_only():
    ext = FakeExtension()
    eps = [
        _EntryPoint("other", lambda epoch, config: object()),
        _EntryPoint("fake", lambda epoch, config: ext),
    ]
    assert load_region_extension("fake", "e1", "", entry_points=eps) is ext
    assert isinstance(ext, RegionExtension)
    with pytest.raises(ValueError, match="no package registers"):
        load_region_extension("missing", "e1", "", entry_points=eps)
    with pytest.raises(ValueError, match="does not implement"):
        load_region_extension("other", "e1", "", entry_points=eps)


def test_the_data_path_is_unchanged_and_the_hello_says_which_hints_it_takes(setup):
    w, ext = setup
    c = w.client("a")
    assert put(c, "k", b"x" * PAGE)[0]
    assert read(c, ["k"]) == [b"x" * PAGE]
    assert ext.mapped_regions and not ext.hints
    advertised = w.node.service.hello()["region_extension"]
    assert hint_actions(advertised) == frozenset(HINT_ACTIONS)
    # An agent without an extension, or of an earlier protocol, takes none.
    assert hint_actions(None) == frozenset()
    assert hint_actions(True) == frozenset()
    assert hint_actions({"version": 0, "actions": list(HINT_ACTIONS)}) == frozenset()


def test_only_the_first_map_of_a_region_its_owner_just_got_is_fresh(setup):
    w, ext = setup
    c = w.client("a")
    (region,) = c.regions.values()
    rid = region.handle["region_id"]
    assert ext.fresh == {rid: True}
    assert put(c, "k", b"x" * PAGE)[0]
    # The agent restarts: the region is mapped again, with its KV in it.
    w.node.restart()
    c._data.forget(region.pool_id)
    assert read(c, ["k"]) == [b"x" * PAGE]
    assert ext.fresh[rid] is False


def test_fresh_is_taken_only_with_the_owner_handle_on_a_first_map(setup):
    w, ext = setup
    c = w.client("a")
    (region,) = c.regions.values()
    rid = region.handle["region_id"]
    service = w.node.service
    conn = next(iter(service.holders()[rid]))
    ext.fresh.clear()
    service.map(conn, region.handle, fresh=True)  # mapped already: shared
    assert ext.fresh == {}


def test_a_region_stays_until_the_extension_lets_it_go(setup):
    w, ext = setup
    c = w.client("a")
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


def test_a_region_the_extension_refuses_is_not_mapped():
    ext = FakeExtension()
    ext.refuse_map = True
    w = World(extension=ext)
    try:
        c = w.client("a")
        # The agent refused every region: no page can be taken for a store.
        with pytest.raises(MemoryError, match="No remote CXL region is reachable"):
            c.alloc(PAGE)
        assert not ext.mapped_regions
    finally:
        w.close()


@pytest.mark.parametrize("hold", [False, True])
def test_a_region_whose_mapped_hook_fails_stays_out_of_use(monkeypatch, hold):
    monkeypatch.setattr(datapath, "DOWN_BACKOFF_S", 0.0)  # retry the map at once
    ext = FakeExtension()
    ext.fail_mapped = True
    ext.hold = hold  # the extension may keep the region (a command in flight)
    w = World(extension=ext)
    try:
        c = w.client("a")
        with pytest.raises(MemoryError, match="No remote CXL region is reachable"):
            c.alloc(PAGE)
        region = next(iter(c.regions.values()))
        rid = region.handle["region_id"]
        service = w.node.service
        assert rid not in service.holders()
        assert not w.node.agent.registered  # the NIC no longer reaches it
        if hold:
            with pytest.raises(NotMapped, match="being released"):
                service.map(10_000, region.handle)
        ext.fail_mapped = ext.hold = False
        service.sweep()
        c._retry_unreachable()
        assert region.reachable
        # No map of it succeeded before: the retry was still fresh.
        assert ext.fresh[rid] is True
        assert put(c, "k", b"x" * PAGE)[0]
        assert read(c, ["k"]) == [b"x" * PAGE]
    finally:
        w.close()


def test_closing_the_agent_closes_the_extension(setup):
    w, ext = setup
    c = w.client("a")
    assert put(c, "k", b"x" * PAGE)[0]
    ext.hold = True
    w.node.service.close(extension_drain_s=0.05)
    assert ext.closed


def test_the_extension_hears_every_closed_connection(setup):
    w, ext = setup
    a = w.client("a")
    assert put(a, "k", b"v" * PAGE)[0]
    (conn,) = next(iter(w.node.service.holders().values()))
    client = RegionMapClient(w.node.address, timeout_s=5, key=shared_key())
    client.hello()  # holds no region
    a.close()
    client.close()
    deadline = time.monotonic() + 5
    while len(ext.closed_conns) < 2:
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)
    assert conn in ext.closed_conns
