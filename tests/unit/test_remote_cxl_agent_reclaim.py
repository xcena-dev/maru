# SPDX-License-Identifier: Apache-2.0
"""Remote CXL agent: regions the resource manager frees without a release here.

When MaruServer exits, the resource manager reaps its regions on its own and
may give their device range to a new region. The agent releases a mapping of
such a freed region before it registers a new region over the range (the
fence), and a periodic sweep releases the rest. A mapping whose metadata
failed and whose deregistration failed stays tracked until deregistered.
"""

import ctypes
import threading
import time

import pytest

from maru_remote.transport import NixlTransport
from maru_remote_cxl_agent.mapper import DaxRegionMapper
from maru_remote_cxl_agent.service import MapDenied, RegionMapService
from maru_shm.types import MaruHandle
from tests.unit.remote_fakes import (
    FakeCxl,
    FakeDeviceMapper,
    FakeNixlAgent,
    reset_fake_agents,
)

SIZE = 4096


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


class Node:
    """A region map service with a fake RM and NIXL.

    ``real_mapper`` maps the fake device file for real, so RDMA moves bytes
    into it and an unmapped range is kept out of use, as on a memory node.
    The transport records the order of registrations and deregistrations.
    """

    def __init__(self, real_mapper=False):
        self.cxl = FakeCxl()
        self.agent = FakeNixlAgent("mem")
        self.transport = NixlTransport("mem", agent=self.agent)
        self.mapper = DaxRegionMapper({}) if real_mapper else FakeDeviceMapper(self.cxl)
        self.service = RegionMapService(
            self.cxl,
            self.mapper,
            self.transport,
            node_id="mem1",
            epoch="e1",
            boot_id="b1",
        )
        self.events = []
        register, deregister = self.transport.register, self.transport.deregister

        def recording_register(addr, nbytes, tag=""):
            reg = register(addr, nbytes, tag)
            self.events.append(("register", tag))
            return reg

        def recording_deregister(reg):
            deregister(reg)
            self.events.append(("deregister", reg))

        self.transport.register = recording_register
        self.transport.deregister = recording_deregister

    def region(self, owner="owner"):
        return self.cxl.reserve(owner, SIZE).to_dict()

    def reap(self, handle):
        """The resource manager frees a region on its own (its client left)."""
        self.cxl.release("owner", handle["region_id"])


def tags(events):
    return [(kind, item if kind == "register" else "-") for kind, item in events]


def refuse(*args, **kwargs):
    raise RuntimeError("NIC busy")


def unreachable(handle):
    raise ConnectionError("Resource manager is not running")


# ------------------------------------------------------------------ fence


def test_mapping_a_new_region_over_a_reaped_range_releases_the_old_one_first():
    node = Node(real_mapper=True)
    old = node.region()
    old_reply = node.service.map(1, old)
    node.reap(old)
    new = node.region("next owner")
    assert new["offset"] == old["offset"]  # the resource manager reuses the range

    new_reply = node.service.map(2, new)

    # The old registration went before the new one came.
    assert [kind for kind, _ in node.events] == ["register", "deregister", "register"]
    assert node.events[2] == ("register", f"region-{new['region_id']}")
    assert node.service.holders() == {new["region_id"]: {2}}
    assert node.agent.registered == [(new_reply["addr"], SIZE)]
    with pytest.raises(MapDenied, match="released"):
        node.service.map(1, old)

    # A worker that loaded both registrations writes through each of them.
    worker = NixlTransport("worker", agent=FakeNixlAgent("worker"))
    buf = ctypes.create_string_buffer(SIZE)
    worker.register(ctypes.addressof(buf), SIZE)
    peer = worker.add_peer(old_reply["metadata"])
    worker.add_peer(new_reply["metadata"])
    ctypes.memset(buf, 0xCD, SIZE)
    worker.write(peer, [(ctypes.addressof(buf), new_reply["addr"], SIZE)])
    assert node.cxl.page(new["offset"], SIZE) == b"\xcd" * SIZE
    ctypes.memset(buf, 0xAB, SIZE)
    with pytest.raises(RuntimeError, match="ERR"):
        worker.write(peer, [(ctypes.addressof(buf), old_reply["addr"], SIZE)])
    assert node.cxl.page(new["offset"], SIZE) == b"\xcd" * SIZE  # B is untouched
    node.service.close()


def test_a_held_region_that_the_resource_manager_still_knows_stays():
    node = Node()
    first, second = node.region(), node.region()
    node.service.map(1, first)
    node.service.map(2, second)  # another range: nothing to check
    assert node.cxl.accesses == [first["region_id"], second["region_id"]]
    assert node.service.holders() == {first["region_id"]: {1}, second["region_id"]: {2}}


def test_a_new_region_is_refused_while_the_resource_manager_cannot_vouch_for_the_old():
    node = Node()
    old = node.region()
    node.service.map(1, old)
    node.reap(old)
    new = node.region("next owner")
    get_access = node.cxl.get_access

    def old_unreachable(handle):
        if handle.region_id == old["region_id"]:
            unreachable(handle)
        return get_access(handle)

    node.cxl.get_access = old_unreachable
    with pytest.raises(ConnectionError, match="cannot check mapped region"):
        node.service.map(2, new)
    # Nothing registered over the old range; the old mapping stays.
    assert [kind for kind, _ in node.events] == ["register"]
    assert node.service.holders() == {old["region_id"]: {1}}

    node.cxl.get_access = get_access  # the resource manager answers again
    node.service.map(2, new)
    assert node.service.holders() == {new["region_id"]: {2}}
    assert [kind for kind, _ in node.events] == ["register", "deregister", "register"]


def test_a_new_region_is_refused_while_the_old_one_stays_registered():
    node = Node()
    old = node.region()
    node.service.map(1, old)
    node.reap(old)
    new = node.region("next owner")
    deregister = node.transport.deregister
    node.transport.deregister = refuse
    with pytest.raises(RuntimeError, match="still registered"):
        node.service.map(2, new)
    assert node.service.holders() == {}  # the old one is out of use
    assert len(node.agent.registered) == 1
    with pytest.raises(MapDenied, match="released"):
        node.service.map(1, old)

    node.transport.deregister = deregister
    node.service.map(2, new)  # the fence deregisters the old one first
    assert node.service.holders() == {new["region_id"]: {2}}
    assert [kind for kind, _ in node.events] == ["register", "deregister", "register"]


def test_a_reused_region_id_maps_once_the_old_region_is_gone():
    node = Node()
    old = node.region()
    node.service.map(1, old)
    # A restarted resource manager lost the old region and gave its id out
    # again with a new token.
    reused = {**old, "auth_token": 9999}
    node.cxl.regions[old["region_id"]] = MaruHandle(**reused)
    assert node.service.map(2, reused)["region_id"] == old["region_id"]
    assert node.service.holders() == {old["region_id"]: {2}}
    assert [kind for kind, _ in node.events] == ["register", "deregister", "register"]


def test_a_first_map_waits_for_an_earlier_one_of_its_range():
    # The old region passed GET_ACCESS and is registering when the resource
    # manager reaps it and gives the range to a new region.
    node = Node()
    old = node.region()
    registering, go = threading.Event(), threading.Event()
    register = node.transport.register

    def slow(addr, nbytes, tag=""):
        if tag == f"region-{old['region_id']}":
            registering.set()
            go.wait(5)
        return register(addr, nbytes, tag)

    node.transport.register = slow
    first = threading.Thread(target=lambda: node.service.map(1, old))
    first.start()
    assert registering.wait(5)
    node.reap(old)
    new = node.region("next owner")
    replies = []
    second = threading.Thread(target=lambda: replies.append(node.service.map(2, new)))
    second.start()
    time.sleep(0.1)
    assert not replies  # waits for the earlier map of the range
    go.set()
    first.join(5)
    second.join(5)
    assert replies and node.service.holders() == {new["region_id"]: {2}}
    assert [kind for kind, _ in node.events] == ["register", "deregister", "register"]


# ------------------------------------------------------------------ sweep


def test_the_sweep_releases_a_reaped_region_and_keeps_the_rest():
    node = Node()
    reaped, live = node.region(), node.region()
    reaped_reply = node.service.map(1, reaped)
    live_reply = node.service.map(2, live)
    node.reap(reaped)

    node.service.sweep()

    assert node.service.holders() == {live["region_id"]: {2}}
    assert node.agent.registered == [(live_reply["addr"], SIZE)]
    assert node.mapper.unmapped == [reaped["offset"]]  # kept out of use
    with pytest.raises(MapDenied, match="released"):
        node.service.map(1, reaped)
    node.service.release(reaped)  # MaruServer's late release succeeds
    assert reaped_reply["addr"] not in [a for a, _ in node.agent.registered]


def test_the_sweep_keeps_every_mapping_while_the_resource_manager_is_away():
    node = Node()
    reaped, live = node.region(), node.region()
    node.service.map(1, reaped)
    node.service.map(2, live)
    node.reap(reaped)
    get_access = node.cxl.get_access
    node.cxl.get_access = unreachable

    node.service.sweep()

    assert node.service.holders() == {reaped["region_id"]: {1}, live["region_id"]: {2}}
    assert len(node.agent.registered) == 2
    node.cxl.get_access = get_access
    node.service.sweep()
    assert node.service.holders() == {live["region_id"]: {2}}


def test_the_sweep_runs_in_the_background_until_close():
    node = Node()
    handle = node.region()
    node.service.map(1, handle)
    node.service.start_sweep(interval_s=0.01)
    try:
        node.reap(handle)
        deadline = time.monotonic() + 5
        while node.service.holders() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert node.service.holders() == {}
        assert node.agent.registered == []
    finally:
        node.service.close()


# ------------------------------------------------- failed map cleanup


def test_a_failed_map_whose_deregistration_fails_stays_until_released():
    node = Node()
    handle = node.region()
    deregister = node.transport.deregister
    node.transport.partial_metadata = lambda registration: refuse()
    node.transport.deregister = refuse
    with pytest.raises(RuntimeError, match="NIC busy"):
        node.service.map(1, handle)
    # Still registered, out of use, and tracked.
    assert len(node.agent.registered) == 1 and node.service.holders() == {}

    with pytest.raises(RuntimeError, match="deregistering"):
        node.service.release(handle)
    assert len(node.agent.registered) == 1 and node.mapper.unmapped == []

    node.transport.deregister = deregister
    node.service.release(handle)  # deregistered now, then it succeeds
    assert node.agent.registered == [] and node.mapper.unmapped == [handle["offset"]]
    with pytest.raises(MapDenied, match="released"):
        node.service.map(1, handle)


def test_a_later_map_does_not_reuse_a_half_failed_mapping():
    node = Node()
    handle = node.region()
    partial_metadata, deregister = (
        node.transport.partial_metadata,
        node.transport.deregister,
    )
    node.transport.partial_metadata = lambda registration: refuse()
    node.transport.deregister = refuse
    with pytest.raises(RuntimeError):
        node.service.map(1, handle)
    node.transport.partial_metadata = partial_metadata
    with pytest.raises(RuntimeError, match="still registered"):
        node.service.map(1, handle)  # the old registration is not handed out

    node.transport.deregister = deregister
    reply = node.service.map(1, handle)  # deregisters the old one, maps anew
    assert node.agent.registered == [(reply["addr"], SIZE)] and reply["metadata"]
    assert [kind for kind, _ in node.events] == ["register", "deregister", "register"]


def test_the_sweep_and_close_deregister_a_half_failed_mapping():
    node = Node()
    handle = node.region()
    deregister = node.transport.deregister
    node.transport.partial_metadata = lambda registration: refuse()
    node.transport.deregister = refuse
    with pytest.raises(RuntimeError):
        node.service.map(1, handle)
    node.service.sweep()
    assert len(node.agent.registered) == 1
    node.transport.deregister = deregister
    node.service.sweep()
    assert node.agent.registered == []

    node.transport.deregister = refuse
    other = node.region()
    with pytest.raises(RuntimeError):
        node.service.map(1, other)
    node.transport.deregister = deregister
    node.service.close()
    assert node.agent.registered == []


def test_the_sweep_deregisters_a_mapping_whose_last_holder_left():
    # The last holder's unmap could not deregister; the mapping is unused
    # but still registered, and the region is still allocated.
    node = Node()
    in_use, handle = node.region(), node.region()
    node.service.map(1, in_use)  # mapped first: the sweep asks the RM about it first
    node.service.map(1, handle)
    deregister = node.transport.deregister
    node.transport.deregister = refuse
    node.service.unmap(1, handle["region_id"])
    assert len(node.agent.registered) == 2 and node.mapper.unmapped == []
    node.service.sweep()
    assert len(node.agent.registered) == 2  # still refused: kept for later
    node.transport.deregister = deregister
    node.cxl.get_access = unreachable  # an unused mapping needs no RM answer
    node.service.sweep()
    assert len(node.agent.registered) == 1 and node.mapper.unmapped == [
        handle["offset"]
    ]
