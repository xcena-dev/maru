# SPDX-License-Identifier: Apache-2.0
"""Remote CXL agent: regions the resource manager frees without a release here.

When MaruServer exits, the resource manager reaps its regions on its own. The
agent holds every region it registers, so the resource manager keeps a reaped
region's range out of use until the agent's sweep has released it and sent
the unhold. A range freed without the hold (an operator's discard) may go to
a new region at once: the agent releases a mapping of such a range before it
registers a new region over it (the fence). A mapping whose metadata failed
and whose deregistration failed stays tracked until deregistered.
"""

import ctypes
import threading
import time

import pytest

from maru_remote.transport import NixlTransport
from maru_remote_cxl_agent.mapper import DaxRegionMapper
from maru_remote_cxl_agent.service import MapDenied, RegionMapService
from maru_shm.client import HoldRefusedError
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

    def discard(self, handle):
        """The resource manager frees a region and drops its holds."""
        self.cxl.discard(handle["region_id"])


def tags(events):
    return [(kind, item if kind == "register" else "-") for kind, item in events]


def refuse(*args, **kwargs):
    raise RuntimeError("NIC busy")


def unreachable(handle):
    raise ConnectionError("Resource manager is not running")


# ------------------------------------------------------------------ fence


def test_mapping_a_new_region_over_a_discarded_range_releases_the_old_one_first():
    node = Node(real_mapper=True)
    old = node.region()
    old_reply = node.service.map(1, old)
    node.discard(old)
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
    node.discard(old)
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
    node.discard(old)
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
    # The old region passed its hold and is registering when the resource
    # manager drops it and gives the range to a new region.
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
    node.discard(old)
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


# ------------------------------------------------------------------ holds


def test_a_reaped_region_keeps_its_range_until_the_agent_releases_it():
    node = Node()
    reaped = node.region()
    node.service.map(1, reaped)
    node.reap(reaped)
    assert node.cxl.holds == {reaped["region_id"]: {"mem1": "e1"}}
    # The range stays out of use while the agent still registers it.
    assert node.region("next owner")["offset"] != reaped["offset"]

    node.service.sweep()  # releases it and unholds it

    assert node.cxl.holds == {} and node.cxl.draining == {}
    assert node.region("third owner")["offset"] == reaped["offset"]


def test_another_agent_cannot_map_a_new_region_over_a_held_range():
    """A shared pool: the region's range goes to a region that agent Y maps
    only after agent X, which registered the reaped region, released it."""
    x = Node(real_mapper=True)
    y_agent = FakeNixlAgent("mem-y")
    y = RegionMapService(
        x.cxl,
        DaxRegionMapper({}),
        NixlTransport("mem-y", agent=y_agent),
        node_id="mem2",
        epoch="e1",
    )
    old = x.region()
    old_reply = x.service.map(1, old)
    x.reap(old)
    new = x.region("next owner")
    assert new["offset"] != old["offset"]
    new_reply = y.map(2, new)

    worker = NixlTransport("worker", agent=FakeNixlAgent("worker"))
    buf = ctypes.create_string_buffer(SIZE)
    worker.register(ctypes.addressof(buf), SIZE)
    old_peer = worker.add_peer(old_reply["metadata"])
    new_peer = worker.add_peer(new_reply["metadata"])
    ctypes.memset(buf, 0xCD, SIZE)
    worker.write(new_peer, [(ctypes.addressof(buf), new_reply["addr"], SIZE)])
    ctypes.memset(buf, 0xAB, SIZE)
    # A late WRITE through X's old registration lands in the old range only.
    worker.write(old_peer, [(ctypes.addressof(buf), old_reply["addr"], SIZE)])
    assert x.cxl.page(new["offset"], SIZE) == b"\xcd" * SIZE

    x.service.sweep()
    assert x.region("third owner")["offset"] == old["offset"]
    x.service.close()
    y.close()


def test_a_release_unholds_before_it_returns():
    node = Node()
    handle = node.region()
    node.service.map(1, handle)
    node.service.release(handle)
    assert node.cxl.holds == {}
    node.reap(handle)  # MaruServer gives it back: freed at once
    assert node.cxl.draining == {}


def test_an_unhold_that_fails_is_sent_again_by_the_sweep():
    node = Node()
    handle = node.region()
    node.service.map(1, handle)
    unhold = node.cxl.unhold
    node.cxl.unhold = lambda *a: unreachable(None)
    node.service.release(handle)
    node.reap(handle)
    assert handle["region_id"] in node.cxl.draining  # kept out of use

    node.cxl.unhold = unhold
    node.service.sweep()
    assert node.cxl.holds == {} and node.cxl.draining == {}


def test_a_map_that_fails_after_its_hold_unholds_the_region():
    node = Node()
    handle = node.region()
    node.transport.register = refuse
    with pytest.raises(RuntimeError, match="NIC busy"):
        node.service.map(1, handle)
    node.service.sweep()
    assert node.cxl.holds == {}


def test_a_restarted_agent_drops_the_holds_of_its_earlier_process():
    node = Node()
    handle = node.region()
    node.service.map(1, handle)  # the process then ends without a release
    node.reap(handle)
    restarted = RegionMapService(
        node.cxl, node.mapper, node.transport, node_id="mem1", epoch="e2"
    )
    restarted.sweep()
    assert node.cxl.holds == {} and node.cxl.draining == {}


def test_a_hold_of_the_current_process_survives_a_reset():
    node = Node()
    handle = node.region()
    node.service.map(1, handle)
    node.service.sweep()  # resets once, for epoch e1
    assert node.cxl.holds == {handle["region_id"]: {"mem1": "e1"}}


def test_a_resource_manager_without_holds_refuses_the_map():
    from maru_shm.client import HoldsUnsupportedError

    node = Node()
    handle = node.region()

    def old_rm(*args):
        raise HoldsUnsupportedError(
            "the resource manager does not support region holds"
        )

    node.cxl.reset_holds = old_rm
    with pytest.raises(HoldsUnsupportedError, match="does not support"):
        node.service.map(1, handle)
    assert node.agent.registered == []


def test_a_refused_map_leaves_no_unhold_that_blocks_the_others():
    node = Node()
    good = node.region()
    node.service.map(1, good)
    bad = {**node.region(), "auth_token": 1}  # the RM refuses this handle
    with pytest.raises(RuntimeError, match="GetAccess"):
        node.service.map(2, bad)
    node.service.release(good)
    assert node.cxl.holds == {}


def test_a_refused_unhold_does_not_hold_back_the_rest():
    node = Node()
    first, second = node.region(), node.region()
    node.service.map(1, first)
    node.service.map(1, second)
    unhold = node.cxl.unhold

    def refuse_first(handle, holder, instance):
        if handle.region_id == first["region_id"]:
            raise HoldRefusedError("Unhold failed (-13): invalid auth token")
        unhold(handle, holder, instance)

    node.cxl.unhold = refuse_first
    node.service.drop(1)  # both go; the first unhold is refused
    assert node.cxl.holds == {first["region_id"]: {"mem1": "e1"}}
    node.service.sweep()
    assert node.cxl.holds == {first["region_id"]: {"mem1": "e1"}}  # not retried


def test_unmap_drop_and_close_unhold():
    node = Node()
    a, b, c = node.region(), node.region(), node.region()
    node.service.map(1, a)
    node.service.map(2, b)
    node.service.map(3, c)
    node.service.unmap(1, a["region_id"])
    assert set(node.cxl.holds) == {b["region_id"], c["region_id"]}
    node.service.drop(2)
    assert set(node.cxl.holds) == {c["region_id"]}
    node.service.close()
    assert node.cxl.holds == {}


def test_a_failed_reset_is_retried_and_said_once(monkeypatch):
    import maru_remote_cxl_agent.service as service

    said = []
    monkeypatch.setattr(service.logger, "warning", lambda msg, *a: said.append(msg))
    node = Node()
    reset = node.cxl.reset_holds
    node.cxl.reset_holds = lambda *a: unreachable(None)
    handle = node.region()
    for _ in range(3):
        with pytest.raises(ConnectionError):
            node.service.map(1, handle)
        node.service.sweep()
    assert len([m for m in said if "earlier processes" in m]) == 1
    assert node.cxl.holds == {}  # nothing was held, nothing is queued
    node.cxl.reset_holds = reset
    node.service.map(1, handle)
    assert node.cxl.holds == {handle["region_id"]: {"mem1": "e1"}}


def test_an_unhold_the_rm_could_not_save_is_sent_again():
    node = Node()
    handle = node.region()
    node.service.map(1, handle)
    unhold, calls = node.cxl.unhold, []

    def unsaved_once(h, holder, instance):
        calls.append(h.region_id)
        if len(calls) == 1:
            raise RuntimeError("Unhold failed with status -5")
        unhold(h, holder, instance)

    node.cxl.unhold = unsaved_once
    node.service.release(handle)
    assert node.cxl.holds  # kept: the outcome is unknown
    node.service.sweep()
    assert node.cxl.holds == {} and calls == [handle["region_id"]] * 2


def test_a_hold_the_rm_could_not_save_is_unheld():
    node = Node()
    handle = node.region()
    hold = node.cxl.hold_access

    def unsaved(h, holder, instance):
        hold(h, holder, instance)  # recorded in memory, not synced
        raise RuntimeError("Hold failed with status -5")

    node.cxl.hold_access = unsaved
    with pytest.raises(RuntimeError, match="-5"):
        node.service.map(1, handle)
    node.service.sweep()
    assert node.cxl.holds == {}


def test_the_sweep_deregisters_a_failed_mapping_while_the_rm_is_away():
    node = Node()
    live = node.region()
    node.service.map(1, live)  # checked first, and the RM does not answer
    handle = node.region()
    deregister = node.transport.deregister
    node.transport.partial_metadata = lambda registration: refuse()
    node.transport.deregister = refuse
    with pytest.raises(RuntimeError):
        node.service.map(1, handle)
    assert len(node.agent.registered) == 2
    node.transport.deregister = deregister
    node.cxl.get_access = unreachable  # the sweep cannot ask the RM
    node.service.sweep()
    assert len(node.agent.registered) == 1  # only the live one stays
    assert node.service.holders() == {live["region_id"]: {1}}
