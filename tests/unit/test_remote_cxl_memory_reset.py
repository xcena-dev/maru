# SPDX-License-Identifier: Apache-2.0
"""A reset of memory that loses what it held, without a reboot of its node.

A region extension tells the memory's generation; the agent advertises it as
part of its boot, so a reset reaches workers as a reboot of the memory node
does. The directory, the agent and the clients are real; the extension is a
fake.
"""

import itertools
import threading
import time

import pytest

from maru_remote.region_map import RegionMapClient, RegionMapError
from maru_remote_cxl_agent.service import RegionMapService
from tests.unit.remote_fakes import FakeCxl, shared_key
from tests.unit.test_remote_cxl_region_extension import FakeExtension
from tests.unit.test_remote_cxl_storage import PAGE, World, only_region, put, read


class ResettableExtension(FakeExtension):
    """A fake extension of memory that can be reset."""

    def __init__(self):
        super().__init__()
        self.generation = "g1"
        self.generation_error = None

    def memory_generation(self):
        if self.generation_error is not None:
            raise self.generation_error
        return self.generation


@pytest.fixture
def setup():
    ext = ResettableExtension()
    w = World(extension=ext)
    yield w, ext
    w.close()


def test_the_boot_carries_the_memory_generation(setup):
    w, ext = setup
    node_boot = w.node.service.boot_id
    assert w.node.service.hello()["boot"] == f"{node_boot}:g1"
    plain = World(extension=FakeExtension())  # no memory_generation
    try:
        assert plain.node.service.hello()["boot"] == plain.node.service.boot_id
    finally:
        plain.close()


def test_a_memory_reset_deregisters_every_region_and_changes_the_boot(setup):
    w, ext = setup
    a = w.client("a")
    assert put(a, "k", b"x" * PAGE)[0]
    region = only_region(a)
    service = w.node.service
    assert w.node.agent.registered
    ext.generation = "g2"
    service.sweep()
    # RDMA through an old registration now fails at the NIC.
    assert not w.node.agent.registered
    assert service.holders() == {}
    # The region may be mapped again; the reply says the new boot.
    client = RegionMapClient(w.node.address, timeout_s=5, key=shared_key())
    try:
        reply = client.map(region.handle)
        assert reply.boot == f"{service.boot_id}:g2"
        assert client.hello()["boot"] == reply.boot
    finally:
        client.close()


def test_the_owner_gives_back_a_region_whose_memory_was_reset(setup):
    w, ext = setup
    a, b = w.client("a"), w.client("b")
    assert put(a, "k", b"v" * PAGE)[0]
    region = only_region(a)
    ext.generation = "g2"
    a._check_boots()  # the heartbeat's boot check; its hello sees the reset
    assert region.retired
    assert b.batch_exists(["k"]) == [False]
    assert region.region_id in w.cxl.released
    assert put(a, "k2", b"w" * PAGE)[0]  # a new region on the new generation


def test_a_store_after_a_reset_voids_the_region_keys(setup):
    w, ext = setup
    a, b = w.client("a"), w.client("b")
    assert put(a, "k", b"v" * PAGE)[0]
    ext.generation = "g2"
    w.node.service.sweep()
    assert put(a, "k2", b"w" * PAGE)[0] is False  # the old registration is gone
    assert put(a, "k2", b"w" * PAGE)[0] is True  # mapped again on the new boot
    assert b.batch_exists(["k", "k2"]) == [False, True]


def test_a_reader_loads_a_miss_after_a_reset(setup):
    w, ext = setup
    a, b = w.client("a"), w.client("b")
    assert put(a, "k", b"v" * PAGE)[0]
    region = only_region(a)
    assert read(b, ["k"]) == [b"v" * PAGE]  # b holds an access on the old boot
    ext.generation = "g2"
    w.node.service.sweep()
    assert read(b, ["k"]) == [None]  # the old registration is gone
    # The owner's region is mapped again before the owner noticed the reset:
    # maru-server still expects the old boot, and the reader refuses it.
    w.node.service.map(999, region.handle)
    assert read(b, ["k"]) == [None]


def test_a_generation_the_extension_cannot_tell_keeps_the_regions(setup):
    w, ext = setup
    a = w.client("a")
    assert put(a, "k", b"v" * PAGE)[0]
    ext.generation_error = RuntimeError("device service away")
    w.node.service.sweep()
    assert w.node.service.hello()["boot"].endswith(":g1")
    assert read(a, ["k"]) == [b"v" * PAGE]


def test_an_agent_whose_generation_is_unknown_at_start_does_not_start():
    ext = ResettableExtension()
    ext.generation_error = RuntimeError("device service away")
    with pytest.raises(RuntimeError, match="device service away"):
        RegionMapService(
            FakeCxl(), None, None, node_id="mem1", epoch="e1", extension=ext
        )


def _memory_reset_world(**options):
    ext = ResettableExtension()
    return World(extension=ext, **options), ext


def test_a_generation_unanswered_past_the_grace_fences_until_it_answers():
    w, ext = _memory_reset_world(generation_grace_s=0.0)
    try:
        a, b = w.client("a"), w.client("b")
        assert put(a, "k", b"v" * PAGE)[0]
        region = only_region(a)
        service = w.node.service
        ext.generation_error = RuntimeError("device service away")
        service.sweep()
        assert service.boot == f"{service.boot_id}:unknown"
        assert not w.node.agent.registered and service.holders() == {}
        client = RegionMapClient(w.node.address, timeout_s=5, key=shared_key())
        try:
            with pytest.raises(RegionMapError, match="generation is unknown"):
                client.map(region.handle)
            # The extension answers again, with the generation it had (a
            # reset while it could not tell would not show): the boot is
            # still new, so nothing mapped before is trusted.
            ext.generation_error = None
            reply = client.map(region.handle)
            assert reply.boot == f"{service.boot_id}:g1~1"
            # Each unknown period makes another boot.
            ext.generation_error = RuntimeError("device service away again")
            service.sweep()
            ext.generation_error = None
            assert client.hello()["boot"] == f"{service.boot_id}:g1~2"
        finally:
            client.close()
        a._check_boots()
        assert region.retired
        assert b.batch_exists(["k"]) == [False]
        assert put(a, "k2", b"w" * PAGE)[0]
        assert read(b, ["k2"]) == [b"w" * PAGE]
    finally:
        w.close()


def test_the_generation_thread_notices_a_reset_without_any_request(setup):
    w, ext = setup
    a = w.client("a")
    assert put(a, "k", b"v" * PAGE)[0]
    service = w.node.service
    service.start_generation_watch(0.01)
    ext.generation = "g2"
    deadline = time.monotonic() + 5
    while not service.boot.endswith(":g2"):
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)
    assert not w.node.agent.registered


def test_concurrent_generation_reads_never_set_an_older_one(setup):
    w, ext = setup
    service = w.node.service
    count = itertools.count(2)
    ext.memory_generation = lambda: f"g{next(count)}"
    service._generation_of = ext.memory_generation  # noqa: SLF001 (the hook)
    threads = [
        threading.Thread(
            target=lambda: [service._check_generation() for _ in range(50)]  # noqa: SLF001
        )
        for _ in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Read, compare and set run one at a time: the last value read is kept.
    assert service.boot == f"{service.boot_id}:g{next(count) - 1}"


class _ResetDuringMap(ResettableExtension):
    def mapped(self, handle, access, *, fresh):
        super().mapped(handle, access, fresh=fresh)
        if self.generation == "g1":
            self.generation = "g2"
            self.service.sweep()  # the reset is seen before the reply


def test_a_map_the_memory_reset_under_fails():
    ext = _ResetDuringMap()
    w = World(extension=ext)
    ext.service = w.node.service
    try:
        client = RegionMapClient(w.node.address, timeout_s=5, key=shared_key())
        try:
            handle = w.cxl.reserve("owner", PAGE).to_dict()
            with pytest.raises(RegionMapError, match="reset while it was mapped"):
                client.map(handle)
            assert w.node.service.holders() == {}
            assert not w.node.agent.registered
            reply = client.map(handle)  # mapped again on the new generation
            assert reply.boot == f"{w.node.service.boot_id}:g2"
        finally:
            client.close()
    finally:
        w.close()
