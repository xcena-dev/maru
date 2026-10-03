# SPDX-License-Identifier: Apache-2.0
"""Pool-side staging window: which objects are asked for, and when."""

import pytest

from maru_common.config import MaruConfig
from maru_handler import MaruHandler
from maru_remote.server import RemoteServer
from maru_remote.stager import Stager
from maru_remote.transport import NixlTransport
from tests.unit.remote_fakes import FakeClock, FakeNixlAgent, reset_fake_agents

OBJ = 100


class Device:
    """Records prefetch requests; can refuse or raise."""

    def __init__(self, result=True):
        self.asked: list[tuple[int, int]] = []
        self.result = result

    def __call__(self, address: int, size: int) -> bool:
        self.asked.append((address, size))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _ranges(n, base=0):
    return [(base + i * OBJ, OBJ) for i in range(n)]


def _keys(n, tag="r"):
    return [f"{tag}{i}" for i in range(n)]


class TestStager:
    def test_lookup_asks_for_the_first_window_in_prefix_order(self):
        dev = Device()
        s = Stager(3, dev)
        s.on_lookup(_keys(10), _ranges(10))
        assert dev.asked == _ranges(3)

    def test_reads_move_the_window(self):
        dev = Device()
        s = Stager(3, dev)
        s.on_lookup(_keys(10), _ranges(10))
        s.on_read(["r0", "r1"])  # read position 2 -> stage up to index 5
        assert dev.asked == _ranges(5)
        s.on_read(["r4"])  # a later key moves the position past it
        assert dev.asked == _ranges(8)

    def test_window_stops_at_the_last_object_and_group_ends(self):
        dev = Device()
        s = Stager(4, dev)
        s.on_lookup(_keys(5), _ranges(5))
        s.on_read(_keys(5))  # the last object is read before it is asked for
        assert dev.asked == _ranges(4)
        assert s.stats()["live_groups"] == 0

    def test_only_the_leading_found_run_is_staged(self):
        dev = Device()
        s = Stager(8, dev)
        ranges = _ranges(5)
        ranges[2] = None  # a missing key breaks the prefix chain
        s.on_lookup(_keys(5), ranges)
        assert dev.asked == _ranges(2)

    def test_no_found_key_starts_nothing(self):
        dev = Device()
        s = Stager(4, dev)
        s.on_lookup(_keys(3), [None, None, None])
        assert dev.asked == [] and s.stats()["groups"] == 0

    def test_repeated_lookup_of_a_waiting_request_does_not_ask_again(self):
        dev = Device()
        s = Stager(2, dev)
        s.on_lookup(_keys(6), _ranges(6))
        s.on_lookup(_keys(6), _ranges(6))
        assert dev.asked == _ranges(2)

    def test_two_requests_are_staged_independently(self):
        dev = Device()
        s = Stager(2, dev)
        s.on_lookup(_keys(4, "a"), _ranges(4, base=0))
        s.on_lookup(_keys(4, "b"), _ranges(4, base=10_000))
        s.on_read(["b0"])
        assert dev.asked == _ranges(2) + _ranges(3, base=10_000)

    def test_idle_group_expires(self):
        clock = FakeClock()
        dev = Device()
        s = Stager(2, dev, group_ttl_s=5.0, clock=clock)
        s.on_lookup(_keys(6), _ranges(6))
        clock.advance(6.0)
        s.on_lookup(_keys(2, "x"), _ranges(2, base=5000))  # sweeps expired groups
        s.on_read(["r0"])  # the expired group no longer advances
        assert dev.asked == _ranges(2) + _ranges(2, base=5000)

    def test_refused_or_failing_prefetch_never_raises(self):
        for result in (False, RuntimeError("device busy")):
            dev = Device(result)
            s = Stager(2, dev)
            s.on_lookup(_keys(3), _ranges(3))
            s.on_read(["r0"])
            assert s.stats()["refused"] == 3

    def test_window_must_be_positive(self):
        with pytest.raises(ValueError):
            Stager(0, Device())


# ---------------------------------------------------------------------------
# Remote server: scheduler lookups start staging, worker reads move it.
# ---------------------------------------------------------------------------

PAGE = 64 * 1024


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def handler(server_thread, server_port):
    h = MaruHandler(
        MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=16 * PAGE,
            chunk_size_bytes=PAGE,
            auto_connect=False,
            use_async_rpc=False,
        )
    )
    h.connect()
    yield h
    h.close()


def _server(handler, dev, window=2):
    return RemoteServer(
        handler,
        NixlTransport("pool", agent=FakeNixlAgent("pool")),
        pool_id="test-pool",
        stager=Stager(window, dev) if dev is not None else None,
    )


def _store(server, keys):
    r = server.handle({"op": "reserve", "client_id": "w1", "sizes": [PAGE] * len(keys)})
    assert r["ok"], r
    entries = [
        {"ticket": p["ticket"], "key": k} for p, k in zip(r["pages"], keys, strict=True)
    ]
    assert server.handle({"op": "publish", "entries": entries})["ok"]


def _locations(server, keys):
    r = server.handle(
        {"op": "lookup", "keys": keys, "ticket_id": "t-loc", "protect": False}
    )
    return r["entries"]


class TestServerStaging:
    def test_scheduler_lookup_stages_device_ranges(self, handler):
        dev = Device()
        server = _server(handler, dev)
        keys = ["k0", "k1", "k2", "k3"]
        _store(server, keys)
        entries = _locations(server, keys)
        dev.asked.clear()
        region = entries[0]["region_id"]
        base = handler.get_region_device_offset(region)

        r = server.handle({"op": "exists", "keys": keys, "stage": True})

        assert r["found"] == [True] * 4
        assert dev.asked == [(base + e["offset"], e["length"]) for e in entries[:2]]
        server.close()

    def test_worker_read_moves_the_window(self, handler):
        dev = Device()
        server = _server(handler, dev)
        keys = ["k0", "k1", "k2", "k3"]
        _store(server, keys)
        server.handle({"op": "exists", "keys": keys, "stage": True})
        dev.asked.clear()

        server.handle(
            {"op": "lookup", "keys": ["k0", "k1"], "ticket_id": "t1", "protect": True}
        )

        assert len(dev.asked) == 2  # k2 and k3
        assert server.handle({"op": "stats"})["stager"]["asked"] == 4
        server.close()

    def test_exists_without_stage_does_not_stage(self, handler):
        dev = Device()
        server = _server(handler, dev)
        _store(server, ["k0", "k1"])
        dev.asked.clear()
        server.handle({"op": "exists", "keys": ["k0", "k1"]})
        assert dev.asked == []
        server.close()

    def test_server_without_stager_accepts_stage_flag(self, handler):
        server = _server(handler, None)
        _store(server, ["k0"])
        r = server.handle({"op": "exists", "keys": ["k0"], "stage": True})
        assert r["found"] == [True]
        assert server.handle({"op": "stats"})["stager"] is None
        server.close()
