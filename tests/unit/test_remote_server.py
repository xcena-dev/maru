# SPDX-License-Identifier: Apache-2.0
"""Pool-node remote server over a real MaruServer and CXL-backend MaruHandler."""

from unittest.mock import patch

import numpy as np
import pytest

from maru_common.config import MaruConfig
from maru_handler import MaruHandler
from maru_remote.server import RemoteServer
from maru_remote.transport import NixlTransport, buffer_address
from tests.unit.remote_fakes import FakeClock, FakeNixlAgent, reset_fake_agents

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
            auto_expand=True,
            expand_size=16 * PAGE,
        )
    )
    h.connect()
    yield h
    h.close()


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def server(handler, clock):
    srv = RemoteServer(
        handler,
        NixlTransport("pool", agent=FakeNixlAgent("pool")),
        pool_id="test-pool",
        reservation_ttl_s=10.0,
        ticket_ttl_s=20.0,
        clock=clock,
    )
    yield srv
    srv.close()


@pytest.fixture
def worker():
    t = NixlTransport("worker", agent=FakeNixlAgent("worker"))
    yield t
    t.close()


def _reserve(server, *sizes):
    r = server.handle({"op": "reserve", "client_id": "w1", "sizes": list(sizes)})
    assert r["ok"], r
    return r["pages"]


def _publish(server, *pairs):
    entries = [{"ticket": t, "key": k} for t, k in pairs]
    return server.handle({"op": "publish", "entries": entries})


def _write_remote(worker, peer, page, payload: bytes):
    src = np.frombuffer(bytearray(payload), dtype=np.uint8)
    worker.register(buffer_address(src), src.nbytes)
    worker.write(
        peer, [(buffer_address(src), page["base"] + page["offset"], src.nbytes)]
    )


def _read_remote(worker, peer, entry) -> bytes:
    dst = np.zeros(entry["length"], dtype=np.uint8)
    worker.register(buffer_address(dst), dst.nbytes)
    worker.read(
        peer, [(buffer_address(dst), entry["base"] + entry["offset"], dst.nbytes)]
    )
    return dst.tobytes()


def test_hello_reports_layout_lifetimes_and_registered_region(server):
    r = server.handle({"op": "hello", "client_id": "w1"})
    assert r["ok"] and r["page_bytes"] == PAGE and r["protocol"] == 1
    assert r["reservation_ttl_s"] == 10.0 and r["ticket_ttl_s"] == 20.0
    assert r["pool_id"] == "test-pool" and r["generation"] == server.generation
    assert len(r["regions"]) == 1 and r["regions"][0]["length"] == 16 * PAGE
    assert (r["regions"][0]["base"], 16 * PAGE) in FakeNixlAgent.registry[
        "pool"
    ].registered


def test_every_reply_carries_the_generation(server):
    assert server.handle({"op": "ping"})["generation"] == server.generation
    bad = server.handle({"op": "nope"})
    assert bad["ok"] is False and bad["generation"] == server.generation


def test_sized_reserve_write_publish_lookup_read_roundtrip(server, worker):
    peer = worker.add_peer(server.handle({"op": "hello", "client_id": "w1"})["nixl_md"])
    pages = _reserve(server, 1000, PAGE)
    assert [p["length"] for p in pages] == [1000, PAGE]
    _write_remote(worker, peer, pages[0], b"a" * 1000)
    _write_remote(worker, peer, pages[1], b"b" * PAGE)
    pub = _publish(server, (pages[0]["ticket"], "k-a"), (pages[1]["ticket"], "k-b"))
    assert pub["statuses"] == ["CREATED", "CREATED"]
    found = server.handle({"op": "exists", "keys": ["k-a", "k-b", "k-c"]})["found"]
    assert found == [True, True, False]
    look = server.handle(
        {"op": "lookup", "keys": ["k-a", "k-c"], "ticket_id": "r1", "protect": True}
    )
    assert look["entries"][1] is None
    assert look["entries"][0]["length"] == 1000  # the stored size, not the page
    assert _read_remote(worker, peer, look["entries"][0]) == b"a" * 1000
    assert server.handle({"op": "release", "ticket_id": "r1"})["released"] == 1


def test_duplicate_publish_keeps_first_value_and_frees_page(server):
    pages = _reserve(server, PAGE, PAGE)
    assert _publish(server, (pages[0]["ticket"], "dup"))["statuses"] == ["CREATED"]
    assert _publish(server, (pages[1]["ticket"], "dup"))["statuses"] == [
        "ALREADY_PRESENT"
    ]
    assert server.handle({"op": "stats"})["reservations"] == 0


def test_oversized_or_empty_reserve_is_rejected(server):
    assert (
        server.handle({"op": "reserve", "client_id": "w", "sizes": [PAGE + 1]})["ok"]
        is False
    )
    assert (
        server.handle({"op": "reserve", "client_id": "w", "sizes": []})["ok"] is False
    )
    assert (
        server.handle({"op": "reserve", "client_id": "w", "sizes": [True]})["ok"]
        is False
    )


def test_unknown_ticket_is_an_error(server):
    r = _publish(server, ("nope", "k"))
    assert r["ok"] is False and "ticket" in r["error"]


def test_abandon_frees_reserved_pages(server):
    pages = _reserve(server, PAGE, PAGE, PAGE)
    r = server.handle({"op": "abandon", "tickets": [p["ticket"] for p in pages[:2]]})
    assert r["freed"] == 2 and server.handle({"op": "stats"})["reservations"] == 1


def test_expired_reservation_is_reclaimed_by_sweep(server, clock):
    _reserve(server, PAGE)
    clock.advance(9.0)
    server.sweep()
    assert server.handle({"op": "stats"})["reservations"] == 1
    clock.advance(2.0)
    server.sweep()
    assert server.handle({"op": "stats"})["reservations"] == 0


def test_quarantined_pages_survive_expiry_until_abandoned(server, handler, clock):
    pages = _reserve(server, PAGE, PAGE)
    tickets = [p["ticket"] for p in pages]
    assert server.handle({"op": "quarantine", "tickets": tickets})["quarantined"] == 2
    allocated = handler.owned_region_manager.get_stats()["total_allocated_pages"]
    clock.advance(100.0)  # past the reservation lifetime, within quarantine
    server.sweep()
    stats = server.handle({"op": "stats"})
    assert stats["quarantined"] == 2 and stats["reservations"] == 0
    assert (
        handler.owned_region_manager.get_stats()["total_allocated_pages"] == allocated
    )
    assert _publish(server, (tickets[0], "k"))["ok"] is False  # never publishable
    assert server.handle({"op": "abandon", "tickets": tickets})["freed"] == 2
    assert server.handle({"op": "stats"})["quarantined"] == 0
    assert (
        handler.owned_region_manager.get_stats()["total_allocated_pages"]
        == allocated - 2
    )


def test_protected_key_refuses_delete_until_release(server, handler):
    page = _reserve(server, PAGE)[0]
    _publish(server, (page["ticket"], "pinned"))
    server.handle(
        {"op": "lookup", "keys": ["pinned"], "ticket_id": "r9", "protect": True}
    )
    assert handler.delete("pinned") is False
    server.handle({"op": "release", "ticket_id": "r9"})
    assert handler.delete("pinned") is True


def test_expired_ticket_is_unpinned_by_sweep(server, handler, clock):
    page = _reserve(server, PAGE)[0]
    _publish(server, (page["ticket"], "late"))
    server.handle(
        {"op": "lookup", "keys": ["late"], "ticket_id": "r5", "protect": True}
    )
    clock.advance(21.0)
    server.sweep()
    assert (
        server.handle({"op": "stats"})["tickets"] == 0
        and handler.delete("late") is True
    )


def test_reused_ticket_id_is_rejected(server):
    page = _reserve(server, PAGE)[0]
    _publish(server, (page["ticket"], "k"))
    msg = {"op": "lookup", "keys": ["k"], "ticket_id": "same", "protect": True}
    assert server.handle(msg)["ok"] is True
    assert server.handle(msg)["ok"] is False


def test_new_region_bumps_md_version_and_registers_it(server):
    v0 = server.handle({"op": "hello", "client_id": "w1"})["md_version"]
    pages = _reserve(server, *([PAGE] * 20))  # more than one region holds
    assert len({p["region_id"] for p in pages}) == 2
    meta = server.handle({"op": "metadata"})
    assert meta["md_version"] > v0 and len(meta["regions"]) == 2
    fake = FakeNixlAgent.registry["pool"]
    assert all((r["base"], r["length"]) in fake.registered for r in meta["regions"])


def _fail_first_call(real, exc):
    calls = []

    def side_effect(*args, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            raise exc
        return real(*args, **kwargs)

    return side_effect


def test_sweep_keeps_failed_ticket_and_retries_next_sweep(server, handler, clock):
    page = _reserve(server, PAGE)[0]
    _publish(server, (page["ticket"], "flaky"))
    server.handle(
        {"op": "lookup", "keys": ["flaky"], "ticket_id": "rf", "protect": True}
    )
    clock.advance(21.0)
    flaky = _fail_first_call(handler.batch_unpin, TimeoutError("rpc timeout"))
    with patch.object(handler, "batch_unpin", side_effect=flaky):
        server.sweep()  # must not raise
        assert server.handle({"op": "stats"})["tickets"] == 1
        assert handler.delete("flaky") is False  # still pinned
        server.sweep()
    assert server.handle({"op": "stats"})["tickets"] == 0
    assert handler.delete("flaky") is True


def test_release_keeps_the_ticket_when_unpin_fails(server, handler):
    page = _reserve(server, PAGE)[0]
    _publish(server, (page["ticket"], "sticky"))
    server.handle(
        {"op": "lookup", "keys": ["sticky"], "ticket_id": "rs", "protect": True}
    )
    flaky = _fail_first_call(handler.batch_unpin, TimeoutError("rpc timeout"))
    with patch.object(handler, "batch_unpin", side_effect=flaky):
        assert server.handle({"op": "release", "ticket_id": "rs"})["ok"] is False
        assert server.handle({"op": "stats"})["tickets"] == 1
        assert server.handle({"op": "release", "ticket_id": "rs"})["released"] == 1
    assert handler.delete("sticky") is True


def test_keys_left_by_an_earlier_run_are_missing_and_replaced(
    server, handler, server_port
):
    page = _reserve(server, PAGE)[0]
    assert _publish(server, (page["ticket"], "old"))["statuses"] == ["CREATED"]
    # A new server run on the same MaruServer: a new handler owns new regions,
    # the old region stays alive because its key still references it.
    fresh = MaruHandler(
        MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=16 * PAGE,
            chunk_size_bytes=PAGE,
            auto_connect=False,
            use_async_rpc=False,
        )
    )
    fresh.connect()
    reset_fake_agents()
    srv2 = RemoteServer(
        fresh, NixlTransport("pool2", agent=FakeNixlAgent("pool2")), pool_id="p2"
    )
    try:
        assert srv2.handle({"op": "hello", "client_id": "w"})["ok"]
        assert srv2.handle({"op": "exists", "keys": ["old"]})["found"] == [False]
        look = srv2.handle(
            {"op": "lookup", "keys": ["old"], "ticket_id": "t", "protect": True}
        )
        assert look["ok"] and look["entries"] == [None]
        page2 = _reserve(srv2, PAGE)[0]
        assert _publish(srv2, (page2["ticket"], "old"))["statuses"] == ["CREATED"]
        assert srv2.handle({"op": "exists", "keys": ["old"]})["found"] == [True]
    finally:
        srv2.close()
        fresh.close()


def test_never_abandoned_quarantine_expires(server, handler, clock):
    pages = _reserve(server, PAGE)
    server.handle({"op": "quarantine", "tickets": [pages[0]["ticket"]]})
    allocated = handler.owned_region_manager.get_stats()["total_allocated_pages"]
    clock.advance(601.0)
    server.sweep()
    assert server.handle({"op": "stats"})["quarantined"] == 0
    assert (
        handler.owned_region_manager.get_stats()["total_allocated_pages"]
        == allocated - 1
    )


def test_full_pool_is_reported_with_a_code(server, handler):
    with patch.object(
        handler,
        "alloc",
        side_effect=ValueError(
            "Cannot allocate page: pool exhausted after expansion attempt"
        ),
    ):
        r = server.handle({"op": "reserve", "client_id": "w", "sizes": [PAGE]})
    assert r["ok"] is False and r["code"] == "POOL_FULL"


def test_other_allocation_faults_are_not_reported_as_full(server, handler):
    fault = ValueError("Failed to get buffer view for region 7")
    with patch.object(handler, "alloc", side_effect=fault):
        r = server.handle({"op": "reserve", "client_id": "w", "sizes": [PAGE]})
    assert r["ok"] is False and "code" not in r and "buffer view" in r["error"]


def test_requests_from_an_earlier_run_are_refused_unexecuted(server):
    page = _reserve(server, PAGE)[0]
    r = server.handle(
        {
            "op": "publish",
            "generation": "old-run",
            "entries": [{"ticket": page["ticket"], "key": "k"}],
        }
    )
    assert r["ok"] is False and r["generation"] == server.generation
    assert server.handle({"op": "stats"})["reservations"] == 1  # nothing ran


def test_a_pinned_stale_key_is_rejected_not_reported_stored(
    server, handler, server_port
):
    page = _reserve(server, PAGE)[0]
    _publish(server, (page["ticket"], "old"))
    assert handler.batch_pin(["old"]) == [True]  # an earlier run died holding a read
    fresh = MaruHandler(
        MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=16 * PAGE,
            chunk_size_bytes=PAGE,
            auto_connect=False,
            use_async_rpc=False,
        )
    )
    fresh.connect()
    reset_fake_agents()
    srv2 = RemoteServer(
        fresh, NixlTransport("pool2", agent=FakeNixlAgent("pool2")), pool_id="p2"
    )
    try:
        allocated = fresh.owned_region_manager.get_stats()["total_allocated_pages"]
        page2 = _reserve(srv2, PAGE)[0]
        assert _publish(srv2, (page2["ticket"], "old"))["statuses"] == ["REJECTED"]
        assert (
            fresh.owned_region_manager.get_stats()["total_allocated_pages"] == allocated
        )
    finally:
        srv2.close()
        fresh.close()
