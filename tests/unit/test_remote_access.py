# SPDX-License-Identifier: Apache-2.0
"""MaruServer's remote access over a real in-process MaruServer.

The resource manager and NIXL are faked (MockShmClient maps anonymous
memory; FakeNixlAgent copies bytes between registered ranges).
"""

import threading
import time
from unittest.mock import patch

import pytest

from maru_remote.transport import NixlTransport, buffer_address
from maru_server.kv_manager import DeleteResult
from maru_server.remote_access import RemoteAccess
from maru_server.server import MaruServer
from tests.unit.remote_fakes import FakeClock, FakeNixlAgent, reset_fake_agents

PAGE = 64 * 1024


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def maru():
    srv = MaruServer()
    yield srv
    srv.close()


@pytest.fixture
def clock():
    return FakeClock()


def _access(maru, name="pool", **kw):
    kw.setdefault("pool_size", 16 * PAGE)
    kw.setdefault("page_bytes", PAGE)
    kw.setdefault("pool_id", "test-pool")
    return RemoteAccess(maru, NixlTransport(name, agent=FakeNixlAgent(name)), **kw)


@pytest.fixture
def access(maru, clock):
    acc = _access(maru, reservation_ttl_s=10.0, ticket_ttl_s=20.0, clock=clock)
    yield acc
    acc.close()


@pytest.fixture
def worker():
    t = NixlTransport("worker", agent=FakeNixlAgent("worker"))
    yield t
    t.close()


def _reserve(access, *sizes):
    r = access.handle({"op": "reserve", "client_id": "w1", "sizes": list(sizes)})
    assert r["ok"], r
    return r["pages"]


def _publish(access, *pairs):
    entries = [{"ticket": t, "key": k} for t, k in pairs]
    return access.handle({"op": "publish", "entries": entries})


def _used(access):
    return access.handle({"op": "stats"})["used_pages"]


def _delete_remote(maru, access, key):
    """Delete a remote key the way its owner does (compare-and-delete)."""
    look = access.handle(
        {"op": "lookup", "keys": [key], "ticket_id": "peek", "protect": False}
    )
    (entry,) = look["entries"]
    return maru.delete_kv_at(key, entry["region_id"], entry["offset"])


def _write_remote(worker, peer, page, payload: bytes):
    src = bytearray(payload)
    worker.register(buffer_address(src), len(src))
    worker.write(peer, [(buffer_address(src), page["base"] + page["offset"], len(src))])


def _read_remote(worker, peer, entry) -> bytes:
    dst = bytearray(entry["length"])
    worker.register(buffer_address(dst), len(dst))
    worker.read(
        peer, [(buffer_address(dst), entry["base"] + entry["offset"], len(dst))]
    )
    return bytes(dst)


def _fail_first_call(real, exc):
    calls = []

    def side_effect(*args, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            raise exc
        return real(*args, **kwargs)

    return side_effect


def test_hello_reports_layout_lifetimes_and_registered_region(access):
    r = access.handle({"op": "hello", "client_id": "w1"})
    assert r["ok"] and r["page_bytes"] == PAGE and r["protocol"] == 1
    assert r["reservation_ttl_s"] == 10.0 and r["ticket_ttl_s"] == 20.0
    assert r["pool_id"] == "test-pool" and r["generation"] == access.generation
    assert len(r["regions"]) == 1 and r["regions"][0]["length"] == 16 * PAGE
    assert (r["regions"][0]["base"], 16 * PAGE) in FakeNixlAgent.registry[
        "pool"
    ].registered


def test_every_reply_carries_the_generation(access):
    assert access.handle({"op": "ping"})["generation"] == access.generation
    bad = access.handle({"op": "nope"})
    assert bad["ok"] is False and bad["generation"] == access.generation


def test_sized_reserve_write_publish_lookup_read_roundtrip(access, worker):
    peer = worker.add_peer(access.handle({"op": "hello", "client_id": "w1"})["nixl_md"])
    pages = _reserve(access, 1000, PAGE)
    assert [p["length"] for p in pages] == [1000, PAGE]
    _write_remote(worker, peer, pages[0], b"a" * 1000)
    _write_remote(worker, peer, pages[1], b"b" * PAGE)
    pub = _publish(access, (pages[0]["ticket"], "k-a"), (pages[1]["ticket"], "k-b"))
    assert pub["statuses"] == ["CREATED", "CREATED"]
    found = access.handle({"op": "exists", "keys": ["k-a", "k-b", "k-c"]})["found"]
    assert found == [True, True, False]
    look = access.handle(
        {"op": "lookup", "keys": ["k-a", "k-c"], "ticket_id": "r1", "protect": True}
    )
    assert look["entries"][1] is None
    assert look["entries"][0]["length"] == 1000  # the stored size, not the page
    assert _read_remote(worker, peer, look["entries"][0]) == b"a" * 1000
    assert access.handle({"op": "release", "ticket_id": "r1"})["released"] == 1


def test_published_keys_are_in_the_servers_ledger(access, maru):
    page = _reserve(access, 1000)[0]
    _publish(access, (page["ticket"], "k"))
    entry = maru.lookup_kv("k")
    assert entry["handle"].region_id == page["region_id"]
    assert (entry["kv_offset"], entry["kv_length"]) == (page["offset"], 1000)


def test_duplicate_publish_keeps_first_value_and_frees_page(access):
    pages = _reserve(access, PAGE, PAGE)
    assert _publish(access, (pages[0]["ticket"], "dup"))["statuses"] == ["CREATED"]
    assert _publish(access, (pages[1]["ticket"], "dup"))["statuses"] == [
        "ALREADY_PRESENT"
    ]
    stats = access.handle({"op": "stats"})
    assert stats["reservations"] == 0 and stats["used_pages"] == 1


def test_oversized_or_empty_reserve_is_rejected(access):
    for sizes in ([PAGE + 1], [], [True]):
        r = access.handle({"op": "reserve", "client_id": "w", "sizes": sizes})
        assert r["ok"] is False


def test_unknown_ticket_is_an_error(access):
    r = _publish(access, ("nope", "k"))
    assert r["ok"] is False and "ticket" in r["error"]


def test_abandon_frees_reserved_pages(access):
    pages = _reserve(access, PAGE, PAGE, PAGE)
    r = access.handle({"op": "abandon", "tickets": [p["ticket"] for p in pages[:2]]})
    assert r["freed"] == 2 and access.handle({"op": "stats"})["reservations"] == 1
    assert _used(access) == 1


def test_expired_reservation_is_reclaimed_by_sweep(access, clock):
    _reserve(access, PAGE)
    clock.advance(9.0)
    access.sweep()
    assert access.handle({"op": "stats"})["reservations"] == 1
    clock.advance(2.0)
    access.sweep()
    assert access.handle({"op": "stats"})["reservations"] == 0
    assert _used(access) == 0


def test_quarantined_pages_survive_expiry_until_abandoned(access, clock):
    pages = _reserve(access, PAGE, PAGE)
    tickets = [p["ticket"] for p in pages]
    assert access.handle({"op": "quarantine", "tickets": tickets})["quarantined"] == 2
    clock.advance(100.0)  # past the reservation lifetime, within quarantine
    access.sweep()
    stats = access.handle({"op": "stats"})
    assert stats["quarantined"] == 2 and stats["reservations"] == 0
    assert stats["used_pages"] == 2
    assert _publish(access, (tickets[0], "k"))["ok"] is False  # never publishable
    assert access.handle({"op": "abandon", "tickets": tickets})["freed"] == 2
    assert access.handle({"op": "stats"})["quarantined"] == 0
    assert _used(access) == 0


def test_never_abandoned_quarantine_expires(access, clock):
    pages = _reserve(access, PAGE)
    access.handle({"op": "quarantine", "tickets": [pages[0]["ticket"]]})
    clock.advance(601.0)
    access.sweep()
    assert access.handle({"op": "stats"})["quarantined"] == 0
    assert _used(access) == 0


def test_protected_key_refuses_delete_until_release(access, maru):
    page = _reserve(access, PAGE)[0]
    _publish(access, (page["ticket"], "pinned"))
    access.handle(
        {"op": "lookup", "keys": ["pinned"], "ticket_id": "r9", "protect": True}
    )
    assert _delete_remote(maru, access, "pinned") is DeleteResult.PINNED
    access.handle({"op": "release", "ticket_id": "r9"})
    assert _delete_remote(maru, access, "pinned") is DeleteResult.DELETED


def test_expired_ticket_is_unpinned_by_sweep(access, maru, clock):
    page = _reserve(access, PAGE)[0]
    _publish(access, (page["ticket"], "late"))
    access.handle(
        {"op": "lookup", "keys": ["late"], "ticket_id": "r5", "protect": True}
    )
    clock.advance(21.0)
    access.sweep()
    assert access.handle({"op": "stats"})["tickets"] == 0
    assert _delete_remote(maru, access, "late") is DeleteResult.DELETED


def test_reused_ticket_id_is_rejected(access):
    page = _reserve(access, PAGE)[0]
    _publish(access, (page["ticket"], "k"))
    msg = {"op": "lookup", "keys": ["k"], "ticket_id": "same", "protect": True}
    assert access.handle(msg)["ok"] is True
    assert access.handle(msg)["ok"] is False


def test_new_region_bumps_md_version_and_registers_it(access):
    v0 = access.handle({"op": "hello", "client_id": "w1"})["md_version"]
    pages = _reserve(access, *([PAGE] * 20))  # more than one region holds
    assert len({p["region_id"] for p in pages}) == 2
    meta = access.handle({"op": "metadata"})
    assert meta["md_version"] > v0 and len(meta["regions"]) == 2
    fake = FakeNixlAgent.registry["pool"]
    assert all((r["base"], r["length"]) in fake.registered for r in meta["regions"])


def test_remote_regions_are_hidden_from_local_clients(access, maru):
    _reserve(access, *([PAGE] * 20))  # two remote regions
    assert maru.list_allocations() == []
    assert maru.get_stats()["allocation_manager"]["num_allocations"] == 2


def test_sweep_keeps_failed_ticket_and_retries_next_sweep(access, maru, clock):
    page = _reserve(access, PAGE)[0]
    _publish(access, (page["ticket"], "flaky"))
    access.handle(
        {"op": "lookup", "keys": ["flaky"], "ticket_id": "rf", "protect": True}
    )
    clock.advance(21.0)
    flaky = _fail_first_call(maru.batch_unpin, TimeoutError("unpin failed"))
    with patch.object(maru, "batch_unpin", side_effect=flaky):
        access.sweep()  # must not raise
        assert access.handle({"op": "stats"})["tickets"] == 1
        assert _delete_remote(maru, access, "flaky") is DeleteResult.PINNED
        access.sweep()
    assert access.handle({"op": "stats"})["tickets"] == 0
    assert _delete_remote(maru, access, "flaky") is DeleteResult.DELETED


def test_release_keeps_the_ticket_when_unpin_fails(access, maru):
    page = _reserve(access, PAGE)[0]
    _publish(access, (page["ticket"], "sticky"))
    access.handle(
        {"op": "lookup", "keys": ["sticky"], "ticket_id": "rs", "protect": True}
    )
    flaky = _fail_first_call(maru.batch_unpin, TimeoutError("unpin failed"))
    with patch.object(maru, "batch_unpin", side_effect=flaky):
        assert access.handle({"op": "release", "ticket_id": "rs"})["ok"] is False
        assert access.handle({"op": "stats"})["tickets"] == 1
        assert access.handle({"op": "release", "ticket_id": "rs"})["released"] == 1
    assert _delete_remote(maru, access, "sticky") is DeleteResult.DELETED


def test_a_key_a_local_client_holds_is_missing_and_rejected(access, maru):
    handle = maru.request_alloc("local-client", 16 * PAGE)
    assert maru.register_kv("local-key", handle.region_id, 0, PAGE) is True
    assert access.handle({"op": "exists", "keys": ["local-key"]})["found"] == [False]
    look = access.handle(
        {"op": "lookup", "keys": ["local-key"], "ticket_id": "t", "protect": True}
    )
    assert look["ok"] and look["entries"] == [None]
    assert access.handle({"op": "stats"})["tickets"] == 0  # nothing pinned
    page = _reserve(access, PAGE)[0]
    assert _publish(access, (page["ticket"], "local-key"))["statuses"] == ["REJECTED"]
    assert _used(access) == 0  # the reserved page went back
    assert maru.lookup_kv("local-key")["handle"].region_id == handle.region_id


def test_a_local_client_cannot_delete_or_free_remote_state(access, maru):
    page = _reserve(access, PAGE)[0]
    _publish(access, (page["ticket"], "k"))
    owner = access._owner
    assert maru.delete_kv("k") is False  # the RPC path refuses remote keys
    assert maru.exists_kv("k")
    assert maru.return_alloc(owner, page["region_id"]) is False
    assert maru.request_alloc(owner, 16 * PAGE) is None
    assert maru.get_stats()["allocation_manager"]["num_allocations"] == 1


def test_a_same_named_local_key_is_never_deleted_by_eviction(maru):
    acc = _access(maru, capacity_bytes=1 * PAGE)
    try:
        _fill(acc, "x")
        # The ledger entry is replaced behind remote access's back.
        local = maru.request_alloc("local-client", 16 * PAGE)
        entry = maru._kv_manager.lookup("x")
        entry.region_id, entry.kv_offset = local.region_id, 0
        _fill(acc, "y")  # needs a page: remote access tries to evict x
        assert maru.lookup_kv("x")["handle"].region_id == local.region_id
        assert acc.handle({"op": "stats"})["evicted"] == 0
    finally:
        acc.close()


def test_a_key_that_vanishes_before_its_pin_does_not_hide_later_keys(access, maru):
    pages = _reserve(access, PAGE, PAGE)
    _publish(access, (pages[0]["ticket"], "a"), (pages[1]["ticket"], "b"))
    real = maru.batch_pin_lookup

    def vanish(keys, region_ids):
        maru.delete_kv_at("a", pages[0]["region_id"], pages[0]["offset"])
        return real(keys, region_ids)

    with patch.object(maru, "batch_pin_lookup", side_effect=vanish):
        look = access.handle(
            {"op": "lookup", "keys": ["a", "b"], "ticket_id": "t", "protect": True}
        )
    assert look["entries"][0] is None and look["entries"][1] is not None
    assert access._tickets["t"].keys == ["b"]  # only what was pinned


def test_growing_the_pool_does_not_hold_up_ledger_calls(maru):
    acc = _access(maru, pool_size=4 * PAGE)
    real = acc._transport.register

    def slow_register(*args, **kwargs):
        time.sleep(0.3)  # NIC registration of a large region
        return real(*args, **kwargs)

    worst = 0.0
    done = threading.Event()

    def grow():
        with patch.object(acc._transport, "register", side_effect=slow_register):
            _reserve(acc, *([PAGE] * 6))  # needs a second region
        done.set()

    t = threading.Thread(target=grow)
    try:
        t.start()
        while not done.wait(0.01):
            t0 = time.monotonic()
            maru.batch_lookup_kv(["k"])  # takes the server lock, like most RPCs
            worst = max(worst, time.monotonic() - t0)
        t.join()
        assert acc.handle({"op": "stats"})["regions"] == 2
        assert worst < 0.05
    finally:
        acc.close()


def test_remote_regions_are_mapped_without_prefault(maru):
    acc = _access(maru)
    calls = []
    real = acc._mapper.map_region

    def record(handle, prefault=True):
        calls.append(prefault)
        return real(handle, prefault=prefault)

    try:
        with patch.object(acc._mapper, "map_region", side_effect=record):
            _reserve(acc, *([PAGE] * 20))  # grows the pool by one region
        assert calls and not any(calls)
    finally:
        acc.close()


def test_a_failed_publish_returns_the_pages(access, maru):
    page = _reserve(access, PAGE)[0]
    with patch.object(
        maru, "batch_register_or_lookup", side_effect=RuntimeError("boom")
    ):
        r = _publish(access, (page["ticket"], "k"))
    assert r["ok"] is False and "boom" in r["error"]
    assert _used(access) == 0 and access.handle({"op": "stats"})["reservations"] == 0


def test_full_pool_is_reported_with_a_code(access, maru):
    with (
        patch.object(access._owned, "allocate", return_value=None),
        patch.object(maru, "request_alloc", return_value=None),
    ):
        r = access.handle({"op": "reserve", "client_id": "w", "sizes": [PAGE]})
    assert r["ok"] is False and r["code"] == "POOL_FULL"


def test_other_allocation_faults_are_not_reported_as_full(access):
    fault = RuntimeError("Failed to get buffer view for region 7")
    with patch.object(access._owned, "allocate", side_effect=fault):
        r = access.handle({"op": "reserve", "client_id": "w", "sizes": [PAGE]})
    assert r["ok"] is False and "code" not in r and "buffer view" in r["error"]


def test_requests_from_an_earlier_run_are_refused_unexecuted(access):
    page = _reserve(access, PAGE)[0]
    r = access.handle(
        {
            "op": "publish",
            "generation": "old-run",
            "entries": [{"ticket": page["ticket"], "key": "k"}],
        }
    )
    assert r["ok"] is False and r["generation"] == access.generation
    assert access.handle({"op": "stats"})["reservations"] == 1  # nothing ran


def test_close_removes_the_keys_and_returns_the_regions(maru):
    acc = _access(maru)
    _fill(acc, "a", "b")
    acc.handle({"op": "lookup", "keys": ["a"], "ticket_id": "t", "protect": True})
    acc.close()
    assert maru.batch_exists_kv(["a", "b"]) == [False, False]
    assert maru.get_stats()["allocation_manager"]["num_allocations"] == 0
    assert acc.handle({"op": "ping"})["ok"] is False  # closed
    acc.close()  # idempotent


def test_mapped_regions_are_not_registered_with_cuda(maru):
    acc = _access(maru)
    try:
        assert acc._mapper._cuda_register is False
        regions = [acc._mapper.get_region(r) for r in acc._regions]
        assert regions and not any(r._cuda_pinned for r in regions)
    finally:
        acc.close()


def _fill(acc, *keys):
    for key in keys:
        (page,) = _reserve(acc, PAGE)
        assert _publish(acc, (page["ticket"], key))["statuses"] == ["CREATED"]


def test_a_full_pool_evicts_the_least_recently_read_key(maru):
    acc = _access(maru, capacity_bytes=3 * PAGE)
    try:
        _fill(acc, "k1", "k2", "k3")
        r = acc.handle(
            {"op": "lookup", "keys": ["k1"], "ticket_id": "t", "protect": False}
        )
        assert r["entries"][0] is not None  # k1 is now the most recently read
        _fill(acc, "k4")
        assert maru.batch_exists_kv(["k1", "k2", "k3", "k4"]) == [
            True,
            False,
            True,
            True,
        ]
        stats = acc.handle({"op": "stats"})
        assert stats["evicted"] == 1 and stats["used_pages"] == 3
        assert acc.handle({"op": "ping"})["evictions"] == 1  # on every reply
        r = acc.handle({"op": "evicted_since", "since": 0})
        assert r["keys"] == ["k2"] and r["complete"] is True
        r = acc.handle({"op": "evicted_since", "since": 1})
        assert r["keys"] == [] and r["complete"] is True
    finally:
        acc.close()


def test_eviction_skips_pinned_keys(maru):
    acc = _access(maru, capacity_bytes=2 * PAGE)
    try:
        _fill(acc, "a", "b")
        assert maru.batch_pin_kv(["a"]) == [True]  # a reader holds the oldest key
        _fill(acc, "c")
        assert maru.batch_exists_kv(["a", "b", "c"]) == [True, False, True]
        maru.batch_unpin(["a"])
    finally:
        acc.close()


def test_a_full_pool_without_eviction_reports_pool_full(maru):
    acc = _access(maru, capacity_bytes=PAGE, evict=False)
    try:
        _fill(acc, "only")
        r = acc.handle({"op": "reserve", "client_id": "w", "sizes": [PAGE]})
        assert r["ok"] is False and r["code"] == "POOL_FULL"
    finally:
        acc.close()


def test_a_pool_of_pinned_keys_reports_pool_full(maru):
    acc = _access(maru, capacity_bytes=PAGE)
    try:
        _fill(acc, "held")
        assert maru.batch_pin_kv(["held"]) == [True]
        r = acc.handle({"op": "reserve", "client_id": "w", "sizes": [PAGE]})
        assert r["ok"] is False and r["code"] == "POOL_FULL"
        assert maru.exists_kv("held")
        maru.batch_unpin(["held"])
    finally:
        acc.close()


def test_evicted_since_reports_a_truncated_log(maru):
    acc = _access(maru, capacity_bytes=PAGE, eviction_log_len=1)
    try:
        _fill(acc, "a", "b", "c")  # evicts a, then b; the log keeps only b
        r = acc.handle({"op": "evicted_since", "since": 0})
        assert r["keys"] == ["b"] and r["complete"] is False
        assert acc.handle({"op": "evicted_since", "since": 1})["complete"] is True
        assert acc.handle({"op": "evicted_since", "since": -1})["ok"] is False
    finally:
        acc.close()


def test_a_region_the_device_refuses_at_start_is_an_error(maru):
    with (
        patch.object(maru, "request_alloc", return_value=None),
        pytest.raises(RuntimeError, match="first remote region"),
    ):
        _access(maru)


def _remote_args(**overrides):
    import argparse

    from maru_server.server import _add_remote_arguments

    parser = argparse.ArgumentParser()
    _add_remote_arguments(parser)
    argv = ["--remote-pool-size", "1M", "--remote-page-bytes", "64K"]
    for name, value in overrides.items():
        argv += [f"--{name.replace('_', '-')}", value]
    return parser.parse_args(argv)


def _free_port():
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_remote_options_parse_sizes_and_defaults():
    args = _remote_args(remote_capacity="4M", remote_eviction="none")
    assert args.remote_bind is None  # remote access stays off without it
    assert (args.remote_pool_size, args.remote_page_bytes) == (1024**2, 64 * 1024)
    assert args.remote_capacity == 4 * 1024**2 and args.remote_eviction == "none"
    assert args.remote_pool_id == "maru" and args.remote_quarantine_ttl == 600.0


def _fake_transport(name, ucx_device=""):
    return NixlTransport(name, agent=FakeNixlAgent(name))


def test_maru_server_serves_the_remote_endpoint_on_its_own_thread(maru):
    from maru_remote.client import RemoteClient
    from maru_server.server import _start_remote

    url = f"tcp://127.0.0.1:{_free_port()}"
    args = _remote_args(remote_bind=url, remote_pool_id="cli")
    failures = []
    with patch("maru_remote.transport.NixlTransport", side_effect=_fake_transport):
        endpoint = _start_remote(maru, args, lambda: failures.append(True))
    client = RemoteClient(url, None, client_id="probe")
    try:
        assert endpoint.thread.name == "maru-remote" and endpoint.thread.is_alive()
        assert client.ping() >= 0.0
        assert client.stats()["page_bytes"] == 64 * 1024
    finally:
        client.close()
        endpoint.close()
    assert not endpoint.thread.is_alive() and not failures and not endpoint.failed
    assert maru.get_stats()["allocation_manager"]["num_allocations"] == 0


def test_a_dying_remote_endpoint_stops_the_server(maru):
    from maru_server.server import _start_remote

    args = _remote_args(remote_bind=f"tcp://127.0.0.1:{_free_port()}")
    stopped = threading.Event()

    def broken(access, bind_url, *, stop_event, ready):
        ready.set()  # serving had started
        raise RuntimeError("loop broke")

    with (
        patch("maru_remote.transport.NixlTransport", side_effect=_fake_transport),
        patch("maru_server.remote_access.serve_remote", side_effect=broken),
    ):
        endpoint = _start_remote(maru, args, stopped.set)
        assert stopped.wait(5) and endpoint.failed
    endpoint.close()
    assert maru.get_stats()["allocation_manager"]["num_allocations"] == 0


def test_an_endpoint_that_cannot_bind_fails_start_and_releases_everything(maru):
    import zmq

    from maru_server.server import _start_remote

    url = f"tcp://127.0.0.1:{_free_port()}"
    holder = zmq.Context.instance().socket(zmq.REP)
    holder.bind(url)  # the port is taken
    called = []
    try:
        with (
            patch("maru_remote.transport.NixlTransport", side_effect=_fake_transport),
            pytest.raises(RuntimeError, match="did not start"),
        ):
            _start_remote(maru, _remote_args(remote_bind=url), lambda: called.append(1))
    finally:
        holder.close(linger=0)
    assert called == []  # no signal: start reports the failure itself
    assert maru.get_stats()["allocation_manager"]["num_allocations"] == 0


def test_a_region_that_fails_part_way_is_released(maru):
    acc = _access(maru, name="undo", pool_size=4 * PAGE)
    try:
        before = set(FakeNixlAgent.registry["undo"].registered)
        with patch.object(
            acc._owned, "add_region", side_effect=ValueError("allocator init failed")
        ):
            r = acc.handle(
                {"op": "reserve", "client_id": "w", "sizes": [PAGE] * 5}
            )  # the fifth page needs a second region
        assert r["ok"] is False and "allocator init" in r["error"]
        assert set(FakeNixlAgent.registry["undo"].registered) == before
        assert maru.get_stats()["allocation_manager"]["num_allocations"] == 1
        assert len(acc._mapper._regions) == 1 and _used(acc) == 0
    finally:
        acc.close()
