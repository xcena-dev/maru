# SPDX-License-Identifier: Apache-2.0
"""MaruHandler(storage_backend="remote") against an in-process pool node.

The pool node is real code: a MaruServer, a CXL-backend MaruHandler on
anonymous memory, and a RemoteServer on a ZMQ REP loop. Only NIXL is faked,
by an agent that copies bytes between registered ranges.
"""

import threading
import time
from unittest.mock import patch

import pytest

from maru_common.config import MaruConfig
from maru_common.storage_types import StorageError, StorageUnavailableError
from maru_handler import MaruHandler
from maru_remote.server import RemoteServer, serve_forever
from maru_remote.transport import NixlTransport
from tests.unit.remote_fakes import FakeClock, FakeNixlAgent, reset_fake_agents

PAGE = 64 * 1024
POOL_AGENT = "maru-remote-test"


@pytest.fixture(autouse=True)
def _no_background_maintenance(monkeypatch):
    """Tests run maintenance rounds themselves unless they shorten the period."""
    import maru_handler.storage.remote as remote_mod

    monkeypatch.setattr(remote_mod, "_MAINTAIN_INTERVAL_S", 3600.0)


@pytest.fixture(autouse=True)
def _fake_nixl():
    reset_fake_agents()

    def factory(name, ucx_device):
        return NixlTransport(name, agent=FakeNixlAgent(name))

    with patch("maru_handler.storage.remote._default_transport", side_effect=factory):
        yield
    reset_fake_agents()


@pytest.fixture
def pool_handler(server_thread, server_port):
    h = MaruHandler(
        MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=32 * PAGE,
            chunk_size_bytes=PAGE,
            auto_connect=False,
            use_async_rpc=False,
        )
    )
    h.connect()
    yield h
    h.close()


class PoolNode:
    """A RemoteServer on its own REP thread; restartable on the same port."""

    def __init__(self, handler, port, ttl=10.0, **server_kw):
        self.handler = handler
        self.server_kw = server_kw
        self.url = f"tcp://127.0.0.1:{port}"
        self.ttl = ttl
        self.server = None
        self._stop = None
        self._thread = None

    def start(self):
        self.server = RemoteServer(
            self.handler,
            NixlTransport(POOL_AGENT, agent=FakeNixlAgent(POOL_AGENT)),
            pool_id="test",
            reservation_ttl_s=self.ttl,
            **self.server_kw,
        )
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=serve_forever,
            args=(self.server, self.url),
            kwargs={"stop_event": self._stop, "sweep_interval_s": 0.05},
            daemon=True,
        )
        self._thread.start()
        time.sleep(0.05)

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)
        self.server.close()

    def stats(self):
        return self.server.handle({"op": "stats"})


@pytest.fixture
def pool(pool_handler, unused_port):
    node = PoolNode(pool_handler, unused_port)
    node.start()
    yield node
    node.stop()


@pytest.fixture
def pool_no_evict(pool_handler, unused_port):
    node = PoolNode(pool_handler, unused_port, evict=False)
    node.start()
    yield node
    node.stop()


@pytest.fixture
def unused_port():
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def remote_handler(
    url, *, namespace="ns-a", staging=8 * PAGE, metadata_only=False, **kw
):
    cfg = MaruConfig(
        storage_backend="remote",
        remote_url=url,
        cache_namespace=namespace,
        pool_size=0 if metadata_only else staging,
        chunk_size_bytes=PAGE,
        metadata_only=metadata_only,
        auto_connect=False,
        timeout_ms=300,
        remote_transfer_timeout_s=kw.pop("transfer_timeout", 0.05),
        remote_retry_s=kw.pop("retry_s", 5.0),
        **kw,
    )
    h = MaruHandler(cfg)
    assert h.connect()
    return h


def _store(h, key, payload: bytes):
    a = h.alloc(len(payload))
    a.buf[:] = payload
    return h.batch_store([key], [a])[0]


def test_store_exists_retrieve_release_roundtrip(pool):
    h = remote_handler(pool.url)
    assert _store(h, "k1", b"x" * 1000) is True
    assert h.batch_exists(["k1", "k2"]) == [True, False]
    assert h.has_local("k1")
    lease, missing = h.batch_retrieve(["k1", "k2"])
    assert missing is None and bytes(lease.view) == b"x" * 1000
    staging = h.get_stats()["remote_storage"]
    assert staging["outstanding_leases"] == 1
    h.release_retrieved([lease, missing])
    stats = h.get_stats()["remote_storage"]
    assert (
        stats["outstanding_leases"] == 0
        and stats["staging_free"] == stats["staging_slots"]
    )
    assert stats["server"]["tickets"] == 0  # unpinned right after the READ
    h.close()


def test_two_workers_share_one_namespace(pool):
    a = remote_handler(pool.url)
    b = remote_handler(pool.url)
    assert _store(a, "shared", b"\x01\x02" * 500)
    (lease,) = b.batch_retrieve(["shared"])
    assert bytes(lease.view) == b"\x01\x02" * 500
    lease.release()
    assert _store(b, "shared", b"other" * 10) is True  # already present: True
    (again,) = a.batch_retrieve(["shared"])
    assert bytes(again.view) == b"\x01\x02" * 500  # first value wins
    again.release()
    a.close()
    b.close()


def test_namespaces_do_not_share_keys(pool):
    a = remote_handler(pool.url, namespace="ns-a")
    other = remote_handler(pool.url, namespace="ns-b")
    assert _store(a, "k", b"a" * 10)
    assert other.batch_exists(["k"]) == [False]
    assert other.batch_retrieve(["k"]) == [None]
    a.close()
    other.close()


def test_metadata_only_handler_checks_existence_only(pool):
    a = remote_handler(pool.url)
    _store(a, "k", b"z")
    sched = remote_handler(pool.url, metadata_only=True)
    assert sched.batch_exists(["k", "nope"]) == [True, False]
    with pytest.raises(RuntimeError):
        sched.alloc(1)
    assert FakeNixlAgent.registry.keys() >= {POOL_AGENT}
    assert not any(
        n.startswith("maru-remote-client") and n.endswith(sched.instance_id)
        for n in FakeNixlAgent.registry
    )  # no NIXL agent for control-only
    sched.close()
    a.close()


def test_unreachable_server_is_unavailable_not_a_config_error(unused_port):
    with pytest.raises(StorageUnavailableError):
        remote_handler(f"tcp://127.0.0.1:{unused_port}")


def test_page_smaller_than_an_object_is_a_config_error(pool):
    cfg = MaruConfig(
        storage_backend="remote",
        remote_url=pool.url,
        cache_namespace="ns",
        pool_size=8 * PAGE * 2,
        chunk_size_bytes=2 * PAGE,
        auto_connect=False,
        timeout_ms=300,
    )
    with pytest.raises(StorageError, match="page"):
        MaruHandler(cfg).connect()


def test_reservation_lifetime_must_cover_two_transfer_timeouts(pool):
    with pytest.raises(StorageError, match="twice"):
        remote_handler(pool.url, transfer_timeout=6.0)  # pool TTL is 10 s


def test_outage_skips_calls_until_retry_then_reconnects(pool):
    h = remote_handler(pool.url)
    clock = FakeClock()
    h._storage._clock = clock
    assert _store(h, "before", b"1")
    pool.stop()
    t0 = time.monotonic()
    assert h.batch_exists(["before"]) == [False]  # one control timeout trips it
    assert h.batch_exists(["before"]) == [False]  # skipped without waiting
    assert time.monotonic() - t0 < 1.0
    with pytest.raises(StorageUnavailableError):
        h.alloc(1)
    with pytest.raises(StorageUnavailableError):
        h.batch_retrieve(["before"])
    pool.start()  # a new server generation
    clock.advance(6.0)
    assert h.batch_exists(["before"]) == [False]  # the scheduler path never reconnects
    h._storage.maintain()  # the maintenance thread's round reconnects
    assert h.batch_exists(["before"]) == [True]  # keys live in the MaruServer
    assert not h.has_local("before")  # the restart cleared what this handler knew
    assert _store(h, "after", b"2")
    h.close()


def test_restart_is_detected_and_the_call_retried(pool):
    h = remote_handler(pool.url)
    assert _store(h, "k", b"1")
    pool.stop()
    pool.start()
    # The scheduler path reports misses and leaves the reconnect to the
    # maintenance thread, which answers from the new run.
    h._storage.maintain()
    assert h.batch_exists(["k"]) == [True]
    assert not h.has_local("k")  # what this handler knew belongs to the old run
    (lease,) = h.batch_retrieve(["k"])
    assert bytes(lease.view) == b"1"
    lease.release()
    pool.stop()
    pool.start()
    assert _store(h, "k2", b"2") is True  # reserve retried after the restart
    h.close()


def test_write_timeout_isolates_slots_and_pages_until_it_ends(pool):
    h = remote_handler(pool.url, staging=4 * PAGE)
    client_agent = next(
        a
        for n, a in FakeNixlAgent.registry.items()
        if n.startswith("maru-remote-client")
    )
    client_agent.stall = True
    assert _store(h, "slow", b"s" * PAGE) is False
    stats = h.get_stats()["remote_storage"]
    assert stats["quarantined_slots"] == 1 and stats["staging_free"] == 3
    server = pool.stats()
    assert server["quarantined"] == 1 and server["reservations"] == 0
    client_agent.stall = False
    client_agent.finish_stalled()  # the late WRITE lands in the quarantined page
    h._storage.maintain()  # the server answers the probe: reconnect now
    assert _store(h, "next", b"n" * 10) is True
    stats = h.get_stats()["remote_storage"]
    assert stats["quarantined_slots"] == 0 and stats["staging_free"] == 4
    assert pool.stats()["quarantined"] == 0
    assert h.batch_exists(["slow"]) == [False]  # the timed-out key was never published
    h.close()


def test_read_timeout_isolates_slots_and_releases_protection(pool):
    h = remote_handler(pool.url, staging=4 * PAGE)
    assert _store(h, "k", b"r" * 100)
    agent = next(
        a
        for n, a in FakeNixlAgent.registry.items()
        if n.startswith("maru-remote-client")
    )
    agent.stall = True
    with pytest.raises(StorageUnavailableError):
        h.batch_retrieve(["k"])
    assert h.get_stats()["remote_storage"]["quarantined_slots"] == 1
    assert pool.stats()["tickets"] == 0  # a late READ only lands in the isolated slot
    agent.stall = False
    agent.finish_stalled()
    h._storage.maintain()  # the server answers the probe: reconnect now
    (lease,) = h.batch_retrieve(["k"])
    assert bytes(lease.view) == b"r" * 100
    lease.release()
    assert h.get_stats()["remote_storage"]["quarantined_slots"] == 0
    h.close()


def test_full_staging_fails_fast_and_releases_protection(pool):
    h = remote_handler(pool.url, staging=2 * PAGE)
    assert _store(h, "a", b"1") and _store(h, "b", b"2")
    held = h.alloc(1)
    with pytest.raises(MemoryError):
        h.batch_retrieve(["a", "b"])  # two objects, one free slot
    assert pool.stats()["tickets"] == 0
    h.alloc(1)
    with pytest.raises(MemoryError):
        h.alloc(1)
    h.free(held)
    h.close()


def test_close_refuses_while_leases_are_outstanding(pool):
    h = remote_handler(pool.url)
    _store(h, "k", b"1")
    (lease,) = h.batch_retrieve(["k"])
    with pytest.raises(RuntimeError, match="lease"):
        h.close()
    lease.release()
    h.close()


def test_pin_and_delete_are_not_part_of_the_lease_contract(pool):
    h = remote_handler(pool.url)
    with pytest.raises(NotImplementedError, match="remote"):
        h.pin("k")
    with pytest.raises(NotImplementedError, match="remote"):
        h.delete("k")
    h.close()


def test_healthcheck_follows_the_remote_server(pool):
    h = remote_handler(pool.url)
    assert h.healthcheck() is True
    pool.stop()
    assert h.healthcheck() is False
    pool.start()
    h._storage.maintain()  # the server answers the probe: reconnect now
    assert h.healthcheck() is True
    h.close()


def test_connector_store_on_one_worker_load_on_another(pool):
    torch = pytest.importorskip("torch")
    pytest.importorskip("vllm")
    from types import SimpleNamespace

    from maru_vllm.connector import MaruConnectorMetadata, MaruReqMeta
    from tests.unit.vllm_connector_helpers import (
        make_flash_attn_metadata,
        make_scheduler,
        make_worker,
        store_metadata,
    )

    extra = {
        "maru_storage_backend": "remote",
        "maru_remote_url": pool.url,
        "maru_cache_namespace": "test-model-v1",
        "maru_remote_staging_size": "64K",
        "maru_remote_timeout_s": 0.05,
    }

    def caches(fill):
        return {
            f"model.layers.{i}.self_attn": torch.arange(
                8 * 2 * 4 * 2 * 8, dtype=torch.float32
            ).reshape(8, 2, 4, 2, 8)
            * fill
            + i * 2048
            for i in range(2)
        }

    writer, reader = (
        make_worker(4, 4, extra, num_kv_heads=2, head_size=8) for _ in range(2)
    )
    scheduler = make_scheduler(4, 4, extra)
    src, dst = caches(1.0), caches(0.0)
    for t in dst.values():
        t.zero_()
    attn = make_flash_attn_metadata()
    tokens = list(range(9))  # two full chunks plus one token of compute
    try:
        writer.register_kv_caches(src)
        reader.register_kv_caches(dst)
        meta = store_metadata(
            token_ids=tokens, block_ids=[0, 1, 2], num_scheduled_tokens=9
        )
        for name, tensor in reversed(list(src.items())):
            writer.save_kv_layer(name, tensor, attn, meta)
        request = SimpleNamespace(request_id="load", prompt_token_ids=tokens)
        assert scheduler.get_num_new_matched_tokens(request, 0) == (8, False)
        context = SimpleNamespace(
            attn_metadata=attn,
            no_compile_layers={n: SimpleNamespace(kv_cache=t) for n, t in dst.items()},
        )
        load = MaruConnectorMetadata(
            requests=[
                MaruReqMeta(
                    req_id="load",
                    token_ids=tokens,
                    block_ids=[4, 5, 6],
                    is_store=False,
                    num_matched_chunks=2,
                )
            ]
        )
        reader.start_load_kv(context, load)
        assert not reader.take_failed_load_blocks()
        for name in src:
            torch.testing.assert_close(dst[name][4:6], src[name][0:2], rtol=0, atol=0)
            assert torch.count_nonzero(dst[name][6]) == 0
        stats = reader._handler.get_stats()["remote_storage"]
        assert stats["outstanding_leases"] == 0 and stats["counters"]["loads"] == 1
        # Storing the same prefix again is skipped: the writer knows it is there.
        before = pool.stats()
        for name, tensor in src.items():
            writer.save_kv_layer(name, tensor, attn, meta)
        assert pool.stats()["reservations"] == before["reservations"] == 0
        # The reader read the prefix in this run, so it does not store it back.
        for name, tensor in dst.items():
            reader.save_kv_layer(name, tensor, attn, meta)
        counters = reader._handler.get_stats()["remote_storage"]["counters"]
        assert counters["stores"] == 0 and counters["store_bytes"] == 0
        # The pool goes away: the hit is reported as a load error (recompute).
        pool.stop()
        reader.start_load_kv(context, load)
        assert reader.take_failed_load_blocks() == {4, 5}
        pool.start()
    finally:
        for w in (writer, reader):
            w.shutdown()
        if scheduler._handler:
            scheduler._handler.close()


def test_a_restart_refuses_old_run_requests_before_running_them(pool):
    h = remote_handler(pool.url)
    assert _store(h, "k", b"1")
    pool.stop()
    pool.start()
    (lease,) = h.batch_retrieve(["k"])  # refused, reconnected, retried
    assert bytes(lease.view) == b"1"
    lease.release()
    assert pool.stats()["tickets"] == 0  # the refused attempt pinned nothing
    h.close()


def test_remembered_keys_are_revalidated_by_the_maintenance_thread(pool):
    h = remote_handler(pool.url)
    clock = FakeClock()
    h._storage._clock = clock
    h._storage._last_contact = clock()
    assert _store(h, "k", b"1") and h.has_local("k")
    pool.stop()
    pool.start()  # a new run that this handler has not talked to yet
    assert h.has_local("k")  # answered from memory: no call is made
    h._storage.maintain()  # the maintenance thread's ping sees the new run
    assert not h.has_local("k")
    h.close()


def test_cool_down_skips_abandon_of_isolated_pages(pool):
    h = remote_handler(pool.url, staging=4 * PAGE)
    agent = next(
        a
        for n, a in FakeNixlAgent.registry.items()
        if n.startswith("maru-remote-client")
    )
    agent.stall = True
    assert _store(h, "slow", b"s" * 10) is False  # isolates and trips the breaker
    pool.stop()
    agent.stall = False
    agent.finish_stalled()
    t0 = time.monotonic()
    for _ in range(3):
        with pytest.raises(StorageUnavailableError):
            h.alloc(1)  # the slot returns locally; no abandon call while cooling down
    assert time.monotonic() - t0 < 0.2
    assert h.get_stats()["remote_storage"]["quarantined_slots"] == 0
    pool.start()
    h.close()


def test_close_abandons_pages_of_ended_writes(pool):
    h = remote_handler(pool.url, staging=4 * PAGE)
    agent = next(
        a
        for n, a in FakeNixlAgent.registry.items()
        if n.startswith("maru-remote-client")
    )
    agent.stall = True
    assert _store(h, "slow", b"s" * 10) is False
    assert pool.stats()["quarantined"] == 1
    agent.stall = False
    agent.finish_stalled()
    h.close()
    assert pool.stats()["quarantined"] == 0


def test_a_full_pool_pauses_stores_but_not_loads(pool_no_evict):
    pool = pool_no_evict
    h = remote_handler(pool.url, staging=4 * PAGE)
    clock = FakeClock()
    h._storage._clock = clock
    assert _store(h, "kept", b"1")
    with patch.object(
        pool.handler,
        "alloc",
        side_effect=ValueError(
            "Cannot allocate page: pool exhausted after expansion attempt"
        ),
    ):
        assert _store(h, "more", b"2") is False  # reserve reports POOL_FULL
    with pytest.raises(StorageUnavailableError, match="full"):
        h.alloc(1)  # stores pause for the retry period
    (lease,) = h.batch_retrieve(["kept"])  # loads still work
    assert bytes(lease.view) == b"1"
    lease.release()
    clock.advance(6.0)
    assert _store(h, "more", b"2") is True  # the pause has ended
    h.close()


def test_a_refused_key_is_not_written_again_until_the_retry_period(pool):
    h = remote_handler(pool.url, staging=4 * PAGE)
    clock = FakeClock()
    h._storage._clock = clock

    def refuse(keys, handles):
        for handle in handles:
            pool.handler.free(handle)
        return [False] * len(keys)

    with patch.object(pool.handler, "batch_store", side_effect=refuse):
        assert _store(h, "stuck", b"1") is False  # the server answers REJECTED
        written = h.get_stats()["remote_storage"]["counters"]["store_bytes"]
        assert _store(h, "stuck", b"1") is False
        assert (
            h.get_stats()["remote_storage"]["counters"]["store_bytes"] == written
        )  # no WRITE
        assert _store(h, "other", b"2") is False  # other keys are still tried
        assert h.get_stats()["remote_storage"]["counters"]["store_bytes"] > written
    clock.advance(6.0)
    assert _store(h, "stuck", b"1") is True  # tried again after the period
    h.close()


def test_local_staging_errors_are_configuration_errors(pool):
    with patch(
        "maru_handler.storage.remote.StagingBuffer", side_effect=ValueError("bad size")
    ):
        with pytest.raises(StorageError, match="staging"):
            remote_handler(pool.url)


def test_alloc_and_lease_release_do_not_wait_for_a_transfer_in_flight(pool):
    import threading
    import time as _time

    h = remote_handler(pool.url, staging=4 * PAGE)
    assert _store(h, "k", b"x")
    (lease,) = h.batch_retrieve(["k"])
    io_lock = h._storage._lock
    held, done = threading.Event(), threading.Event()

    def hold_io_lock():  # stands in for a store or load running its RDMA
        with io_lock:
            held.set()
            done.wait(5)

    t = threading.Thread(target=hold_io_lock)
    t.start()
    held.wait(5)
    try:
        t0 = _time.monotonic()
        a = h.alloc(1)
        lease.release()
        h.free(a)
        assert h.retrieve_capacity() == 4
        assert _time.monotonic() - t0 < 0.5
    finally:
        done.set()
        t.join()
    h.close()


def test_has_local_does_not_wait_for_a_transfer_in_flight(pool):
    h = remote_handler(pool.url, staging=4 * PAGE)
    clock = FakeClock()
    h._storage._clock = clock
    assert _store(h, "k", b"x")
    held, done = threading.Event(), threading.Event()

    def hold_io_lock():
        with h._storage._lock:
            held.set()
            done.wait(5)

    t = threading.Thread(target=hold_io_lock)
    t.start()
    held.wait(5)
    try:
        t0 = time.monotonic()
        clock.advance(10.0)  # a run check would be due
        assert h.has_local("k")  # answered from memory, without the lock
        assert not h.has_local("other")
        assert time.monotonic() - t0 < 0.5
    finally:
        done.set()
        t.join()
    h.close()


def test_stores_beyond_capacity_evict_the_least_recently_read(
    pool_handler, unused_port
):
    node = PoolNode(pool_handler, unused_port, capacity_bytes=2 * PAGE)
    node.start()
    try:
        h = remote_handler(node.url, staging=4 * PAGE)
        assert _store(h, "old", b"1") and _store(h, "warm", b"2")
        (lease,) = h.batch_retrieve(["warm"])  # read: now most recent
        lease.release()
        assert _store(h, "new", b"3") is True
        assert h.batch_exists(["old", "warm", "new"]) == [False, True, True]
        assert node.stats()["evicted"] == 1
        h.close()
    finally:
        node.stop()


def test_a_key_read_in_this_run_is_not_stored_again(pool):
    writer, reader = remote_handler(pool.url), remote_handler(pool.url)
    assert _store(writer, "k", b"1" * 100)
    assert not reader.has_local("k")
    (lease,) = reader.batch_retrieve(["k"])
    lease.release()
    assert reader.has_local("k")
    (missing,) = reader.batch_retrieve(["gone"])
    assert missing is None and not reader.has_local("gone")
    writer.close()
    reader.close()


def test_a_store_of_a_present_key_skips_the_write(pool):
    writer, other = remote_handler(pool.url), remote_handler(pool.url)
    assert _store(writer, "k", b"1" * 100)
    assert _store(other, "k", b"1" * 100) is True  # stored by another writer
    counters = other.get_stats()["remote_storage"]["counters"]
    assert counters["store_skipped_present"] == 1 and counters["store_bytes"] == 0
    assert pool.stats()["reservations"] == 0
    assert other.has_local("k")
    writer.close()
    other.close()


def test_an_eviction_makes_the_writer_store_evicted_keys_again(
    pool_handler, unused_port
):
    node = PoolNode(pool_handler, unused_port, capacity_bytes=2 * PAGE)
    node.start()
    try:
        h = remote_handler(node.url, staging=4 * PAGE)
        assert _store(h, "old", b"1") and _store(h, "warm", b"2")
        (lease,) = h.batch_retrieve(["warm"])
        lease.release()
        assert _store(h, "new", b"3")  # evicts "old"
        # Only the evicted key is forgotten; the others stay remembered.
        assert not h.has_local("old")
        assert h.has_local("warm") and h.has_local("new")
        assert _store(h, "old", b"1")  # written again (evicts "warm" this time)
        assert h.batch_exists(["old"]) == [True]
        assert not h.has_local("warm")
        h.close()
    finally:
        node.stop()


def test_a_key_another_worker_evicted_is_dropped_on_the_next_call(
    pool_handler, unused_port
):
    node = PoolNode(pool_handler, unused_port, capacity_bytes=1 * PAGE)
    node.start()
    try:
        a, b = (remote_handler(node.url, staging=4 * PAGE) for _ in range(2))
        assert _store(a, "k1", b"1")
        assert _store(b, "k2", b"2")  # evicts k1
        assert a.has_local("k1")  # a has not talked to the server since
        (lease,) = a.batch_retrieve(["k2"])  # the reply reports an eviction
        lease.release()
        assert not a.has_local("k1") and a.has_local("k2")
        a.close()
        b.close()
    finally:
        node.stop()


def test_a_truncated_eviction_log_forgets_every_remembered_key(
    pool_handler, unused_port
):
    node = PoolNode(
        pool_handler, unused_port, capacity_bytes=1 * PAGE, eviction_log_len=1
    )
    node.start()
    try:
        a, b = (remote_handler(node.url, staging=4 * PAGE) for _ in range(2))
        assert _store(a, "mine", b"0")
        assert _store(b, "k1", b"1") and _store(b, "k2", b"2")  # 2 evictions
        (lease,) = a.batch_retrieve(["k2"])
        lease.release()
        assert not a.has_local("mine")  # the log lost an eviction
        assert a.has_local("k2")  # read after the sync
        a.close()
        b.close()
    finally:
        node.stop()


def test_the_maintenance_thread_reconnects_after_an_outage(pool, monkeypatch):
    import maru_handler.storage.remote as remote_mod

    monkeypatch.setattr(remote_mod, "_MAINTAIN_INTERVAL_S", 0.05)
    h = remote_handler(pool.url, retry_s=0.2)
    assert _store(h, "k", b"1")
    pool.stop()
    assert h.batch_exists(["k"]) == [False]  # a control timeout trips the backend
    pool.start()
    deadline = time.monotonic() + 5.0
    while h.batch_exists(["k"]) != [True] and time.monotonic() < deadline:
        time.sleep(0.05)
    assert h.batch_exists(["k"]) == [True]  # reconnected off the caller's path
    h.close()


def test_existence_checks_do_not_wait_for_a_maintenance_probe(pool, monkeypatch):
    h = remote_handler(pool.url)
    probe = h._storage._probe
    started, release = threading.Event(), threading.Event()

    from maru_remote.client import RemoteTimeout

    def slow_connect():
        started.set()
        release.wait(5)  # a server that does not answer the probe
        raise RemoteTimeout("probe timed out")

    monkeypatch.setattr(probe, "connect", slow_connect)
    t = threading.Thread(target=h._storage.maintain)
    t.start()
    started.wait(5)
    try:
        t0 = time.monotonic()
        assert h.batch_exists(["k"]) == [False]
        assert h.has_local("k") is False
        assert time.monotonic() - t0 < 0.5  # neither waited for the probe
    finally:
        release.set()
        t.join()
    assert h._storage._retry_at == 0.0  # one failed probe does not stop calls
    release.set()
    h._storage.maintain()  # the second failure in a row does
    assert h._storage._retry_at > 0.0
    h.close()


def test_the_maintenance_thread_drops_keys_another_worker_evicted(
    pool_handler, unused_port
):
    node = PoolNode(pool_handler, unused_port, capacity_bytes=1 * PAGE)
    node.start()
    try:
        a, b = (remote_handler(node.url, staging=4 * PAGE) for _ in range(2))
        assert _store(a, "k1", b"1")
        assert _store(b, "k2", b"2")  # evicts k1
        assert a.has_local("k1")
        a._storage.maintain()  # an idle worker learns of the eviction by probe
        assert not a.has_local("k1")
        a.close()
        b.close()
    finally:
        node.stop()


def test_a_server_that_returns_is_reconnected_before_the_retry_period_ends(pool):
    h = remote_handler(pool.url, retry_s=600.0)
    assert _store(h, "k", b"1")
    pool.stop()
    h._storage.maintain()  # one failed probe: a slow server is not an outage
    h._storage.maintain()  # the second in a row stops calls for the retry period
    assert h.batch_exists(["k"]) == [False]
    h._storage.maintain()  # still down: still stopped, no exception
    pool.start()
    h._storage.maintain()  # the server answers: reconnect at once
    assert h.batch_exists(["k"]) == [True]
    h.close()


def test_loads_and_stores_leave_the_reconnect_to_the_maintenance_thread(pool):
    h = remote_handler(pool.url)
    clock = FakeClock()
    h._storage._clock = clock
    assert _store(h, "k", b"1")
    pool.stop()
    h._storage.maintain()
    h._storage.maintain()  # two failed probes in a row: calls stop
    clock.advance(10.0)  # the retry period is over, the server still down
    t0 = time.monotonic()
    assert _store(h, "k2", b"2") is False
    with pytest.raises(StorageUnavailableError):
        h.batch_retrieve(["k"])
    assert time.monotonic() - t0 < 0.2  # neither tried to reconnect
    pool.start()
    h._storage.maintain()
    assert _store(h, "k2", b"2") is True
    h.close()


def test_an_error_reply_to_a_probe_does_not_stop_calls(pool, monkeypatch):
    from maru_remote.client import RemoteError

    h = remote_handler(pool.url)
    assert _store(h, "k", b"1")

    def error_reply():
        raise RemoteError("remote ping: busy")

    monkeypatch.setattr(h._storage._probe, "connect", error_reply)
    for _ in range(3):
        h._storage.maintain()
    assert h.batch_exists(["k"]) == [True]
    h.close()
