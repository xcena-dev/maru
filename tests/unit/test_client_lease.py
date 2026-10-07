# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Client leases: regions of a client that stops renewing are reclaimed.

Without a lease, a client that exits without ``close()`` (crash, SIGKILL)
never returns its regions, so they stay allocated even after every KV entry
in them is deleted.
"""

import threading
import time

import pytest

from maru import MaruConfig, MaruHandler
from maru_server import MaruServer, RpcServer

SIZE = 4096
TTL = 30.0


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _server(ttl: float = TTL) -> tuple[MaruServer, _Clock]:
    clock = _Clock()
    return MaruServer(client_lease_ttl=ttl, clock=clock), clock


def _advance(server: MaruServer, clock: _Clock, seconds: float, *live: tuple):
    """Advance time the way a running server sees it.

    A live client renews every quarter of the TTL; ``ticker`` stands for one,
    and ``live`` names (instance_id, lease_id) pairs that keep renewing too.
    """
    step = server.client_lease_ttl / 4
    end = clock.now + seconds
    while clock.now < end:
        clock.now = min(end, clock.now + step)
        server.renew_lease("ticker", "ticker-lease")
        for instance_id, lease_id in live:
            server.renew_lease(instance_id, lease_id)


def _num_allocations(server: MaruServer) -> int:
    return server.get_stats()["allocation_manager"]["num_allocations"]


def _owned_by(server: MaruServer, instance_id: str) -> int:
    return sum(
        1
        for owner in server._allocation_manager.region_owners().values()
        if owner == instance_id
    )


class TestServerLease:
    def test_regions_of_a_client_that_stopped_renewing_are_reclaimed(self):
        server, clock = _server()
        server.renew_lease("crashed", "lease-1")
        empty = server.request_alloc("crashed", SIZE, lease_id="lease-1")
        used = server.request_alloc("crashed", SIZE, lease_id="lease-1")
        assert server.register_kv("k", used.region_id, 0, 128)

        _advance(server, clock, TTL + 1)

        # The empty region is freed now; the referenced one when its KV goes.
        assert server._allocation_manager.get_handle(empty.region_id) is None
        assert server.lookup_kv("k") is not None
        server.delete_kv("k")
        assert _owned_by(server, "crashed") == 0

    def test_allocation_request_reaps_before_allocating(self):
        server, clock = _server()
        server.renew_lease("crashed", "lease-1")
        server.request_alloc("crashed", SIZE, lease_id="lease-1")
        _advance(server, clock, TTL - 1)

        clock.now += 2
        server.request_alloc("other", SIZE)

        assert _owned_by(server, "crashed") == 0

    def test_renewing_client_keeps_its_regions(self):
        server, clock = _server()
        server.renew_lease("live", "lease-1")
        server.request_alloc("live", SIZE, lease_id="lease-1")

        _advance(server, clock, 5 * TTL, ("live", "lease-1"))

        assert _owned_by(server, "live") == 1

    def test_renewal_that_waited_through_a_server_stall_keeps_its_lease(self):
        """A stalled server must not expire leases whose renewals queued up."""
        server, clock = _server()
        server.renew_lease("live", "lease-1")
        server.request_alloc("live", SIZE, lease_id="lease-1")
        server.renew_lease("crashed", "lease-2")
        server.request_alloc("crashed", SIZE, lease_id="lease-2")

        clock.now += 3 * TTL  # e.g. one request held the server this long
        server.request_alloc("other", SIZE)  # processed first after the stall

        assert server.renew_lease("live", "lease-1")["lease_expired"] is False
        assert _owned_by(server, "live") == 1
        # Lease time counts again once the server runs; the dead one expires.
        _advance(server, clock, TTL + 1, ("live", "lease-1"))
        assert _owned_by(server, "crashed") == 0
        assert _owned_by(server, "live") == 1

    def test_client_without_a_lease_is_never_reclaimed(self):
        server, clock = _server()
        server.request_alloc("old-client", SIZE)

        _advance(server, clock, 5 * TTL)

        assert _owned_by(server, "old-client") == 1

    def test_released_lease_is_not_reaped_or_reopened(self, monkeypatch):
        server, clock = _server()
        reaped = []
        disconnect = server._allocation_manager.disconnect_lease
        monkeypatch.setattr(
            server._allocation_manager,
            "disconnect_lease",
            lambda *a: reaped.append(a) or disconnect(*a),
        )
        server.renew_lease("clean", "lease-1")
        handle = server.request_alloc("clean", SIZE, lease_id="lease-1")
        server.return_alloc("clean", handle.region_id)
        server.renew_lease("clean", "lease-1", release=True)

        _advance(server, clock, 2 * TTL)

        assert reaped == []
        # A renewal that was in flight during close() does not reopen it.
        assert server.renew_lease("clean", "lease-1")["lease_expired"] is True
        assert "lease-1" not in server._leases

    def test_dead_lease_is_reclaimed_soon_after_an_idle_period(self):
        """A client restarted long after a crash gets the space back quickly."""
        server, clock = _server()
        server.renew_lease("crashed", "lease-1")
        server.request_alloc("crashed", SIZE, lease_id="lease-1")

        clock.now += 10 * TTL  # nothing talks to the server
        server.renew_lease("restarted", "lease-2")
        assert _owned_by(server, "crashed") == 1  # renewals may have queued

        _advance(server, clock, TTL / 2, ("restarted", "lease-2"))
        assert _owned_by(server, "crashed") == 0

    def test_sparse_traffic_does_not_keep_a_dead_lease_alive(self):
        server, clock = _server()
        server.renew_lease("crashed", "lease-1")
        server.request_alloc("crashed", SIZE, lease_id="lease-1")

        for _ in range(4):  # only old clients, each gap between TTL/2 and TTL
            clock.now += 0.9 * TTL
            server.request_alloc("old-client", SIZE)

        assert _owned_by(server, "crashed") == 0

    def test_expired_lease_is_told_and_refused(self):
        server, clock = _server()
        server.renew_lease("late", "lease-1")
        handle = server.request_alloc("late", SIZE, lease_id="lease-1")

        _advance(server, clock, TTL + 1)

        assert server.renew_lease("late", "lease-1") == {
            "lease_ttl": TTL,
            "lease_expired": True,
        }
        assert server.request_alloc("late", SIZE, lease_id="lease-1") is None
        # A page written before the client noticed is not published.
        assert server.register_kv("k1", handle.region_id, 0, 128) is False
        assert server.batch_register_kv([("k2", handle.region_id, 0, 128)]) == [False]
        assert server.batch_exists_kv(["k1", "k2"]) == [False, False]

    def test_expiry_is_per_lease_not_per_instance(self):
        """A restart that reuses the instance id keeps its new regions."""
        server, clock = _server()
        server.renew_lease("worker", "old-run")
        old = server.request_alloc("worker", SIZE, lease_id="old-run")
        _advance(server, clock, TTL / 2)
        server.renew_lease("worker", "new-run")
        new = server.request_alloc("worker", SIZE, lease_id="new-run")

        _advance(server, clock, TTL, ("worker", "new-run"))

        assert server._allocation_manager.get_handle(old.region_id) is None
        assert server._allocation_manager.get_handle(new.region_id) is not None

    def test_reused_region_id_is_registrable_again(self, monkeypatch):
        server, clock = _server()
        server.renew_lease("crashed", "lease-1")
        handle = server.request_alloc("crashed", SIZE, lease_id="lease-1")
        _advance(server, clock, TTL + 1)

        # The resource manager hands the same region id to a new client.
        manager = server._allocation_manager
        monkeypatch.setattr(manager._client, "alloc", lambda size, dax_path="": handle)
        again = server.request_alloc("other", SIZE)

        assert again.region_id == handle.region_id
        assert server.register_kv("k", handle.region_id, 0, 128) is True

    def test_zero_ttl_disables_leases(self):
        server, clock = _server(ttl=0)
        assert server.renew_lease("client", "lease-1") == {}
        server.request_alloc("client", SIZE, lease_id="lease-1")

        clock.now = 1e9
        server.reap_expired_leases()

        assert _num_allocations(server) == 1

    def test_negative_ttl_is_rejected(self):
        with pytest.raises(ValueError):
            MaruServer(client_lease_ttl=-1)

    def test_plain_heartbeat_reply_is_unchanged(self):
        server, _ = _server()
        assert server.renew_lease("", "") == {}


# ---------------------------------------------------------------------------
# Handler ↔ server over RPC. The server runs on a fake clock, so expiry is
# driven by the test; the handler renews on its real-time thread.
# ---------------------------------------------------------------------------

HANDLER_TTL = 0.8  # handler renews every 0.2 s and stops writing after 0.6 s


@pytest.fixture
def lease_server(server_port):
    server, clock = _server(ttl=HANDLER_TTL)
    rpc_server = RpcServer(server, host="127.0.0.1", port=server_port)
    thread = threading.Thread(target=rpc_server.start, daemon=True)
    thread.start()
    time.sleep(0.05)
    yield server, clock, f"tcp://127.0.0.1:{server_port}"
    rpc_server.stop()


def _handler(url: str) -> MaruHandler:
    handler = MaruHandler(
        MaruConfig(
            server_url=url,
            pool_size=SIZE * 4,
            chunk_size_bytes=SIZE,
            auto_expand=False,
        )
    )
    assert handler.connect()
    return handler


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _stub_renewals(monkeypatch, handler: MaruHandler) -> None:
    """Make renewals time out the way the RPC client reports it."""
    monkeypatch.setattr(
        handler._lease_rpc,
        "renew_lease",
        lambda *args, **kwargs: {"error": "timeout", "success": False},
    )


class TestHandlerLease:
    def test_region_of_a_handler_that_stops_is_reclaimed(self, lease_server):
        server, clock, url = lease_server
        crashed = _handler(url)
        crashed._lease_stop.set()  # renewals stop; close() never runs
        crashed._lease_thread.join(timeout=2)
        assert _owned_by(server, crashed.instance_id) == 1

        _advance(server, clock, HANDLER_TTL + 0.1)

        assert _owned_by(server, crashed.instance_id) == 0

    def test_clean_close_returns_regions_and_ends_the_lease(self, lease_server):
        server, clock, url = lease_server
        handler = _handler(url)
        lease_id = handler._lease_id
        handler.close()

        _advance(server, clock, 2 * HANDLER_TTL)

        assert _num_allocations(server) == 0
        assert lease_id not in server._leases

    def test_handler_stops_writing_before_the_server_can_reclaim(
        self, lease_server, monkeypatch
    ):
        server, clock, url = lease_server
        handler = _handler(url)
        page = handler.alloc(SIZE)
        renew = handler._lease_rpc.renew_lease
        _stub_renewals(monkeypatch, handler)

        # Within three quarters of the TTL without a renewal, writes stop.
        assert _wait_until(lambda: not handler._lease_valid(), timeout=2)
        with pytest.raises(RuntimeError, match="lease"):
            handler.alloc(SIZE)
        assert handler.store("k", page) is False
        assert handler.exists("k") is False

        # A renewal that gets through again re-enables writes.
        monkeypatch.setattr(handler._lease_rpc, "renew_lease", renew)
        assert _wait_until(handler._lease_valid, timeout=2)
        assert handler.alloc(SIZE) is not None
        handler.close()

    def test_handler_learns_its_lease_expired(self, lease_server, monkeypatch):
        server, clock, url = lease_server
        handler = _handler(url)
        renew = handler._lease_rpc.renew_lease
        _stub_renewals(monkeypatch, handler)
        _advance(server, clock, HANDLER_TTL + 0.1)
        monkeypatch.setattr(handler._lease_rpc, "renew_lease", renew)

        assert _wait_until(handler._lease_lost.is_set)
        with pytest.raises(RuntimeError, match="lease"):
            handler.alloc(SIZE)
        handler.close()

    def test_handler_without_server_lease_support_runs_as_before(self, server_port):
        server = MaruServer(client_lease_ttl=0)
        rpc_server = RpcServer(server, host="127.0.0.1", port=server_port)
        threading.Thread(target=rpc_server.start, daemon=True).start()
        time.sleep(0.05)
        try:
            handler = _handler(f"tcp://127.0.0.1:{server_port}")
            assert handler._lease_thread is None
            page = handler.alloc(SIZE)
            assert handler.store("k", page) is True
            handler.close()
        finally:
            rpc_server.stop()
