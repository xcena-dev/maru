# SPDX-License-Identifier: Apache-2.0
"""NIXL wrapper: registration bounds, batched transfers, unreleased timeouts."""

import pytest

from maru_remote.transport import (
    NixlTransport,
    TransferTimeout,
    TransferUncertainError,
    buffer_address,
)
from tests.unit.remote_fakes import FakeNixlAgent, reset_fake_agents


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def pair():
    a = NixlTransport("a", agent=FakeNixlAgent("a"))
    b = NixlTransport("b", agent=FakeNixlAgent("b"))
    peer = a.add_peer(b.metadata())
    yield a, b, peer
    a.close()
    b.close()


def _buf(n, fill=0):
    arr = bytearray([fill]) * n
    return arr, buffer_address(arr)


def test_multi_range_write_then_read(pair):
    a, b, peer = pair
    src, src_addr = _buf(64, 7)
    dst, dst_addr = _buf(64)
    remote, remote_addr = _buf(128)
    for arr, addr, t in (
        (src, src_addr, a),
        (dst, dst_addr, a),
        (remote, remote_addr, b),
    ):
        t.register(addr, len(arr))
    a.write(peer, [(src_addr, remote_addr, 32), (src_addr + 32, remote_addr + 96, 32)])
    assert list(remote[:32]) == [7] * 32 and list(remote[96:]) == [7] * 32
    assert list(remote[32:96]) == [0] * 64
    a.read(peer, [(dst_addr, remote_addr + 96, 32)])
    assert list(dst[:32]) == [7] * 32


def test_unregistered_remote_range_fails(pair):
    a, b, peer = pair
    src, src_addr = _buf(16)
    a.register(src_addr, 16)
    with pytest.raises(RuntimeError, match="ERR"):
        a.write(peer, [(src_addr, 0xDEAD0000, 16)])


def test_partial_metadata_names_only_its_ranges():
    a = NixlTransport("a", agent=FakeNixlAgent("a"))
    b = NixlTransport("b", agent=FakeNixlAgent("b"))
    src, src_addr = _buf(16, 5)
    r1, r1_addr = _buf(16)
    r2, r2_addr = _buf(16)
    a.register(src_addr, 16)
    reg1 = b.register(r1_addr, 16)
    reg2 = b.register(r2_addr, 16)
    peer = a.add_peer(b.partial_metadata(reg1))
    a.write(peer, [(src_addr, r1_addr, 16)])
    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        a.write(peer, [(src_addr, r2_addr, 16)])
    assert a.add_peer(b.partial_metadata(reg2)) == peer  # same peer, one more range
    a.write(peer, [(src_addr, r2_addr, 16)])
    assert r1 == src and r2 == src


def test_a_write_after_deregistration_fails_and_breaks_the_connection():
    a = NixlTransport("a", agent=FakeNixlAgent("a"))
    b = NixlTransport("b", agent=FakeNixlAgent("b"))
    src, src_addr = _buf(16, 9)
    r1, r1_addr = _buf(16)
    r2, r2_addr = _buf(16)
    a.register(src_addr, 16)
    reg1 = b.register(r1_addr, 16)
    reg2 = b.register(r2_addr, 16)
    peer = a.add_peer(b.partial_metadata(reg1))
    a.add_peer(b.partial_metadata(reg2))
    b.deregister(reg1)
    with pytest.raises(RuntimeError, match="ERR"):
        a.write(peer, [(src_addr, r1_addr, 16)])
    assert r1 == bytearray(16)  # nothing landed
    with pytest.raises(RuntimeError, match="ERR"):
        a.write(peer, [(src_addr, r2_addr, 16)])  # the connection is broken
    assert a.has_peer(peer)
    assert not a.peer_usable(peer)  # a transfer to it failed
    assert a.disconnect(peer)
    assert not a.has_peer(peer)
    a.add_peer(b.partial_metadata(reg2))
    assert a.has_peer(peer) and a.peer_usable(peer)
    a.write(peer, [(src_addr, r2_addr, 16)])
    assert r2 == src


def test_disconnect_keeps_unconfirmed_transfers_pending(pair):
    a, b, peer = pair
    src, src_addr = _buf(16, 3)
    remote, remote_addr = _buf(16)
    a.register(src_addr, 16)
    b.register(remote_addr, 16)
    fake = FakeNixlAgent.registry["a"]
    fake.stall = True
    with pytest.raises(TransferTimeout) as info:
        a.write(peer, [(src_addr, remote_addr, 16)], timeout_s=0.01)
    pending = info.value.pending
    assert a.disconnect(peer)
    assert peer not in fake.peers
    # Removing the peer is no proof that the NIC stopped using the buffers.
    assert not pending.finished and a.pending_peers() == {peer}
    fake.finish_stalled()
    assert pending.poll() and a.pending_peers() == set()


def test_an_interrupted_transfer_is_handed_over_unfinished(pair, monkeypatch):
    a, b, peer = pair
    src, src_addr = _buf(16, 3)
    remote, remote_addr = _buf(16)
    a.register(src_addr, 16)
    b.register(remote_addr, 16)
    fake = FakeNixlAgent.registry["a"]
    fake.stall = True

    def interrupted(handle):
        raise KeyboardInterrupt

    monkeypatch.setattr(fake, "check_xfer_state", interrupted)
    with pytest.raises(KeyboardInterrupt) as info:
        a.write(peer, [(src_addr, remote_addr, 16)], timeout_s=5)
    pending = info.value.pending_transfer
    # Still unfinished and known: close keeps the registered memory.
    assert not pending.in_flight and not pending.finished
    assert a.pending_peers() == {peer}
    monkeypatch.undo()
    fake.finish_stalled()
    assert pending.poll() and a.pending_peers() == set()


def test_an_interrupt_while_releasing_a_done_transfer_hands_it_over(pair, monkeypatch):
    a, b, peer = pair
    src, src_addr = _buf(16, 3)
    remote, remote_addr = _buf(16)
    a.register(src_addr, 16)
    b.register(remote_addr, 16)
    fake = FakeNixlAgent.registry["a"]

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(a, "release_handle", interrupted)
    with pytest.raises(KeyboardInterrupt):
        a.write(peer, [(src_addr, remote_addr, 16)], timeout_s=5)
    monkeypatch.undo()
    (pending,) = a._pending.values()
    assert not pending.in_flight  # a later poll may release it
    assert pending.poll() and a.pending_peers() == set()
    del fake


def test_two_pollers_release_a_transfer_once(pair):
    import threading

    a, b, peer = pair
    src, src_addr = _buf(16, 3)
    remote, remote_addr = _buf(16)
    a.register(src_addr, 16)
    b.register(remote_addr, 16)
    fake = FakeNixlAgent.registry["a"]
    fake.stall = True
    with pytest.raises(TransferTimeout) as info:
        a.write(peer, [(src_addr, remote_addr, 16)], timeout_s=0.01)
    fake.finish_stalled()
    threads = [threading.Thread(target=info.value.pending.poll) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(fake.released) == 1


def test_disconnect_leaves_a_transfer_its_poster_still_polls(pair, monkeypatch):
    import threading
    import time as real_time

    import maru_remote.transport as transport_module

    a, b, peer = pair
    src, src_addr = _buf(16, 4)
    remote, remote_addr = _buf(16)
    a.register(src_addr, 16)
    b.register(remote_addr, 16)
    fake = FakeNixlAgent.registry["a"]
    fake.stall = True
    polling, go_on = threading.Event(), threading.Event()
    outcome = {}

    class PausedTime:
        perf_counter = staticmethod(real_time.perf_counter)

        @staticmethod
        def sleep(seconds):
            if threading.current_thread() is writer:
                polling.set()  # the writer is between two of its polls
                go_on.wait(5)
            else:
                real_time.sleep(seconds)

    def write():
        try:
            a.write(peer, [(src_addr, remote_addr, 16)], timeout_s=5)
            outcome["done"] = True
        except Exception as exc:
            outcome["error"] = exc

    monkeypatch.setattr(transport_module, "time", PausedTime)
    writer = threading.Thread(target=write)
    writer.start()
    assert polling.wait(5)
    assert fake.finish_stalled() == 1  # done, but the writer has not looked yet
    assert a.disconnect(peer)
    assert fake.released == []  # the writer still owns the handle
    go_on.set()
    writer.join(5)
    assert outcome == {"done": True} and len(fake.released) == 1
    assert list(remote) == [4] * 16


def test_timeout_keeps_the_handle_until_the_transfer_ends(pair):
    a, b, peer = pair
    src, src_addr = _buf(16, 3)
    remote, remote_addr = _buf(16)
    a.register(src_addr, 16)
    b.register(remote_addr, 16)
    fake = FakeNixlAgent.registry["a"]
    fake.stall = True
    with pytest.raises(TransferTimeout) as info:
        a.write(peer, [(src_addr, remote_addr, 16)], timeout_s=0.01)
    pending = info.value.pending
    assert fake.released == []  # the NIC may still touch the buffers
    assert a.peer_usable(peer)  # a slow connection is not a failed one
    assert pending.poll() is False
    assert fake.finish_stalled() == 1
    assert list(remote) == [3] * 16  # the late transfer landed
    assert pending.poll() is True and len(fake.released) == 1
    assert pending.poll() is True and len(fake.released) == 1  # idempotent


def test_error_state_raises_and_releases(pair):
    a, b, peer = pair
    src, src_addr = _buf(16)
    remote, remote_addr = _buf(16)
    a.register(src_addr, 16)
    b.register(remote_addr, 16)
    fake = FakeNixlAgent.registry["a"]
    fake.fail_next = True
    with pytest.raises(RuntimeError, match="ERR"):
        a.read(peer, [(src_addr, remote_addr, 16)])
    assert len(fake.released) == 1


def test_ucx_device_selects_the_backend_device():
    agent = FakeNixlAgent("dev")
    NixlTransport("dev", ucx_device="mlx5_0:1", agent=agent)
    assert agent.backends == [("UCX", {"ucx_devices": "mlx5_0:1"})]


def test_close_removes_peers_and_registrations(pair):
    a, b, peer = pair
    arr, addr = _buf(8)
    a.register(addr, 8)
    a.close()
    fake = FakeNixlAgent.registry["a"]
    assert fake.registered == [] and fake.peers == set()


@pytest.mark.parametrize(
    "failure", ["transfer", "check_xfer_state", "release_xfer_handle"]
)
def test_unconfirmed_failure_retains_handle_and_blocks_cleanup(
    pair, monkeypatch, failure
):
    a, b, peer = pair
    src, src_addr = _buf(16, 3)
    remote, remote_addr = _buf(16)
    a.register(src_addr, 16)
    b.register(remote_addr, 16)
    fake = FakeNixlAgent.registry["a"]
    fake.stall = failure != "release_xfer_handle"
    original_post = fake.transfer

    def post_then_raise(handle):
        original_post(handle)
        raise RuntimeError("post result unavailable")

    def fail(*args):
        raise RuntimeError("completion/cancellation unavailable")

    with monkeypatch.context() as m:
        if failure == "transfer":
            m.setattr(fake, failure, post_then_raise)
        elif failure == "check_xfer_state":
            m.setattr(fake, failure, fail)
        m.setattr(fake, "release_xfer_handle", fail)
        with pytest.raises(TransferUncertainError) as info:
            a.write(peer, [(src_addr, remote_addr, 16)])
        pending = info.value.pending
        # Even a failed query followed by failed cancellation proves nothing.
        m.setattr(fake, "check_xfer_state", fail)
        assert pending.poll() is False and not pending.finished
        with pytest.raises(RuntimeError, match="transfers"):
            a.close()
        with pytest.raises(RuntimeError, match="transfers"):
            a.remove_peer(peer)
        assert fake.registered and peer in fake.peers
    fake.finish_stalled()
    assert remote == src
    assert pending.poll()
    a.close()
    assert fake.registered == []


def test_deregistration_failure_is_retryable(pair, monkeypatch):
    a, b, peer = pair
    src, src_addr = _buf(16)
    a.register(src_addr, len(src))
    fake = FakeNixlAgent.registry["a"]

    def fail(reg):
        raise RuntimeError("deregistration failed")

    with monkeypatch.context() as m:
        m.setattr(fake, "deregister_memory", fail)
        with pytest.raises(RuntimeError, match="deregistration"):
            a.close()
        assert fake.registered
    a.close()
    assert fake.registered == []


def test_peer_usable_tells_a_failed_connection_and_the_peer_drops_the_checks(pair):
    a, b, peer = pair
    assert a.peer_usable(peer)
    assert b._agent.notifs  # the check reached the peer
    b.discard_notifs()
    assert not b._agent.notifs
    a._agent.fail_endpoint(peer)  # UCX failed the connection on its own
    assert not a.peer_usable(peer)
    assert not b._agent.notifs  # nothing was sent on the failed connection
