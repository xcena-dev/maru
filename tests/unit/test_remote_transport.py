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


def test_unregistered_remote_range_is_refused(pair):
    a, b, peer = pair
    src, src_addr = _buf(16)
    a.register(src_addr, 16)
    with pytest.raises(AssertionError):
        a.write(peer, [(src_addr, 0xDEAD0000, 16)])


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
