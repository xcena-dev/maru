# SPDX-License-Identifier: Apache-2.0
"""Posted transfers: post returns at once, poll_state reports, only DONE ends.

Also covers FakeTransport: handle end and packet end are apart, the same
range gets the same key, and an agent's end takes its connections.
"""

import logging

import pytest

from maru_remote.transport import TransferState
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.remote_transport_fakes import FakeTransport, mapped

N = 4096


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


class Pair:
    """A GPU node's transport with a local page, a memory node's with a pool."""

    def __init__(self, mem_name: str = "mem") -> None:
        self.gpu = FakeTransport("gpu")
        self.local, self.local_addr = mapped(N, 7)
        self.local_reg = self.gpu.register(self.local_addr, N)
        self.mem = FakeTransport(mem_name)
        self.pool, self.pool_addr = mapped(N, 9)
        self.pool_reg = self.mem.register(self.pool_addr, N)
        self.peer = self.gpu.add_peer(self.mem.partial_metadata(self.pool_reg))

    def post(self, op: str):
        return self.gpu.post(op, self.peer, [(self.local_addr, self.pool_addr, N)])


@pytest.fixture
def pair():
    return Pair()


def test_states_tell_released_from_ended():
    assert [(s.released, s.ended) for s in TransferState] == [
        (False, False),  # PENDING
        (True, True),  # DONE
        (True, False),  # ERR: the handle is gone, the bytes may not be
    ]


def test_posted_write_and_read_end_done_with_every_byte_moved(pair):
    write = pair.post("WRITE")
    assert write.poll_state() is TransferState.DONE
    assert bytes(pair.pool) == bytes([7]) * N
    pair.pool[:] = bytes([3]) * N
    read = pair.post("READ")
    assert read.poll_state() is TransferState.DONE
    assert bytes(pair.local) == bytes([3]) * N
    assert pair.gpu.pending_peers() == set() and len(pair.gpu.agent.released) == 2
    assert read.poll_state() is TransferState.DONE  # idempotent


def test_a_stalled_transfer_stays_pending_and_holds_the_peer(pair):
    pair.gpu.stall()
    pending = pair.post("WRITE")  # returns at once
    assert pending.poll_state() is TransferState.PENDING
    assert bytes(pair.pool) == bytes([9]) * N
    assert pair.gpu.pending_peers() == {pair.peer}
    assert not pair.gpu.disconnect(pair.peer)
    with pytest.raises(RuntimeError, match="unreleased"):
        pair.gpu.add_peer(pair.mem.metadata(), peer=pair.peer)
    with pytest.raises(RuntimeError, match="transfers"):
        pair.gpu.close()
    assert pair.gpu.agent.released == []
    assert pair.gpu.finish_stalled() == 1
    assert pending.poll_state() is TransferState.DONE
    assert bytes(pair.pool) == bytes([7]) * N
    assert pair.gpu.disconnect(pair.peer)


def test_err_at_post_releases_the_handle_and_marks_the_peer_failed(pair):
    pair.gpu.fail_next()
    pending = pair.post("WRITE")
    assert pending.poll_state() is TransferState.ERR
    assert pending.finished and not pending.completed
    assert not pair.gpu.peer_usable(pair.peer)
    assert bytes(pair.pool) == bytes([9]) * N  # this fault moved nothing


@pytest.mark.parametrize("op", ["READ", "WRITE"])
def test_err_then_a_late_packet_still_lands_in_the_destination(pair, op):
    pair.gpu.stall()
    pending = pair.post(op)
    assert pair.gpu.fail_stalled(late=True) == 1
    assert pending.poll_state() is TransferState.ERR
    assert len(pair.gpu.agent.released) == 1  # the handle is gone
    assert pair.gpu.disconnect(pair.peer)  # and so is the peer
    pair.gpu.add_peer(pair.mem.partial_metadata(pair.pool_reg))  # reconnected
    assert pair.gpu.deliver_late() == 1
    # The destination took the bytes after all: it must not have been reused.
    if op == "READ":
        assert bytes(pair.local) == bytes([9]) * N
    else:
        assert bytes(pair.pool) == bytes([7]) * N


ENDS = ["remote deregister", "local deregister", "memory node ends", "gpu ends"]
# The ends that drop a late packet: the destination's
DROPS = {
    "READ": {"local deregister", "gpu ends"},
    "WRITE": {"remote deregister", "memory node ends"},
}


def _end(pair, end: str) -> None:
    if end == "remote deregister":
        pair.mem.deregister(pair.pool_reg)
    elif end == "local deregister":
        pair.gpu.deregister(pair.local_reg)
    elif end == "memory node ends":
        pair.mem.kill()
    else:
        pair.gpu.kill()


@pytest.mark.parametrize("op", ["READ", "WRITE"])
@pytest.mark.parametrize("end", ENDS)
def test_only_the_destinations_end_drops_a_late_packet(pair, op, end):
    pair.gpu.stall()
    pending = pair.post(op)
    pair.gpu.fail_stalled(late=True)  # the NIC has read the source
    assert pending.poll_state() is TransferState.ERR
    _end(pair, end)
    dropped = end in DROPS[op]
    assert pair.gpu.deliver_late() == (0 if dropped else 1)
    dest = pair.local if op == "READ" else pair.pool
    before, landed = (7, 9) if op == "READ" else (9, 7)
    assert bytes(dest) == bytes([before if dropped else landed]) * N


@pytest.mark.parametrize("op", ["READ", "WRITE"])
def test_a_late_packet_carries_the_source_as_it_was_read(pair, op):
    pair.gpu.stall()
    pair.post(op)
    pair.gpu.fail_stalled(late=True)
    source = pair.pool if op == "READ" else pair.local
    source[:] = bytes([1]) * N  # reused after the NIC read it
    assert pair.gpu.deliver_late() == 1
    dest = pair.local if op == "READ" else pair.pool
    assert bytes(dest) == bytes([9 if op == "READ" else 7]) * N


@pytest.mark.parametrize("op", ["READ", "WRITE"])
def test_a_source_gone_before_the_nic_read_it_sends_nothing(pair, op):
    pair.gpu.stall()
    pair.post(op)
    if op == "READ":
        pair.mem.deregister(pair.pool_reg)
    else:
        pair.gpu.deregister(pair.local_reg)
    pair.gpu.fail_stalled(late=True)
    assert pair.gpu.deliver_late() == 0
    assert bytes(pair.local) == bytes([7]) * N and bytes(pair.pool) == bytes([9]) * N


def test_a_notification_never_reaches_the_next_run():
    pair = Pair()
    assert pair.gpu.announce(pair.peer)
    assert pair.mem.take_notifs() == {"gpu": [b""]}
    pair.mem.kill()
    nxt = FakeTransport("mem")  # the next run, same name
    assert not pair.gpu.announce(pair.peer)
    assert not pair.gpu.peer_usable(pair.peer)
    assert nxt.take_notifs() == {}
    # Only a connection to the next run reaches it.
    assert pair.gpu.disconnect(pair.peer)
    peer = pair.gpu.add_peer(nxt.metadata())
    assert pair.gpu.announce(peer)
    assert nxt.take_notifs() == {"gpu": [b""]}


def test_an_ended_agent_sends_no_notification(pair):
    pair.gpu.kill()
    assert not pair.gpu.announce(pair.peer)
    assert not pair.gpu.peer_usable(pair.peer)
    assert pair.mem.take_notifs() == {}


@pytest.mark.parametrize("op", ["READ", "WRITE"])
def test_a_post_after_deregistration_fails_with_nothing_written(pair, op):
    pair.mem.deregister(pair.pool_reg)
    pending = pair.post(op)
    assert pending.poll_state() is TransferState.ERR
    assert bytes(pair.local) == bytes([7]) * N and bytes(pair.pool) == bytes([9]) * N


def test_a_transfer_posted_before_deregistration_lands_nothing(pair):
    pair.gpu.stall()
    pending = pair.post("WRITE")
    pair.mem.deregister(pair.pool_reg)
    assert pair.gpu.finish_stalled() == 0
    assert pending.poll_state() is TransferState.ERR
    assert bytes(pair.pool) == bytes([9]) * N


def test_the_same_range_registered_again_gets_the_same_key(pair):
    key = pair.mem.agent.key_of(pair.pool_addr, N)
    pair.mem.deregister(pair.pool_reg)
    assert pair.mem.agent.key_of(pair.pool_addr, N) is None
    pair.pool_reg = pair.mem.register(pair.pool_addr, N)
    assert pair.mem.agent.key_of(pair.pool_addr, N) == key
    other = FakeTransport("mem-next-run")
    other.register(pair.pool_addr, N)
    assert other.agent.key_of(pair.pool_addr, N) == key
    _, addr = mapped(N)
    other.register(addr, N)
    assert other.agent.key_of(addr, N) != key


@pytest.mark.parametrize("graceful", [True, False])
def test_a_closed_agents_connection_never_reaches_the_next_run(graceful):
    pair = Pair()
    pair.gpu.stall()
    stalled = pair.post("WRITE")  # posted on the old run's connection
    key = pair.mem.agent.key_of(pair.pool_addr, N)
    if graceful:
        pair.mem.deregister(pair.pool_reg)
        pair.mem.agent.close()
    else:
        pair.mem.kill()  # no deregistration
    # The next run (same name here) registers the same range: the same key.
    nxt = FakeTransport("mem")
    reg = nxt.register(pair.pool_addr, N)
    assert nxt.agent.key_of(pair.pool_addr, N) == key
    pair.gpu.stall(False)
    assert pair.post("WRITE").poll_state() is TransferState.ERR  # a new post
    assert pair.gpu.finish_stalled() == 0  # one already posted
    assert stalled.poll_state() is TransferState.ERR
    assert bytes(pair.pool) == bytes([9]) * N
    # Only a connection to the next run reaches it.
    assert pair.gpu.disconnect(pair.peer)
    peer = pair.gpu.add_peer(nxt.partial_metadata(reg))
    write = pair.gpu.post("WRITE", peer, [(pair.local_addr, pair.pool_addr, N)])
    assert write.poll_state() is TransferState.DONE
    assert bytes(pair.pool) == bytes([7]) * N


def test_a_closed_agent_posts_nothing(pair):
    pair.gpu.stall()
    pending = pair.post("READ")
    pair.gpu.kill()
    assert pair.gpu.finish_stalled() == 0
    assert pending.poll_state() is TransferState.ERR
    assert bytes(pair.local) == bytes([7]) * N
    with pytest.raises(RuntimeError, match="closed"):
        pair.post("READ")
    assert pair.gpu.pending_peers() == set()


def test_close_ends_the_fake_agent(pair):
    pair.gpu.close()
    assert pair.gpu.agent.closed and pair.gpu.agent.regs == []
    assert pair.gpu.agent.peers == set()


def test_an_unreadable_state_is_pending_never_err(pair, monkeypatch):
    pair.gpu.stall()
    pending = pair.post("WRITE")

    def unavailable(_handle):
        raise RuntimeError("state temporarily unavailable")

    with monkeypatch.context() as m:
        m.setattr(pair.gpu.agent, "check_xfer_state", unavailable)
        assert pending.poll_state() is TransferState.PENDING
    assert pair.gpu.agent.released == []
    pair.gpu.finish_stalled()
    assert pending.poll_state() is TransferState.DONE


def test_done_with_an_unreleased_handle_stays_pending(pair, monkeypatch):
    def refuse(_handle):
        raise RuntimeError("release failed")

    with monkeypatch.context() as m:
        m.setattr(pair.gpu.agent, "release_xfer_handle", refuse)
        pending = pair.post("WRITE")
        assert pending.completed and pending.poll_state() is TransferState.PENDING
        assert pair.gpu.pending_peers() == {pair.peer}
    assert pending.poll_state() is TransferState.DONE


def test_a_failed_post_returns_the_transfer_for_polling(pair, monkeypatch):
    pair.gpu.stall()
    post = pair.gpu.agent.transfer

    def post_then_raise(handle):
        post(handle)
        raise RuntimeError("post result unavailable")

    monkeypatch.setattr(pair.gpu.agent, "transfer", post_then_raise)
    pending = pair.post("WRITE")
    assert not pair.gpu.peer_usable(pair.peer)
    assert pending.poll_state() is TransferState.PENDING
    pair.gpu.finish_stalled()
    assert pending.poll_state() is TransferState.DONE
    assert bytes(pair.pool) == bytes([7]) * N


def test_a_refused_request_posts_nothing(pair):
    _, other_addr = mapped(N)
    pair.mem.register(other_addr, N)  # not in the loaded metadata
    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        pair.gpu.post("WRITE", pair.peer, [(pair.local_addr, other_addr, N)])
    with pytest.raises(ValueError, match="operation"):
        pair.post("COPY")
    with pytest.raises(ValueError, match="range"):
        pair.gpu.post("WRITE", pair.peer, [])
    assert pair.gpu.pending_peers() == set()
    assert pair.gpu.disconnect(pair.peer)


def test_an_interrupted_post_hands_the_transfer_over(pair, monkeypatch):
    pair.gpu.stall()
    post = pair.gpu.agent.transfer

    def post_then_interrupt(handle):
        post(handle)
        raise KeyboardInterrupt

    monkeypatch.setattr(pair.gpu.agent, "transfer", post_then_interrupt)
    with pytest.raises(KeyboardInterrupt) as info:
        pair.post("WRITE")
    pending = info.value.pending_transfer
    assert not pending.in_flight and pending.poll_state() is TransferState.PENDING
    pair.gpu.finish_stalled()
    assert pending.poll_state() is TransferState.DONE


class TwoPools:
    """A GPU node with two local pages, a memory node with two registered
    pools: one transfer names both."""

    def __init__(self) -> None:
        self.gpu = FakeTransport("gpu")
        self.locals = [mapped(N, 7), mapped(N, 7)]
        self.local_regs = [self.gpu.register(addr, N) for _, addr in self.locals]
        self.mem = FakeTransport("mem")
        self.pools = [mapped(N, 9), mapped(N, 9)]
        self.regs = [self.mem.register(addr, N) for _, addr in self.pools]
        md = [self.mem.partial_metadata(r) for r in self.regs]
        self.peer = self.gpu.add_peer(md[0])
        self.gpu.add_peer(md[1], peer=self.peer)

    def post(self, op: str):
        pairs = [
            (local_addr, addr, N)
            for (_, local_addr), (_, addr) in zip(self.locals, self.pools, strict=True)
        ]
        return self.gpu.post(op, self.peer, pairs)


@pytest.mark.parametrize("op", ["READ", "WRITE"])
def test_a_late_packet_lands_in_every_destination_still_held(op):
    two = TwoPools()
    two.gpu.stall()
    two.post(op)
    two.gpu.fail_stalled(late=True)
    if op == "READ":  # the second destination ends
        two.gpu.deregister(two.local_regs[1])
    else:
        two.mem.deregister(two.regs[1])
    assert two.gpu.deliver_late() == 1
    if op == "READ":
        assert bytes(two.locals[0][0]) == bytes([9]) * N
        assert bytes(two.locals[1][0]) == bytes([7]) * N
    else:
        assert bytes(two.pools[0][0]) == bytes([7]) * N
        assert bytes(two.pools[1][0]) == bytes([9]) * N


def test_an_access_error_may_still_move_the_ranges_it_did_not_touch():
    two = TwoPools()
    two.mem.deregister(two.regs[1])
    pending = two.post("WRITE")
    assert pending.poll_state() is TransferState.ERR
    assert bytes(two.pools[0][0]) == bytes([7]) * N  # it may have landed
    assert bytes(two.pools[1][0]) == bytes([9]) * N


def test_no_notification_crosses_a_connection_an_access_error_broke(pair):
    pair.mem.deregister(pair.pool_reg)
    assert pair.post("WRITE").poll_state() is TransferState.ERR
    assert not pair.gpu.announce(pair.peer)
    assert pair.mem.take_notifs() == {}


def test_stale_metadata_never_connects_to_a_later_agent(pair):
    stale = pair.mem.partial_metadata(pair.pool_reg)
    pair.mem.kill()
    nxt = FakeTransport("mem")  # same name, range and key
    nxt.register(pair.pool_addr, N)
    assert pair.gpu.disconnect(pair.peer)
    peer = pair.gpu.add_peer(stale)  # loads, but reaches the ended agent
    for op in ("WRITE", "READ"):
        assert (
            pair.gpu.post(op, peer, [(pair.local_addr, pair.pool_addr, N)]).poll_state()
            is TransferState.ERR
        )
    assert bytes(pair.pool) == bytes([9]) * N
    assert bytes(pair.local) == bytes([7]) * N
    assert not pair.gpu.announce(peer) and nxt.take_notifs() == {}


def test_a_transfer_to_an_invalidated_peer_is_released_as_an_error(pair):
    pair.gpu.stall()
    kept = pair.post("WRITE")
    released = pair.post("WRITE")
    # The peer's process died; once its RC retries ran out, NIXL invalidated
    # the peer agent and reports NOT_FOUND for transfers to it
    pair.gpu.agent.invalidated.add(pair.peer)
    assert kept.poll_state() is TransferState.PENDING  # not asked to release
    assert released.poll_state(release_disconnected=True) is TransferState.ERR
    assert not released.completed and len(pair.gpu.agent.released) == 1


def test_a_failed_state_query_is_logged_once(pair, monkeypatch):
    pair.gpu.stall()
    pending = pair.post("WRITE")

    def unavailable(_handle):
        raise RuntimeError("state temporarily unavailable")

    class Records(logging.Handler):
        def __init__(self):
            super().__init__(logging.WARNING)
            self.messages = []

        def emit(self, record):
            self.messages.append(record.getMessage())

    records = Records()
    logger = logging.getLogger("maru_remote.transport")
    monkeypatch.setattr(pair.gpu.agent, "check_xfer_state", unavailable)
    logger.addHandler(records)
    try:
        for _ in range(5):
            assert pending.poll_state() is TransferState.PENDING
    finally:
        logger.removeHandler(records)
    assert sum("state check failed" in m for m in records.messages) == 1


def test_an_invalidated_peer_is_removed_and_the_transport_closes(pair):
    pair.gpu.stall()
    pending = pair.post("WRITE")
    pair.gpu.agent.invalidated.add(pair.peer)
    assert pending.poll_state(release_disconnected=True) is TransferState.ERR
    # NIXL answers NOT_FOUND to removing a peer it invalidated: it is gone
    assert pair.gpu.disconnect(pair.peer)
    assert not pair.gpu.has_peer(pair.peer)
    pair.gpu.close()
    assert pair.gpu.agent.regs == []
