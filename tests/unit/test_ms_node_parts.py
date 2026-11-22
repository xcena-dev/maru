# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""DRAM driver, PlacementPolicy, LocalPins, OpLedger, OpOrigin and MsRpcClient."""

import fcntl
import mmap
import os
import time

import pytest

from maru_common.ms_rpc import PLACEHOLDER_OP, MsRpcClient, OpOrigin, RpcUnknownError
from maru_common.protocol import LookupAndPinRequest, MessageType, NodeOpenRequest
from maru_common.serializer import op_digest
from maru_common.storage_types import KeptCause, Key, OpRef
from maru_storage_service.drivers import dram
from maru_storage_service.local_pin import LocalPins
from maru_storage_service.op_ledger import OpLedger, Verdict
from maru_storage_service.placement import PlacementPolicy
from maru_storage_service.pool_manager import PageRef
from tests.unit.ms_fake_server import FakeMaruServer

M = 1 << 20


class TestDramDriver:
    def test_pool_is_allocated_sealed_and_named(self):
        pool = dram.create_dram_pool("n1", "e1", 8 * M + 5, M, hugepage=False)
        try:
            assert pool.pool_id == "dram:n1:e1"
            assert pool.capacity == 8 * M  # whole pages
            assert os.fstat(pool.fd).st_size == 8 * M
            with pytest.raises(PermissionError):
                os.ftruncate(pool.fd, M)
            with pytest.raises(PermissionError):
                os.ftruncate(pool.fd, 9 * M)
            assert fcntl.fcntl(pool.fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
            info = pool.info()
            assert (info.capacity, info.page_size, info.pool_generation) == (
                8 * M,
                M,
                1,
            )
            # every mapping shares the bytes
            a = mmap.mmap(pool.fd, pool.capacity)
            b = mmap.mmap(pool.fd, pool.capacity)
            a[M : M + 3] = b"kv!"
            assert b[M : M + 3] == b"kv!"
            a.close()
            b.close()
        finally:
            pool.close()

    def test_no_page_fits(self):
        with pytest.raises(dram.DramPoolError, match="holds no page"):
            dram.create_dram_pool("n", "e", M - 1, M, hugepage=False)

    def test_huge_pages_fail_clearly_without_fallback(self):
        huge = dram.default_huge_page_size()
        free = dram._meminfo().get("HugePages_Free", 0)
        size = 4 * (huge or 2 * M)
        if huge and free * huge >= size:
            pool = dram.create_dram_pool("n", "e", size, huge, hugepage=True)
            pool.close()
            pytest.skip(
                "this host has free huge pages; the failure path is not reachable"
            )
        with pytest.raises(dram.DramPoolError, match="huge pages") as err:
            dram.create_dram_pool("n", "e", size, huge or 2 * M, hugepage=True)
        assert "hugepage to false" in str(err.value)

    def test_huge_page_multiple(self, monkeypatch):
        monkeypatch.setattr(dram, "default_huge_page_size", lambda: 2 * M)
        with pytest.raises(dram.DramPoolError, match="multiple of the huge page"):
            dram.create_dram_pool("n", "e", 3 * M, M, hugepage=True)
        monkeypatch.setattr(dram, "default_huge_page_size", lambda: None)
        with pytest.raises(dram.DramPoolError, match="no huge page size"):
            dram.create_dram_pool("n", "e", 4 * M, M, hugepage=True)

    def test_meets_the_driver_contract(self):
        from maru_common.storage_types import PoolDescriptor
        from maru_storage_service.config import TierConfig
        from maru_storage_service.drivers import PoolAccess
        from maru_storage_service.tiers import L1Pool

        driver = dram.DramDriver()
        tier = TierConfig(store="dram", size=4 * M + 1, hugepage=False)
        pool = driver.create(tier, node_id="n", node_epoch="e", page_size=M)
        try:
            assert isinstance(pool, L1Pool)
            assert pool.access == PoolAccess.MAPPED
            assert (pool.pool_id, pool.capacity) == ("dram:n:e", 4 * M)
            assert pool.descriptor(memfd=0) == PoolDescriptor(pool.info(), memfd=0)
            assert pool.worker_fd() == pool.fd >= 0
            # a DRAM pool ends with its run: nothing to adopt
            with pytest.raises(dram.DramPoolError, match="ends with its run"):
                driver.open(pool.info())
        finally:
            pool.close()
        assert pool.worker_fd() is None


def ref(i: int, pool: str = "p") -> PageRef:
    return PageRef(pool, i, 100 + i)


class TestPlacement:
    def policy(self, now):
        return PlacementPolicy(0.75, 0.5, kept_backoff_s=1.0, clock=lambda: now[0])

    def test_nothing_below_the_high_watermark(self):
        now = [0.0]
        p = self.policy(now)
        for i in range(8):
            p.on_ready(ref(i))
        kw = {"capacity": 8, "retiring_bytes": 0, "page_size": 1, "limit": 100}
        assert (
            p.candidates("t", ["p"], free_bytes=2, **kw) == []
        )  # 6/8 = 0.75, not above

    def test_lru_down_to_the_low_watermark(self):
        now = [0.0]
        p = self.policy(now)
        for i in range(8):
            p.on_ready(ref(i))
        kw = {"capacity": 8, "retiring_bytes": 0, "page_size": 1, "limit": 100}
        assert p.candidates("t", ["p"], free_bytes=1, **kw) == [ref(0), ref(1), ref(2)]
        # in flight pages count as leaving: nothing more until they resolve
        assert p.candidates("t", ["p"], free_bytes=1, **kw) == []
        for r in (ref(0), ref(1), ref(2)):
            p.retired(r)
        # 5/8 is below high but removal goes on until low (hysteresis)
        assert p.candidates("t", ["p"], free_bytes=3, **kw) == [ref(3)]
        p.retired(ref(3))
        assert p.candidates("t", ["p"], free_bytes=4, **kw) == []

    def test_the_watermark_is_the_tiers_over_its_pools(self):
        now = [0.0]
        p = self.policy(now)
        for i in range(4):
            p.on_ready(ref(i, "a"))
        kw = {"retiring_bytes": 0, "page_size": 1, "limit": 100}
        # pool a is full but the tier (a and an empty b) is half used
        assert p.candidates("t", ["a", "b"], capacity=8, free_bytes=4, **kw) == []
        assert p.candidates("t", ["a"], capacity=4, free_bytes=0, **kw) == [
            ref(0, "a"),
            ref(1, "a"),
        ]

    def test_a_demand_evicts_below_the_watermarks_preferred_first(self):
        now = [0.0]
        p = self.policy(now)
        for i in range(4):
            p.on_ready(ref(i))
        kw = {"capacity": 8, "retiring_bytes": 0, "page_size": 1, "limit": 100}
        # half used: no pressure, but a fetch wants 6 FREE (4 are)
        assert p.candidates("t", ["p"], free_bytes=4, **kw) == []
        got = p.candidates(
            "t", ["p"], free_bytes=4, demand_bytes=6, prefer={ref(2)}, **kw
        )
        assert got == [ref(2), ref(0)]
        # what is leaving counts toward the demand
        assert p.candidates("t", ["p"], free_bytes=4, demand_bytes=6, **kw) == []
        assert p.candidates("t", ["p"], free_bytes=4, demand_bytes=7, **kw) == [ref(1)]

    def test_lru_runs_across_the_tiers_pools(self):
        now = [0.0]
        p = self.policy(now)
        p.on_ready(ref(0, "a"))
        p.on_ready(ref(0, "b"))
        p.on_ready(ref(1, "a"))
        p.on_ready(ref(1, "b"))
        assert p.on_access([ref(0, "a")]) == 1
        kw = {"retiring_bytes": 0, "page_size": 1, "limit": 100}
        # order now b0, a1, b1, a0; 4 of 4 used, low 0.5 leaves 2
        got = p.candidates("t", ["a", "b"], capacity=4, free_bytes=0, **kw)
        assert got == [ref(0, "b"), ref(1, "a")]
        # watermarks and hysteresis are per tier
        p.set_watermarks("u", 0.9, 0.1)
        assert p.candidates("u", ["a", "b"], capacity=4, free_bytes=0, **kw) == [
            ref(1, "b"),
            ref(0, "a"),
        ]

    def test_access_moves_pages_to_the_recent_end(self):
        now = [0.0]
        p = self.policy(now)
        for i in range(5):
            p.on_ready(ref(i))
        p.on_gone(ref(4))
        # oldest read first; pages no longer READY are not added back
        assert p.on_access([ref(0), ref(4), ref(1), ref(9)]) == 2
        kw = {"capacity": 4, "retiring_bytes": 0, "page_size": 1, "limit": 100}
        # order now 2, 3, 0, 1
        assert p.candidates("t", ["p"], free_bytes=0, **kw) == [ref(2), ref(3)]
        p.release([ref(2), ref(3)], backoff=False)
        assert p.on_access([ref(2), ref(1), ref(0)]) == 3
        # order now 3, 2, 1, 0
        assert p.candidates("t", ["p"], free_bytes=0, **kw) == [ref(3), ref(2)]

    def test_kept_causes(self):
        now = [0.0]
        p = self.policy(now)
        for i in range(4):
            p.on_ready(ref(i))
        kw = {"capacity": 4, "retiring_bytes": 0, "page_size": 1, "limit": 1}
        (first,) = p.candidates("t", ["p"], free_bytes=0, **kw)
        p.kept(first, KeptCause.PINNED)
        (second,) = p.candidates("t", ["p"], free_bytes=0, **kw)
        p.kept(second, KeptCause.MOVED)
        (third,) = p.candidates("t", ["p"], free_bytes=0, **kw)
        p.kept(third, KeptCause.INVALID)
        assert (first, second, third) == (ref(0), ref(1), ref(2))
        assert p.is_held(second) and p.is_held(third) and not p.is_held(first)
        assert p.candidates("t", ["p"], free_bytes=0, **kw) == [ref(3)]
        p.release([ref(3)])
        assert p.candidates("t", ["p"], free_bytes=0, **kw) == []
        now[0] = 1.5  # the PINNED one is tried again after the backoff
        assert p.candidates("t", ["p"], free_bytes=0, **kw) == [ref(0)]


class TestLocalPins:
    def test_off_but_blocks_are_tracked(self):
        pins = LocalPins()
        assert pins.enabled is False
        pins.block([ref(1), ref(2)])
        assert pins.is_blocked(ref(1)) and pins.readers(ref(1)) == 0
        pins.after_kept(ref(1), KeptCause.PINNED)
        pins.after_kept(ref(2), KeptCause.MOVED)
        assert not pins.is_blocked(ref(1)) and pins.is_blocked(ref(2))
        pins.forget(ref(2))
        assert not pins.is_blocked(ref(2))


def op(seq, ack=0, digest=b"d"):
    return OpRef(origin="w", seq=seq, digest=digest, ack=ack)


class TestOpLedger:
    def test_rules(self):
        ledger = OpLedger(window=2)
        verdict, entry = ledger.begin("w", op(1), b"d")
        assert verdict == Verdict.NEW
        assert ledger.begin("w", op(1), b"d") == (Verdict.REPLAY, entry)
        assert ledger.begin("w", op(1), b"x")[0] == Verdict.CONFLICT
        ledger.finish("w", entry, "result-1")
        verdict, entry2 = ledger.begin("w", op(2), b"d")
        assert verdict == Verdict.NEW
        # the window holds two results; a third op waits
        assert ledger.begin("w", op(3), b"d")[0] == Verdict.BUSY
        ledger.finish("w", entry2, "result-2")
        # the ack drops seq 1; its resend is STALE, never run again
        assert ledger.begin("w", op(3, ack=1), b"d")[0] == Verdict.NEW
        assert ledger.begin("w", op(1, ack=1), b"d") == (Verdict.STALE, None)
        assert ledger.size("w") == 2

    def test_waiters_get_the_same_result(self):
        ledger = OpLedger()
        _, entry = ledger.begin("w", op(1), b"d")
        got = []
        assert ledger.attach(entry, got.append, "conn-1") == (True, None)
        assert ledger.attach(entry, got.extend, "conn-1") == (True, None)  # a resend
        assert ledger.attach(entry, print, "conn-2") == (True, None)
        ledger.forget_waiter("w", "conn-2")  # that connection closed
        assert ledger.waiters("w") == 2
        assert ledger.finish("w", entry, "r") == [got.append, got.extend]
        assert ledger.attach(entry, got.append, "conn-1") == (False, None)
        assert entry.response == "r"

    def test_waiters_of_one_connection_are_bounded(self):
        from maru_storage_service.op_ledger import WAITERS_PER_CONNECTION

        ledger = OpLedger()
        _, entry = ledger.begin("w", op(1), b"d")
        replies = [object() for _ in range(WAITERS_PER_CONNECTION + 1)]
        for reply in replies[:-1]:
            assert ledger.attach(entry, reply, "c") == (True, None)
        assert ledger.attach(entry, replies[-1], "c") == (True, replies[0])
        assert ledger.waiters("w") == WAITERS_PER_CONNECTION

    def test_drop_waits_for_running_ops(self):
        ledger = OpLedger()
        _, entry = ledger.begin("w", op(1), b"d")
        ledger.drop("w")
        assert ledger.size("w") == 1
        ledger.finish("w", entry, "r")
        assert ledger.size("w") == 0


class TestOpOrigin:
    def test_seq_digest_and_ack(self):
        origin = OpOrigin("inc")
        req = LookupAndPinRequest(
            session="s", server_epoch="e", op=PLACEHOLDER_OP, items=[Key("ns", "k")]
        )
        a = origin.stamp(req)
        b = origin.stamp(req)
        assert (a.op.seq, b.op.seq) == (1, 2)
        assert a.op.digest == op_digest(req) == b.op.digest
        assert (a.op.ack, b.op.ack) == (0, 0)
        origin.resolved(2)
        assert origin.ack == 0  # seq 1 is still unknown
        origin.resolved(1)
        assert origin.ack == 2
        # the digest leaves the session out: a reopened session resends the same op
        import dataclasses

        moved = dataclasses.replace(a, session="other", server_epoch="new")
        assert op_digest(moved) == a.op.digest


class TestMsRpcClient:
    def test_unreachable_is_unknown_and_never_sent_late(self):
        fake = FakeMaruServer()
        url = fake.url
        fake.close()
        client = MsRpcClient(url, timeout_s=0.2)
        t0 = time.monotonic()
        with pytest.raises(RpcUnknownError):
            client.call(MessageType.NODE_OPEN, _open())
        assert time.monotonic() - t0 < 1.0
        client.close()

    def test_round_trip_and_lost_reply(self):
        fake = FakeMaruServer()
        client = MsRpcClient(fake.url, timeout_s=0.3)
        try:
            response = client.call(MessageType.NODE_OPEN, _open())
            assert response.status.value == "OK" and response.node_session
            fake.drop_responses(MessageType.NODE_OPEN)
            with pytest.raises(RpcUnknownError):
                client.call(MessageType.NODE_OPEN, _open())
            # a late reply to a request given up is dropped, not mismatched
            fake.delay(MessageType.NODE_OPEN, 0.5)
            with pytest.raises(RpcUnknownError):
                client.call(MessageType.NODE_OPEN, _open())
            fake.delay(MessageType.NODE_OPEN, 0)
            time.sleep(0.4)
            assert client.call(MessageType.NODE_OPEN, _open()).status.value == "OK"
        finally:
            client.close()
            fake.close()


def _open() -> NodeOpenRequest:
    return NodeOpenRequest(
        node_id="n", node_epoch="e", host_boot_id="b", protocol="ms1"
    )
