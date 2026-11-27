# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PoolManager states, budgets and the bytes invariant, including a stateful
property test over every transition."""

import dataclasses

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    invariant,
    multiple,
    rule,
)

from maru_common.node_protocol import Budgets
from maru_common.storage_types import (
    GENERATION_COUNTER_MAX,
    Key,
    Location,
    PoolInfo,
    PoolKind,
    generation_epoch_seq,
    make_generation,
)
from maru_storage_service.pool_manager import (
    RECOVERY_TAG,
    InvalidTransitionError,
    PageRef,
    PageState,
    PoolManager,
    holder_tag,
)

PS = 4096
PAGES = 16


def budgets(**overrides) -> Budgets:
    values = {
        "credit_max_bytes": 4 * PS,
        "credit_total_max_bytes": 8 * PS,
        "credit_refill_low": PS,
        "credit_refill_high": 4 * PS,
        "commit_pending_max_bytes": 3 * PS,
        "transfer_reserve_bytes": {"dram": 2 * PS},
    }
    values.update(overrides)
    return Budgets(**values)


def dram(pool_id="dram:n:e", pages=PAGES) -> PoolInfo:
    return PoolInfo(
        pool_id=pool_id,
        kind=PoolKind.DRAM,
        node_id="n",
        pool_generation=1,
        capacity=pages * PS,
        page_size=PS,
    )


def manager(epoch_seq=3, **overrides) -> PoolManager:
    pm = PoolManager(budgets(**overrides), PS)
    pm.add_pool(dram(), tier="dram")
    pm.set_epoch_seq(epoch_seq)
    return pm


def ref_of(credit: Location) -> PageRef:
    return PageRef(credit.pool_id, credit.offset // PS, credit.generation)


class TestTiers:
    def test_a_pool_is_in_the_tier_it_was_added_to(self):
        pm = PoolManager(budgets(transfer_reserve_bytes={"fast": 14 * PS}), PS)
        pm.add_pool(dram(), tier="dram")
        pm.add_pool(dram("dram:n:f"), tier="fast")
        pm.set_epoch_seq(3)
        assert [pm.tier_of(p) for p in ("dram:n:e", "dram:n:f", "x")] == [
            "dram",
            "fast",
            None,
        ]
        # credit and the transfer reserve go by tier, not by kind
        got = pm.credit("w", 4 * PS, "fast")
        assert [c.pool_id for c in got] == ["dram:n:f"] * 2
        pm.check()


class TestCredit:
    def test_no_credit_before_epoch_seq(self):
        pm = PoolManager(budgets(), PS)
        pm.add_pool(dram(), tier="dram")
        assert pm.credit("w", PS) == []
        pm.set_epoch_seq(1)
        assert len(pm.credit("w", PS)) == 1

    def test_credit_is_whole_pages_with_new_generations(self):
        pm = manager()
        credits = pm.credit("w", PS + 1)
        assert len(credits) == 2
        assert all(c.length == PS and c.offset % PS == 0 for c in credits)
        assert all(generation_epoch_seq(c.generation) == 3 for c in credits)
        assert credits[0].generation != credits[1].generation
        assert credits[0].allocation_id != credits[1].allocation_id
        assert pm.held_bytes("w") == 2 * PS
        pm.check()

    def test_per_worker_budget(self):
        pm = manager()
        assert len(pm.credit("w", 100 * PS)) == 4  # credit_max_bytes
        assert pm.credit("w", PS) == []
        assert len(pm.credit("other", PS)) == 1

    def test_node_budget(self):
        pm = manager()
        for worker in ("a", "b"):
            assert len(pm.credit(worker, 4 * PS)) == 4
        assert pm.credit("c", PS) == []  # credit_total_max_bytes is 8 pages

    def test_waiting_worker_gets_its_fair_share_back(self):
        pm = manager(credit_max_bytes=2 * PS, credit_total_max_bytes=2 * PS)
        a = pm.credit("a", 2 * PS)
        assert len(a) == 2 and pm.credit_asks("a") == []
        assert pm.credit("b", 2 * PS) == []  # the node-total budget is A's
        assert pm.credit_asks("b") == [("a", PS)]  # A is above its fair share
        assert pm.credit_asks("b") == []  # asked once per CREDIT_ASK_S
        assert pm.credit_return("a", a[0])[0]
        # the page returned is owed to B: A cannot take it back
        assert pm.credit("a", PS) == [] and pm.credit_asks("a") == []
        assert len(pm.credit("b", 2 * PS)) == 1
        assert pm.credit_asks("b") == []  # B holds its fair share now
        assert pm.held_bytes("a") == pm.held_bytes("b") == PS
        pm.check()

    def test_a_waiting_claim_ends(self, monkeypatch):
        import types

        from maru_storage_service import pool_manager

        now = [1000.0]
        monkeypatch.setattr(
            pool_manager, "time", types.SimpleNamespace(monotonic=lambda: now[0])
        )
        pm = manager(credit_max_bytes=2 * PS, credit_total_max_bytes=2 * PS)
        a = pm.credit("a", 2 * PS)
        assert pm.credit("b", PS) == []
        pm.credit_return("a", a[0])
        assert pm.credit("a", PS) == []  # owed to B
        now[0] += pool_manager.CREDIT_WAIT_S  # B stopped asking
        assert len(pm.credit("a", PS)) == 1
        # a holder that ends, or is forgotten, waits no more
        assert pm.credit("b", PS) == [] and "b" in pm._waiting
        pm.quarantine_holder("b")
        assert "b" not in pm._waiting
        assert pm.credit("b", PS) == [] and pm.credit_asks("b") == [("a", PS)]
        assert pm.forget_holder("b") and "b" not in pm._waiting
        assert not pm.forget_holder("a")  # it holds credit
        pm.check()

    def test_transfer_reserve_is_not_credit(self):
        pm = manager(credit_max_bytes=16 * PS, credit_total_max_bytes=16 * PS)
        assert len(pm.credit("w", 16 * PS)) == PAGES - 2
        # the reserve stays for transfers
        assert pm.bytes_in(PageState.FREE) == 2 * PS
        pm.check()

    def test_pending_cap_stops_credit(self):
        pm = manager()
        credits = pm.credit("w", 4 * PS)
        for i, c in enumerate(credits[:3]):
            pm.write_done("w", c, Key("ns", f"k{i}"), 10)
        assert pm.bytes_in(PageState.PENDING) == 3 * PS  # the cap
        assert pm.credit("x", PS) == []
        ref = pm.find_allocation(
            credits[0].pool_id, credits[0].allocation_id, credits[0].generation
        )
        pm.commit_added(ref)
        assert len(pm.credit("x", PS)) == 1

    def test_stopped_credit(self):
        pm = manager()
        pm.stop_credit("safe state")
        assert pm.credit("w", PS) == []
        assert pm.credit_stopped == "safe state"
        assert pm.credit_short["stopped"] == 1

    def test_a_short_grant_names_the_limit(self):
        pm = manager()  # cap 4 pages a worker, 8 in all, 14 free past the reserve
        assert len(pm.credit("w", 6 * PS)) == 4
        assert dict(pm.credit_short) == {"worker_cap": 1}
        assert dict(pm.credit_short_bytes) == {"worker_cap": 2 * PS}
        pm.credit("x", 4 * PS)
        assert len(pm.credit("y", 4 * PS)) == 0
        assert pm.credit_short["node_cap"] == 1


def cxl(pool_id: str, pages: int) -> PoolInfo:
    return dataclasses.replace(dram(pool_id, pages), kind=PoolKind.CXL)


class TestCreditAcrossTiers:
    def test_a_pool_off_credit_gives_none(self):
        pm = manager()
        pm.disable_pool("dram:n:e")
        assert pm.credit("w", PS) == []

    def test_generation_counter_exhaustion_stops_credit(self):
        pm = manager()
        pm._counter = GENERATION_COUNTER_MAX
        assert len(pm.credit("w", 4 * PS)) == 1  # the last generation
        assert pm.credit("w", PS) == []
        assert "restart" in pm.credit_stopped

    def test_epoch_seq_is_set_once(self):
        pm = manager(epoch_seq=3)
        pm.set_epoch_seq(3)
        with pytest.raises(InvalidTransitionError):
            pm.set_epoch_seq(4)


class TestWorkerTransitions:
    def test_write_done_moves_to_pending_with_logical_length(self):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        ref, location, why = pm.write_done("w", credit, Key("ns", "k"), 100)
        assert why == ""
        assert location.length == 100 and location.offset == credit.offset
        assert pm.page(ref).state == PageState.PENDING
        assert pm.held_bytes("w") == 0
        pm.check()

    @pytest.mark.parametrize(
        "mutate, reason",
        [
            (
                lambda c: c.__class__(**{**c.__dict__, "pool_id": "dram:x:y"}),
                "not a pool",
            ),
            (
                lambda c: c.__class__(
                    **{**c.__dict__, "generation": make_generation(2, 0)}
                ),
                "another StorageService run",
            ),
            (
                lambda c: c.__class__(
                    **{**c.__dict__, "generation": c.generation + 99}
                ),
                "another generation",
            ),
            (
                lambda c: c.__class__(**{**c.__dict__, "allocation_id": "x"}),
                "allocation id",
            ),
            (lambda c: c.__class__(**{**c.__dict__, "offset": c.offset + 1}), "offset"),
            (lambda c: c.__class__(**{**c.__dict__, "length": 1}), "length"),
            (lambda c: c.__class__(**{**c.__dict__, "kind": PoolKind.CXL}), "kind"),
        ],
    )
    def test_write_done_refuses_credit_that_does_not_match(self, mutate, reason):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        ref, _, why = pm.write_done("w", mutate(credit), Key("ns", "k"), 10)
        assert ref is None and reason in why
        assert pm.bytes_in(PageState.CREDITED) == PS  # nothing changed
        pm.check()

    def test_write_done_refuses_another_holder_and_bad_sizes(self):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        assert "another worker" in pm.write_done("x", credit, Key("ns", "k"), 10)[2]
        assert pm.write_done("w", credit, Key("ns", "k"), 0)[0] is None
        assert pm.write_done("w", credit, Key("ns", "k"), PS + 1)[0] is None
        assert pm.write_done("w", credit, Key("ns", "k"), PS)[0] is not None
        # the same credit twice: the page is PENDING now
        assert "PENDING" in pm.write_done("w", credit, Key("ns", "k"), 10)[2]

    def test_abort_frees_only_the_holders_page(self):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        assert pm.abort("x", credit) == (
            False,
            "the page is credited to another worker",
        )
        assert pm.abort("w", credit) == (True, "")
        assert pm.bytes_in(PageState.FREE) == PAGES * PS
        assert pm.abort("w", credit)[0] is False
        pm.check()

    def test_credit_return_is_idempotent(self):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        assert pm.credit_return("w", credit) == (True, "")
        assert pm.credit_return("w", credit) == (True, "already returned")
        # the page moved on to another worker: the old credit stays a no-op
        while True:
            (again,) = pm.credit("v", PS)
            if again.offset == credit.offset:
                break
            pm.credit_return("v", again)
        assert pm.credit_return("w", credit) == (True, "already returned")
        assert pm.page(ref_of(again)).holder == "v"
        # a pending page is not returned
        pm.write_done("v", again, Key("ns", "k"), 1)
        assert pm.credit_return("v", again)[0] is False
        pm.check()


class TestServiceTransitions:
    def test_commit_results(self):
        pm = manager()
        a, b = pm.credit("w", 2 * PS)
        ra, la, _ = pm.write_done("w", a, Key("ns", "a"), 10)
        rb, _, _ = pm.write_done("w", b, Key("ns", "b"), 10)
        assert pm.commit_added(ra) == la
        pm.commit_freed(rb)
        assert pm.page(ra).state == PageState.READY
        assert pm.page(rb).state == PageState.FREE
        # never freed twice
        with pytest.raises(InvalidTransitionError):
            pm.commit_freed(rb)
        with pytest.raises(InvalidTransitionError):
            pm.commit_added(rb)
        pm.check()

    def test_retire_then_reclaim(self):
        pm = manager()
        (c,) = pm.credit("w", PS)
        ref, location, _ = pm.write_done("w", c, Key("ns", "a"), 10)
        pm.commit_added(ref)
        assert pm.find(location) == ref
        pm.retired(ref)
        assert pm.page(ref).state == PageState.RETIRING
        pm.reclaim(ref)
        assert pm.page(ref).state == PageState.FREE
        assert pm.find(location) is None
        with pytest.raises(InvalidTransitionError):
            pm.reclaim(ref)
        pm.check()

    def test_quarantine_and_release_by_holder(self):
        pm = manager()
        pm.credit("w", 3 * PS)
        pm.credit("v", PS)
        refs = pm.quarantine_holder("w")
        assert len(refs) == 3 and pm.held_bytes("w") == 0
        assert pm.bytes_in(PageState.QUARANTINED) == 3 * PS
        assert pm.release_quarantine(holder_tag("v")) == 0
        assert pm.release_quarantine("w") == 0  # tags are not bare incarnations
        assert pm.release_quarantine(holder_tag("w")) == 3
        assert pm.bytes_in(PageState.CREDITED) == PS
        pm.check()

    def test_adopted_pool_ledger(self):
        pm = manager()
        info = PoolInfo(
            pool_id="cxl:rm:1",
            kind=PoolKind.CXL,
            node_id="n",
            pool_generation=1,
            capacity=4 * PS,
            page_size=PS,
        )
        pm.add_pool(
            info, creditable=False, state=PageState.QUARANTINED, tier="local_cxl"
        )
        loc = Location(
            "cxl:rm:1", "a1", make_generation(2, 7), 2 * PS, 100, PoolKind.CXL
        )
        (ref,) = pm.recovered("cxl:rm:1", [(Key("ns", "r"), loc)])
        assert pm.page(ref).state == PageState.READY
        assert pm.find(loc) == ref
        assert pm.bytes_in(PageState.QUARANTINED, "cxl:rm:1") == 3 * PS
        assert pm.release_quarantine(RECOVERY_TAG, "cxl:rm:1") == 3
        bad = Location("cxl:rm:1", "b", 1, -PS, 1, PoolKind.CXL)
        pm2 = PoolManager(budgets(), PS)
        pm2.add_pool(
            info, creditable=False, state=PageState.QUARANTINED, tier="local_cxl"
        )
        with pytest.raises(InvalidTransitionError):
            pm2.recovered("cxl:rm:1", [(Key("ns", "x"), bad)])
        # credit never comes from a pool workers cannot map yet
        pm2 = PoolManager(budgets(), PS)
        pm2.add_pool(
            info, creditable=False, state=PageState.QUARANTINED, tier="local_cxl"
        )
        pm2.set_epoch_seq(1)
        assert pm2.credit("w", PS) == []
        pm.check()

    def test_a_quarantined_pool_frees_nothing_until_its_quarantine_ends(self):
        """An earlier run may still write it: retired pages stay QUARANTINED,
        and no credit comes from it, until it ends."""
        pm = PoolManager(budgets(), PS)
        info = dataclasses.replace(
            dram(), pool_id="cxl:rm:5", kind=PoolKind.CXL, capacity=4 * PS
        )
        pm.add_pool(
            info, creditable=False, state=PageState.QUARANTINED, tier="local_cxl"
        )
        pm.quarantine_pool(info.pool_id)
        pm.set_epoch_seq(2)
        locs = [
            Location(info.pool_id, f"a{i}", make_generation(1, i), i * PS, 9, info.kind)
            for i in (0, 1)
        ]
        first, second = pm.recovered(
            info.pool_id, [(Key("ns", f"k{i}"), loc) for i, loc in enumerate(locs)]
        )
        assert pm.pool_quarantined(info.pool_id)
        assert pm.credit("w", PS, "local_cxl") == []
        for ref in (first, second):
            pm.retired(ref)
            pm.reclaim(ref)
        assert pm.bytes_in(PageState.QUARANTINED, info.pool_id) == 4 * PS
        assert pm.bytes_in(PageState.FREE, info.pool_id) == 0
        assert pm.end_pool_quarantine(info.pool_id, creditable=True) == 4
        assert not pm.pool_quarantined(info.pool_id)
        assert pm.creditable(info.pool_id)
        assert len(pm.credit("w", PS, "local_cxl")) == 1
        pm.check()

    def _ready(self, pm, key="a"):
        (c,) = pm.credit("w", PS)
        ref, location, _ = pm.write_done("w", c, Key("ns", key), 10)
        pm.commit_added(ref)
        return ref, location

    def test_reconcile_takes_the_occupant(self):
        pm = manager()
        ref, location = self._ready(pm)
        occupant = Location(
            location.pool_id,
            "theirs",
            location.generation + 5,
            location.offset,
            20,
            PoolKind.DRAM,
        )
        new = pm.reconcile(ref, Key("ns", "other"), occupant)
        page = pm.page(new)
        assert (page.state, page.key, page.location) == (
            PageState.READY,
            Key("ns", "other"),
            occupant,
        )
        assert (
            pm.find_allocation(occupant.pool_id, "theirs", occupant.generation) == new
        )
        assert pm.find(location) is None  # the old allocation is gone
        pm.check()

    @pytest.mark.parametrize(
        "change",
        [
            {"pool_id": "dram:x:y"},
            {"offset": PS},
            {"kind": PoolKind.CXL},
            {"length": PS + 1},
            {"length": 0},
        ],
        ids=["pool", "offset", "kind", "too-long", "empty"],
    )
    def test_reconcile_refuses_what_does_not_fit(self, change):
        pm = manager()
        ref, location = self._ready(pm)
        occupant = Location(
            **{**location.__dict__, "allocation_id": "theirs", **change}
        )
        with pytest.raises(InvalidTransitionError):
            pm.reconcile(ref, Key("ns", "other"), occupant)
        assert pm.page(ref).location == location  # unchanged
        pm.check()

    def test_reconcile_refuses_an_allocation_of_another_page(self):
        pm = manager()
        ref, location = self._ready(pm, "a")
        _, elsewhere = self._ready(pm, "b")
        occupant = dataclasses.replace(location, allocation_id=elsewhere.allocation_id)
        with pytest.raises(InvalidTransitionError, match="already"):
            pm.reconcile(ref, Key("ns", "b"), occupant)
        pm.check()

    def test_find_ready_keeps_the_order_and_skips_the_rest(self):
        pm = manager()
        ref_a, a = self._ready(pm, "a")
        ref_b, b = self._ready(pm, "b")
        (credited,) = pm.credit("w", PS)
        reads = [
            (b.pool_id, b.offset, b.generation),
            (a.pool_id, a.offset, a.generation + 1),  # another generation
            (credited.pool_id, credited.offset, credited.generation),  # not READY
            ("dram:x:y", a.offset, a.generation),  # unknown pool
            (a.pool_id, a.offset + 1, a.generation),  # not a page start
            (a.pool_id, PAGES * PS, a.generation),  # past the pool
            (a.pool_id, a.offset, a.generation),
        ]
        assert pm.find_ready(reads) == [ref_b, ref_a]

    def test_usage_reports_each_state(self):
        pm = manager()
        a, b, c = pm.credit("w", 3 * PS)
        ra, _, _ = pm.write_done("w", a, Key("ns", "a"), 1)
        pm.commit_added(ra)
        pm.write_done("w", b, Key("ns", "b"), 1)
        (usage,) = pm.usage()
        assert (usage.ready, usage.pending, usage.credited) == (PS, PS, PS)
        assert usage.free == (PAGES - 3) * PS
        assert (
            usage.free
            + usage.credited
            + usage.transfer
            + usage.pending
            + usage.ready
            + usage.retiring
            + usage.quarantined
            == usage.capacity
        )


class LedgerMachine(RuleBasedStateMachine):
    """Every transition in any order the service could ask for."""

    credits = Bundle("credits")
    pending = Bundle("pending")
    ready = Bundle("ready")
    retiring = Bundle("retiring")

    def __init__(self):
        super().__init__()
        self.pm = PoolManager(budgets(), PS)
        self.pm.add_pool(dram(), tier="dram")
        self.pm.set_epoch_seq(5)
        self.keys = 0
        self.history = []  # every credit ever given, for stale replays

    @rule(
        target=credits,
        worker=st.sampled_from(["a", "b", "c"]),
        want=st.integers(min_value=1, max_value=6 * PS),
    )
    def credit(self, worker, want):
        pending_before = self.pm.write_pending_bytes()
        held = self.pm.held_bytes(worker)
        got = self.pm.credit(worker, want)
        if pending_before >= self.pm.budgets.commit_pending_max_bytes:
            assert got == []
        assert held + len(got) * PS <= self.pm.budgets.credit_max_bytes
        self.history.extend((worker, c) for c in got)
        return multiple(*[(worker, c) for c in got])

    @rule(target=pending, credit=consumes(credits))
    def write_done(self, credit):
        worker, location = credit
        self.keys += 1
        ref, _, _ = self.pm.write_done(worker, location, Key("ns", f"k{self.keys}"), 7)
        # None when the worker ended and its page was quarantined meanwhile
        return multiple(ref) if ref is not None else multiple()

    @rule(credit=consumes(credits))
    def abort(self, credit):
        self.pm.abort(*credit)

    @rule(credit=consumes(credits))
    def credit_return(self, credit):
        worker, location = credit
        page = self.pm.page(ref_of(location))
        # Held, or already back (FREE at that generation, or moved past it)
        expected = (
            page is None
            or page.state == PageState.FREE
            or (page.state == PageState.CREDITED and page.holder == worker)
        )
        assert self.pm.credit_return(worker, location)[0] == expected
        if expected:
            assert self.pm.credit_return(worker, location) == (True, "already returned")

    @rule(target=ready, ref=consumes(pending))
    def added(self, ref):
        self.pm.commit_added(ref)
        return ref

    @rule(ref=consumes(pending))
    def duplicate_or_rejected(self, ref):
        self.pm.commit_freed(ref)

    @rule(target=retiring, ref=consumes(ready))
    def retired(self, ref):
        self.pm.retired(ref)
        return ref

    @rule(ref=consumes(retiring))
    def reclaimed(self, ref):
        self.pm.reclaim(ref)

    @rule(worker=st.sampled_from(["a", "b", "c"]))
    def worker_ends(self, worker):
        self.pm.quarantine_holder(worker)

    @rule(worker=st.sampled_from(["a", "b", "c"]))
    def grace_over(self, worker):
        self.pm.release_quarantine(holder_tag(worker))

    @rule(
        data=st.data(),
        worker=st.sampled_from(["a", "b", "c"]),
        action=st.sampled_from(["write_done", "abort", "credit_return"]),
    )
    def stale_or_reused_credit(self, data, worker, action):
        """Any credit ever given, by any worker, at any later time."""
        if not self.history:
            return
        _, credit = data.draw(st.sampled_from(self.history))
        page = self.pm.page(ref_of(credit))
        held = (
            page is not None
            and page.state == PageState.CREDITED
            and page.holder == worker
        )
        before = self.pm.usage()
        if action == "write_done":
            self.keys += 1
            ok = self.pm.write_done(worker, credit, Key("ns", f"s{self.keys}"), 3)[0]
            assert (ok is not None) == held
        elif action == "abort":
            assert self.pm.abort(worker, credit)[0] == held
        else:
            back = page is None or page.state == PageState.FREE
            assert self.pm.credit_return(worker, credit)[0] == (held or back)
        if not held:
            assert self.pm.usage() == before  # refused: nothing changed

    @rule(
        pool=st.sampled_from(["dram:n:e", "dram:other:x"]),
        offset=st.integers(min_value=-PS, max_value=(PAGES + 1) * PS),
        generation=st.integers(min_value=0, max_value=make_generation(6, 3)),
        worker=st.sampled_from(["a", "b", "c"]),
    )
    def foreign_credit(self, pool, offset, generation, worker):
        credit = Location(pool, "nope", generation, offset, PS, PoolKind.DRAM)
        before = self.pm.usage()
        assert self.pm.write_done(worker, credit, Key("ns", "f"), 1)[0] is None
        assert self.pm.abort(worker, credit)[0] is False
        self.pm.credit_return(worker, credit)
        assert self.pm.usage() == before

    @invariant()
    def bytes_add_up(self):
        (u,) = self.pm.usage()
        # credit + pending + used + quarantine (+ transfer, retiring, free)
        assert (
            u.credited
            + u.pending
            + u.ready
            + u.quarantined
            + u.transfer
            + u.retiring
            + u.free
            == u.capacity
            == PAGES * PS
        )
        assert u.credited <= self.pm.budgets.credit_total_max_bytes
        self.pm.check()


LedgerMachine.TestCase.settings = settings(
    max_examples=200,
    stateful_step_count=60,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
TestLedgerProperty = LedgerMachine.TestCase
