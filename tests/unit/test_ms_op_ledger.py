# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Op ledger rules, on their own and through MsService."""

import dataclasses
import threading

import pytest

from maru_common.protocol import CommitItem, CommitRequest, MessageType
from maru_common.storage_types import OpRef, Status
from maru_server.catalog import Catalog
from maru_server.op_ledger import OpLedger, Verdict
from tests.unit.ms_fakes import FakeClock, FakeNode, FakeWorker, Wire, key, make_service

A = ("node", "a")


def op(seq, digest=b"d" * 16, ack=0, origin="a"):
    return OpRef(origin=origin, seq=seq, digest=digest, ack=ack)


def commit_item(k, loc):
    return CommitItem(key=k, location=loc, logical_bytes=loc.length)


class TestLedger:
    def test_same_op_replays_recorded_result(self):
        ledger = OpLedger()
        verdict, record = ledger.begin(A, op(1))
        assert verdict == Verdict.NEW
        result = object()
        ledger.finish(A, record, result, 10)
        verdict, again = ledger.begin(A, op(1))
        assert verdict == Verdict.REPLAY and again.result is result

    def test_same_seq_other_digest_is_rejected(self):
        ledger = OpLedger()
        _, record = ledger.begin(A, op(1))
        ledger.finish(A, record, "r", 1)
        assert ledger.begin(A, op(1, digest=b"x" * 16))[0] == Verdict.REJECTED

    def test_seq_at_or_below_the_origin_ack_is_stale(self):
        ledger = OpLedger()
        for seq in (1, 2, 3):
            _, record = ledger.begin(A, op(seq, ack=seq - 1))
            ledger.finish(A, record, seq, 1)
        # an old resend after a later request acknowledged it
        assert ledger.begin(A, op(2, ack=1))[0] == Verdict.STALE

    def test_records_only_above_ack(self):
        ledger = OpLedger()
        for seq in (1, 2, 3):
            _, record = ledger.begin(A, op(seq))
            ledger.finish(A, record, seq, 100)
        assert ledger.pending(A) == 3 and ledger.stored_bytes(A) == 300
        _, record = ledger.begin(A, op(4, ack=2))
        ledger.finish(A, record, 4, 100)
        assert ledger.pending(A) == 2 and ledger.stored_bytes(A) == 200
        assert ledger.ack(A) == 2

    def test_window_full_is_busy_until_acknowledged(self):
        ledger = OpLedger(op_window=2)
        for seq in (1, 2):
            _, record = ledger.begin(A, op(seq))
            ledger.finish(A, record, seq, 1)
        assert ledger.begin(A, op(3))[0] == Verdict.BUSY
        assert ledger.begin(A, op(3, ack=1))[0] == Verdict.NEW
        assert ledger.begin(A, op(4, ack=1))[0] == Verdict.BUSY
        # a resend of a recorded op is still answered when full
        assert ledger.begin(A, op(2, ack=1))[0] == Verdict.REPLAY

    def test_byte_cap_is_busy_until_acknowledged(self):
        ledger = OpLedger(op_bytes_max=1000)
        _, record = ledger.begin(A, op(1))
        ledger.finish(A, record, "big", 1000)
        assert ledger.begin(A, op(2))[0] == Verdict.BUSY
        assert ledger.begin(A, op(2, ack=1))[0] == Verdict.NEW

    def test_kinds_never_collide(self):
        ledger = OpLedger(op_window=1)
        _, node = ledger.begin(("node", "x"), op(1, origin="x"))
        ledger.finish(("node", "x"), node, "node", 1)
        verdict, worker = ledger.begin(("worker", "x"), op(1, origin="x"))
        assert verdict == Verdict.NEW

    def test_drop_origin_respects_its_owner(self):
        ledger = OpLedger()
        origin = ("worker", "w")
        _, record = ledger.begin(origin, op(1, origin="w"), owner="n1")
        ledger.finish(origin, record, "r", 1)
        assert not ledger.drop_origin(origin, owner="n2")
        assert origin in ledger
        assert ledger.drop_origin(origin, owner="n1")
        assert origin not in ledger
        assert ledger.begin(origin, op(1, origin="w"))[0] == Verdict.NEW

    def test_resend_waits_for_the_running_op(self):
        ledger = OpLedger()
        _, record = ledger.begin(A, op(1))
        verdict, same = ledger.begin(A, op(1))
        assert verdict == Verdict.WAIT and same is record
        seen = []

        def waiter():
            same.event.wait(5)
            seen.append(ledger.begin(A, op(1)))

        thread = threading.Thread(target=waiter)
        thread.start()
        ledger.finish(A, record, "done", 1)
        thread.join(5)
        assert seen[0][0] == Verdict.REPLAY and seen[0][1].result == "done"

    def test_abort_lets_a_resend_run(self):
        ledger = OpLedger()
        _, record = ledger.begin(A, op(1))
        ledger.abort(A, record)
        assert record.event.is_set()
        assert ledger.begin(A, op(1))[0] == Verdict.NEW

    def test_limits_must_be_positive(self):
        with pytest.raises(ValueError):
            OpLedger(op_window=0)
        with pytest.raises(ValueError):
            OpLedger(op_bytes_max=0)


class TestServiceOps:
    @pytest.fixture
    def env(self):
        service = make_service(FakeClock(), op_window=3)
        wire = Wire(service)
        node = FakeNode(wire, "n1")
        assert node.open().status == Status.OK
        return service, wire, node

    def test_commit_resend_gives_same_result_without_running_again(self, env):
        service, wire, node = env
        loc = node.loc(node.dram(), 1)
        request = node.build(
            CommitRequest,
            **node.envelope(),
            items=[commit_item(key("k"), loc)],
        )
        first = wire.call(MessageType.COMMIT, request)
        assert first.items[0].result == "ADDED"
        assert wire.call(MessageType.COMMIT, request) == first
        assert len(service.catalog.locations(key("k"))) == 1
        assert len(service.pins) == 0

    def test_digest_must_match_the_raw_payload(self, env):
        _, wire, node = env
        bad = node.build(CommitRequest, **node.envelope(), items=[])
        bad = dataclasses.replace(bad, op=dataclasses.replace(bad.op, digest=b"0" * 16))
        response = wire.call(MessageType.COMMIT, bad)
        assert response.status == Status.REJECTED and "digest" in response.error

    def test_origin_must_be_the_sessions_run(self, env):
        _, wire, node = env
        request = node.build(CommitRequest, **node.envelope(), items=[])
        request = dataclasses.replace(
            request, op=dataclasses.replace(request.op, origin="someone-else")
        )
        response = wire.call(MessageType.COMMIT, request)
        assert response.status == Status.REJECTED and "origin" in response.error

    def test_ack_at_or_above_seq_is_rejected(self, env):
        service, wire, node = env
        node.seq, node.ack = 4, 5  # the next op is seq 5 with ack 5
        response = node.commit((key("k"), node.loc(node.dram(), 1)))
        assert response.status == Status.REJECTED and "ack" in response.error
        assert not service.catalog.locations(key("k"))

    def test_same_seq_other_request_is_rejected(self, env):
        _, wire, node = env
        node.commit((key("a"), node.loc(node.dram(), 1)))
        node.seq, node.ack = 0, 0  # reuse seq 1 for a different request
        response = node.commit((key("b"), node.loc(node.dram(), 2)))
        assert response.status == Status.REJECTED

    def test_stale_resend_answers_stale_items(self, env):
        _, wire, node = env
        first = node.build(
            CommitRequest,
            **node.envelope(),
            items=[commit_item(key("k"), node.loc(node.dram(), 1))],
        )
        wire.call(MessageType.COMMIT, first)
        node.ack = first.op.seq
        node.commit((key("j"), node.loc(node.dram(), 2)))  # carries ack=1
        response = wire.call(MessageType.COMMIT, first)
        assert response.status == Status.OK
        assert [i.result for i in response.items] == ["STALE"]

    def test_window_full_is_busy(self, env):
        _, wire, node = env
        for page in range(3):
            request = node.build(
                CommitRequest,
                **node.envelope(),
                items=[commit_item(key(f"k{page}"), node.loc(node.dram(), page))],
            )
            node.ack = 0
            assert wire.call(MessageType.COMMIT, request).status == Status.OK
        request = node.build(
            CommitRequest,
            **node.envelope(),
            items=[commit_item(key("k9"), node.loc(node.dram(), 9))],
        )
        assert wire.call(MessageType.COMMIT, request).status == Status.BUSY
        request = dataclasses.replace(
            request, op=dataclasses.replace(request.op, ack=request.op.seq - 1)
        )
        assert wire.call(MessageType.COMMIT, request).status == Status.OK

    def test_byte_cap_is_busy(self):
        service = make_service(FakeClock(), op_bytes_max=1)
        wire = Wire(service)
        node = FakeNode(wire, "n1")
        node.open()
        node.commit((key("a"), node.loc(node.dram(), 1)))
        node.ack = 0  # nothing acknowledged
        response = node.commit((key("b"), node.loc(node.dram(), 2)))
        assert response.status == Status.BUSY

    def test_concurrent_duplicates_run_once(self, env):
        # the service lock serializes them: the first runs, the rest replay
        service, wire, node = env
        calls = []
        original = service.catalog.commit
        gate = threading.Event()

        def slow_commit(*args, **kwargs):
            calls.append(1)
            gate.wait(5)
            return original(*args, **kwargs)

        service.catalog.commit = slow_commit
        request = node.build(
            CommitRequest,
            **node.envelope(),
            items=[commit_item(key("k"), node.loc(node.dram(), 1))],
        )
        results = []
        threads = [
            threading.Thread(
                target=lambda: results.append(wire.call(MessageType.COMMIT, request))
            )
            for _ in range(3)
        ]
        for t in threads:
            t.start()
        gate.set()
        for t in threads:
            t.join(5)
        assert len(calls) == 1
        assert len(results) == 3 and all(r == results[0] for r in results)

    def test_failure_before_any_change_leaves_no_record(self, env):
        service, wire, node = env
        original = service.catalog.check_new_location
        service.catalog.check_new_location = lambda *a: 1 / 0
        request = node.build(
            CommitRequest,
            **node.envelope(),
            items=[commit_item(key("k"), node.loc(node.dram(), 1))],
        )
        with pytest.raises(ZeroDivisionError):
            wire.call(MessageType.COMMIT, request)
        service.catalog.check_new_location = original
        assert [i.result for i in wire.call(MessageType.COMMIT, request).items] == [
            "ADDED"
        ]

    def test_partial_run_is_recorded_and_never_freed_by_a_resend(self, env):
        service, wire, node = env
        original = Catalog._add
        calls = []

        def failing_add(self, entry):
            calls.append(entry)
            if len(calls) == 2:
                raise RuntimeError("boom")
            original(self, entry)

        service.catalog._add = failing_add.__get__(service.catalog)
        locs = [node.loc(node.dram(), page) for page in (1, 2, 3)]
        request = node.build(
            CommitRequest,
            **node.envelope(),
            items=[commit_item(key(f"k{i}"), loc) for i, loc in enumerate(locs)],
        )
        first = wire.call(MessageType.COMMIT, request)
        assert first.status == Status.OK
        assert [i.result for i in first.items] == ["ADDED", "REJECTED", "REJECTED"]
        del service.catalog._add
        # the resend replays: the published page is not answered DUPLICATE
        assert wire.call(MessageType.COMMIT, request) == first
        assert [e.location for e in service.catalog.locations(key("k0"))] == [locs[0]]
        assert not service.catalog.locations(key("k1"))

    def test_a_partial_pin_run_replays_its_first_result(self, env):
        from maru_common.protocol import LookupAndPinRequest

        service, wire, node = env
        node.commit(*[(key(f"k{i}"), node.loc(node.dram(), i + 1)) for i in range(2)])
        worker = FakeWorker(wire, node)
        worker.open()
        original = service.pins.pin
        calls = []

        def failing(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("boom")
            return original(*args, **kwargs)

        service.pins.pin = failing
        request = worker.build(
            LookupAndPinRequest, **worker.envelope(), items=[key("k0"), key("k1")]
        )
        response = wire.call(MessageType.LOOKUP_AND_PIN, request)
        service.pins.pin = original
        assert [i.result for i in response.items] == ["HIT", "REJECTED"]
        # A resend replays the successful pin and the failed item.
        assert len(service.pins) == 1
        assert wire.call(MessageType.LOOKUP_AND_PIN, request) == response
        assert len(service.pins) == 1

    def test_a_pin_is_rolled_back_when_record_access_fails(self, env, monkeypatch):
        from maru_common.protocol import LookupAndPinRequest

        service, wire, node = env
        keys = [key("k0"), key("k1")]
        locs = [node.loc(node.dram(), i + 1) for i in range(2)]
        node.commit(*zip(keys, locs, strict=True))
        worker = FakeWorker(wire, node)
        assert worker.open().status == Status.OK
        original = service.registry.record_access
        pins = []

        def failing(pool, location):
            held = service.pins.held_by(worker.token)
            pins.append(next(p.pin_id for p in held if p.location == location))
            if len(pins) == 2:
                assert len(held) == 2
                assert service.pins.count(keys[1], locs[1]) == 1
                raise RuntimeError("record_access failed")
            return original(pool, location)

        request = worker.build(LookupAndPinRequest, **worker.envelope(), items=keys)
        with monkeypatch.context() as patch:
            patch.setattr(service.registry, "record_access", failing)
            first = wire.call(MessageType.LOOKUP_AND_PIN, request)

        assert first.status == Status.OK
        assert [i.result for i in first.items] == ["HIT", "REJECTED"]
        assert len(pins) == 2 and first.items[0].pin_id == pins[0]
        assert service.pins.get(pins[0]).location == locs[0]
        assert service.pins.get(pins[1]) is None
        assert service.pins.count(keys[0], locs[0]) == 1
        assert service.pins.count(keys[1], locs[1]) == 0
        assert len(service.pins) == 1
        assert wire.call(MessageType.LOOKUP_AND_PIN, request) == first
        assert [p.pin_id for p in service.pins.held_by(worker.token)] == [pins[0]]
        assert service.pins.count(keys[1], locs[1]) == 0
        assert len(service.pins) == 1

    def test_closing_a_probe_keeps_the_workers_records(self, env):
        from maru_common.protocol import LookupAndPinRequest
        from maru_common.storage_types import SessionRole

        service, wire, node = env
        k = key("k")
        node.commit((k, node.loc(node.dram(), 1)))
        worker = FakeWorker(wire, node)
        worker.open()
        probe = FakeWorker(wire, node, role=SessionRole.PROBE)
        probe.incarnation = worker.incarnation  # one process, both roles
        probe.open()
        request = worker.build(LookupAndPinRequest, **worker.envelope(), items=[k])
        first = wire.call(MessageType.LOOKUP_AND_PIN, request)
        probe.close()
        # the reply was lost: the resend replays instead of pinning again
        assert wire.call(MessageType.LOOKUP_AND_PIN, request) == first
        assert len(service.pins) == 1

    def test_worker_close_drops_the_worker_records(self, env):
        service, wire, node = env
        worker = FakeWorker(wire, node)
        worker.open()
        worker.lookup(key("nothing"))
        worker.close()
        assert ("worker", worker.incarnation) not in service.ledger

    def test_worker_exit_drops_the_worker_records(self, env):
        service, wire, node = env
        worker = FakeWorker(wire, node)
        worker.open()
        worker.lookup(key("nothing"))
        origin = ("worker", worker.incarnation)
        assert origin in service.ledger
        other = FakeNode(wire, "n2")
        other.open()
        other.worker_exit(worker.ref())  # not its worker: REJECTED, kept
        assert origin in service.ledger
        node.worker_exit(worker.ref())
        assert origin not in service.ledger

    def test_node_epoch_replaced_drops_its_records(self, env):
        service, wire, node = env
        node.commit((key("k"), node.loc(node.dram(), 1)))
        old = ("node", node.epoch)
        assert old in service.ledger
        node.restart()
        assert node.open().status == Status.OK
        assert old not in service.ledger
