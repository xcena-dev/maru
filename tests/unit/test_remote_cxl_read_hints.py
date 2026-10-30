# SPDX-License-Identifier: Apache-2.0
"""Read hints: Maru tells the memory side which bytes a load will read.

The directory, the remote CXL agent and both clients (a worker and its
metadata-only scheduler) are real; a fake region extension stands in for a
device package.
"""

import threading
import time

import pytest

import maru_handler.storage.remote_cxl.hints as hints_module
from maru_remote.region_map import RegionMapClient, read_handle
from maru_remote_cxl_agent.service import MapDenied, NotMapped
from tests.unit.remote_fakes import shared_key
from tests.unit.test_remote_cxl_region_extension import FakeExtension
from tests.unit.test_remote_cxl_storage import PAGE, World, only_region, put, read


def _until(condition, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)


@pytest.fixture
def setup():
    ext = FakeExtension()
    w = World(extension=ext)
    yield w, ext
    w.close()


def _actions(ext):
    return [(m["action"], m["request"], len(r)) for _, m, r in ext.hints]


def test_a_scheduler_hints_the_leading_hits_and_ends_the_hint(setup):
    w, ext = setup
    worker = w.client("a")
    for i in range(3):
        assert put(worker, f"k{i}", bytes([i + 1]) * PAGE)[0]
    scheduler = w.client("a", metadata_only=True, read_hints=True)
    # Keys are in prompt order: a miss ends what the load can read.
    scheduler.hint_read("r1", ["k0", "k2", "missing", "k1"])
    _until(lambda: ext.hints)
    conn, message, ranges = ext.hints[0]
    assert message["action"] == "read_hint" and message["request"] == "r1"
    assert set(message) == {"op", "action", "request", "ranges", "epoch"}
    region = next(iter(worker.regions.values()))
    assert [r["handle"] for r in ranges] == [read_handle(region.handle)] * 2
    # The scheduler holds no region: the agent took the hint as advice.
    assert conn not in next(iter(w.node.service.holders().values()))
    # The worker's load is not hinted (its config has no read hints).
    assert read(worker, ["k0", "k2"]) == [b"\x01" * PAGE, b"\x03" * PAGE]
    scheduler.end_read_hint("r1")
    _until(lambda: len(ext.hints) == 2)
    assert _actions(ext) == [("read_hint", "r1", 2), ("read_hint_end", "r1", 0)]


def test_a_request_is_hinted_once_and_a_miss_hints_nothing(setup):
    w, ext = setup
    worker = w.client("a")
    assert put(worker, "k0", b"x" * PAGE)[0]
    scheduler = w.client("a", metadata_only=True, read_hints=True)
    scheduler.hint_read("r1", ["k0"])
    scheduler.hint_read("r1", ["k0"])
    scheduler.hint_read("r2", ["missing", "k0"])
    scheduler.end_read_hint("r2")  # never reached an agent: nothing to end
    scheduler.end_read_hint("r1")
    _until(lambda: len(ext.hints) == 2)
    assert _actions(ext) == [("read_hint", "r1", 1), ("read_hint_end", "r1", 0)]


def test_a_load_without_an_early_hint_is_hinted_as_it_starts(setup):
    w, ext = setup
    worker = w.client("a", read_hints=True)
    assert put(worker, "k0", b"x" * PAGE)[0]
    assert put(worker, "k1", b"y" * PAGE)[0]
    assert read(worker, ["k0", "k1"]) == [b"x" * PAGE, b"y" * PAGE]
    _until(lambda: len(ext.hints) == 2)
    (_, hint, ranges), (_, end, _) = ext.hints
    assert hint["action"] == "read_hint" and len(ranges) == 2
    assert hint["request"].startswith("maru-load-")
    assert (end["action"], end["request"]) == ("read_hint_end", hint["request"])
    region = next(iter(worker.regions.values()))
    assert ranges[0]["handle"] == region.handle  # its own region


def test_without_read_hints_or_an_extension_nothing_is_sent():
    w = World()  # the agent has no region extension
    try:
        worker = w.client("a")
        assert put(worker, "k0", b"x" * PAGE)[0]
        plain = w.client("a", metadata_only=True)
        plain.hint_read("r1", ["k0"])  # read hints off: ignored
        assert plain._hints is None and not plain.read_hints_enabled
        scheduler = w.client("a", metadata_only=True, read_hints=True)
        scheduler.hint_read("r1", ["k0"])
        scheduler.end_read_hint("r1")
        _until(lambda: scheduler._hints._queue.empty())
        assert scheduler._hints.close()
        # The agent said it takes no hints: none was sent, so none to end.
        (agent,) = scheduler._hint_agents.values()
        assert agent.hint_actions == frozenset()
    finally:
        w.close()


def test_the_agent_takes_a_hint_only_for_a_mapped_region(setup):
    w, ext = setup
    worker = w.client("a")
    assert put(worker, "k", b"x" * PAGE)[0]
    service = w.node.service
    region = next(iter(worker.regions.values()))
    stranger = 10_000  # a connection that holds no region

    def message(handle, *, action="read_hint", offset=0, epoch=None):
        return {
            "action": action,
            "request": "r1",
            "epoch": service.epoch if epoch is None else epoch,
            "ranges": [{"handle": handle, "offset": offset, "length": PAGE}],
        }

    service.hint(stranger, message(read_handle(region.handle)))
    service.hint(stranger, message(region.handle))
    assert len(ext.hints) == 2
    with pytest.raises(ValueError, match="action"):
        service.hint(stranger, message(region.handle, action="prepare"))
    with pytest.raises(MapDenied):
        service.hint(stranger, message(region.handle, epoch="old"))
    with pytest.raises(ValueError, match="lie in its region"):
        service.hint(stranger, message(region.handle, offset=region.capacity))
    other = dict(region.handle, region_id=region.handle["region_id"] + 99)
    with pytest.raises(NotMapped):
        service.hint(stranger, message(read_handle(other)))
    wrong = dict(read_handle(region.handle), read_token="0" * 32)
    with pytest.raises(MapDenied):
        service.hint(stranger, message(wrong))
    assert len(ext.hints) == 2  # none of these reached the extension


def test_a_hint_needs_a_region_some_connection_holds(setup):
    w, ext = setup
    worker = w.client("a")
    assert put(worker, "k", b"x" * PAGE)[0]
    service = w.node.service
    region = next(iter(worker.regions.values()))
    m = service._maps[region.handle["region_id"]]
    message = {
        "action": "read_hint",
        "request": "r1",
        "epoch": service.epoch,
        "ranges": [{"handle": read_handle(region.handle), "offset": 0, "length": 1}],
    }
    held = set(m.holders)
    m.holders.clear()  # the last holder left; the sweep has not released it yet
    try:
        with pytest.raises(NotMapped):
            service.hint(10_000, message)
    finally:
        m.holders.update(held)


def test_a_probe_says_where_hits_are_only_when_asked(setup):
    w, ext = setup
    worker = w.client("a")
    assert put(worker, "k0", b"x" * PAGE)[0]
    plain = w.directory.execute(
        "probe", {"engine_id": "a", "namespace": "ns", "keys": ["k0"]}
    )["result"]
    assert "entries" not in plain
    found = w.directory.execute(
        "probe",
        {
            "engine_id": "a",
            "namespace": "ns",
            "keys": ["k0", "nope"],
            "locations": True,
        },
    )["result"]
    entry, missing = found["entries"]
    assert missing is None and entry["length"] == PAGE
    (handle,) = found["regions"].values()
    region = next(iter(worker.regions.values()))
    assert handle == read_handle(region.handle)  # never the owner's handle


def test_a_long_hint_is_split_into_messages_the_agent_takes(monkeypatch):
    monkeypatch.setattr(hints_module, "MAX_HINT_RANGES", 2)
    sent = []
    hints = hints_module.MemoryHints(
        lambda keys: [("agent", [{"offset": i} for i in range(5)])],
        lambda agent, message: sent.append(message) or True,
    )
    hints.hint("r1", ["k"])
    hints.end("r1")
    _until(lambda: len(sent) == 4)
    assert [len(m["ranges"]) for m in sent] == [2, 2, 1, 0]
    assert sent[-1]["action"] == "read_hint_end"
    assert hints.close()


class _Blocked:
    """A hint sender whose first message blocks until released."""

    def __init__(self):
        self.release = threading.Event()
        self.messages = []

    def __call__(self, agent, message):
        self.release.wait(5)
        self.messages.append((message["action"], message["request"]))
        return True


def test_ends_are_kept_when_too_many_hints_wait(monkeypatch):
    monkeypatch.setattr(hints_module, "MAX_PENDING_HINTS", 2)
    warnings = []
    monkeypatch.setattr(
        hints_module.logger, "warning", lambda msg, *a: warnings.append(msg % a)
    )
    send = _Blocked()
    hints = hints_module.MemoryHints(lambda keys: [("agent", [{}])], send)
    hints.hint("r0", ["k"])  # the thread takes it and blocks
    _until(lambda: hints._pending == 0)
    hints.hint("r1", ["k"])
    hints.hint("r2", ["k"])
    hints.hint("r3", ["k"])  # dropped
    assert len(warnings) == 1 and "read hints dropped" in warnings[0]
    for rid in ("r0", "r1", "r2", "r3"):
        hints.end(rid)
    send.release.set()
    _until(lambda: len(send.messages) == 6)
    assert send.messages == [
        ("read_hint", "r0"),
        ("read_hint", "r1"),
        ("read_hint", "r2"),
        ("read_hint_end", "r0"),
        ("read_hint_end", "r1"),
        ("read_hint_end", "r2"),
    ]  # r3 never reached an agent
    assert hints.close()


def test_hints_after_close_are_ignored():
    send = _Blocked()
    send.release.set()
    hints = hints_module.MemoryHints(lambda keys: [("agent", [{}])], send)
    hints.hint("r1", ["k"])
    _until(lambda: send.messages)
    assert hints.close()
    hints.hint("r2", ["k"])
    hints.end("r1")
    assert hints._thread is None and send.messages == [("read_hint", "r1")]


def _hint_message(service, handle, *, request="r1", action="read_hint", ranges=None):
    return {
        "action": action,
        "request": request,
        "epoch": service.epoch,
        "ranges": (
            [{"handle": handle, "offset": 0, "length": PAGE}]
            if ranges is None
            else ranges
        ),
    }


def test_the_extension_takes_a_hint_outside_the_agent_lock(setup):
    w, ext = setup
    worker = w.client("a")
    assert put(worker, "k", b"x" * PAGE)[0]
    service = w.node.service
    region = next(iter(worker.regions.values()))
    seen = []
    # The agent's lock is not reentrant: this would hang under it.
    ext.hint = lambda conn, message, ranges: seen.append(service.holders())
    done = threading.Thread(
        target=service.hint, args=(10_000, _hint_message(service, region.handle))
    )
    done.start()
    done.join(5)
    assert not done.is_alive() and len(seen) == 1


def test_a_connection_keeps_a_bounded_number_of_hint_requests_open():
    ext = FakeExtension()
    w = World(extension=ext, max_hint_requests=2)
    try:
        worker = w.client("a")
        assert put(worker, "k", b"x" * PAGE)[0]
        service = w.node.service
        region = next(iter(worker.regions.values()))
        conn = 10_000

        def hint(request, action="read_hint"):
            ranges = None if action == "read_hint" else []
            message = _hint_message(
                service, region.handle, request=request, action=action, ranges=ranges
            )
            service.hint(conn, message)

        hint("r1")
        hint("r2")
        hint("r2")  # open already: not counted twice
        with pytest.raises(ValueError, match="hint requests open"):
            hint("r3")
        hint("r1", "read_hint_end")
        hint("r3")
        # An extension that fails a hint leaves its request closed.
        hint("r3", "read_hint_end")
        ext.hint = lambda *args: (_ for _ in ()).throw(RuntimeError("full"))
        with pytest.raises(RuntimeError):
            hint("r4")
        del ext.hint
        hint("r5")  # r4 was not left open
        service.drop(conn)  # the connection closed: its requests go
        hint("r6")
        hint("r7")
    finally:
        w.close()


def test_the_agent_refuses_too_many_or_malformed_ranges(setup):
    w, ext = setup
    worker = w.client("a")
    assert put(worker, "k", b"x" * PAGE)[0]
    service = w.node.service
    region = next(iter(worker.regions.values()))
    good = {"handle": region.handle, "offset": 0, "length": PAGE}
    malformed = [
        [good] * 4097,  # more than one message may name
        "not a list",
        ["not a map"],
        [{**good, "offset": True}],
        [{**good, "offset": -1}],
        [{**good, "length": 0}],
        [{**good, "length": "4096"}],
        [{k: v for k, v in good.items() if k != "length"}],
        [{**good, "handle": {"region_id": "x"}}],
    ]
    for ranges in malformed:
        with pytest.raises(ValueError):
            service.hint(10_000, _hint_message(service, None, ranges=ranges))
    with pytest.raises(ValueError, match="request"):
        service.hint(10_000, {**_hint_message(service, region.handle), "request": 7})
    assert not ext.hints
    service.hint(10_000, _hint_message(service, None, ranges=[good] * 4096))
    assert len(ext.hints[-1][2]) == 4096


def test_a_hint_for_a_region_going_back_is_refused(setup):
    w, ext = setup
    worker = w.client("a")
    assert put(worker, "k", b"x" * PAGE)[0]
    service = w.node.service
    region = next(iter(worker.regions.values()))
    ext.hold = True  # a device command on it still runs
    with pytest.raises(RuntimeError, match="still held"):
        service.release(region.handle)
    with pytest.raises(NotMapped):
        service.hint(10_000, _hint_message(service, read_handle(region.handle)))
    ext.hold = False


def test_hint_lookups_run_beside_the_scheduler_lookups(setup):
    w, ext = setup
    worker = w.client("a")
    keys = [f"k{i}" for i in range(3)]
    for key in keys:
        assert put(worker, key, b"x" * PAGE)[0]
    scheduler = w.client("a", metadata_only=True, read_hints=True)
    failures = []

    def look_up():
        for _ in range(100):
            if scheduler.batch_exists([*keys, "missing"]) != [True] * 3 + [False]:
                failures.append("lookup")

    thread = threading.Thread(target=look_up)
    thread.start()
    for i in range(100):
        scheduler.hint_read(f"r{i}", keys)
    thread.join()
    _until(lambda: len(ext.hints) == 100)
    assert not failures
    assert all(len(ranges) == 3 for _, _, ranges in ext.hints)


def test_the_oldest_open_hint_is_ended_past_the_limit(monkeypatch):
    monkeypatch.setattr(hints_module, "MAX_OPEN_HINTS", 2)
    sent = []
    hints = hints_module.MemoryHints(
        lambda keys: [("agent", [{"offset": 0}])],
        lambda agent, message: (
            sent.append((message["action"], message["request"])) or True
        ),
    )
    for rid in ("r1", "r2", "r3"):
        hints.hint(rid, ["k"])
    _until(lambda: len(sent) == 4)
    assert sent[-1] == ("read_hint_end", "r1")
    hints.end("r1")  # ended already: nothing more is sent
    hints.end("r2")
    _until(lambda: len(sent) == 5)
    assert sent[-1] == ("read_hint_end", "r2")
    assert hints.close()


def test_a_region_given_back_is_forgotten_by_the_data_path(setup):
    w, ext = setup
    a = w.client("a")
    assert put(a, "k", b"x" * PAGE)[0]
    region = only_region(a)
    assert region.pool_id in a._data._replied  # noqa: SLF001
    w.node.restart(reboot=True)
    a._check_boots()  # the region goes back
    assert region.pool_id not in a._data._replied  # noqa: SLF001


class WriteHintExtension(FakeExtension):
    hint_actions = ("read_hint", "read_hint_end", "write_hint")


def test_a_store_hints_its_pages_before_its_writes():
    ext = WriteHintExtension()
    w = World(extension=ext, max_hint_requests=1)
    try:
        worker = w.client("a", write_hints=True)
        handles = []
        for i in range(3):
            h = worker.alloc(PAGE)
            h.buf[:PAGE] = bytes([i + 1]) * PAGE
            handles.append(h)
        offsets = [h.location.offset for h in handles]
        assert worker.batch_store(["k0", "k1", "k2"], handles) == [True] * 3
        _until(lambda: ext.hints)
        _, message, ranges = ext.hints[0]
        assert message["action"] == "write_hint"
        assert message["request"].startswith("maru-store-")
        region = only_region(worker)
        assert [r["offset"] for r in ranges] == offsets  # in write order
        assert all(r["handle"] == region.handle for r in ranges)
        # A write hint has no end and keeps no request open at the agent:
        # more stores are hinted past the one-request limit.
        for key in ("k3", "k4"):
            assert put(worker, key, b"y" * PAGE)[0]
        _until(lambda: len(ext.hints) == 3)
        assert {m["action"] for _, m, _ in ext.hints} == {"write_hint"}
        assert read(worker, ["k0"]) == [b"\x01" * PAGE]
    finally:
        w.close()


def test_write_hints_go_only_where_asked_and_taken(setup):
    w, ext = setup  # an extension that takes read hints only
    worker = w.client("a", write_hints=True)
    plain = w.client("b")
    assert put(worker, "k0", b"x" * PAGE)[0]
    assert put(plain, "k1", b"x" * PAGE)[0]
    _until(lambda: worker._hints._queue.empty())
    assert worker._hints.close()
    assert not ext.hints
    (agent,) = worker._hint_agents.values()
    assert "write_hint" not in agent.hint_actions
    service = w.node.service
    region = only_region(worker)
    with pytest.raises(ValueError, match="action"):
        service.hint(10_000, _hint_message(service, region.handle, action="write_hint"))


class PromoteExtension(FakeExtension):
    hint_actions = ("read_hint", "read_hint_end", "read_promote")


def test_a_worker_promotes_the_load_its_scheduler_hinted():
    ext = PromoteExtension()
    w = World(extension=ext, max_hint_requests=1)
    try:
        worker = w.client("a", read_promotes=True)
        assert put(worker, "k0", b"x" * PAGE)[0]
        scheduler = w.client("a", metadata_only=True, read_hints=True)
        scheduler.hint_read("r1", ["k0"])
        _until(lambda: len(ext.hints) == 1)
        worker.promote_read("r1")
        _until(lambda: len(ext.hints) == 2)
        (hint_conn, hint, _), (promote_conn, promote, ranges) = ext.hints
        assert (promote["action"], promote["request"], ranges) == (
            "read_promote",
            "r1",
            [],
        )
        # The promote comes from the loading process, on its own connection.
        assert promote_conn != hint_conn
        # It opens no request at the agent: more promotes pass the limit of 1.
        worker.promote_read("r2")
        worker.promote_read("r3")
        _until(lambda: len(ext.hints) == 4)
    finally:
        w.close()


def test_read_promotes_go_only_where_asked_and_taken(setup):
    w, ext = setup  # an extension that takes read hints only
    worker = w.client("a", read_promotes=True)
    plain = w.client("b")
    assert put(worker, "k0", b"x" * PAGE)[0]
    worker.promote_read("r1")
    plain.promote_read("r1")  # read promotes off: ignored
    _until(lambda: worker._hints._queue.empty())
    assert worker._hints.close()
    assert not ext.hints and plain._hints is None
    service = w.node.service
    region = only_region(worker)
    with pytest.raises(ValueError, match="action"):
        service.hint(
            10_000,
            _hint_message(service, region.handle, action="read_promote", ranges=[]),
        )


class WriteWaitExtension(FakeExtension):
    hint_actions = ("read_hint", "read_hint_end", "write_hint", "write_wait")

    def __init__(self):
        super().__init__()
        self.release = threading.Event()
        self.release.set()
        self.answer = {"ready": True}
        self.waited = []

    def hint(self, conn, message, ranges):
        super().hint(conn, message, ranges)
        if message["action"] == "write_wait":
            self.waited.append(time.monotonic())
            self.release.wait(message["timeout_ms"] / 1000)
            return self.answer
        return None


def _store(worker, keys):
    handles = []
    for i, _ in enumerate(keys):
        h = worker.alloc(PAGE)
        h.buf[:PAGE] = bytes([i + 1]) * PAGE
        handles.append(h)
    return handles, worker.batch_store(keys, handles)


def test_a_store_waits_for_its_pages_before_its_writes():
    ext = WriteWaitExtension()
    w = World(extension=ext)
    try:
        worker = w.client("a", write_hints=True, write_wait_ms=1000)
        writes = []
        post = worker._transport.write

        def write(*args, **kwargs):
            writes.append(time.monotonic())
            return post(*args, **kwargs)

        worker._transport.write = write
        handles, stored = _store(worker, ["k0", "k1", "k2"])
        assert stored == [True] * 3
        # The wait carried the store's pages (no separate write hint) and
        # ended before the first WRITE was posted.
        actions = [m["action"] for _, m, _ in ext.hints]
        assert actions == ["write_wait"]
        _, message, ranges = ext.hints[0]
        assert message["request"].startswith("maru-store-")
        assert 0 < message["timeout_ms"] <= 1000
        assert [r["offset"] for r in ranges] == [h.location.offset for h in handles]
        assert ext.waited[0] < writes[0]
        assert read(worker, ["k0"]) == [b"\x01" * PAGE]
    finally:
        w.close()


def test_a_wait_that_runs_out_still_writes():
    ext = WriteWaitExtension()
    ext.release.clear()  # the pages never get ready
    ext.answer = {"ready": False}
    w = World(extension=ext)
    try:
        worker = w.client("a", write_hints=True, write_wait_ms=50)
        started = time.monotonic()
        assert _store(worker, ["k0"])[1] == [True]
        assert time.monotonic() - started < 2.0
        assert read(worker, ["k0"]) == [b"\x01" * PAGE]
    finally:
        w.close()


def test_a_waiting_store_holds_up_no_other_connection():
    ext = WriteWaitExtension()
    ext.release.clear()
    w = World(extension=ext)
    try:
        worker = w.client("a", write_hints=True, write_wait_ms=5000)
        result = []
        store = threading.Thread(target=lambda: result.append(_store(worker, ["k"])))
        store.start()
        _until(lambda: ext.waited)
        # Another connection is served, and the agent's lock is free.
        client = RegionMapClient(w.node.address, timeout_s=1, key=shared_key())
        try:
            started = time.monotonic()
            client.hello()
            w.node.service.holders()
            assert time.monotonic() - started < 0.5
        finally:
            client.close()
        ext.release.set()
        store.join(5)
        assert result and result[0][1] == [True]
    finally:
        w.close()


def test_without_a_wait_the_store_sends_a_write_hint():
    ext = WriteWaitExtension()
    w = World(extension=ext)
    try:
        worker = w.client("a", write_hints=True, write_wait_ms=0)
        assert _store(worker, ["k0"])[1] == [True]
        _until(lambda: ext.hints)
        assert [m["action"] for _, m, _ in ext.hints] == ["write_hint"]
        assert not ext.waited
    finally:
        w.close()


def test_the_agent_checks_a_write_wait():
    ext = WriteWaitExtension()
    w = World(extension=ext)
    try:
        worker = w.client("a")
        assert put(worker, "k", b"x" * PAGE)[0]
        service = w.node.service
        region = only_region(worker)

        def wait(timeout_ms):
            message = _hint_message(service, region.handle, action="write_wait")
            return service.hint(10_000, {**message, "timeout_ms": timeout_ms})

        assert wait(100) == {"ready": True}
        for bad in (0, -1, 10_001, "50", None):
            with pytest.raises(ValueError, match="timeout_ms"):
                wait(bad)
        ext.answer = ["not", "a", "map"]
        with pytest.raises(ValueError, match="no map"):
            wait(100)
        ext.answer = None
        assert wait(100) == {}
    finally:
        w.close()
    plain = World(extension=FakeExtension())  # takes no write waits
    try:
        worker = plain.client("a")
        assert put(worker, "k", b"x" * PAGE)[0]
        service = plain.node.service
        message = _hint_message(
            service, only_region(worker).handle, action="write_wait"
        )
        with pytest.raises(ValueError, match="action"):
            service.hint(10_000, {**message, "timeout_ms": 100})
    finally:
        plain.close()


def test_the_write_wait_is_bounded_in_the_config():
    from maru_common.config import MaruConfig

    base = {"storage_backend": "remote_cxl", "engine_id": "e", "cache_namespace": "ns"}
    assert MaruConfig(**base).write_wait_ms == 50
    for bad in (-1, 10_001, 1.5):
        with pytest.raises(ValueError, match="write_wait_ms"):
            MaruConfig(**base, write_wait_ms=bad)
