# SPDX-License-Identifier: Apache-2.0
"""Read advice through UDS and GPU control; write advice on RESERVE."""

import dataclasses
import threading
import time
from types import SimpleNamespace

import pytest

import maru_handler.hints as hints_module
from maru_common.config import MaruConfig
from maru_common.node_protocol import HintRequest, NodeMessageType
from maru_common.remote_protocol import (
    PingRequest,
    RemoteHintRequest,
    RemoteMessageType,
    ReserveRequest,
)
from maru_common.storage_types import HintAction, ItemResult, Key, Status
from maru_handler.client.maru_client import MaruClient
from maru_handler.hints import MemoryHints
from maru_storage_service.config import NodeConfigError, parse_node_config
from maru_storage_service.service import StorageService
from tests.unit.ms_node_helpers import PAGE, wait_until
from tests.unit.test_ms_region_extension import (  # noqa: F401
    FakeExtension,
    extension_env,
)


class Extension(FakeExtension):
    hint_actions = tuple(HintAction)

    def __init__(self):
        super().__init__()
        self.release = threading.Event()
        self.release.set()
        self.waited = []

    def hint(self, conn, message, ranges):
        super().hint(conn, message, ranges)
        if message["action"] == "write_wait":
            self.waited.append(time.monotonic())
            return {"ready": self.release.wait(message["timeout_ms"] / 1000)}


@pytest.fixture
def world(extension_env, monkeypatch):  # noqa: F811
    made = []
    from tests.unit.remote_transport_fakes import FakeTransport

    monkeypatch.setattr(
        "maru_handler.client.worker_rdma._transport",
        lambda config, name: FakeTransport(name),
    )

    def make(ext=None, hint_requests_max=4096, **options):
        ext = ext or Extension()
        env = extension_env(ext, hint_requests_max=hint_requests_max)
        raw = {
            "node_id": "gpu",
            "socket": f"{env.directory}/gpu.sock",
            "state_dir": env.directory,
            "server": env.metadata.address,
            "page_size": PAGE,
            "tiers": [
                {
                    "store": "remote",
                    "url": env.config.remote_server.listen,
                    "key_file": env.key_path,
                    "staging_size": 4 * PAGE,
                    "staging_max_bytes": 32 * PAGE,
                    "timeout_s": 1,
                    **options,
                }
            ],
            "budgets": {
                "credit_max_bytes": 4 * PAGE,
                "credit_total_max_bytes": 8 * PAGE,
            },
        }
        gpu = StorageService(
            parse_node_config(raw), rpc_timeout_s=0.3, evict_interval_s=100
        )
        gpu.start()
        clients = []
        made.append((gpu, clients))

        def client(engine_id="engine", **kwargs):
            c = MaruClient(
                MaruConfig(
                    maru_node=raw["socket"],
                    engine_id=engine_id,
                    cache_namespace="ns",
                    instance_id=engine_id,
                    chunk_size_bytes=PAGE,
                    pin_pools=False,
                    auto_connect=False,
                    **kwargs,
                ),
                rpc_timeout_s=1,
                reconnect_s=0.02,
            )
            clients.append(c)
            assert c.connect()
            return c

        return SimpleNamespace(env=env, ext=ext, gpu=gpu, client=client, raw=raw)

    yield make
    for gpu, clients in reversed(made):
        for client in reversed(clients):
            client.close()
        gpu.stop()
        assert gpu.teardown_failed is None


def put(c, keys=("k0", "k1", "k2")):
    handles = []
    for i, _ in enumerate(keys):
        wait_until(lambda: c.credits.unused_bytes >= PAGE)
        handle = c.alloc(PAGE)
        handle.buf[:] = bytes([i + 1]) * PAGE
        handles.append(handle)
    assert c.batch_store(list(keys), handles) == [True] * len(keys)
    return handles


def read(c, keys):
    refs = c.batch_retrieve(keys)
    try:
        return [bytes(r.view) if r is not None else None for r in refs]
    finally:
        c.release_retrieved(refs)


def drain(w, *clients):
    for c in clients:
        assert c._hints.close(end_open=True)
    assert w.gpu.store._hints.close(end_open=True)


def actions(w):
    return [(m["action"], m["request"], len(r)) for _, m, r in w.ext.hints]


def memory_hint(w, request="r", ranges=(), action=HintAction.READ_HINT, conn=999):
    answers = []
    w.env.node.role.hint(
        conn, RemoteHintRequest(action, request, list(ranges)), answers.append
    )
    return answers[0]


def test_a_scheduler_hints_the_leading_hits_and_ends_the_hint(world):
    w = world()
    worker = w.client()
    handles = put(worker)
    scheduler = w.client(metadata_only=True, read_hints=True)
    scheduler.hint_read("r1", ["k0", "k2", "missing", "k1"])
    scheduler.end_read_hint("r1")
    drain(w, scheduler)
    assert actions(w) == [
        ("read_hint", "6:engine:0:r1", 2),
        ("read_hint_end", "6:engine:0:r1", 0),
    ]
    assert [r["offset"] for r in w.ext.hints[0][2]] == [
        handles[i].location.offset for i in (0, 2)
    ]
    assert read(worker, ["k0", "k2"]) == [b"\x01" * PAGE, b"\x03" * PAGE]
    assert not scheduler._rdma and not scheduler.pools.pool_ids()


def test_a_request_is_hinted_once_and_a_miss_hints_nothing(world):
    w = world()
    put(w.client(), ["k0"])
    c = w.client(metadata_only=True, read_hints=True)
    for _ in range(2):
        c.hint_read("r", ["k0"])
    c.hint_read("miss", ["absent", "k0"])
    c.end_read_hint("miss")
    drain(w, c)
    assert [a[0] for a in actions(w)].count("read_hint") == 1


def test_a_load_without_an_early_hint_is_hinted_as_it_starts(world):
    w = world()
    c = w.client(read_hints=True)
    put(c, ["k0"])
    assert read(c, ["k0"]) == [b"\x01" * PAGE]
    drain(w, c)
    assert [a[0] for a in actions(w)] == ["read_hint", "read_hint_end"]


@pytest.mark.parametrize("supported", [False, True])
def test_without_read_hints_or_an_extension_nothing_is_sent(world, supported):
    ext = Extension()
    ext.hint_actions = tuple(HintAction) if supported else ()
    w = world(ext)
    c = w.client(read_hints=not supported)
    put(c, ["k0"])
    assert read(c, ["k0"]) == [b"\x01" * PAGE]
    drain(w, c)
    assert not ext.hints


def test_the_memory_node_takes_a_hint_only_for_a_current_page(world):
    w = world()
    h = put(w.client(), ["k0"])[0].location
    assert memory_hint(w, ranges=[h]).status == Status.OK
    for changed in (
        dataclasses.replace(h, generation=0),
        dataclasses.replace(h, pool_id="other"),
        dataclasses.replace(h, allocation_id="other"),
        dataclasses.replace(h, offset=h.offset + PAGE),
    ):
        assert memory_hint(w, ranges=[changed]).status == Status.REJECTED


def test_a_hint_needs_a_page_some_memory_node_holds(world):
    w = world()
    c = w.client()
    wait_until(lambda: c.credits.unused_bytes >= PAGE)
    h = c.alloc(PAGE)
    loc = h.location
    c.free(h)
    assert memory_hint(w, ranges=[loc]).status == Status.OK
    pool = w.env.node.role._regions.pop(loc.pool_id)
    try:
        assert memory_hint(w, ranges=[loc]).status == Status.REJECTED
    finally:
        w.env.node.role._regions[loc.pool_id] = pool


def test_a_probe_says_where_hits_are_only_when_asked(world):
    w = world()
    put(w.client(), ["k0"])
    c = w.client(metadata_only=True)
    key = Key("ns", "k0")
    assert c._server.probe([key])[0].location is None
    hit = c._server.probe([key], with_locations=True)[0]
    assert (
        hit.result == ItemResult.HIT and hit.location is not None and hit.pin_id is None
    )


def test_a_long_hint_is_split_into_messages_the_memory_node_takes(world, monkeypatch):
    w = world()
    put(w.client())
    monkeypatch.setattr(hints_module, "MAX_HINT_RANGES", 2)
    c = w.client(metadata_only=True, read_hints=True)
    c.hint_read("r", ["k0", "k1", "k2"])
    drain(w, c)
    assert [a[2] for a in actions(w) if a[0] == "read_hint"] == [2, 1]


def test_ends_are_kept_when_too_many_hints_wait(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    sent = []

    def send(target, message):
        sent.append(message["action"])
        if message["action"] == "read_hint":
            entered.set()
            assert release.wait(3)
        return True

    monkeypatch.setattr(hints_module, "MAX_PENDING_HINTS", 1)
    q = MemoryHints(lambda keys: [("node", [{}])], send)
    try:
        q.hint("r", ["k"])
        assert entered.wait(3)
        q.hint("queued", ["k"])
        q.hint("dropped", ["k"])
        q.end("r")
    finally:
        release.set()
        assert q.close(end_open=True)
    assert (
        sent == ["read_hint", "read_hint", "read_hint_end", "read_hint_end"]
        and q._dropped == 1
    )


def test_hints_after_close_are_ignored(world):
    w = world()
    c = w.client(metadata_only=True, read_hints=True)
    c.close()
    c.hint_read("r", ["k0"])
    drain(w, c)
    assert not w.ext.hints


def test_the_extension_takes_a_hint_outside_the_node_locks(world):
    w = world()
    h = put(w.client(), ["k0"])[0].location
    acquired = []

    def hint(*args):
        def probe():
            with (
                w.env.node.role._extension_lock,
                w.env.ops._lock,
                w.env.parts.server._cv,
            ):
                acquired.append(True)

        t = threading.Thread(target=probe)
        t.start()
        t.join(1)
        assert not t.is_alive()

    w.ext.hint = hint
    assert memory_hint(w, ranges=[h]).status == Status.OK
    assert acquired == [True]


def test_a_connection_keeps_a_bounded_number_of_hint_requests_open(world):
    w = world(hint_requests_max=1)
    h = put(w.client(), ["k0"])[0].location
    assert memory_hint(w, "a", [h]).status == Status.OK
    assert memory_hint(w, "a", [h]).status == Status.OK
    assert memory_hint(w, "b", [h]).status == Status.REJECTED
    assert memory_hint(w, "a", action=HintAction.READ_HINT_END).status == Status.OK
    assert memory_hint(w, "b", [h]).status == Status.OK
    w.env.node.role.connection_closed(999)
    assert 999 not in w.env.node.role._hint_requests


def test_the_memory_node_refuses_too_many_or_malformed_ranges(world):
    w = world()
    h = put(w.client(), ["k0"])[0].location
    for ranges in (
        [h] * 4097,
        [dataclasses.replace(h, length=0)],
        [dataclasses.replace(h, offset=h.offset + PAGE - 1, length=2)],
    ):
        assert memory_hint(w, ranges=ranges).status == Status.REJECTED
    assert memory_hint(w, request="").status == Status.REJECTED
    assert memory_hint(w, ranges=[h] * 4096).status == Status.OK


def test_a_hint_for_a_region_going_back_is_refused(world):
    w = world()
    h = put(w.client(), ["k0"])[0].location
    with w.env.node.role._extension_lock:
        regions = w.env.node.role._regions.copy()
        w.env.node.role._regions.clear()
    try:
        assert memory_hint(w, ranges=[h]).status == Status.REJECTED
    finally:
        w.env.node.role._regions.update(regions)


def test_hint_lookups_run_beside_the_scheduler_lookups(world, monkeypatch):
    w = world()
    put(w.client(), ["k0"])
    c = w.client(metadata_only=True, read_hints=True)
    entered, release = threading.Event(), threading.Event()
    original = c._server.probe

    def probe(keys, *, with_locations=False):
        if with_locations:
            entered.set()
            assert release.wait(3)
        return original(keys, with_locations=with_locations)

    monkeypatch.setattr(c._server, "probe", probe)
    c.hint_read("r", ["k0"])
    try:
        assert entered.wait(3)
        assert c.batch_exists(["k0"]) == [True]
    finally:
        release.set()
        drain(w, c)


def test_the_oldest_open_hint_is_ended_past_the_limit(world, monkeypatch):
    w = world()
    put(w.client(), ["k0"])
    monkeypatch.setattr(hints_module, "MAX_OPEN_HINTS", 1)
    c = w.client(metadata_only=True, read_hints=True)
    c.hint_read("a", ["k0"])
    c.hint_read("b", ["k0"])
    drain(w, c)
    assert [a[:2] for a in actions(w)] == [
        ("read_hint", "6:engine:0:a"),
        ("read_hint", "6:engine:0:b"),
        ("read_hint_end", "6:engine:0:a"),
        ("read_hint_end", "6:engine:0:b"),
    ]


def test_a_page_given_back_is_forgotten_by_the_data_path(world):
    w = world()
    c = w.client()
    h = put(c, ["k0"])[0].location
    page = w.env.node.pools.find(h)
    from maru_common.storage_types import KeyLocation

    result = w.env.node.store.retire([page], [KeyLocation(Key("ns", "k0"), h)])
    assert result
    assert memory_hint(w, ranges=[h]).status == Status.REJECTED
    assert read(c, ["k0"]) == [None]


def test_a_store_hints_its_pages_before_its_writes(world):
    w = world(write_hints=True, write_wait_ms=0, hint_requests_max=1)
    c = w.client()
    handles = put(c)
    assert w.ext.hints and all(a[0] == "write_hint" for a in actions(w))
    offsets = [r["offset"] for _, _, ranges in w.ext.hints for r in ranges]
    assert offsets[:3] == [h.location.offset for h in handles]
    assert read(c, ["k0"]) == [b"\x01" * PAGE]
    assert not any(w.env.node.role._hint_requests.values())


def test_write_hints_go_only_where_asked_and_taken(world):
    ext = Extension()
    ext.hint_actions = ("read_hint", "read_hint_end")
    w = world(ext, write_hints=True)
    put(w.client(), ["k0"])
    assert not ext.hints


def test_a_worker_promotes_the_load_its_scheduler_hinted(world):
    w = world()
    c = w.client(read_promotes=True)
    put(c, ["k0"])
    scheduler = w.client(metadata_only=True, read_hints=True)
    scheduler.hint_read("r", ["k0"])
    wait_until(lambda: w.ext.hints)
    c.promote_read("r")
    wait_until(lambda: len(w.ext.hints) == 2)
    scheduler.end_read_hint("r")
    drain(w, scheduler, c)
    assert [a[0] for a in actions(w)] == ["read_hint", "read_promote", "read_hint_end"]
    assert len({a[1] for a in actions(w)}) == 1


@pytest.mark.parametrize("supported", [False, True])
def test_read_promotes_go_only_where_asked_and_taken(world, supported):
    ext = Extension()
    ext.hint_actions = ("read_promote",) if supported else ()
    w = world(ext)
    c = w.client(load_only=True, read_promotes=not supported)
    c.promote_read("r")
    drain(w, c)
    assert not ext.hints


def test_a_store_waits_for_its_pages_before_its_writes(world):
    w = world(write_hints=True, write_wait_ms=1000)
    w.ext.release.clear()
    clients = []
    t = threading.Thread(target=lambda: clients.append(w.client()))
    t.start()
    try:
        wait_until(lambda: w.ext.waited)
        assert not clients
    finally:
        w.ext.release.set()
        t.join(3)
    assert clients and actions(w)[0][0] == "write_wait"
    put(clients[0], ["k0"])
    assert read(clients[0], ["k0"]) == [b"\x01" * PAGE]


def test_a_wait_that_runs_out_still_writes(world):
    w = world(write_hints=True, write_wait_ms=10)
    w.ext.release.clear()
    c = w.client()
    put(c, ["k0"])
    assert w.ext.waited and read(c, ["k0"]) == [b"\x01" * PAGE]


def test_a_waiting_store_holds_up_no_other_connection(world):
    w = world(write_hints=True, write_wait_ms=3000, timeout_s=5)
    w.ext.release.clear()
    clients = []
    t = threading.Thread(target=lambda: clients.append(w.client()))
    t.start()
    try:
        wait_until(lambda: w.ext.waited)
        other, _ = w.env.connect(node_id="other-gpu")
        from maru_common.remote_protocol import PingRequest

        assert (
            other.call(RemoteMessageType.PING, PingRequest(), 1.0).status == Status.OK
        )
        assert not clients
    finally:
        w.ext.release.set()
        t.join(3)
    assert clients


def test_without_a_wait_the_store_sends_a_write_hint(world):
    w = world(write_hints=True, write_wait_ms=0)
    put(w.client(), ["k0"])
    assert actions(w)[0][0] == "write_hint" and not w.ext.waited


def test_the_memory_node_checks_a_write_wait(world):
    w = world()
    h = put(w.client(), ["k0"])[0].location
    role = w.env.node.role
    for wait in (0, -1, 10001):
        role.reserve_hint(
            999, ReserveRequest(w.env.op(), 1, HintAction.WRITE_WAIT, wait), [h]
        )
    assert not w.ext.hints
    role.reserve_hint(999, ReserveRequest(w.env.op(), 1, HintAction.WRITE_WAIT, 1), [h])
    assert actions(w)[0][0] == "write_wait"


@pytest.mark.parametrize("bad", [-1, 10001, 1.5, True, "50"])
def test_the_write_wait_is_bounded_in_the_node_config(world, bad):
    w = world()
    raw = w.raw
    raw["tiers"][0]["write_wait_ms"] = bad
    with pytest.raises(NodeConfigError, match="write_wait_ms"):
        parse_node_config(raw)


def test_a_store_whose_hint_fails_still_stores(world, monkeypatch):
    w = world(write_hints=True, write_wait_ms=1)

    def broken(*args):
        raise RuntimeError("hint unavailable")

    monkeypatch.setattr(w.ext, "hint", broken)
    c = w.client()
    put(c, ["k0"])
    assert read(c, ["k0"]) == [b"\x01" * PAGE]


def test_hint_code_that_raises_never_fails_a_transfer(world, monkeypatch):
    w = world()

    def broken(*args):
        raise RuntimeError("hint unavailable")

    monkeypatch.setattr(w.ext, "hint", broken)
    c = w.client(read_hints=True)
    put(c, ["k0"])
    assert read(c, ["k0"]) == [b"\x01" * PAGE]
    drain(w, c)
    assert c.storage_connected and not c._stopped.is_set()
    assert not any(w.env.node.role._hint_requests.values())


def test_hints_reuse_the_key_the_control_path_read_at_start(world, monkeypatch):
    w = world()
    put(w.client(), ["k0"])

    def broken(*args):
        raise AssertionError("key reopened")

    monkeypatch.setattr("maru_storage_service.gpu.remote.load_key", broken)
    c = w.client(metadata_only=True, read_hints=True)
    c.hint_read("r", ["k0"])
    drain(w, c)
    assert actions(w)[0][0] == "read_hint"


def test_a_memory_node_whose_wait_timed_out_is_not_asked_again_for_a_while(world):
    w = world(write_hints=True, write_wait_ms=1)
    w.ext.release.clear()
    w.client()
    remote = w.gpu.store.remote
    assert remote._wait_retry_at > time.monotonic()
    assert remote.reserve_hint() == HintAction.WRITE_HINT


def test_a_hint_connection_that_raises_does_not_end_the_run(world, monkeypatch):
    w = world()
    put(w.client(), ["k0"])
    conn = w.gpu.store.remote.connection
    original = conn.call

    def call(code, *args):
        if code == RemoteMessageType.HINT:
            raise TimeoutError("hint late")
        return original(code, *args)

    monkeypatch.setattr(conn, "call", call)
    c = w.client(metadata_only=True, read_hints=True)
    c.hint_read("r", ["k0"])
    drain(w, c)
    assert not conn.closed and w.gpu.ended is None


def test_engine_request_ids_are_isolated_and_write_actions_are_rejected(world):
    w = world()
    put(w.client(), ["k0"])
    for engine in ("one", "two"):
        c = w.client(engine, metadata_only=True, read_hints=True)
        c.hint_read("r", ["k0"])
        assert c._hints.close(end_open=True)
    assert w.gpu.store._hints.close(end_open=True)
    assert {a[1] for a in actions(w)} == {"3:one:0:r", "3:two:0:r"}
    c = w.client(metadata_only=True)
    response = c._storage.request(
        NodeMessageType.HINT, HintRequest(HintAction.WRITE_HINT, "r"), 1
    )
    assert response.status == Status.REJECTED


def test_a_stopped_worker_sends_no_promote(world):
    w = world()
    c = w.client(load_only=True, read_promotes=True)
    c._stopped.set()
    try:
        c.promote_read("r")
        drain(w, c)
        assert not w.ext.hints
    finally:
        c._stopped.clear()


def test_hint_capability_is_required_on_the_uds_connection(world):
    w = world()
    c = w.client(metadata_only=True)
    rec = w.gpu.store.workers._probes[c.identity.incarnation]
    rec.capabilities = frozenset()
    assert (
        c._storage.request(
            NodeMessageType.HINT, HintRequest(HintAction.READ_PROMOTE, "r"), 1
        ).status
        == Status.REJECTED
    )


def test_malformed_uds_hint_ranges_are_rejected_before_relay(world):
    w = world()
    location = put(w.client(), ["k0"])[0].location
    c = w.client(metadata_only=True)
    good = dataclasses.asdict(location)
    malformed = [
        "invalid",
        ["invalid"],
        [{"pool_id": "missing-fields"}],
        [{**good, "offset": True}],
        [{**good, "offset": "0"}],
        [{**good, "length": None}],
    ]
    for ranges in malformed:
        response = c._storage.request(
            NodeMessageType.HINT,
            {
                "action": "read_hint",
                "request": "r",
                "ranges": ranges,
            },
            1,
        )
        assert response.status == Status.REJECTED
    assert not w.ext.hints


def test_slow_read_hint_does_not_delay_the_ack_or_worker_read(world):
    w = world()
    worker = w.client()
    location = put(worker, ["k0"])[0].location
    scheduler = w.client(metadata_only=True)
    entered, release = threading.Event(), threading.Event()

    def hint(*args):
        entered.set()
        assert release.wait(3)

    w.ext.hint = hint
    try:
        response = scheduler._storage.request(
            NodeMessageType.HINT, HintRequest(HintAction.READ_HINT, "r", [location]), 1
        )
        assert response.status == Status.OK and entered.wait(1)
        assert read(worker, ["k0"]) == [b"\x01" * PAGE]
        assert not release.is_set()
    finally:
        release.set()
        drain(w, scheduler)


@pytest.mark.parametrize("explicit_ping", [False, True])
def test_slow_read_hint_keeps_control_and_the_run_alive(
    world, monkeypatch, explicit_ping
):
    w = world()
    worker = w.client()
    put(worker, ["k0"])
    scheduler = w.client(metadata_only=True, read_hints=True)
    remote = w.gpu.store.remote
    entered, release, pinged = (threading.Event() for _ in range(3))
    original = remote.call
    pings = []

    def call(code, *args, **kwargs):
        answer = original(code, *args, **kwargs)
        if entered.is_set() and code == RemoteMessageType.PING:
            pings.append(answer.status)
            if len(pings) >= 2:
                pinged.set()
        return answer

    def hint(*args):
        entered.set()
        release.wait(10)

    monkeypatch.setattr(remote, "call", call)
    monkeypatch.setattr(w.ext, "hint", hint)
    try:
        scheduler.hint_read("r", ["k0"])
        assert entered.wait(1)
        if explicit_ping:
            with remote.lock:
                assert (
                    remote.call(RemoteMessageType.PING, PingRequest()).status
                    == Status.OK
                )
        assert pinged.wait(4)
        assert pings == [Status.OK] * len(pings)
        assert read(worker, ["k0"]) == [b"\x01" * PAGE]
        writer = w.client("writer")
        put(writer, ["k1"])
        assert read(writer, ["k1"]) == [b"\x01" * PAGE]
        assert not release.is_set()
        assert not remote.connection.closed and w.gpu.ended is None
    finally:
        release.set()
        drain(w, scheduler)
