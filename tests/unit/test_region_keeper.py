# SPDX-License-Identifier: Apache-2.0
"""MaruServer's region keeper: holding kept regions and giving regions back."""

import pytest

from maru_remote.region_map import (
    MapReply,
    RegionMapError,
    RegionMapUnavailable,
    pick_mapper,
)
from maru_server.region_keeper import CHECK_S, DOWN_S, RegionKeeper
from maru_server.replica_directory import ReplicaDirectory
from tests.unit.remote_fakes import FakeClock
from tests.unit.test_remote_cxl_directory import FakeRm, call, commit, open_session

MAPPERS = ["mem1:9860", "mem2:9860"]


class FakeMapperClient:
    """Records calls; ``fail`` makes the next call of an op raise."""

    def __init__(self, address, boots):
        self.address = address
        self.boots = boots
        self.generation = 1
        self.calls = []
        self.fail: dict[str, Exception] = {}

    def _maybe_fail(self, op):
        exc = self.fail.pop(op, None)
        if exc is not None:
            raise exc

    def map(self, handle):
        self.calls.append(("map", handle["region_id"]))
        self._maybe_fail("map")
        return MapReply(
            handle["region_id"], 0, handle["length"], b"", "e", self.boots[self.address]
        )

    def hello(self):
        self.calls.append(("hello",))
        self._maybe_fail("hello")
        return {"boot": self.boots[self.address]}

    def release(self, handle):
        self.calls.append(("release", handle["region_id"]))
        self._maybe_fail("release")

    def close(self):
        self.calls.append(("close",))


@pytest.fixture
def env():
    clock = FakeClock()
    rm = FakeRm()
    directory = ReplicaDirectory(
        reserve_cxl=rm.reserve, release_cxl=rm.release, clock=clock, session_ttl=30
    )
    boots = dict.fromkeys(MAPPERS, "boot-1")
    clients = {}

    def factory(address):
        clients[address] = FakeMapperClient(address, boots)
        return clients[address]

    keeper = RegionKeeper(
        directory, MAPPERS, b"k" * 32, client_factory=factory, clock=clock
    )
    return directory, rm, clock, keeper, clients, boots


def leave(directory, engine, key=None):
    """Open a session (with one key if given) and close it drained."""
    creds, pool = open_session(directory, engine)
    if key:
        commit(directory, creds, pool, key, 0)
    call(directory, "close", **creds, drained=True)
    region_id = next(h["region_id"] for p, h, *_ in directory.departed() if p == pool)
    return pool, region_id


def test_a_kept_region_is_held_and_one_without_keys_is_released_then_freed(env):
    directory, rm, _, keeper, clients, _ = env
    kept, kept_id = leave(directory, "a", key="k")
    empty, empty_id = leave(directory, "b")
    keeper.run_once()
    calls = [c for client in clients.values() for c in client.calls]
    assert ("map", kept_id) in calls and ("release", empty_id) in calls
    assert rm.released == [empty_id]
    assert [p for p, *_ in directory.departed()] == [kept]
    # A held region is not mapped again until the next check.
    before = sum(len(c.calls) for c in clients.values())
    keeper.run_once()
    assert sum(len(c.calls) for c in clients.values()) == before


def test_the_region_is_freed_only_after_its_release_succeeds(env):
    directory, rm, clock, keeper, clients, _ = env
    pool, region_id = leave(directory, "a")
    address = pick_mapper(MAPPERS, region_id)
    keeper._client(address).fail["release"] = RegionMapError("busy", "unavailable")
    keeper.run_once()
    assert rm.released == []  # the deregistration failed: still registered
    keeper.run_once()
    assert rm.released == [region_id]


def test_a_region_mapper_that_does_not_answer_is_left_alone_for_a_while(env):
    directory, rm, clock, keeper, clients, _ = env
    pool, region_id = leave(directory, "a")
    address = pick_mapper(MAPPERS, region_id)
    client = keeper._client(address)
    client.fail["release"] = RegionMapUnavailable("down")
    keeper.run_once()
    keeper.run_once()
    assert client.calls.count(("release", region_id)) == 1
    clock.advance(DOWN_S + 0.1)
    keeper.run_once()
    assert rm.released == [region_id]


def test_a_refused_hold_gives_the_region_back(env):
    # Its owner released it meanwhile, so its keys cannot be read any more.
    directory, rm, _, keeper, _, _ = env
    pool, region_id = leave(directory, "a", key="k")
    address = pick_mapper(MAPPERS, region_id)
    keeper._client(address).fail["map"] = RegionMapError("released", "denied")
    keeper.run_once()
    b, _ = open_session(directory, "b")
    assert call(directory, "acquire", **b, keys=["k"])["entries"] == [None]
    keeper.run_once()
    assert rm.released == [region_id]


def test_a_reboot_after_the_owners_report_gives_the_region_back(env):
    directory, rm, _, keeper, _, boots = env
    creds, pool = open_session(directory, "a")
    commit(directory, creds, pool, "k", 0)
    call(
        directory,
        "heartbeat",
        **creds,
        sequence=1,
        pools=[
            {
                "pool_id": pool,
                "allocated_bytes": 64,
                "quarantined_bytes": 0,
                "boot": "boot-1",
            }
        ],
    )
    call(directory, "close", **creds, drained=True)
    region_id = rm.next_id - 1
    boots[pick_mapper(MAPPERS, region_id)] = "boot-2"
    keeper.run_once()
    keeper.run_once()
    assert rm.released == [region_id]


def test_a_reboot_while_held_gives_the_region_back(env):
    directory, rm, clock, keeper, _, boots = env
    pool, region_id = leave(directory, "a", key="k")
    keeper.run_once()
    boots[pick_mapper(MAPPERS, region_id)] = "boot-2"
    keeper.run_once()
    assert rm.released == []  # not checked yet
    clock.advance(CHECK_S)
    keeper.run_once()
    keeper.run_once()
    assert rm.released == [region_id]


def test_a_refused_region_request_makes_room(env):
    directory, rm, _, keeper, _, _ = env
    rm.limit = 2 * 2 * 64  # two regions
    pool, region_id = leave(directory, "a", key="k")
    keeper.run_once()
    c, _ = open_session(directory, "c")
    with pytest.raises(ValueError, match="reservation failed"):
        call(directory, "expand", **c, op_id="x")
    keeper.run_once()
    keeper.run_once()
    assert rm.released == [region_id]
    call(directory, "expand", **c, op_id="x")


def test_expiry_is_noticed_without_requests(env):
    # The keeper's pass checks expiry, so a crashed owner's regions are
    # taken over even when no request reaches MaruServer.
    directory, rm, clock, keeper, clients, _ = env
    creds, pool = open_session(directory, "a")
    commit(directory, creds, pool, "k", 0)
    clock.advance(31)
    keeper.run_once()
    region_id = rm.next_id - 1
    assert ("map", region_id) in clients[pick_mapper(MAPPERS, region_id)].calls


def test_the_thread_wakes_on_work_and_stops(env):
    directory, rm, _, keeper, clients, _ = env
    keeper.start()
    try:
        pool, region_id = leave(directory, "a")
        import time

        deadline = time.monotonic() + 5
        while rm.released != [region_id]:
            assert time.monotonic() < deadline
            time.sleep(0.01)
    finally:
        keeper.stop()
    assert all(("close",) in c.calls for c in clients.values())


def test_maruserver_runs_a_keeper_only_with_region_mappers():
    from maru_server.server import MaruServer

    server = MaruServer(enable_cxl=False)
    assert server._region_keeper is None
    server.close()


def test_maru_server_refuses_region_mappers_without_the_key(
    monkeypatch, tmp_path, capsys
):
    from maru_server import server

    monkeypatch.setattr(
        "sys.argv",
        [
            "maru-server",
            "--region-mapper",
            "mem1:9860",
            "--region-mapper-key-file",
            str(tmp_path / "missing"),
        ],
    )
    with pytest.raises(SystemExit) as info:
        server.main()
    assert info.value.code == 2
    assert "region mapper key" in capsys.readouterr().err


def test_a_kept_region_its_owner_released_meanwhile_is_given_back(env):
    # An owner that only seemed gone releases its region after MaruServer
    # took it over: the next check maps it again, is refused, and gives the
    # region back instead of believing it still holds it.
    directory, rm, clock, keeper, clients, _ = env
    pool, region_id = leave(directory, "a", key="k")
    keeper.run_once()
    address = pick_mapper(MAPPERS, region_id)
    keeper._client(address).fail["map"] = RegionMapError("released", "denied")
    clock.advance(CHECK_S)
    keeper.run_once()
    keeper.run_once()
    assert rm.released == [region_id]


def test_a_reconnected_mapper_gets_the_kept_region_mapped_again(env):
    directory, rm, clock, keeper, clients, _ = env
    pool, region_id = leave(directory, "a", key="k")
    keeper.run_once()
    client = keeper._client(pick_mapper(MAPPERS, region_id))
    client.generation += 1  # the connection was opened again
    keeper.run_once()
    assert client.calls.count(("map", region_id)) == 2
    assert rm.released == []


def test_every_check_maps_a_held_region_again(env):
    directory, _, clock, keeper, clients, _ = env
    pool, region_id = leave(directory, "a", key="k")
    keeper.run_once()
    client = keeper._client(pick_mapper(MAPPERS, region_id))
    clock.advance(CHECK_S)
    keeper.run_once()
    assert client.calls.count(("map", region_id)) == 2
    assert ("hello",) not in client.calls
