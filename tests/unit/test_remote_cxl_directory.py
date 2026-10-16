# SPDX-License-Identifier: Apache-2.0
"""ReplicaDirectory rules for remote CXL regions: expand, sharing, leases, eviction,
expiry and the return of regions after a drained close."""

import uuid

import pytest

from maru_server.replica_directory import ReplicaDirectory
from maru_shm.types import MaruHandle
from tests.unit.remote_fakes import FakeClock

PAGE = 64


class FakeRm:
    """Reserves regions up to a byte limit and records releases."""

    def __init__(self, limit=1 << 20):
        self.limit = limit
        self.used = 0
        self.next_id = 1
        self.regions: dict[int, int] = {}
        self.released: list[int] = []
        self.fail_release = False

    def reserve(self, owner, size):
        if self.used + size > self.limit:
            return None
        handle = MaruHandle(region_id=self.next_id, length=size, auth_token=7)
        self.regions[self.next_id] = size
        self.used += size
        self.next_id += 1
        return handle

    def release(self, owner, region_id):
        if self.fail_release:
            return False
        self.used -= self.regions.pop(region_id)
        self.released.append(region_id)
        return True


@pytest.fixture
def env():
    clock = FakeClock()
    rm = FakeRm()
    directory = ReplicaDirectory(
        reserve_cxl=rm.reserve, release_cxl=rm.release, clock=clock, session_ttl=30
    )
    return directory, rm, clock


def call(directory, action, **payload):
    result = directory.execute(action, payload)
    if not result["success"]:
        raise ValueError(result["error"])
    return result["result"]


def open_session(directory, engine, pages=2, namespace="ns", page=PAGE, token=None):
    creds = {"server_epoch": directory.server_epoch, "session_token": token or engine}
    grant = call(
        directory,
        "open",
        **creds,
        engine_id=engine,
        namespace=namespace,
        node_id="node",
        schema="opaque-bytes-v1",
        capacity=0,
        remote_cxl_region_size=pages * page,
        page_size=page,
    )
    pool = grant["pools"][0]
    assert pool["medium"] == "remote_cxl" and "handle" in pool
    return creds, pool["pool_id"]


def location(pool_id, page, allocation=None, length=8):
    return {
        "pool_id": pool_id,
        "allocation_id": allocation or f"{pool_id}-{page}",
        "generation": page + 1,
        "offset": page * PAGE,
        "length": length,
        "medium": "remote_cxl",
    }


def commit(directory, creds, pool_id, key, page, op=None):
    return call(
        directory,
        "commit",
        **creds,
        op_id=op or f"{key}-{pool_id}-{page}",
        entries=[{"key": key, "location": location(pool_id, page)}],
    )["statuses"][0]


def expand(directory, creds, op=None):
    return call(directory, "expand", **creds, op_id=op or uuid.uuid4().hex)


def heartbeat(directory, creds, seq=1):
    call(directory, "heartbeat", **creds, sequence=seq, pools=[])


# ------------------------------------------------------------------ regions


def test_expand_gives_the_session_one_more_region(env):
    directory, rm, _ = env
    a, first = open_session(directory, "a")
    region = expand(directory, a)["pool"]
    assert region["medium"] == "remote_cxl" and region["pool_id"] != first
    assert region["capacity"] == 2 * PAGE and region["handle"]["region_id"] == 2
    assert commit(directory, a, region["pool_id"], "k", 1) == "CREATED"
    assert sorted(rm.regions) == [1, 2]


def test_a_resent_expand_gets_the_same_region(env):
    directory, rm, _ = env
    a, _ = open_session(directory, "a")
    first = expand(directory, a, op="x")["pool"]
    again = expand(directory, a, op="x")["pool"]
    assert again == first and sorted(rm.regions) == [1, 2]
    assert expand(directory, a, op="y")["pool"]["pool_id"] != first["pool_id"]


def test_expand_needs_an_op_id(env):
    directory, _, _ = env
    a, _ = open_session(directory, "a")
    with pytest.raises(ValueError, match="op_id"):
        call(directory, "expand", **a)


def test_expand_fails_when_the_rm_is_full(env):
    directory, rm, _ = env
    rm.limit = 2 * PAGE
    a, _ = open_session(directory, "a")
    with pytest.raises(ValueError, match="reservation failed"):
        expand(directory, a)


def test_a_session_cannot_commit_into_another_sessions_region(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    with pytest.raises(ValueError, match="another pool"):
        commit(directory, b, pool_a, "k", 0)


def test_heartbeat_of_a_remote_cxl_session_may_report_some_regions(env):
    directory, _, _ = env
    a, first = open_session(directory, "a")
    expand(directory, a)
    report = {"pool_id": first, "allocated_bytes": PAGE, "quarantined_bytes": 0}
    call(directory, "heartbeat", **a, sequence=1, pools=[report])
    with pytest.raises(ValueError):
        bad = {"pool_id": "nope", "allocated_bytes": 0, "quarantined_bytes": 0}
        call(directory, "heartbeat", **a, sequence=2, pools=[bad])


def test_reopening_with_the_same_token_returns_every_region(env):
    directory, _, _ = env
    a, first = open_session(directory, "a")
    second = expand(directory, a)["pool"]["pool_id"]
    again = call(
        directory,
        "open",
        **a,
        engine_id="a",
        namespace="ns",
        node_id="node",
        schema="opaque-bytes-v1",
        capacity=0,
        remote_cxl_region_size=2 * PAGE,
        page_size=PAGE,
    )
    assert [p["pool_id"] for p in again["pools"]] == [first, second]


# ------------------------------------------------------------------- sharing


def test_a_key_committed_by_one_session_is_leased_and_read_by_another(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    assert commit(directory, a, pool_a, "k", 0) == "CREATED"
    probe = call(directory, "probe", engine_id="b", namespace="ns", keys=["k", "x"])
    assert probe["results"] == [True, False]
    got = call(directory, "acquire", **b, keys=["k", "x"])
    assert got["entries"][0]["pool_id"] == pool_a and got["entries"][1] is None
    assert got["regions"][pool_a]["region_id"] == 1
    assert call(directory, "release", **b, lease_id=got["lease_id"]) == {"held": True}


def test_first_commit_wins_across_sessions(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    b, pool_b = open_session(directory, "b")
    assert commit(directory, a, pool_a, "k", 0) == "CREATED"
    assert commit(directory, b, pool_b, "k", 0) == "ALREADY_PRESENT"
    got = call(directory, "acquire", **b, keys=["k"])
    assert got["entries"][0]["pool_id"] == pool_a


def test_sessions_of_a_namespace_agree_on_page_size_and_schema(env):
    directory, _, _ = env
    open_session(directory, "a")
    with pytest.raises(ValueError, match="another schema or page size"):
        open_session(directory, "b", page=2 * PAGE)
    open_session(directory, "c", namespace="other", page=2 * PAGE)


def test_a_named_lease_can_be_released_without_its_reply(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    commit(directory, a, pool_a, "k", 0)
    got = call(directory, "acquire", **b, keys=["k"], lease_id="mine")
    assert got["lease_id"] == "mine"
    with pytest.raises(ValueError, match="already in use"):
        call(directory, "acquire", **b, keys=["k"], lease_id="mine")
    assert call(directory, "release", **b, lease_id="mine") == {"held": True}
    assert [v["key"] for v in call(directory, "victims", **a, count=1)["entries"]] == [
        "k"
    ]


def test_lease_of_an_expired_reader_is_not_held(env):
    directory, _, clock = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    commit(directory, a, pool_a, "k", 0)
    lease = call(directory, "acquire", **b, keys=["k"])["lease_id"]
    clock.advance(20)
    heartbeat(directory, a)
    clock.advance(11)  # b expires, a stays
    assert call(directory, "release", **b, lease_id=lease) == {"held": False}
    # The expired reader's lease no longer protects the key from eviction.
    assert [v["key"] for v in call(directory, "victims", **a, count=1)["entries"]] == [
        "k"
    ]


# ------------------------------------------------------------------ eviction


def test_victims_are_least_recently_used_across_regions_and_skip_leases(env):
    directory, _, clock = env
    a, first = open_session(directory, "a", pages=2)
    second = expand(directory, a)["pool"]["pool_id"]
    b, _ = open_session(directory, "b")
    for pool_id, page, key in [
        (first, 0, "old"),
        (second, 0, "mid"),
        (first, 1, "new"),
    ]:
        commit(directory, a, pool_id, key, page)
        clock.advance(1)
    # A read by another session counts as use; a held lease protects the key.
    lease = call(directory, "acquire", **b, keys=["old"])["lease_id"]
    victims = call(directory, "victims", **a, count=2)["entries"]
    assert [v["key"] for v in victims] == ["mid", "new"]
    assert victims[0]["location"]["pool_id"] == second
    call(directory, "release", **b, lease_id=lease)
    victims = call(directory, "victims", **a, count=1)["entries"]
    assert [v["key"] for v in victims] == ["mid"]


def test_remove_needs_the_same_location_and_no_lease(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    commit(directory, a, pool_a, "k", 0)
    commit(directory, a, pool_a, "j", 1)
    lease = call(directory, "acquire", **b, keys=["k"])["lease_id"]
    statuses = call(
        directory,
        "remove",
        **a,
        entries=[
            {"key": "k", "location": location(pool_a, 0)},
            {"key": "j", "location": location(pool_a, 0, allocation="stale")},
        ],
    )["statuses"]
    assert statuses == ["KEPT", "KEPT"]
    call(directory, "release", **b, lease_id=lease)
    statuses = call(
        directory,
        "remove",
        **a,
        entries=[{"key": "k", "location": location(pool_a, 0)}],
    )["statuses"]
    assert statuses == ["REMOVED"]
    assert call(directory, "acquire", **b, keys=["k"])["entries"] == [None]


def test_a_session_cannot_remove_keys_of_another_session(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    commit(directory, a, pool_a, "k", 0)
    entry = {"key": "k", "location": location(pool_a, 0)}
    assert call(directory, "remove", **b, entries=[entry])["statuses"] == ["KEPT"]
    assert call(directory, "probe", engine_id="b", namespace="ns", keys=["k"])[
        "results"
    ] == [True]


def test_a_retried_remove_learns_the_page_is_free(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    entry = {"key": "k", "location": location(pool_a, 0)}
    assert call(directory, "remove", **a, entries=[entry])["statuses"] == ["REMOVED"]
    assert call(directory, "remove", **a, entries=[entry])["statuses"] == ["ABSENT"]
    # A page another key now holds is never reported free.
    commit(directory, a, pool_a, "j", 0, op="j-again")
    assert call(directory, "remove", **a, entries=[entry])["statuses"] == ["KEPT"]


# ---------------------------------------------------------- expiry and return


def test_an_expired_owner_hides_its_keys_and_keeps_its_regions(env):
    directory, rm, clock = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    clock.advance(31)
    with pytest.raises(ValueError, match="expired"):
        commit(directory, a, pool_a, "z", 1)
    c, _ = open_session(directory, "c")
    assert call(directory, "acquire", **c, keys=["k"])["entries"] == [None]
    # A timeout does not prove the owner stopped writing: the region is held.
    assert rm.released == [] and 1 in rm.regions
    with pytest.raises(ValueError, match="expired"):
        call(
            directory,
            "remove",
            **a,
            entries=[{"key": "k", "location": location(pool_a, 0)}],
        )


def test_a_drained_close_of_an_expired_session_returns_its_regions(env):
    directory, rm, clock = env
    a, _ = open_session(directory, "a")
    expand(directory, a)
    clock.advance(31)
    call(directory, "close", **a, drained=True)
    assert sorted(rm.released) == [1, 2]


def test_a_drained_close_returns_regions_nobody_reads(env):
    directory, rm, _ = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    commit(directory, a, pool_a, "k", 0)
    call(directory, "close", **a, drained=True)
    assert rm.released == [1]
    assert call(directory, "acquire", **b, keys=["k"])["entries"] == [None]


def test_a_closed_region_returns_at_once_and_its_readers_lose_their_lease(env):
    directory, rm, _ = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    commit(directory, a, pool_a, "k", 0)
    lease = call(directory, "acquire", **b, keys=["k"])["lease_id"]
    # The owner closes only after its region mapper released the region, so
    # no READ can reach it any more: it goes back without waiting for b.
    call(directory, "close", **a, drained=True)
    assert rm.released == [1]
    assert call(directory, "acquire", **b, keys=["k"])["entries"] == [None]
    assert call(directory, "release", **b, lease_id=lease) == {"held": False}


def test_return_region_gives_back_one_region_and_keeps_the_others(env):
    directory, rm, _ = env
    a, first = open_session(directory, "a")
    second = expand(directory, a)["pool"]["pool_id"]
    b, _ = open_session(directory, "b")
    commit(directory, a, first, "k1", 0)
    commit(directory, a, second, "k2", 0)
    lease = call(directory, "acquire", **b, keys=["k1"])["lease_id"]
    call(directory, "return_region", **a, pool_id=first)
    assert rm.released == [1]
    assert call(directory, "release", **b, lease_id=lease) == {"held": False}
    entries = call(directory, "acquire", **b, keys=["k1", "k2"])["entries"]
    assert entries[0] is None and entries[1]["pool_id"] == second
    # A resent return succeeds; a region of another session is refused.
    call(directory, "return_region", **a, pool_id=first)
    with pytest.raises(ValueError, match="Not a region"):
        call(directory, "return_region", **b, pool_id=second)
    call(directory, "close", **a, drained=True)
    assert rm.released == [1, 2]


def test_a_heartbeat_may_still_report_a_region_just_given_back(env):
    directory, _, _ = env
    a, first = open_session(directory, "a")
    second = expand(directory, a)["pool"]["pool_id"]
    call(directory, "return_region", **a, pool_id=first)
    reports = [
        {"pool_id": pool, "allocated_bytes": 0, "quarantined_bytes": 0}
        for pool in (first, second)
    ]
    call(directory, "heartbeat", **a, sequence=1, pools=reports)


def test_an_expired_session_can_return_a_region(env):
    directory, rm, clock = env
    a, first = open_session(directory, "a")
    clock.advance(31)
    call(directory, "return_region", **a, pool_id=first)
    assert rm.released == [1]


def test_an_unconfirmed_release_is_retried(env):
    directory, rm, _ = env
    a, _ = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    rm.fail_release = True
    call(directory, "close", **a, drained=True)
    assert rm.released == []
    rm.fail_release = False
    heartbeat(directory, b)
    assert rm.released == [1]


# ----------------------------------------------------------------- open rules


def test_remote_cxl_sessions_hold_no_cpu_or_cxl_pool(env):
    directory, _, _ = env
    with pytest.raises(ValueError, match="cannot also hold"):
        call(
            directory,
            "open",
            server_epoch=directory.server_epoch,
            session_token="t",
            engine_id="e",
            namespace="ns",
            node_id="node",
            schema="s",
            capacity=PAGE,
            remote_cxl_region_size=PAGE,
            page_size=PAGE,
        )


def test_remote_cxl_needs_a_cxl_enabled_server():
    directory = ReplicaDirectory()
    with pytest.raises(ValueError, match="CXL-enabled"):
        open_session(directory, "a")


def test_cpu_sessions_cannot_expand(env):
    directory, _, _ = env
    creds = {"server_epoch": directory.server_epoch, "session_token": "cpu"}
    call(
        directory,
        "open",
        **creds,
        engine_id="cpu",
        namespace="ns",
        node_id="node",
        schema="s",
        capacity=PAGE,
        page_size=PAGE,
    )
    with pytest.raises(ValueError, match="no remote CXL regions"):
        expand(directory, creds)


def test_cpu_release_still_succeeds_without_a_session(env):
    directory, _, _ = env
    reply = call(
        directory,
        "release",
        server_epoch=directory.server_epoch,
        session_token="gone",
        lease_id="x",
    )
    assert reply == {"held": False}
