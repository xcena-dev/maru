# SPDX-License-Identifier: Apache-2.0
"""ReplicaDirectory rules for remote CXL regions: expand, sharing, leases, eviction,
expiry, and what MaruServer does with the regions of an owner that left."""

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
        self.unreachable = False

    def reserve(self, owner, size):
        if self.used + size > self.limit:
            return None
        handle = MaruHandle(region_id=self.next_id, length=size, auth_token=7)
        self.regions[self.next_id] = size
        self.used += size
        self.next_id += 1
        return handle

    def release(self, owner, region_id):
        if self.unreachable:
            raise ConnectionError("Resource manager is not running")
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


def released_by_keeper(directory):
    """What the region keeper does when every release succeeds."""
    for pool_id, _handle, fate, _boot in directory.departed():
        if fate == "return":
            directory.given_back(pool_id)


def fates(directory):
    return {pool_id: fate for pool_id, _h, fate, _b in directory.departed()}


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


def test_a_cpu_session_of_the_same_namespace_finds_no_remote_cxl_keys(env):
    # It could not load them: its handler resolves CPU locations only.
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    cpu = {"server_epoch": directory.server_epoch, "session_token": "cpu"}
    call(
        directory,
        "open",
        **cpu,
        engine_id="cpu",
        namespace="ns",
        node_id="node",
        schema="opaque-bytes-v1",
        capacity=PAGE,
        page_size=PAGE,
    )
    probe = call(directory, "probe", engine_id="cpu", namespace="ns", keys=["k"])
    assert probe["results"] == [False]
    got = call(directory, "acquire", **cpu, keys=["k"])
    assert got["entries"] == [None] and got["lease_id"] is None


def test_an_engine_without_a_session_finds_no_remote_cxl_keys(env):
    # An engine still waiting for its first region could not load them.
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    probe = call(directory, "probe", engine_id="b", namespace="ns", keys=["k"])
    assert probe["results"] == [False]
    open_session(directory, "b")
    probe = call(directory, "probe", engine_id="b", namespace="ns", keys=["k"])
    assert probe["results"] == [True]


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


def test_a_kept_region_binds_the_layout_of_its_namespace(env):
    """Keys of an owner that left stay found, so their layout still binds."""
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    call(directory, "close", **a, drained=True)
    assert fates(directory) == {pool_a: "keep"}
    with pytest.raises(ValueError, match="another schema or page size"):
        open_session(directory, "b", page=2 * PAGE)
    open_session(directory, "c")


def test_a_session_whose_regions_all_went_back_gets_a_new_one(env):
    directory, rm, _ = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    call(directory, "return_region", **a, pool_id=pool_a)
    assert call(directory, "probe", **a, namespace="ns", keys=["k"])["results"] == [
        False
    ]
    call(directory, "heartbeat", **a, sequence=1, pools=[])
    assert call(directory, "victims", **a, count=1)["entries"] == []
    pool = call(directory, "expand", **a, op_id="x")["pool"]
    assert pool["pool_id"] != pool_a and pool["capacity"] == 2 * PAGE
    assert commit(directory, a, pool["pool_id"], "k2", 0) == "CREATED"
    # The layout stays bound while the session holds no region.
    with pytest.raises(ValueError, match="another schema or page size"):
        open_session(directory, "b", page=2 * PAGE)


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


def test_victims_can_be_limited_to_some_regions(env):
    directory, _, clock = env
    a, first = open_session(directory, "a")
    second = expand(directory, a)["pool"]["pool_id"]
    commit(directory, a, first, "old", 0)
    clock.advance(1)
    commit(directory, a, second, "new", 0)
    victims = call(directory, "victims", **a, count=2, pools=[second, "other"])
    assert [v["key"] for v in victims["entries"]] == ["new"]
    assert call(directory, "victims", **a, count=2, pools=[])["entries"] == []
    with pytest.raises(ValueError, match="pools"):
        call(directory, "victims", **a, count=1, pools=second)


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


def test_an_expired_owners_keys_stay_found_and_maruserver_keeps_its_region(env):
    directory, rm, clock = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    clock.advance(31)
    with pytest.raises(ValueError, match="expired"):
        commit(directory, a, pool_a, "z", 1)
    c, _ = open_session(directory, "c")
    # The owner wrote "k" before it went; nothing writes the page again.
    entries = call(directory, "acquire", **c, keys=["k"])["entries"]
    assert entries[0]["pool_id"] == pool_a
    assert fates(directory) == {pool_a: "keep"}
    released_by_keeper(directory)
    assert rm.released == [] and 1 in rm.regions
    with pytest.raises(ValueError, match="expired"):
        call(
            directory,
            "remove",
            **a,
            entries=[{"key": "k", "location": location(pool_a, 0)}],
        )


def test_regions_without_keys_go_back_after_the_keeper_releases_them(env):
    directory, rm, clock = env
    a, _ = open_session(directory, "a")
    expand(directory, a)
    clock.advance(31)
    call(directory, "close", **a, drained=True)
    assert rm.released == []  # not before their remote CXL agent released them
    assert set(fates(directory).values()) == {"return"}
    released_by_keeper(directory)
    assert sorted(rm.released) == [1, 2]
    assert directory.departed() == []


def test_a_drained_close_keeps_the_regions_with_keys(env):
    directory, rm, _ = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    commit(directory, a, pool_a, "k", 0)
    lease = call(directory, "acquire", **b, keys=["k"])["lease_id"]
    call(directory, "close", **a, drained=True)
    released_by_keeper(directory)
    # As a cxl Handler's region stays while keys refer to it.
    assert rm.released == []
    assert call(directory, "release", **b, lease_id=lease) == {"held": True}
    entries = call(directory, "acquire", **b, keys=["k"])["entries"]
    assert entries[0]["pool_id"] == pool_a
    # A new worker of the same engine reads it too.
    a2, _ = open_session(directory, "a", token="a2")
    assert call(directory, "acquire", **a2, keys=["k"])["entries"][0] is not None


def test_a_kept_region_given_back_loses_its_keys_and_its_readers_leases(env):
    directory, rm, _ = env
    a, pool_a = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    commit(directory, a, pool_a, "k", 0)
    call(directory, "close", **a, drained=True)
    lease = call(directory, "acquire", **b, keys=["k"])["lease_id"]
    directory.give_back(pool_a, "test")
    # Keys stop being found at once; the region goes back after the release.
    assert call(directory, "acquire", **b, keys=["k"])["entries"] == [None]
    assert call(directory, "release", **b, lease_id=lease) == {"held": False}
    assert rm.released == []
    released_by_keeper(directory)
    assert rm.released == [1]


def test_a_refused_region_request_gives_back_the_kept_region_read_longest_ago(env):
    directory, rm, clock = env
    rm.limit = 4 * 2 * PAGE  # four regions of two pages
    owners = []
    for name in ("a", "b"):
        creds, pool = open_session(directory, name)
        commit(directory, creds, pool, f"{name}-k", 0)
        owners.append((creds, pool))
    c, _ = open_session(directory, "c")
    clock.advance(1)
    call(directory, "probe", engine_id="c", namespace="ns", keys=["a-k"])
    for creds, _pool in owners:
        call(directory, "close", **creds, drained=True)
    d, _ = open_session(directory, "d")
    with pytest.raises(ValueError, match="reservation failed"):
        expand(directory, d)
    assert directory.make_room() == 1
    # "b-k" was read longest ago (never): its region goes first.
    assert fates(directory) == {owners[0][1]: "keep", owners[1][1]: "return"}
    released_by_keeper(directory)
    assert rm.released == [2]
    expand(directory, d)
    assert directory.make_room() == 0


def test_a_kept_region_shows_in_the_usage_without_an_owner(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    call(directory, "close", **a, drained=True)
    (usage,) = directory.get_usage("remote_cxl")["pools"]
    assert usage["pool_id"] == pool_a and usage["replicas"] == 1
    assert usage["engine_id"] == "" and usage["active"] is False


def test_the_owner_reports_the_boot_of_its_regions_memory_node(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    call(
        directory,
        "heartbeat",
        **a,
        sequence=1,
        pools=[
            {
                "pool_id": pool_a,
                "allocated_bytes": PAGE,
                "quarantined_bytes": 0,
                "boot": "boot-1",
            }
        ],
    )
    call(directory, "close", **a, drained=True)
    assert [boot for *_, boot in directory.departed()] == ["boot-1"]


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
    assert fates(directory) == {second: "keep"}  # k2 is still there


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


def test_an_expired_owners_return_of_a_region_maruserver_took_succeeds(env):
    directory, rm, clock = env
    a, first = open_session(directory, "a")
    clock.advance(31)
    call(directory, "return_region", **a, pool_id=first)
    # MaruServer took it over at the expiry; its keeper gives it back.
    assert fates(directory) == {first: "return"}
    released_by_keeper(directory)
    assert rm.released == [1]


def test_an_unconfirmed_release_is_retried(env):
    directory, rm, _ = env
    a, _ = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    rm.fail_release = True
    call(directory, "close", **a, drained=True)
    released_by_keeper(directory)
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


def test_reopening_an_expired_session_is_refused(env):
    directory, _, clock = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    clock.advance(31)
    with pytest.raises(ValueError, match="Session token reused"):
        open_session(directory, "a")


def test_refusals_give_back_one_kept_region_at_a_time(env):
    directory, rm, _ = env
    for name in ("a", "b", "c"):
        creds, pool = open_session(directory, name)
        commit(directory, creds, pool, f"{name}-k", 0)
        call(directory, "close", **creds, drained=True)
    rm.limit = rm.used  # full
    for _ in range(3):
        with pytest.raises(ValueError):
            open_session(directory, "d")
    assert directory.make_room() == 1
    for _ in range(3):
        with pytest.raises(ValueError):
            open_session(directory, "d")
    # The first one has not reached the RM yet: no other goes.
    assert directory.make_room() == 0
    assert list(fates(directory).values()).count("return") == 1
    released_by_keeper(directory)
    open_session(directory, "d")  # takes the space that came back
    with pytest.raises(ValueError):
        open_session(directory, "e")
    assert directory.make_room() == 1
    assert list(fates(directory).values()).count("return") == 1


def test_room_goes_to_a_worker_without_a_region_before_one_that_wants_more(env):
    """The room a refused first region made is not taken by another's expand."""
    directory, rm, _ = env
    a, pool = open_session(directory, "a")
    commit(directory, a, pool, "a-k", 0)
    call(directory, "close", **a, drained=True)  # its region is kept
    b, _ = open_session(directory, "b")
    rm.limit = rm.used  # full
    with pytest.raises(ValueError, match="reservation failed"):
        open_session(directory, "c")  # c has no region yet
    assert directory.make_room() == 1
    released_by_keeper(directory)  # a's region is back at the RM
    with pytest.raises(ValueError, match="without a region"):
        expand(directory, b)  # b evicts its own keys instead
    open_session(directory, "c")  # c gets the room
    with pytest.raises(ValueError, match="reservation failed"):
        expand(directory, b)
    assert directory.make_room() == 0  # nothing kept is left


def test_a_worker_that_stopped_asking_for_a_region_holds_no_room(env):
    directory, rm, clock = env
    b, _ = open_session(directory, "b")
    rm.limit = rm.used
    with pytest.raises(ValueError, match="reservation failed"):
        open_session(directory, "c")
    rm.limit = rm.used + 2 * PAGE
    with pytest.raises(ValueError, match="without a region"):
        expand(directory, b)
    clock.advance(20)
    heartbeat(directory, b)
    clock.advance(11)  # c has not asked again for 31 s
    expand(directory, b)


def test_a_region_stuck_on_its_way_back_does_not_block_room_for_ever(env):
    directory, rm, clock = env
    for name in ("a", "b"):
        creds, pool = open_session(directory, name)
        commit(directory, creds, pool, f"{name}-k", 0)
        call(directory, "close", **creds, drained=True)
    rm.limit = rm.used
    with pytest.raises(ValueError):
        open_session(directory, "d")
    assert directory.make_room() == 1
    with pytest.raises(ValueError):
        open_session(directory, "d")
    clock.advance(31)  # its remote CXL agent never answered
    assert directory.make_room() == 1


def test_the_drained_close_carries_the_boot_of_each_region(env):
    directory, _, _ = env
    a, pool_a = open_session(directory, "a")
    commit(directory, a, pool_a, "k", 0)
    call(
        directory,
        "close",
        **a,
        drained=True,
        pools=[{"pool_id": pool_a, "boot": "b-7"}],
    )
    assert [boot for *_, boot in directory.departed()] == ["b-7"]


def test_expired_remote_cxl_sessions_leave_the_session_limit(env, monkeypatch):
    from maru_server import replica_directory

    monkeypatch.setattr(replica_directory, "MAX_SESSIONS", 2)
    directory, _, clock = env
    open_session(directory, "a")
    open_session(directory, "b")
    with pytest.raises(ValueError, match="session limit"):
        open_session(directory, "c")
    clock.advance(31)  # both expire; MaruServer takes their regions over
    open_session(directory, "c")
    open_session(directory, "d")
    with pytest.raises(ValueError, match="session limit"):
        open_session(directory, "e")


def test_the_oldest_expired_remote_cxl_session_record_goes(env, monkeypatch):
    from maru_server import replica_directory

    monkeypatch.setattr(replica_directory, "MAX_EXPIRED_REMOTE_SESSIONS", 1)
    directory, _, clock = env
    a, _ = open_session(directory, "a")
    clock.advance(31)
    b, _ = open_session(directory, "b")
    clock.advance(31)
    directory.execute("usage", {})  # a request runs the expiry
    # b's record stays, so its late drained close succeeds; a's went.
    call(directory, "close", **b, drained=True)
    assert "a" not in directory._sessions


def test_a_release_that_raises_is_retried(env):
    directory, rm, _ = env
    a, _ = open_session(directory, "a")
    b, _ = open_session(directory, "b")
    rm.unreachable = True
    call(directory, "close", **a, drained=True)
    released_by_keeper(directory)
    assert rm.released == []
    rm.unreachable = False
    heartbeat(directory, b)
    assert rm.released == [1]


def test_a_region_larger_than_every_pool_is_refused_for_good():
    clock, rm = FakeClock(), FakeRm(limit=1 << 20)
    directory = ReplicaDirectory(
        reserve_cxl=rm.reserve,
        release_cxl=rm.release,
        clock=clock,
        session_ttl=30,
        largest_cxl_pool=lambda: 1 << 20,
    )
    with pytest.raises(ValueError, match="larger than the largest pool") as refused:
        open_session(directory, "a", pages=(2 << 20) // PAGE)
    assert "reservation failed" not in str(refused.value)  # not retried
    assert not directory._first_region_waits  # nobody waits for it
    rm.limit = 0  # a full pool is still a refusal that room heals
    with pytest.raises(ValueError, match="reservation failed"):
        open_session(directory, "b")
