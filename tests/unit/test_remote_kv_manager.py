# SPDX-License-Identifier: Apache-2.0
"""KV Manager: per-request prefetch windows read only once staged."""

from __future__ import annotations

import pytest

from maru_remote.kv_manager import KVManager

MIB = 1024 * 1024
OBJ = 16 * MIB


class FakeExecutor:
    """Records pin/unpin calls; pins complete when the test says so."""

    def __init__(self) -> None:
        self.pending: dict[tuple[int, int], object] = {}
        self.pinned: list[tuple[int, int]] = []
        self.unpinned: list[tuple[int, int]] = []
        self.closed = False

    def pin(self, address, size, done):
        self.pending[(address, size)] = done
        self.pinned.append((address, size))

    def unpin(self, address, size):
        self.unpinned.append((address, size))

    def close(self):
        self.closed = True

    def complete(self, *addrs, ok=True):
        for a in addrs:
            self.pending.pop((a, OBJ))(ok)

    def complete_all(self):
        for rng in list(self.pending):
            self.pending.pop(rng)(True)


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def keys_ranges(n, base=0, prefix="k"):
    keys = [f"{prefix}{i}" for i in range(n)]
    ranges = [(base + i * OBJ, OBJ) for i in range(n)]
    return keys, ranges


def make(window=4, budget=64 * OBJ, ttl=60.0):
    ex, clock = FakeExecutor(), Clock()
    return (
        KVManager(window, ex, max_held_bytes=budget, group_ttl_s=ttl, clock=clock),
        ex,
        clock,
    )


def test_lookup_holds_only_the_window():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(10)
    kv.on_lookup(keys, ranges)
    assert ex.pinned == ranges[:4]


def test_read_waits_until_every_object_is_staged():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(10)
    kv.on_lookup(keys, ranges)
    assert not kv.ready(keys[:2], ranges[:2])
    ex.complete(ranges[0][0])
    assert not kv.ready(keys[:2], ranges[:2])
    ex.complete(ranges[1][0])
    assert kv.ready(keys[:2], ranges[:2])


def test_release_unpins_and_moves_the_window():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(10)
    kv.on_lookup(keys, ranges)
    ex.complete_all()
    kv.on_consumed(keys[:2])
    assert ex.unpinned == ranges[:2]
    assert ex.pinned == ranges[:6]


def test_read_without_lookup_opens_its_own_window():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(2)
    assert not kv.ready(keys, ranges)
    assert ex.pinned == ranges
    ex.complete_all()
    assert kv.ready(keys, ranges)


def test_missing_keys_do_not_block_a_read():
    kv, ex, _ = make()
    keys, ranges = keys_ranges(2)
    kv.on_lookup(keys, ranges)
    ex.complete_all()
    assert kv.ready(keys + ["gone"], ranges + [None])


def test_failed_pin_lets_the_read_go_ahead():
    kv, ex, _ = make()
    keys, ranges = keys_ranges(1)
    kv.on_lookup(keys, ranges)
    ex.complete(ranges[0][0], ok=False)
    assert kv.ready(keys, ranges)
    kv.on_consumed(keys)
    assert ex.unpinned == []  # nothing was pinned


def test_shared_object_is_unpinned_after_its_last_reader():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(3)
    kv.on_lookup(keys[:2], ranges[:2])
    kv.on_lookup(keys, ranges)  # a longer request sharing the first two
    assert ex.pinned == ranges  # each object pinned once
    ex.complete_all()
    kv.on_consumed(keys[:2])
    assert ex.unpinned == ranges[:2]  # both requests read past them


def test_object_released_while_filling_is_unpinned_when_the_fill_ends():
    kv, ex, clock = make(ttl=5.0)
    keys, ranges = keys_ranges(1)
    kv.on_lookup(keys, ranges)
    clock.t = 10.0
    kv.on_lookup(*keys_ranges(1, base=100 * OBJ, prefix="x"))  # expires the first
    assert ex.unpinned == []
    ex.complete(ranges[0][0])
    assert ex.unpinned == ranges
    assert kv.stats()["held_bytes"] == OBJ  # only the new request's object


def test_budget_makes_later_requests_wait():
    kv, ex, _ = make(window=4, budget=4 * OBJ)
    a_keys, a_ranges = keys_ranges(4)
    b_keys, b_ranges = keys_ranges(4, base=100 * OBJ, prefix="b")
    kv.on_lookup(a_keys, a_ranges)
    kv.on_lookup(b_keys, b_ranges)
    assert ex.pinned == a_ranges
    ex.complete_all()
    kv.on_consumed(a_keys[:2])
    assert ex.pinned == a_ranges + b_ranges[:2]


def test_finished_request_frees_its_window():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(3)
    kv.on_lookup(keys, ranges)
    ex.complete_all()
    kv.on_consumed(keys)
    assert kv.stats()["live_requests"] == 0
    assert kv.stats()["held_bytes"] == 0


def test_close_lets_everything_go():
    kv, ex, _ = make()
    keys, ranges = keys_ranges(2)
    kv.on_lookup(keys, ranges)
    ex.complete_all()
    kv.close()
    assert sorted(ex.unpinned) == ranges
    assert not ex.closed  # the executor's owner closes it (it may be shared)


@pytest.mark.parametrize("window, budget", [(0, OBJ), (1, 0)])
def test_rejects_non_positive_settings(window, budget):
    with pytest.raises(ValueError):
        KVManager(window, FakeExecutor(), max_held_bytes=budget)


def test_read_past_the_window_moves_the_window_there():
    # The worker starts reading at chunk 10 when the GPU already holds the
    # first chunks: the window must jump there, not wait at chunk 0.
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(20)
    kv.on_lookup(keys, ranges)
    assert not kv.ready(keys[10:12], ranges[10:12])
    assert ranges[10] in ex.pinned and ranges[13] in ex.pinned
    ex.complete_all()
    assert sorted(ex.unpinned) == ranges[:4]  # skipped objects are let go
    assert kv.ready(keys[10:12], ranges[10:12])


def test_key_published_after_the_first_ask_still_gets_loaded():
    # First ask: the second key is not published yet. Second ask: it is.
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(2)
    assert not kv.ready(keys, [ranges[0], None])
    ex.complete_all()
    assert not kv.ready(keys, ranges)
    assert ranges[1] in ex.pinned
    ex.complete_all()
    assert kv.ready(keys, ranges)


def test_second_reader_of_a_consumed_prefix_gets_it_loaded_again():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(10)
    kv.on_lookup(keys, ranges)  # two requests with the same prefix
    kv.on_lookup(keys, ranges)
    ex.complete_all()
    assert kv.ready(keys[:4], ranges[:4])  # the first reads 0..3 and is done
    kv.on_consumed(keys[:4])
    ex.complete_all()
    assert not kv.ready(keys[:4], ranges[:4])  # unpinned; loaded again
    ex.complete_all()
    assert kv.ready(keys[:4], ranges[:4])


def test_unread_windows_give_way_to_a_read():
    kv, ex, _ = make(window=4, budget=8 * OBJ)
    a, ra = keys_ranges(10, prefix="a")
    b, rb = keys_ranges(10, base=100 * OBJ, prefix="b")
    c, rc = keys_ranges(10, base=200 * OBJ, prefix="c")
    kv.on_lookup(a, ra)
    kv.on_lookup(b, rb)  # two hinted windows fill the budget
    ex.complete_all()
    kv.on_lookup(c, rc)
    assert not kv.ready(c[:2], rc[:2])  # a worker reads c: room is made
    ex.complete_all()
    assert kv.ready(c[:2], rc[:2])
    assert kv.stats()["held_bytes"] <= 8 * OBJ


def test_key_stored_again_elsewhere_is_loaded_at_its_new_range():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(2)
    kv.on_lookup(keys, ranges)
    ex.complete_all()
    moved = [ranges[0], (500 * OBJ, OBJ)]  # k1 evicted and stored again
    assert not kv.ready(keys, moved)
    assert moved[1] in ex.pinned
    ex.complete_all()
    assert kv.ready(keys, moved)


def test_same_read_again_does_not_count_objects_twice():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(6)
    assert not kv.ready(keys, ranges)  # a read nobody hinted: its own request
    ex.complete_all()
    assert kv.ready(keys, ranges)
    kv.on_consumed(keys[:2])
    assert not kv.ready(keys, ranges)  # the same six keys asked again
    ex.complete_all()
    assert kv.ready(keys, ranges)
    kv.on_consumed(keys)
    assert kv.stats()["held_bytes"] == 0
    assert sorted(set(ex.pinned)) == sorted(set(ex.unpinned))


def test_evicted_range_is_let_go_at_once():
    kv, ex, _ = make(window=4)
    keys, ranges = keys_ranges(2)
    kv.on_lookup(keys, ranges)
    ex.complete_all()
    kv.forget_range(ranges[1])  # the server evicts k1; its page may be reused
    assert ranges[1] in ex.unpinned
    kv.on_consumed(keys)
    assert ex.unpinned.count(ranges[1]) == 1  # not unpinned twice
