# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PageTable: one pool's pages, FREE queue, moves and lookups."""

import collections
from dataclasses import replace

import pytest

from maru_common.storage_types import PoolInfo, PoolKind
from maru_storage_service.page_table import (
    RECOVERY_TAG,
    InvalidTransitionError,
    PageRef,
    PageState,
    PageTable,
)

PS = 4096


def table(pages: int = 4, state: PageState = PageState.FREE) -> PageTable:
    info = PoolInfo(
        pool_id="cxl:n:r1",
        kind=PoolKind.CXL,
        node_id="n",
        pool_generation=1,
        capacity=pages * PS,
        page_size=PS,
    )
    return PageTable(info, state, collections.Counter())


def test_capacity_must_be_whole_pages():
    info = table().info
    with pytest.raises(ValueError, match="whole pages"):
        PageTable(replace(info, capacity=PS + 1), PageState.FREE, collections.Counter())


def test_take_and_free_round_trip_and_count_moves():
    t = table()
    page = t.take(PageState.CREDITED, generation=7)
    assert (page.index, page.generation, page.state) == (0, 7, PageState.CREDITED)
    assert t.find_allocation(page.allocation_id, 7) == PageRef(t.pool_id, 0, 7)
    assert t.free_count() == 3 and t.bytes[PageState.CREDITED] == PS
    page.holder = "w"
    assert t.check() == {"w": PS}
    t.free(page, PageState.CREDITED)
    assert t.free_count() == 4 and page.holder is None
    assert t.find_allocation(page.allocation_id, 7) is None
    assert t.take(PageState.TRANSFER, 8).index == 1  # FIFO: the freed page goes last
    assert t._transitions[(PageState.FREE, PageState.CREDITED)] == 1
    t.check()


def test_a_move_from_the_wrong_state_is_refused():
    t = table()
    page = t.take(PageState.PENDING, 1)
    with pytest.raises(InvalidTransitionError):
        t.move(page, PageState.READY, PageState.RETIRING)
    with pytest.raises(InvalidTransitionError):
        t.get(PageRef(t.pool_id, page.index, 2), PageState.PENDING)  # old ref
    with pytest.raises(InvalidTransitionError):
        t.get(PageRef(t.pool_id, 9, 1), PageState.PENDING)
    assert t.get(t.ref(page), PageState.PENDING) is page


def test_quarantine_keeps_the_allocation_taken():
    t = table()
    page = t.take(PageState.READY, 1)
    t.quarantine(page, PageState.READY, "server:e1")
    assert page.tag == "server:e1" and page.key is None
    assert t.find_allocation(page.allocation_id, 1) is not None
    t.check()


def test_lookups_by_offset_and_location():
    t = table()
    page = t.take(PageState.READY, 5)
    loc = t.location(page)
    assert (loc.offset, loc.length, loc.kind) == (0, PS, PoolKind.CXL)
    assert t.find(loc) == t.ref(page)
    assert t.find(replace(loc, generation=4)) is None
    assert t.find(replace(loc, allocation_id="other")) is None
    assert t.at(PS) is t.pages[1]
    assert t.at(PS + 1) is None and t.at(-PS) is None and t.at(4 * PS) is None
    assert t.find(replace(loc, offset=PS)) is None  # FREE


def test_bind_moves_the_allocation_index():
    t = table()
    page = t.take(PageState.READY, 1)
    old = page.allocation_id
    t.bind(page, "catalog-alloc", 9)
    assert t.index_of(old) is None and t.index_of("catalog-alloc") == page.index
    assert t.find_allocation("catalog-alloc", 9) == PageRef(t.pool_id, 0, 9)
    t.check()


def test_an_adopted_pool_starts_quarantined_for_recovery():
    t = table(state=PageState.QUARANTINED)
    assert t.free_count() == 0
    assert all(p.tag == RECOVERY_TAG for p in t.pages)
    assert t.bytes[PageState.QUARANTINED] == 4 * PS
    t.check()
