# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PageTable: the pages of one pool, their states, the FREE queue and lookups
by location.

Not thread safe: the PoolManager calls it under its lock. Which pool gives
credit, budgets, generations and quarantine rules stay in the PoolManager.
"""

import collections
import uuid
from dataclasses import dataclass
from enum import StrEnum, unique

from maru_common.storage_types import Key, Location, PoolInfo


@unique
class PageState(StrEnum):
    FREE = "FREE"
    CREDITED = "CREDITED"
    TRANSFER = "TRANSFER"
    PENDING = "PENDING"
    READY = "READY"
    RETIRING = "RETIRING"
    QUARANTINED = "QUARANTINED"


# Quarantine tag of a taken-over pool's pages: those NODE_RECOVER_LIST does not
# list, and those freed while an earlier run may still write the pool
RECOVERY_TAG = "recovery"


class InvalidTransitionError(RuntimeError):
    """The service asked for a page transition the ledger does not allow."""


@dataclass(frozen=True)
class PageRef:
    """One page at one generation."""

    pool_id: str
    index: int
    generation: int


@dataclass
class Page:
    index: int
    state: PageState = PageState.FREE
    allocation_id: str = ""
    # Last given; kept while FREE
    generation: int = 0
    # CREDITED: the worker incarnation writing it
    holder: str | None = None
    # PENDING, READY, RETIRING
    key: Key | None = None
    location: Location | None = None
    # QUARANTINED: why it waits (see the PoolManager's tags)
    tag: str | None = None


Transitions = collections.Counter[tuple[PageState, PageState]]


class PageTable:
    """The pages of one pool. ``transitions`` counts every move, shared by
    the tables of a node."""

    def __init__(self, info: PoolInfo, state: PageState, transitions: Transitions):
        if info.page_size <= 0 or info.capacity % info.page_size:
            raise ValueError(f"pool {info.pool_id}: capacity is not whole pages")
        self.info = info
        self.page_size = info.page_size
        count = info.capacity // info.page_size
        self.pages = [Page(i, state=state) for i in range(count)]
        if state == PageState.QUARANTINED:
            for page in self.pages:
                page.tag = RECOVERY_TAG
        self._free: collections.deque[int] = collections.deque(
            range(count) if state == PageState.FREE else ()
        )
        self.bytes = dict.fromkeys(PageState, 0)
        self.bytes[state] = info.capacity
        # allocation_id -> page index, for pages out of FREE
        self._by_allocation: dict[str, int] = {}
        self._transitions = transitions

    @property
    def pool_id(self) -> str:
        return self.info.pool_id

    def free_count(self) -> int:
        return len(self._free)

    # ------------------------------------------------------------ transitions

    def take(self, dst: PageState, generation: int) -> Page:
        """The next FREE page, moved to ``dst`` under a new allocation id and
        ``generation``. The caller checked that one is FREE."""
        page = self.pages[self._free.popleft()]
        page.allocation_id = uuid.uuid4().hex
        page.generation = generation
        self._by_allocation[page.allocation_id] = page.index
        self.move(page, PageState.FREE, dst)
        return page

    def move(self, page: Page, src: PageState, dst: PageState) -> None:
        if page.state != src:
            raise InvalidTransitionError(
                f"page {page.index} is {page.state}, not {src}"
            )
        page.state = dst
        self.bytes[src] -= self.page_size
        self.bytes[dst] += self.page_size
        self._transitions[(src, dst)] += 1

    def free(self, page: Page, src: PageState) -> None:
        """``src`` -> FREE: the page and its allocation id can be given again."""
        self.move(page, src, PageState.FREE)
        self._by_allocation.pop(page.allocation_id, None)
        page.holder = None
        page.key = None
        page.location = None
        page.tag = None
        self._free.append(page.index)

    def quarantine(self, page: Page, src: PageState, tag: str) -> None:
        """``src`` -> QUARANTINED under ``tag``; the allocation stays taken."""
        self.move(page, src, PageState.QUARANTINED)
        page.holder = None
        page.key = None
        page.location = None
        page.tag = tag

    def bind(self, page: Page, allocation_id: str, generation: int) -> None:
        """The page takes another allocation and generation (the Catalog's)."""
        self._by_allocation.pop(page.allocation_id, None)
        page.allocation_id = allocation_id
        page.generation = generation
        self._by_allocation[allocation_id] = page.index

    # ---------------------------------------------------------------- lookups

    def get(self, ref: PageRef, state: PageState) -> Page:
        """The page at ``ref`` if it is in ``state`` at that generation."""
        if not 0 <= ref.index < len(self.pages):
            raise InvalidTransitionError(f"no page {ref}")
        page = self.pages[ref.index]
        if page.generation != ref.generation or page.state != state:
            raise InvalidTransitionError(
                f"page {ref} is {page.state} at generation {page.generation}, "
                f"expected {state}"
            )
        return page

    def at(self, offset: int) -> Page | None:
        """The page starting at ``offset``; None if no page starts there."""
        if offset < 0 or offset % self.page_size or offset >= self.info.capacity:
            return None
        return self.pages[offset // self.page_size]

    def index_of(self, allocation_id: str) -> int | None:
        return self._by_allocation.get(allocation_id)

    def ref(self, page: Page) -> PageRef:
        return PageRef(self.info.pool_id, page.index, page.generation)

    def location(self, page: Page) -> Location:
        """The whole page as a location."""
        return Location(
            pool_id=self.info.pool_id,
            allocation_id=page.allocation_id,
            generation=page.generation,
            offset=page.index * self.page_size,
            length=self.page_size,
            kind=self.info.kind,
        )

    def find(self, location: Location) -> PageRef | None:
        """The non-FREE page at exactly this location."""
        page = self.at(location.offset)
        if (
            page is None
            or page.state == PageState.FREE
            or page.generation != location.generation
            or page.allocation_id != location.allocation_id
        ):
            return None
        return self.ref(page)

    def find_allocation(self, allocation_id: str, generation: int) -> PageRef | None:
        index = self._by_allocation.get(allocation_id)
        if index is None or self.pages[index].generation != generation:
            return None
        return PageRef(self.info.pool_id, index, generation)

    def pages_in(self, state: PageState) -> list[PageRef]:
        return [self.ref(p) for p in self.pages if p.state == state]

    def check(self) -> collections.Counter[str]:
        """Assert this pool's invariants; the credited bytes by holder."""
        pool_id = self.info.pool_id
        counts = collections.Counter(p.state for p in self.pages)
        assert sum(self.bytes.values()) == self.info.capacity, (pool_id, self.bytes)
        for state in PageState:
            assert self.bytes[state] == counts[state] * self.page_size, (
                pool_id,
                state,
            )
        assert sorted(self._free) == sorted(
            p.index for p in self.pages if p.state == PageState.FREE
        ), pool_id
        assert self._by_allocation == {
            p.allocation_id: p.index
            for p in self.pages
            if p.state != PageState.FREE and p.allocation_id
        }, pool_id
        held: collections.Counter[str] = collections.Counter()
        for page in self.pages:
            if page.state == PageState.CREDITED:
                assert page.holder is not None
                held[page.holder] += self.page_size
            else:
                assert page.holder is None, page
        return held
