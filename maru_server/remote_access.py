# SPDX-License-Identifier: Apache-2.0
"""Remote access to this node's CXL pool, served from inside MaruServer.

Workers on other nodes reach the pool through MaruHandler's ``remote``
storage backend. They send control requests (reserve, publish, lookup,
release, ...) to the remote endpoint and move KV bytes with one-sided RDMA
between their staging buffers and the pool, so this side never waits for a
transfer.

:class:`RemoteAccess` owns the pool's remote regions: MaruServer allocates
them under one owner name, this process maps them and registers them with
NIXL. It keeps the state only remote access needs (write reservations, read
tickets, quarantined pages, LRU order, the page behind each published key)
and records keys in MaruServer's ledger with direct calls, so the ledger and
that state live and restart together.

:func:`serve_remote` answers the remote endpoint on its own thread, so a long
remote request (growing the pool maps and registers a new region) never holds
up the RPC thread that serves local clients.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import zmq

from maru_handler.memory.mapper import DaxMapper
from maru_handler.memory.owned_region_manager import OwnedRegionManager
from maru_remote import protocol
from maru_remote.transport import buffer_address

from .kv_manager import DeleteResult

if TYPE_CHECKING:
    from maru_remote.transport import NixlTransport

    from .server import MaruServer

logger = logging.getLogger(__name__)

# A failed NIXL teardown must keep mappings alive even if its caller drops the
# access object. A successful retry removes it; process exit is the last resort.
_RETAINED_ACCESSES: set[RemoteAccess] = set()

CREATED = "CREATED"
ALREADY_PRESENT = "ALREADY_PRESENT"
REJECTED = "REJECTED"
POOL_FULL = "POOL_FULL"


class PoolFullError(ValueError):
    """The pool cannot hold more pages (reported to clients as POOL_FULL)."""


def _require_str_list(value: Any, name: str) -> list[str]:
    """Validate that a request field is a list of strings.

    Args:
        value: The field value from the request.
        name: Field name used in the error message.

    Returns:
        The same list.

    Raises:
        ValueError: if ``value`` is not a list of ``str``.
    """
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError(f"{name} must be a list of str")
    return value


def _require_str(value: Any, name: str) -> str:
    """Validate that a request field is a string.

    Args:
        value: The field value from the request.
        name: Field name used in the error message.

    Returns:
        The same string.

    Raises:
        ValueError: if ``value`` is not a ``str``.
    """
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a str")
    return value


@dataclass
class _Region:
    region_id: int
    base: int
    length: int
    registration: Any


@dataclass
class _Page:
    region_id: int
    index: int


@dataclass
class _Reservation:
    page: _Page
    size: int
    deadline: float


@dataclass
class _Ticket:
    keys: list[str]
    deadline: float


class RemoteAccess:
    """Serve reservation, publish, lookup and read protection to remote handlers.

    Every reply carries the access ``generation``, created per maru-server
    run, so a client notices a restart on its next call and requests from an
    earlier run are refused before they execute.
    """

    def __init__(
        self,
        server: MaruServer,
        transport: NixlTransport,
        *,
        pool_size: int,
        page_bytes: int,
        pool_id: str,
        reservation_ttl_s: float = 60.0,
        ticket_ttl_s: float = 120.0,
        quarantine_ttl_s: float = 600.0,
        capacity_bytes: int | None = None,
        evict: bool = True,
        eviction_log_len: int = 65536,
        clock: Callable[[], float] = time.monotonic,
        mapper: DaxMapper | None = None,
    ) -> None:
        """Allocate, map and register the first remote region.

        Args:
            server: The MaruServer whose ledger records the published keys.
            transport: NIXL transport that exposes the regions.
            pool_size: Bytes of each remote region (the first one and every
                region added when the pool grows).
            page_bytes: Page size; one page holds one KV object.
            pool_id: Name reported to clients in ``hello``.
            reservation_ttl_s: Lifetime of an unpublished page reservation.
            ticket_ttl_s: Lifetime of a read ticket (pinned keys).
            quarantine_ttl_s: Time before warning about an unconfirmed WRITE.
                Never authorizes page reuse; only confirmed completion does.
            capacity_bytes: Most pool bytes to hold in pages (reserved,
                published or quarantined); None grows the pool until the
                device refuses a region.
            evict: When the pool is full, delete the least recently used
                published keys (stored or read; never pinned ones) to make
                room. Otherwise a reservation fails with ``POOL_FULL``.
            eviction_log_len: How many of the latest evicted keys to keep for
                ``evicted_since``; a client further behind forgets everything.
            clock: Monotonic time source (tests inject a fake).
            mapper: Region mapper; by default one that maps through the
                server's resource manager without CUDA registration.

        Raises:
            ValueError: if a size is not positive or a region holds no page.
            RuntimeError: if the first region cannot be allocated or mapped.
        """
        if pool_size <= 0 or page_bytes <= 0 or pool_size < page_bytes:
            raise ValueError("pool_size must hold at least one page_bytes page")
        self._server = server
        self._transport = transport
        self._pool_id = pool_id
        self._pool_size = pool_size
        self._page_bytes = page_bytes
        self._reservation_ttl_s = reservation_ttl_s
        self._ticket_ttl_s = ticket_ttl_s
        self._quarantine_ttl_s = quarantine_ttl_s
        self._capacity_pages = (
            None if capacity_bytes is None else capacity_bytes // page_bytes
        )
        self._evict_enabled = evict
        self._clock = clock
        self._generation = uuid.uuid4().hex
        # Owner name of the remote regions in MaruServer's allocation ledger.
        self._owner = f"maru-remote-{self._generation[:12]}"
        self._mapper = mapper or DaxMapper(
            rm_address=server.rm_address, cuda_register=False
        )
        self._owned = OwnedRegionManager(self._mapper, page_bytes)
        server.add_managed_owner(self._owner)
        self._regions: dict[int, _Region] = {}
        self._failed_regions: set[int] = set()
        self._md_version = 0
        # The page behind every key published through this access.
        self._pages: dict[str, _Page] = {}
        # Published keys, least recently used (stored or read) first.
        self._lru: OrderedDict[str, None] = OrderedDict()
        self._evicted = 0
        # (eviction number, key) of the latest evictions, oldest first.
        self._eviction_log: deque[tuple[int, str]] = deque(maxlen=eviction_log_len)
        self._reservations: dict[str, _Reservation] = {}
        self._quarantine: dict[str, _Reservation] = {}
        self._tickets: dict[str, _Ticket] = {}
        self._closed = False
        self._lock = threading.Lock()
        self._ops: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
            "hello": self._op_hello,
            "metadata": self._op_metadata,
            "reserve": self._op_reserve,
            "publish": self._op_publish,
            "abandon": self._op_abandon,
            "quarantine": self._op_quarantine,
            "exists": self._op_exists,
            "lookup": self._op_lookup,
            "release": self._op_release,
            "stats": self._op_stats,
            "ping": self._op_ping,
            "evicted_since": self._op_evicted_since,
        }
        if not self._add_region():
            raise RuntimeError(
                f"could not allocate the first remote region ({pool_size} bytes)"
            )

    @property
    def generation(self) -> str:
        """Identifier created at start-up; changes when maru-server restarts."""
        return self._generation

    def handle(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Dispatch one decoded request; never raises.

        Args:
            msg: Decoded request map with an ``op`` field.

        Returns:
            ``{"ok": True, "generation": ..., "evictions": ..., ...fields}``
            on success, or ``{"ok": False, "generation": ..., "error": str}``
            on failure. ``evictions`` counts the keys evicted in this run, so
            a client can tell that keys it remembers may be gone.
        """
        name = msg.get("op")
        op = self._ops.get(name) if isinstance(name, str) else None
        expected = msg.get("generation")
        if expected is not None and expected != self._generation:
            # The client still holds state (tickets, NIXL peer) of an earlier
            # run. Refuse before executing so its retry is safe.
            return self._error("client is connected to an earlier server run")
        if op is None:
            return self._error(f"unknown op {name!r}")
        try:
            with self._lock:
                if self._closed:
                    return self._error("remote access is closed")
                reply = op(msg)
                return {
                    "ok": True,
                    "generation": self._generation,
                    "evictions": self._evicted,
                    **reply,
                }
        except PoolFullError as exc:
            return {
                "ok": False,
                "generation": self._generation,
                "code": POOL_FULL,
                "error": f"pool full: {exc}",
            }
        except (KeyError, TypeError, ValueError) as exc:
            return self._error(f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # keep serving; the client sees the error
            logger.exception("remote op %s failed", name)
            return self._error(f"{type(exc).__name__}: {exc}")

    def sweep(self) -> None:
        """Quarantine expired writes and reclaim expired read tickets.

        A reservation timeout does not revoke the worker's RDMA descriptor.
        Keep its page until the worker confirms completion with ``abandon``
        or pool shutdown deregisters the memory. Warn once for old quarantine.
        An entry whose unpin fails is kept for the next sweep.
        """
        now = self._clock()
        with self._lock:
            if self._closed:
                return
            for ticket, res in list(self._reservations.items()):
                if res.deadline <= now:
                    self._quarantine[ticket] = _Reservation(
                        res.page, res.size, now + self._quarantine_ttl_s
                    )
                    del self._reservations[ticket]
            for ticket, res in self._quarantine.items():
                if res.deadline <= now:
                    logger.warning(
                        "quarantined WRITE %s has no completion confirmation; "
                        "page retained until abandon or pool shutdown",
                        ticket,
                    )
                    res.deadline = float("inf")
            for ticket_id, tk in list(self._tickets.items()):
                if tk.deadline > now:
                    continue
                try:
                    self._server.batch_unpin(tk.keys)
                except Exception:
                    logger.exception("sweep: unpinning ticket %s failed", ticket_id)
                    continue
                del self._tickets[ticket_id]

    def close(self) -> None:
        """Remove the remote keys, release the regions and close NIXL.

        Keys published here become unreadable once their regions are gone,
        so they leave the ledger first; only then are the regions returned.
        NIXL deregistration must succeed before any mapping or region is
        returned. A failure keeps this object alive and close can be retried.
        """
        with self._lock:
            if self._closed and not self._regions and not self._failed_regions:
                return
            self._closed = True
            _RETAINED_ACCESSES.add(self)
            self._transport.close()
            for tk in self._tickets.values():
                try:
                    self._server.batch_unpin(tk.keys)
                except Exception:
                    logger.exception("close: unpinning a read ticket failed")
            self._tickets.clear()
            for key, page in list(self._pages.items()):
                try:
                    result = self._delete_at(key, page)
                except Exception:
                    logger.exception("close: deleting %s failed", key)
                    continue
                if result is DeleteResult.PINNED:
                    # A local client still reads it: the key and its region
                    # stay until that read ends or the server exits.
                    logger.warning("close: %s is pinned by a local reader; kept", key)
            self._pages.clear()
            self._lru.clear()
            self._reservations.clear()
            self._quarantine.clear()
            self._regions.clear()
            region_ids = set(self._owned.close()) | self._failed_regions
            self._mapper.close()
            for region_id in region_ids:
                try:
                    self._server.return_alloc(self._owner, region_id, managed=True)
                except Exception:
                    logger.exception("close: returning region %d failed", region_id)
            self._failed_regions.clear()
            _RETAINED_ACCESSES.discard(self)

    # ---- ops (all called under self._lock) ----------------------------------

    def _op_hello(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Answer a client's first request with pool identity and layout.

        Args:
            msg: Request with ``client_id``.

        Returns:
            Pool identity, page size, protocol version, lifetimes, NIXL
            metadata and regions.
        """
        _require_str(msg["client_id"], "client_id")
        return {
            "pool_id": self._pool_id,
            "page_bytes": self._page_bytes,
            "protocol": protocol.PROTOCOL_VERSION,
            "reservation_ttl_s": self._reservation_ttl_s,
            "ticket_ttl_s": self._ticket_ttl_s,
            **self._op_metadata(msg),
        }

    def _op_metadata(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Return current NIXL metadata and the registered regions.

        Args:
            msg: Request (no fields used).

        Returns:
            ``nixl_md``, ``md_version`` and ``regions``.
        """
        return {
            "nixl_md": self._transport.metadata(),
            "md_version": self._md_version,
            "regions": self._region_list(),
        }

    def _op_reserve(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Take one page per requested size for remote WRITE.

        All pages of one request succeed together; on failure the pages
        already taken for it are freed before the error propagates. The
        reservation records the requested size, so a later lookup reports
        exactly the bytes written.

        Args:
            msg: Request with ``client_id`` and ``sizes`` (positive ints, each
                at most ``page_bytes``).

        Returns:
            ``pages`` (ticket, region, base, offset, length) and ``md_version``.

        Raises:
            ValueError: if ``sizes`` is malformed.
            PoolFullError: if the pool cannot hold the pages.
        """
        _require_str(msg["client_id"], "client_id")
        sizes = msg["sizes"]
        if (
            not isinstance(sizes, list)
            or not sizes
            or not all(
                isinstance(n, int)
                and not isinstance(n, bool)
                and 0 < n <= self._page_bytes
                for n in sizes
            )
        ):
            raise ValueError(
                f"sizes must be a non-empty list of ints in 1..{self._page_bytes}"
            )
        count = len(sizes)
        if self._capacity_pages is not None:
            if len(sizes) > self._capacity_pages:
                raise PoolFullError("requested batch exceeds the entire pool capacity")
        # Determine the whole batch's shortage before deleting any cached key.
        # Grow only to the configured capacity; physical allocation can fail
        # earlier, in which case existing pages are the only usable capacity.
        while True:
            stats = self._owned.get_stats()
            free = int(stats["total_free_pages"])
            used = int(stats["total_allocated_pages"])
            capacity = free + used
            if self._capacity_pages is not None:
                capacity = min(capacity, self._capacity_pages)
            if count <= capacity - used:
                break
            if self._capacity_pages is not None and free + used >= self._capacity_pages:
                break
            if not self._add_region():
                break
        shortage = count - (capacity - used)
        if shortage > 0 and self._evict(shortage) < shortage:
            raise PoolFullError("not enough free or evictable pages for the batch")
        deadline = self._clock() + self._reservation_ttl_s
        taken: list[_Page] = []
        reserved: dict[str, _Reservation] = {}
        pages: list[dict[str, Any]] = []
        try:
            for size in sizes:
                loc = self._owned.allocate()
                if loc is None:
                    raise PoolFullError("reserved capacity could not be allocated")
                page = _Page(*loc)
                taken.append(page)
                region = self._regions[page.region_id]
                ticket = uuid.uuid4().hex
                reserved[ticket] = _Reservation(page, size, deadline)
                pages.append(
                    {
                        "ticket": ticket,
                        "region_id": region.region_id,
                        "base": region.base,
                        "offset": page.index * self._page_bytes,
                        "length": size,
                    }
                )
        except Exception:
            for page in taken:
                self._free_page(page)
            raise
        self._reservations.update(reserved)
        return {"pages": pages, "md_version": self._md_version}

    def _op_publish(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Record reserved pages as keys in the ledger.

        Every ticket is consumed. A key already readable here keeps its first
        value and this page is freed (``ALREADY_PRESENT``). A key a local
        client holds in its own region cannot be read remotely nor replaced
        (that page is the local client's), so it is ``REJECTED``.

        Args:
            msg: Request with ``entries``: list of ``{"ticket", "key"}``.

        Returns:
            ``statuses``: ``CREATED``, ``ALREADY_PRESENT`` or ``REJECTED`` per
            entry.

        Raises:
            KeyError: if a ticket is unknown or expired (nothing is published).
            ValueError: if the entries are malformed or repeat a ticket or key.
        """
        entries = msg["entries"]
        if not isinstance(entries, list):
            raise ValueError("entries must be a list")
        tickets = [_require_str(e["ticket"], "ticket") for e in entries]
        keys = [_require_str(e["key"], "key") for e in entries]
        if len(set(tickets)) != len(tickets):
            raise ValueError("entries repeat a ticket")
        if len(set(keys)) != len(keys):
            raise ValueError("entries repeat a key")
        reservations: list[_Reservation] = []
        for ticket in tickets:
            res = self._reservations.get(ticket)
            if res is None or res.deadline <= self._clock():
                raise KeyError(f"unknown or expired ticket {ticket}")
            reservations.append(res)
        for ticket in tickets:
            del self._reservations[ticket]
        # Reservations whose page is neither recorded under a key nor freed.
        pending = dict(enumerate(reservations))
        try:
            return {"statuses": self._publish(keys, pending)}
        except Exception:
            for res in pending.values():
                self._free_page(res.page)
            raise

    def _publish(self, keys: list[str], pending: dict[int, _Reservation]) -> list[str]:
        """Register the reserved pages of ``keys``; see :meth:`_op_publish`.

        Removes each entry from ``pending`` once its page is recorded or freed.
        """
        existing = self._server.batch_register_or_lookup(
            [
                (
                    key,
                    pending[i].page.region_id,
                    pending[i].page.index * self._page_bytes,
                    pending[i].size,
                )
                for i, key in enumerate(keys)
            ]
        )
        statuses: list[str] = []
        for i, (key, found) in enumerate(zip(keys, existing, strict=True)):
            res = pending.pop(i)
            if found is None:
                self._pages[key] = res.page
                self._touch(key)
                statuses.append(CREATED)
                continue
            # Every outcome other than CREATED returns the reserved page.
            self._free_page(res.page)
            statuses.append(self._present_status(key, found))
        return statuses

    def _op_abandon(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Free reserved or quarantined pages the client will not publish.

        Args:
            msg: Request with ``tickets``: list of ticket strings.

        Returns:
            ``freed``: number of known tickets freed (unknown ones are skipped).
        """
        freed = 0
        for ticket in _require_str_list(msg["tickets"], "tickets"):
            res = self._reservations.pop(ticket, None)
            if res is None:
                res = self._quarantine.pop(ticket, None)
            if res is None:
                continue
            self._free_page(res.page)
            freed += 1
        return {"freed": freed}

    def _op_quarantine(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Keep reserved pages out of circulation until the client abandons them.

        A client sends this when WRITE completion is unconfirmed. The NIC may
        still deliver bytes into these pages. Expiry also quarantines them;
        this request makes the reservation unpublishable immediately.

        Args:
            msg: Request with ``tickets``: list of ticket strings.

        Returns:
            ``quarantined``: number of reservations moved to quarantine.
        """
        moved = 0
        deadline = self._clock() + self._quarantine_ttl_s
        for ticket in _require_str_list(msg["tickets"], "tickets"):
            res = self._reservations.pop(ticket, None)
            if res is None:
                continue
            self._quarantine[ticket] = _Reservation(res.page, res.size, deadline)
            moved += 1
        return {"quarantined": moved}

    def _op_exists(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Report which keys a remote client can read.

        Args:
            msg: Request with ``keys``: list of str.

        Returns:
            ``found``: one bool per key; a key held in a local client's region
            is reported missing.
        """
        keys = _require_str_list(msg["keys"], "keys")
        return {"found": [e is not None for e in self._locate(keys)]}

    def _op_lookup(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Locate keys for remote READ, optionally pinning them under a ticket.

        With ``protect``, found keys are pinned until ``release`` or ticket
        expiry; a key that could not be pinned is reported missing.

        Args:
            msg: Request with ``keys``, ``ticket_id`` and bool ``protect``.

        Returns:
            ``entries`` (location or None per key) and ``md_version``.

        Raises:
            ValueError: if fields are malformed or ``ticket_id`` is in use.
        """
        keys = _require_str_list(msg["keys"], "keys")
        ticket_id = _require_str(msg["ticket_id"], "ticket_id")
        protect = msg["protect"]
        if not isinstance(protect, bool):
            raise ValueError("protect must be a bool")
        if protect and ticket_id in self._tickets:
            raise ValueError(f"ticket_id {ticket_id} is already in use")
        entries = self._locate(keys)
        if protect:
            self._protect(ticket_id, keys, entries)
        for key, entry in zip(keys, entries, strict=True):
            if entry is not None:
                self._touch(key)
        return {"entries": entries, "md_version": self._md_version}

    def _op_release(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Unpin the keys of a read ticket.

        The ticket is forgotten only after the unpin returns; if it raises,
        the error reaches the client and the ticket stays for a retry or for
        the expiry sweep.

        Args:
            msg: Request with ``ticket_id``.

        Returns:
            ``released``: number of keys unpinned (0 for an unknown ticket).
        """
        ticket_id = _require_str(msg["ticket_id"], "ticket_id")
        tk = self._tickets.get(ticket_id)
        if tk is None:
            return {"released": 0}
        self._server.batch_unpin(tk.keys)
        del self._tickets[ticket_id]
        return {"released": len(tk.keys)}

    def _op_stats(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Report access counters.

        Args:
            msg: Request (no fields used).

        Returns:
            Reservation, quarantine, ticket, region and key counts, page
            usage, ``md_version`` and the page size.
        """
        return {
            "reservations": len(self._reservations),
            "quarantined": len(self._quarantine),
            "tickets": len(self._tickets),
            "regions": len(self._regions),
            "md_version": self._md_version,
            "page_bytes": self._page_bytes,
            "used_pages": self._used_pages(),
            "capacity_pages": self._capacity_pages,
            "lru_keys": len(self._lru),
            "evicted": self._evicted,
        }

    def _op_evicted_since(self, msg: dict[str, Any]) -> dict[str, Any]:
        """List the keys evicted after eviction number ``since``.

        Args:
            msg: Request with ``since``, the ``evictions`` value the client
                saw last (0 for a client that has seen none).

        Returns:
            ``keys`` evicted after ``since`` (oldest first) and ``complete``,
            False when some of them are no longer in the log.
        """
        since = msg["since"]
        if not isinstance(since, int) or isinstance(since, bool) or since < 0:
            raise ValueError("since must be a non-negative int")
        log = self._eviction_log
        oldest = log[0][0] if log else self._evicted + 1
        return {
            "keys": [k for n, k in log if n > since],
            "complete": since >= oldest - 1,
        }

    def _op_ping(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Answer a liveness probe.

        Args:
            msg: Request (no fields used).

        Returns:
            ``time``: the access clock value.
        """
        return {"time": self._clock()}

    # ---- helpers (all called under self._lock) ------------------------------

    def _error(self, message: str) -> dict[str, Any]:
        """Build a failure reply."""
        return {"ok": False, "generation": self._generation, "error": message}

    def _present_status(self, key: str, found: dict[str, Any]) -> str:
        """Status of a key that is already in the ledger.

        Args:
            key: The key.
            found: Where it is (``region_id``, ``kv_offset``, ``kv_length``).

        Returns:
            ``ALREADY_PRESENT`` if a remote client can read it, else
            ``REJECTED`` (a local client holds it in its own region).
        """
        if found["region_id"] in self._regions:
            self._touch(key)
            return ALREADY_PRESENT
        return REJECTED

    def _protect(
        self,
        ticket_id: str,
        keys: list[str],
        entries: list[dict[str, Any] | None],
    ) -> None:
        """Pin found keys under ``ticket_id``; blank entries that failed to pin.

        Args:
            ticket_id: Read ticket name chosen by the client.
            keys: Requested keys, parallel to ``entries``.
            entries: Lookup entries; modified in place for unpinned keys.
        """
        found = [i for i, e in enumerate(entries) if e is not None]
        if not found:
            return
        located = self._server.batch_pin_lookup(
            [keys[i] for i in found], set(self._regions)
        )
        pinned: list[str] = []
        for i, loc in zip(found, located, strict=True):
            if loc is None:
                entries[i] = None  # gone between lookup and pin
                continue
            pinned.append(keys[i])
            entries[i] = self._entry(loc)  # the pinned location
        if pinned:
            deadline = self._clock() + self._ticket_ttl_s
            self._tickets[ticket_id] = _Ticket(pinned, deadline)

    def _touch(self, key: str) -> None:
        """Mark ``key`` most recently used."""
        self._lru[key] = None
        self._lru.move_to_end(key)

    def _used_pages(self) -> int:
        """Pages in use in the remote regions."""
        return int(self._owned.get_stats()["total_allocated_pages"])

    def _free_page(self, page: _Page) -> None:
        """Return one page to its region's allocator."""
        self._owned.free(page.region_id, page.index)

    def _add_region(self) -> bool:
        """Allocate, map and register one more remote region.

        Returns:
            True if a region was added; False if the device refused it.
        """
        handle = self._server.request_alloc(
            self._owner, self._pool_size, legacy_visible=False, managed=True
        )
        if handle is None:
            return False
        rid = handle.region_id
        length = (handle.length // self._page_bytes) * self._page_bytes
        registration = None
        try:
            # No prefault: it holds the GIL for the whole region, which would
            # stall the RPC thread. NIC registration faults the pages in.
            self._mapper.map_region(handle, prefault=False)
            view = self._mapper.get_buffer_view(rid, 0, length)
            if view is None:
                raise RuntimeError(f"region {rid} is not mapped")
            base = buffer_address(view)
            registration = self._transport.register(base, length, f"region-{rid}")
            self._owned.add_region(handle)  # already mapped: reused, no prefault
        except Exception:
            logger.exception("adding remote region %d failed", rid)
            self._undo_region(rid, registration)
            raise
        self._regions[rid] = _Region(rid, base, length, registration)
        self._md_version += 1
        logger.info(
            "remote region %d added (%d bytes, %d regions)",
            rid,
            length,
            len(self._regions),
        )
        return True

    def _undo_region(self, region_id: int, registration: Any) -> None:
        """Release a region whose addition failed part way; never raises."""
        steps = [
            ("deregister", lambda: self._transport.deregister(registration)),
            ("unmap", lambda: self._mapper.unmap_region(region_id)),
            (
                "return",
                lambda: self._server.return_alloc(self._owner, region_id, managed=True),
            ),
        ]
        if registration is None:
            steps = steps[1:]
        for name, step in steps:
            try:
                step()
            except Exception:
                logger.exception("undoing region %d: %s failed", region_id, name)
                # A failed deregistration must not be followed by unmapping.
                self._failed_regions.add(region_id)
                _RETAINED_ACCESSES.add(self)
                break

    def _evict(self, n: int) -> int:
        """Delete ``n`` least recently used unpinned keys, or change nothing.

        Selection and deletion share the ledger's pin lock, so local readers
        cannot invalidate the capacity check. Returns n or zero.
        """
        if not self._evict_enabled or n <= 0:
            return 0
        locations = [
            (key, self._pages[key].region_id, self._pages[key].index * self._page_bytes)
            for key in self._lru
            if key in self._pages
        ]
        reclaimed = self._server.evict_kv_at(locations, n)
        for key, deleted in reclaimed:
            self._lru.pop(key, None)
            page = self._pages.pop(key)
            self._free_page(page)
            if deleted:
                self._evicted += 1
                self._eviction_log.append((self._evicted, key))
        freed = len(reclaimed)
        if freed:
            logger.info("evicted %d least recently used keys", freed)
        return freed

    def _locate(self, keys: list[str]) -> list[dict[str, Any] | None]:
        """Remote locations of ``keys``; None for missing or unreadable keys.

        A key is readable only inside a remote region (registered with NIXL);
        keys local clients keep in their own regions are reported missing.
        """
        entries: list[dict[str, Any] | None] = []
        for found in self._server.batch_lookup_kv(keys):
            region = (
                self._regions.get(found["handle"].region_id)
                if found is not None
                else None
            )
            if found is None or region is None:
                entries.append(None)
                continue
            entries.append(
                self._entry(
                    {
                        "region_id": region.region_id,
                        "kv_offset": found["kv_offset"],
                        "kv_length": found["kv_length"],
                    }
                )
            )
        return entries

    def _entry(self, loc: dict[str, Any]) -> dict[str, Any]:
        """Wire location of a key in a remote region."""
        region = self._regions[loc["region_id"]]
        return {
            "region_id": region.region_id,
            "base": region.base,
            "offset": loc["kv_offset"],
            "length": loc["kv_length"],
        }

    def _delete_at(self, key: str, page: _Page) -> DeleteResult:
        """Delete ``key`` from the ledger only if it still names ``page``."""
        return self._server.delete_kv_at(
            key, page.region_id, page.index * self._page_bytes
        )

    def _region_list(self) -> list[dict[str, int]]:
        """Return the registered regions as wire dicts, ordered by region id."""
        return [
            {"region_id": r.region_id, "base": r.base, "length": r.length}
            for r in sorted(self._regions.values(), key=lambda r: r.region_id)
        ]


def serve_remote(
    access: RemoteAccess,
    bind_url: str,
    *,
    stop_event: threading.Event,
    ready: threading.Event | None = None,
    sweep_interval_s: float = 1.0,
) -> None:
    """Answer the remote endpoint on a ZMQ REP socket until ``stop_event`` is set.

    Runs on the calling thread and calls :meth:`RemoteAccess.sweep` between
    polls at most once per ``sweep_interval_s``.

    Args:
        access: The remote access that answers each request.
        bind_url: ZMQ endpoint to bind (e.g. ``"tcp://0.0.0.0:6600"``).
        stop_event: Set it to stop the loop.
        ready: Set once the socket is bound.
        sweep_interval_s: Poll timeout and sweep period in seconds.
    """
    # Socket.close() only schedules libzmq's cleanup. Own this context so
    # termination waits for the listening port to be released before a
    # same-process restart, without terminating other endpoints or clients.
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    try:
        sock.bind(bind_url)
        if ready is not None:
            ready.set()
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        last_sweep = time.monotonic()
        while not stop_event.is_set():
            if poller.poll(timeout=int(sweep_interval_s * 1000)):
                raw = sock.recv()
                try:
                    reply = access.handle(protocol.decode(raw))
                except ValueError as exc:
                    reply = {
                        "ok": False,
                        "generation": access.generation,
                        "error": str(exc),
                    }
                try:
                    data = protocol.encode("reply", **reply)
                except Exception as exc:  # one bad reply must not stop the server
                    logger.exception("remote reply could not be encoded")
                    data = protocol.encode(
                        "reply",
                        ok=False,
                        generation=access.generation,
                        error=f"reply encoding failed: {exc}",
                    )
                sock.send(data)
            if time.monotonic() - last_sweep >= sweep_interval_s:
                try:
                    access.sweep()
                except Exception:  # the endpoint must outlive a bad sweep
                    logger.exception("remote sweep failed")
                last_sweep = time.monotonic()
    finally:
        sock.close(linger=0)
        ctx.term()
