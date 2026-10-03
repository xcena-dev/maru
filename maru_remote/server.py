# SPDX-License-Identifier: Apache-2.0
"""Pool-node server: Maru-backed page reservation, publish, lookup and read
protection for remote handlers, plus the ZMQ REP loop that serves them.

The server is an ordinary Maru client on the pool node: its ``MaruHandler``
uses the CXL backend, so allocation, key registration and pinning keep a
single ledger in the node's MaruServer. The server only adds what a remote
handler needs: NIXL registration of the mapped regions, reservations for
remote WRITE, read tickets for remote READ, and quarantine for pages a
timed-out WRITE may still reach.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

import zmq

from maru_handler import MaruHandler
from maru_handler.memory.types import AllocHandle, MemoryInfo

from . import protocol
from .kv_manager import KVManager
from .stager import Stager
from .transport import NixlTransport, buffer_address
from .write_buffer import WriteBuffer

logger = logging.getLogger(__name__)

CREATED = "CREATED"
ALREADY_PRESENT = "ALREADY_PRESENT"
REJECTED = "REJECTED"
POOL_FULL = "POOL_FULL"
WRITE_BUSY = "WRITE_BUSY"


class PoolFullError(ValueError):
    """The pool cannot allocate the requested pages (reported as code POOL_FULL)."""


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


class WriteBusyError(Exception):
    """No loaded page is free for a store (reported as code WRITE_BUSY)."""


@dataclass
class _Reservation:
    handle: AllocHandle
    region_id: int
    deadline: float


@dataclass
class _Ticket:
    keys: list[str]
    deadline: float


class RemoteServer:
    """Serve reservation, publish, lookup and protection over a Maru handler.

    The handler owns the CXL regions (mmap + allocator + metadata RPC). This
    class registers each mapped region with NIXL once, hands out page
    reservations for remote WRITE, publishes them as Maru keys, and pins keys
    for the duration of a remote READ ("read ticket"). Every reply carries the
    server's ``generation`` so a client notices a restart on its next call.
    """

    def __init__(
        self,
        handler: MaruHandler,
        transport: NixlTransport,
        *,
        pool_id: str,
        reservation_ttl_s: float = 60.0,
        ticket_ttl_s: float = 120.0,
        quarantine_ttl_s: float = 600.0,
        capacity_bytes: int | None = None,
        evict: bool = True,
        eviction_log_len: int = 65536,
        stager: Stager | None = None,
        kv_manager: KVManager | None = None,
        write_buffer: WriteBuffer | None = None,
        page_loader: Any = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create the server and register the handler's mapped regions.

        Args:
            handler: A connected CXL-backend Maru handler that owns the pool.
            transport: NIXL transport used to expose the regions.
            pool_id: Name reported to clients in ``hello``.
            reservation_ttl_s: Lifetime of an unpublished page reservation.
            ticket_ttl_s: Lifetime of a read ticket (pinned keys).
            quarantine_ttl_s: How long pages of a timed-out WRITE stay out of
                circulation when their client never abandons them (it died).
                Must exceed any time the NIC could still deliver that WRITE.
            capacity_bytes: Most pool bytes this server may hold in pages
                (reserved, published or quarantined); None uses the device
                until allocation fails.
            evict: When a reservation finds the pool full, delete the least
                recently read published keys (never pinned ones) to make
                room. Otherwise the reservation fails with ``POOL_FULL``.
            eviction_log_len: How many of the latest evicted keys to keep for
                ``evicted_since``; a client further behind forgets everything.
            stager: Loads looked-up objects into the device DRAM ahead of
                their reads (pools on an SSD-backed CXL device); None
                disables staging.
            kv_manager: Holds each request's next objects in device DRAM
                and lets a worker read them only once they are there (reads
                that ask with ``wait_staged``); None disables it.
            write_buffer: Free pages loaded into device DRAM ahead of stores;
                a full-page store gets only such a page, or is refused with
                ``WRITE_BUSY`` (the worker skips it). Needs ``page_loader``.
            page_loader: Loads and holds a page in device DRAM: an object
                with ``pin(address, size, done)`` and ``unpin(address, size)``.
            clock: Monotonic time source (tests inject a fake).
        """
        self._handler = handler
        self._capacity_pages = (
            None
            if capacity_bytes is None
            else capacity_bytes // handler.get_chunk_size()
        )
        self._evict_enabled = evict
        # Keys published through this server, least recently read first.
        self._lru: OrderedDict[str, None] = OrderedDict()
        self._evicted = 0
        # (eviction number, key) of the latest evictions, oldest first.
        self._eviction_log: deque[tuple[int, str]] = deque(maxlen=eviction_log_len)
        self._transport = transport
        self._pool_id = pool_id
        self._generation = uuid.uuid4().hex
        self._reservation_ttl_s = reservation_ttl_s
        self._ticket_ttl_s = ticket_ttl_s
        self._quarantine_ttl_s = quarantine_ttl_s
        self._clock = clock
        self._page_bytes = handler.get_chunk_size()
        self._regions: dict[int, _Region] = {}
        self._region_device_offset: dict[int, int] = {}
        self._stager = stager
        self._kv = kv_manager
        if write_buffer is not None and page_loader is None:
            raise ValueError("write_buffer needs page_loader")
        self._wbuf = write_buffer
        self._page_loader = page_loader
        self._refill_stop = threading.Event()
        self._refiller: threading.Thread | None = None
        self._warned_no_device_offset = False
        self._stage_failures = 0
        self._stage_warn_after = 0.0
        self._md_version = 0
        self._reservations: dict[str, _Reservation] = {}
        self._quarantine: dict[str, _Reservation] = {}
        self._tickets: dict[str, _Ticket] = {}
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
        self._sync_regions()
        self._report_foreign_regions()
        if self._wbuf is not None:
            self._refiller = threading.Thread(
                target=self._refill_loop, name="maru-remote-write-buffer", daemon=True
            )
            self._refiller.start()

    @property
    def generation(self) -> str:
        """Identifier created at start-up; changes when the server restarts."""
        return self._generation

    def handle(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Dispatch one decoded request; never raises.

        Args:
            msg: Decoded request map with an ``op`` field.

        Returns:
            ``{"ok": True, "generation": ..., "evictions": ..., ...fields}``
            on success, or ``{"ok": False, "generation": ..., "error": str}``
            on failure. ``evictions`` counts the keys evicted in this server
            run, so a client can tell that keys it remembers may be gone.
        """
        name = msg.get("op")
        op = self._ops.get(name) if isinstance(name, str) else None
        expected = msg.get("generation")
        if expected is not None and expected != self._generation:
            # The client still holds state (tickets, NIXL peer) of an earlier
            # server run. Refuse before executing so its retry is safe.
            return {
                "ok": False,
                "generation": self._generation,
                "error": "client is connected to an earlier server run",
            }
        if op is None:
            return {
                "ok": False,
                "generation": self._generation,
                "error": f"unknown op {name!r}",
            }
        try:
            with self._lock:
                reply = op(msg)
                self._refill_write_buffer()
                return {
                    "ok": True,
                    "generation": self._generation,
                    "evictions": self._evicted,
                    **reply,
                }
        except WriteBusyError as exc:
            return {
                "ok": False,
                "generation": self._generation,
                "code": WRITE_BUSY,
                "error": f"write busy: {exc}",
            }
        except PoolFullError as exc:
            return {
                "ok": False,
                "generation": self._generation,
                "code": POOL_FULL,
                "error": f"pool full: {exc}",
            }
        except (KeyError, TypeError, ValueError) as exc:
            return {
                "ok": False,
                "generation": self._generation,
                "error": f"{type(exc).__name__}: {exc}",
            }
        except Exception as exc:  # keep the server alive; the client sees the error
            logger.exception("remote op %s failed", name)
            return {
                "ok": False,
                "generation": self._generation,
                "error": f"{type(exc).__name__}: {exc}",
            }

    def sweep(self) -> None:
        """Reclaim expired reservations, quarantined pages and read tickets.

        Quarantined pages return only after ``quarantine_ttl_s`` (their client
        normally abandons them sooner). Never raises. An entry whose
        free/unpin fails (e.g. a metadata-server timeout) is logged and kept,
        so the next sweep retries it.
        """
        now = self._clock()
        with self._lock:
            self._refill_write_buffer()
            for ticket, res in list(self._quarantine.items()):
                if res.deadline > now:
                    continue
                try:
                    self._handler.free(res.handle)
                except Exception:
                    logger.exception(
                        "sweep: freeing quarantined page %s failed", ticket
                    )
                    continue
                logger.warning(
                    "sweep: quarantined page %s was never abandoned; freed", ticket
                )
                del self._quarantine[ticket]
            for ticket, res in list(self._reservations.items()):
                if res.deadline > now:
                    continue
                try:
                    self._handler.free(res.handle)
                except Exception:
                    logger.exception("sweep: freeing reservation %s failed", ticket)
                    continue
                del self._reservations[ticket]
            for ticket_id, tk in list(self._tickets.items()):
                if tk.deadline > now:
                    continue
                try:
                    self._handler.batch_unpin(tk.keys)
                except Exception:
                    logger.exception("sweep: unpinning ticket %s failed", ticket_id)
                    continue
                del self._tickets[ticket_id]
                if self._kv is not None:
                    self._stage(self._kv.on_consumed, tk.keys)

    def close(self) -> None:
        """Release everything this server holds (not the handler itself).

        Quarantined pages are freed too: closing the server tears down its
        NIXL agent, so no remote WRITE can reach them afterwards.
        """
        with self._lock:
            for table in (self._reservations, self._quarantine):
                for ticket, res in list(table.items()):
                    try:
                        self._handler.free(res.handle)
                    except Exception:
                        logger.exception("close: freeing reservation %s failed", ticket)
                        continue
                    del table[ticket]
            for ticket_id, tk in list(self._tickets.items()):
                try:
                    self._handler.batch_unpin(tk.keys)
                except Exception:
                    logger.exception("close: unpinning ticket %s failed", ticket_id)
                    continue
                del self._tickets[ticket_id]
        if self._kv is not None:
            self._kv.close()
        if self._wbuf is not None:
            self._refill_stop.set()
            self._wbuf.changed.set()
            if self._refiller is not None:
                self._refiller.join(timeout=5.0)
            deadline = self._clock() + 5.0  # let page loads in flight finish
            while self._wbuf.stats()["loading"] and self._clock() < deadline:
                time.sleep(0.01)
            with self._lock:
                for handle, rng in self._wbuf.close() + self._wbuf.drain_orphans():
                    if rng is not None:
                        self._page_loader.unpin(*rng)
                    self._free_quietly(handle)
        self._transport.close()

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
            "staged_reads": self._kv is not None,
            **self._op_metadata(msg),
        }

    def _op_metadata(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Return current NIXL metadata and the registered regions.

        Args:
            msg: Request (no fields used).

        Returns:
            ``nixl_md``, ``md_version`` and ``regions``.
        """
        self._sync_regions()
        return {
            "nixl_md": self._transport.metadata(),
            "md_version": self._md_version,
            "regions": self._region_list(),
        }

    def _op_reserve(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Allocate one page per requested size for remote WRITE.

        All pages of one request succeed together; on failure the pages
        already allocated for it are freed before the error propagates. The
        page records the requested size, so a later lookup reports exactly
        the bytes written.

        Args:
            msg: Request with ``client_id`` and ``sizes`` (positive ints, each
                at most ``page_bytes``).

        Returns:
            ``pages`` (ticket, region, base, offset, length) and ``md_version``.

        Raises:
            ValueError: if ``sizes`` is malformed or the pool is full.
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
        deadline = self._clock() + self._reservation_ttl_s
        if (
            self._wbuf is not None
            and len(sizes) <= self._wbuf.capacity
            and all(n == self._page_bytes for n in sizes)
        ):
            loaded = self._wbuf.take(len(sizes))
            if loaded is None:
                raise WriteBusyError("no loaded page is free; skip this store")
            for _, rng in loaded:
                if rng is not None:  # loaded and about to be written: let it go
                    self._page_loader.unpin(*rng)
            handles = [h for h, _ in loaded]
            try:
                return self._reserve_pages(handles, sizes, deadline)
            except Exception:
                for handle in handles:
                    self._free_quietly(handle)
                raise
        allocated: list[AllocHandle] = []
        reserved: dict[str, _Reservation] = {}
        pages: list[dict[str, Any]] = []
        if self._capacity_pages is not None:
            over = self._used_pages() + len(sizes) - self._capacity_pages
            if over > 0 and self._evict(over) < over:
                raise PoolFullError(
                    f"capacity of {self._capacity_pages} pages reached and "
                    "nothing more can be evicted"
                )
        try:
            for size in sizes:
                try:
                    handle = self._alloc_page(size)
                except ValueError as exc:
                    # Only exhaustion pauses the client's stores; any other
                    # allocation fault is reported as a plain error.
                    if str(exc).startswith("Cannot allocate page"):
                        raise PoolFullError(str(exc)) from exc
                    raise
                allocated.append(handle)
                addr = buffer_address(handle.buf)
                region = self._region_for_address(addr)
                ticket = uuid.uuid4().hex
                reserved[ticket] = _Reservation(handle, region.region_id, deadline)
                pages.append(
                    {
                        "ticket": ticket,
                        "region_id": region.region_id,
                        "base": region.base,
                        "offset": addr - region.base,
                        "length": size,
                    }
                )
        except Exception:
            for handle in allocated:
                self._handler.free(handle)
            raise
        self._reservations.update(reserved)
        return {"pages": pages, "md_version": self._md_version}

    def _op_publish(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Register reserved pages as Maru keys.

        Every ticket is consumed. A key that already existed keeps its first
        value and this page is freed (``ALREADY_PRESENT``); the remote
        handler reports both outcomes as "the key is in the pool".

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
        handles: list[AllocHandle] = []
        for ticket in tickets:
            res = self._reservations.get(ticket)
            if res is None:
                raise KeyError(f"unknown or expired ticket {ticket}")
            handles.append(res.handle)
        registered = self._handler.batch_exists(keys)
        readable = [e is not None for e in self._locate(keys)]
        existed = [r and ok for r, ok in zip(registered, readable, strict=True)]
        stale = [
            k
            for k, r, ok in zip(keys, registered, readable, strict=True)
            if r and not ok
        ]
        stuck: set[str] = set()
        if stale:
            # Keys from an earlier server run whose regions this server never
            # registered: nobody can read them remotely, so replace them. A
            # key still pinned by that run cannot be deleted and stays unusable.
            logger.warning(
                "replacing %d unreadable keys from an earlier run", len(stale)
            )
            for key in stale:
                if not self._handler.delete(key):
                    stuck.add(key)
        for ticket in tickets:
            del self._reservations[ticket]
        keep = [i for i, k in enumerate(keys) if k not in stuck]
        for i, k in enumerate(keys):
            if k in stuck:
                self._handler.free(handles[i])
        stored_kept = self._handler.batch_store(
            [keys[i] for i in keep], [handles[i] for i in keep]
        )  # takes ownership
        stored = dict(zip(keep, stored_kept, strict=True))
        statuses = []
        for i, (key, was) in enumerate(zip(keys, existed, strict=True)):
            if key in stuck:
                statuses.append(REJECTED)
            elif was:
                statuses.append(ALREADY_PRESENT)
                self._touch(key)
            elif stored[i]:
                statuses.append(CREATED)
                self._touch(key)
            else:
                # Another writer may have registered the key between the
                # existence check and the register RPC.
                statuses.append(
                    ALREADY_PRESENT if self._handler.exists(key) else REJECTED
                )
        return {"statuses": statuses}

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
            self._handler.free(res.handle)
            freed += 1
        return {"freed": freed}

    def _op_quarantine(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Keep reserved pages out of circulation until the client abandons them.

        A client sends this after a WRITE missed its deadline: the NIC may
        still deliver bytes into these pages, so they must not be reallocated
        when their reservation would otherwise expire.

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
            self._quarantine[ticket] = _Reservation(res.handle, res.region_id, deadline)
            moved += 1
        return {"quarantined": moved}

    def _op_exists(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Report which keys a remote client can read.

        A key registered in a region this server has not registered with NIXL
        (left by an earlier server run) is reported missing.

        With ``stage`` (sent by the vLLM scheduler's lookup), the stager
        starts loading the found keys into the device DRAM ahead of the
        worker's reads.

        Args:
            msg: Request with ``keys``: list of str, optional bool ``stage``.

        Returns:
            ``found``: one bool per key.
        """
        keys = _require_str_list(msg["keys"], "keys")
        entries = self._locate(keys)
        if msg.get("stage") is True:
            hooks = [h.on_lookup for h in (self._stager, self._kv) if h is not None]
            if hooks:
                ranges = [self._device_range(e) for e in entries]
                for hook in hooks:
                    self._stage(hook, keys, ranges)
        return {"found": [e is not None for e in entries]}

    def _op_lookup(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Locate keys for remote READ, optionally pinning them under a ticket.

        With ``protect``, found keys are pinned until ``release`` or ticket
        expiry; a key that could not be pinned is reported as missing.

        With ``wait_staged`` (and a KV Manager), the keys are located only
        once every found key sits in device DRAM; until then the reply is
        ``pending`` and nothing is pinned, so the worker asks again.

        Args:
            msg: Request with ``keys``, ``ticket_id``, bool ``protect`` and
                optional bool ``wait_staged``.

        Returns:
            ``entries`` (location or None per key) and ``md_version``, or
            ``pending`` and ``md_version`` while staging is still running.

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
        if self._kv is not None and msg.get("wait_staged") is True:
            ranges = [self._device_range(e) for e in entries]
            try:
                ready = self._kv.ready(keys, ranges)
            except Exception:  # staging must never fail the read itself
                self._stage_failed()
                ready = True
            if not ready:
                return {"pending": True, "md_version": self._md_version}
        if protect:
            self._protect(ticket_id, keys, entries)
        for key, entry in zip(keys, entries, strict=True):
            if entry is not None:
                self._touch(key)
        if self._stager is not None:
            self._stage(
                self._stager.on_read,
                [k for k, e in zip(keys, entries, strict=True) if e],
            )
        return {"entries": entries, "md_version": self._md_version}

    def _op_release(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Unpin the keys of a read ticket.

        The ticket is forgotten only after the unpin succeeds; if the unpin
        fails the error reaches the client and the ticket stays for a retry
        or for the expiry sweep.

        Args:
            msg: Request with ``ticket_id``.

        Returns:
            ``released``: number of keys unpinned (0 for an unknown ticket).
        """
        ticket_id = _require_str(msg["ticket_id"], "ticket_id")
        tk = self._tickets.get(ticket_id)
        if tk is None:
            return {"released": 0}
        self._handler.batch_unpin(tk.keys)
        del self._tickets[ticket_id]
        if self._kv is not None:
            self._stage(self._kv.on_consumed, tk.keys)
        return {"released": len(tk.keys)}

    def _op_stats(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Report server counters.

        Args:
            msg: Request (no fields used).

        Returns:
            Reservation, quarantine, ticket and region counts, ``md_version``
            and the page size.
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
            "stager": (
                {**self._stager.stats(), "failures": self._stage_failures}
                if self._stager is not None
                else None
            ),
            "write_buffer": self._wbuf.stats() if self._wbuf is not None else None,
            "kv_manager": (
                {**self._kv.stats(), "failures": self._stage_failures}
                if self._kv is not None
                else None
            ),
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
            ``time``: the server clock value.
        """
        return {"time": self._clock()}

    # ---- helpers (all called under self._lock) ------------------------------

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
        pinned_flags = self._handler.batch_pin([keys[i] for i in found])
        pinned: list[str] = []
        for i, ok in zip(found, pinned_flags, strict=True):
            if ok:
                pinned.append(keys[i])
            else:
                entries[i] = None
        if pinned:
            deadline = self._clock() + self._ticket_ttl_s
            self._tickets[ticket_id] = _Ticket(pinned, deadline)

    def _touch(self, key: str) -> None:
        """Mark ``key`` most recently used."""
        self._lru[key] = None
        self._lru.move_to_end(key)

    def _used_pages(self) -> int:
        """Pages this server's handler holds in its own regions."""
        stats = self._handler.owned_region_manager.get_stats()
        return int(stats["total_allocated_pages"])

    def _reserve_pages(
        self, handles: list[AllocHandle], sizes: list[int], deadline: float
    ) -> dict[str, Any]:
        """Reserve already allocated pages for one store request."""
        pages: list[dict[str, Any]] = []
        reserved: dict[str, _Reservation] = {}
        for handle, size in zip(handles, sizes, strict=True):
            addr = buffer_address(handle.buf)
            region = self._region_for_address(addr)
            ticket = uuid.uuid4().hex
            reserved[ticket] = _Reservation(handle, region.region_id, deadline)
            pages.append(
                {
                    "ticket": ticket,
                    "region_id": region.region_id,
                    "base": region.base,
                    "offset": addr - region.base,
                    "length": size,
                }
            )
        self._reservations.update(reserved)  # all or nothing
        return {"pages": pages, "md_version": self._md_version}

    def _refill_loop(self) -> None:
        """Keep the write buffer loaded: wake on each change, or every 50 ms."""
        assert self._wbuf is not None
        while not self._refill_stop.is_set():
            self._wbuf.changed.wait(timeout=0.05)
            self._wbuf.changed.clear()
            if self._refill_stop.is_set():
                return
            with self._lock:
                self._refill_write_buffer()

    def _refill_write_buffer(self) -> None:
        """Start loading free pages into device DRAM while no read is staged.

        Never raises: a refill problem must not fail the request it follows.
        """
        if self._wbuf is None:
            return
        try:
            self._refill_write_buffer_locked()
        except Exception:
            logger.exception("write-buffer refill failed")

    def _refill_write_buffer_locked(self) -> None:
        for handle, rng in self._wbuf.drain_orphans():
            if rng is not None:
                self._page_loader.unpin(*rng)
            self._free_quietly(handle)
        if self._kv is not None and self._kv.busy():
            return  # reads first: their objects are being loaded
        for _ in range(self._wbuf.want()):
            if self._capacity_pages is not None and (
                self._used_pages() >= self._capacity_pages and self._evict(1) < 1
            ):
                return
            try:
                handle = self._alloc_page(self._page_bytes)
            except ValueError:
                return  # pool full: stores are refused until reads free room
            addr = buffer_address(handle.buf)
            region = self._region_for_address(addr)
            dev = self._region_device_offset.get(region.region_id)
            if dev is None:  # not a device we can load: hand it out as is
                self._wbuf.loading(handle, None)(True)
                continue
            rng = (dev + addr - region.base, self._page_bytes)
            self._page_loader.pin(*rng, self._wbuf.loading(handle, rng))

    def _free_quietly(self, handle: AllocHandle) -> None:
        try:
            self._handler.free(handle)
        except Exception:
            logger.exception("freeing a write-buffer page failed")

    def _alloc_page(self, size: int) -> AllocHandle:
        """Allocate one page, evicting one LRU key if the allocator is full."""
        try:
            return cast(AllocHandle, self._handler.alloc(size))
        except ValueError as exc:
            if not str(exc).startswith("Cannot allocate page") or self._evict(1) < 1:
                raise
            return cast(AllocHandle, self._handler.alloc(size))

    def _evict(self, n: int) -> int:
        """Delete up to ``n`` least recently read published keys.

        Pinned keys (being read) and keys MaruServer no longer knows are
        skipped. Returns the number of pages freed.
        """
        if not self._evict_enabled or n <= 0:
            return 0
        freed = 0
        for key in list(self._lru):
            if freed >= n:
                break
            del self._lru[key]
            rng = None
            if self._kv is not None:
                rng = self._device_range(self._locate([key])[0])
            try:
                deleted = self._handler.delete(key)
            except Exception:  # keep serving; the key stays where it is
                logger.warning("eviction of %s failed", key, exc_info=True)
                deleted = False
            if deleted:
                if rng is not None:  # the page may be reused: let its pin go now
                    self._kv.forget_range(rng)  # type: ignore[union-attr]
                freed += 1
                self._evicted += 1
                self._eviction_log.append((self._evicted, key))
            elif self._handler.exists(key):
                self._lru[key] = None  # pinned: keep it, most recent end
        if freed:
            logger.info("evicted %d least recently read keys", freed)
        return freed

    def _locate(self, keys: list[str]) -> list[dict[str, Any] | None]:
        """Remote locations of ``keys``; None for missing or unreadable keys.

        A key is readable only inside a region this server registered with
        NIXL (one it owns). Keys an earlier server run left in other regions
        resolve to read-only shared mappings here and are reported missing.
        """
        infos = [
            i if isinstance(i, MemoryInfo) else None
            for i in self._handler.batch_retrieve(keys)
        ]
        if any(i is not None and i.region_id not in self._regions for i in infos):
            self._sync_regions()
        entries: list[dict[str, Any] | None] = []
        for info in infos:
            region = self._regions.get(info.region_id) if info is not None else None
            if info is None or region is None:
                entries.append(None)
                continue
            addr = buffer_address(info.view)
            entries.append(
                {
                    "region_id": region.region_id,
                    "base": region.base,
                    "offset": addr - region.base,
                    "length": info.view.nbytes,
                }
            )
        return entries

    def _stage(self, fn: Callable[..., None], *args: Any) -> None:
        """Run a stager hook; staging is a hint and never fails the request."""
        try:
            fn(*args)
        except Exception:
            self._stage_failed()

    def _stage_failed(self) -> None:
        """Count a staging failure; log it at most once a minute."""
        self._stage_failures += 1
        now = self._clock()
        if now >= self._stage_warn_after:
            self._stage_warn_after = now + 60.0
            logger.warning(
                "staging failed (%d so far); further failures are not logged for 60 s",
                self._stage_failures,
                exc_info=True,
            )

    def _device_range(self, entry: dict[str, Any] | None) -> tuple[int, int] | None:
        """Device (address, size) of a located key, or None."""
        if entry is None:
            return None
        dev = self._region_device_offset.get(entry["region_id"])
        if dev is None:
            if not self._warned_no_device_offset:
                self._warned_no_device_offset = True
                logger.warning(
                    "region %d has no device offset; its keys are not staged",
                    entry["region_id"],
                )
            return None
        return dev + entry["offset"], entry["length"]

    def _report_foreign_regions(self) -> None:
        """Warn about pool regions held by other Maru clients or an earlier run.

        Keys in those regions cannot be served over RDMA (only regions this
        server owns are registered), so they hold device capacity until
        their keys are replaced or MaruServer restarts.
        """
        try:
            stats = self._handler.get_stats()
            total = int(stats["allocation_manager"]["num_allocations"])
        except Exception:  # informational only
            logger.debug("could not read allocation stats", exc_info=True)
            return
        foreign = total - len(self._regions)
        if foreign > 0:
            logger.warning(
                "%d pool regions belong to other Maru clients or an earlier "
                "maru-remote-server run; their keys are not served remotely. Run "
                "maru-server for maru-remote-server alone and restart both "
                "together to reclaim them.",
                foreign,
            )

    def _region_list(self) -> list[dict[str, int]]:
        """Return the registered regions as wire dicts, ordered by region id."""
        return [
            {"region_id": r.region_id, "base": r.base, "length": r.length}
            for r in sorted(self._regions.values(), key=lambda r: r.region_id)
        ]

    def _sync_regions(self) -> None:
        """Register every owned region the handler has that is not yet registered."""
        page = self._handler.get_chunk_size()
        changed = False
        for region_id in self._handler.get_owned_region_ids():
            if region_id in self._regions:
                continue
            pages = self._handler.get_region_page_count(region_id)
            if not pages:
                continue
            size = pages * page
            view = self._handler.get_buffer_view(region_id, 0, size)
            if view is None:  # unmapped between listing and lookup
                continue
            base = buffer_address(view)
            reg = self._transport.register(base, size, f"region-{region_id}")
            self._regions[region_id] = _Region(region_id, base, size, reg)
            dev = self._handler.get_region_device_offset(region_id)
            if dev is not None:
                self._region_device_offset[region_id] = dev
            changed = True
        if changed:
            self._md_version += 1

    def _region_for_address(self, addr: int) -> _Region:
        """Find the registered region containing ``addr``, syncing once if needed.

        Args:
            addr: A host address inside a mapped region.

        Returns:
            The containing region.

        Raises:
            RuntimeError: if no registered region contains ``addr``.
        """
        for attempt in range(2):
            for r in self._regions.values():
                if r.base <= addr < r.base + r.length:
                    return r
            if attempt == 0:
                self._sync_regions()
        raise RuntimeError(f"address {addr:#x} is not inside a registered region")


def serve_forever(
    server: RemoteServer,
    ctrl_url: str,
    *,
    stop_event: threading.Event,
    sweep_interval_s: float = 1.0,
) -> None:
    """Answer requests on a ZMQ REP socket until ``stop_event`` is set.

    Runs on the calling thread and calls :meth:`RemoteServer.sweep` between
    polls at most once per ``sweep_interval_s``.

    Args:
        server: The server that answers each request.
        ctrl_url: ZMQ endpoint to bind (e.g. ``"tcp://0.0.0.0:6600"``).
        stop_event: Set it to stop the loop.
        sweep_interval_s: Poll timeout and sweep period in seconds.
    """
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.REP)
    sock.bind(ctrl_url)
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)
    last_sweep = time.monotonic()
    try:
        while not stop_event.is_set():
            if poller.poll(timeout=int(sweep_interval_s * 1000)):
                raw = sock.recv()
                try:
                    reply = server.handle(protocol.decode(raw))
                except ValueError as exc:
                    reply = {
                        "ok": False,
                        "generation": server.generation,
                        "error": str(exc),
                    }
                sock.send(protocol.encode("reply", **reply))
            if time.monotonic() - last_sweep >= sweep_interval_s:
                try:
                    server.sweep()
                except Exception:  # the control channel must outlive a bad sweep
                    logger.exception("remote sweep failed")
                last_sweep = time.monotonic()
    finally:
        sock.close(linger=0)
