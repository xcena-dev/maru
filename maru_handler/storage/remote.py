# SPDX-License-Identifier: Apache-2.0
"""Remote CXL storage backend: KV bytes live in another node's CXL pool.

The handler talks to the pool node's ``maru-remote-server`` over ZMQ and moves
bytes with NIXL between a local staging buffer and the pool's registered
regions. Callers see the CPU backend's contract:

- ``alloc`` returns a writable staging slot;
- ``batch_store`` takes ownership of the slots, reserves pool pages, RDMA
  WRITEs the slots into them and publishes the keys;
- ``batch_retrieve`` pins the keys on the server, RDMA READs them into staging
  slots, unpins them, and returns read leases over the slots that the caller
  releases after its last copy.

The staging buffer is anonymous memory registered with NIXL only. CUDA treats
it as pageable, so a GPU copy has finished reading or writing it when the copy
call returns; releasing a lease right after the copy is safe.
"""

from __future__ import annotations

import functools
import logging
import mmap
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from maru_common.storage_types import StorageError, StorageUnavailableError

logger = logging.getLogger(__name__)

_CREATED = "CREATED"
_ALREADY_PRESENT = "ALREADY_PRESENT"


def _default_transport(agent_name: str, ucx_device: str) -> Any:
    """Create the NIXL transport (imports NIXL lazily)."""
    from maru_remote.transport import NixlTransport

    return NixlTransport(agent_name, ucx_device=ucx_device)


def _default_client(
    url: str, transport: Any, *, client_id: str, timeout_ms: int
) -> Any:
    """Create the control-channel client."""
    from maru_remote.client import RemoteClient

    return RemoteClient(url, transport, client_id=client_id, timeout_ms=timeout_ms)


class _Staging:
    """Fixed-size slots carved from one anonymous mapping."""

    def __init__(self, capacity: int, slot_bytes: int):
        count = capacity // slot_bytes
        if count < 1:
            raise ValueError("remote staging capacity must hold at least one slot")
        self.slot_bytes = slot_bytes
        self.count = count
        self.nbytes = count * slot_bytes
        self._mapping = mmap.mmap(-1, self.nbytes)
        self._view = memoryview(self._mapping)
        from maru_remote.transport import buffer_address

        self.address = buffer_address(self._view)
        self._free: deque[int] = deque(range(count))

    def take(self, n: int = 1) -> list[int] | None:
        """Take ``n`` free slots, or None (and take nothing) if fewer remain."""
        if len(self._free) < n:
            return None
        return [self._free.popleft() for _ in range(n)]

    def give(self, slot: int) -> None:
        """Return one slot."""
        self._free.append(slot)

    def free_count(self) -> int:
        """Number of free slots."""
        return len(self._free)

    def view(self, slot: int) -> memoryview:
        """Writable view of one whole slot."""
        start = slot * self.slot_bytes
        return self._view[start : start + self.slot_bytes]

    def addr(self, slot: int) -> int:
        """Address of the first byte of a slot."""
        return self.address + slot * self.slot_bytes

    def close(self) -> None:
        """Unmap the buffer unless a caller still holds an exported view."""
        try:
            self._view.release()
            self._mapping.close()
        except BufferError:
            logger.warning("remote staging buffer still exported; leaving it mapped")


@dataclass(eq=False)
class RemoteAllocation:
    """A staging slot handed out by ``alloc``; ``buf`` is written by the caller."""

    buf: memoryview
    slot: int
    size: int
    state: str = "writing"  # writing -> submitted (batch_store) -> freed


@dataclass(eq=False)
class RemoteReadLease:
    """Staging bytes of one retrieved key, valid until :meth:`release`.

    Release after the final read or copy (or use a context manager); GC is not
    the synchronization mechanism.
    """

    view: memoryview
    key: str
    _release: Callable[[], None]
    _released: bool = False

    def release(self) -> None:
        """Return the staging slot; idempotent."""
        if self._released:
            return
        self._released = True
        try:
            self.view.release()
        except BufferError:  # a caller-held tensor still aliases the view
            logger.debug("remote lease view of %s still exported at release", self.key)
        self._release()

    def __enter__(self) -> RemoteReadLease:
        return self

    def __exit__(self, *_: Any) -> None:
        self.release()


@dataclass
class _Quarantined:
    """Buffers a timed-out transfer may still touch."""

    pending: Any
    slots: list[int]
    tickets: list[str] = field(default_factory=list)  # WRITE pages to abandon


class RemoteStorageClient:
    """MaruHandler storage backend for a CXL pool on another node."""

    def __init__(
        self,
        config: Any,
        *,
        transport_factory: Callable[[str, str], Any] | None = None,
        client_factory: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        """Create the backend; nothing is contacted until :meth:`connect`.

        Args:
            config: The handler's ``MaruConfig`` (``storage_backend="remote"``).
            transport_factory: ``(agent_name, ucx_device) -> transport``;
                tests inject a fake NIXL transport.
            client_factory: ``(url, transport, client_id=, timeout_ms=) ->
                client``; tests inject a client bound to a fake server.
            clock: Monotonic time source.
        """
        self.config = config
        self.connected = False
        self._lock = threading.RLock()
        self._clock = clock
        self._transport_factory = transport_factory or _default_transport
        self._client_factory = client_factory or _default_client
        self._namespace = config.cache_namespace
        self._client: Any = None
        self._transport: Any = None
        self._staging: _Staging | None = None
        self._stored: set[str] = set()
        self._retry_at = 0.0
        self._reconnect = False
        self._quarantine: list[_Quarantined] = []
        self._quarantined_slots: set[int] = set()
        self._reads = 0
        self._closed = False
        self.counters = {
            "stores": 0,
            "store_bytes": 0,
            "stores_failed": 0,
            "loads": 0,
            "load_bytes": 0,
            "loads_failed": 0,
            "quarantined_transfers": 0,
            "write_seconds": 0.0,
            "read_seconds": 0.0,
        }

    # ---- lifecycle ------------------------------------------------------------

    def connect(self) -> bool:
        """Connect to the remote server.

        Returns:
            True on success.

        Raises:
            StorageUnavailableError: if the server cannot be reached; the
                caller may retry later.
            StorageError: if the server's layout cannot serve this handler
                (page smaller than an object, protocol or lifetime mismatch)
                or NIXL is not installed.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("Create a fresh remote handler after close")
            if self.connected:
                return True
            if self.config.metadata_only:
                return self._connect_control_only()
            return self._connect_data()

    def close(self) -> None:
        """Release the staging buffer, the NIXL agent and the control socket.

        Raises:
            RuntimeError: if read leases are still outstanding.
        """
        with self._lock:
            if self._reads:
                raise RuntimeError(
                    "Release remote read leases before closing the handler"
                )
            self._closed = True
            self.connected = False
            for q in self._quarantine:
                q.pending.poll()  # releases finished handles; closing drops the rest
            self._quarantine.clear()
            if self._client is not None:
                self._client.close()
            if self._transport is not None:
                self._transport.close()
            if self._staging is not None:
                self._staging.close()
            self._client = self._transport = self._staging = None
            self._stored.clear()

    def _ensure(self, data: bool = True) -> None:
        """Raise unless the backend is connected (and has a data path).

        Args:
            data: Require the staging buffer (False for existence checks).

        Raises:
            StorageUnavailableError: if not connected.
            RuntimeError: if ``data`` is requested on a metadata-only handler.
        """
        if not self.connected or self._closed:
            raise StorageUnavailableError("remote storage is not connected")
        if data and self._staging is None:
            raise RuntimeError("A metadata-only handler cannot access remote buffers")

    # ---- handler API ------------------------------------------------------------

    def alloc(self, size: int) -> RemoteAllocation:
        """Take one staging slot for an object of ``size`` bytes.

        Raises:
            ValueError: if ``size`` does not fit in one slot.
            StorageUnavailableError: while the server is considered down.
            MemoryError: if every slot is in use.
        """
        with self._lock:
            self._ensure()
            self._reap()
            assert self._staging is not None
            if type(size) is not int or not 0 < size <= self._staging.slot_bytes:
                raise ValueError("remote allocation must fit in one staging slot")
            if self._clock() < self._retry_at:
                raise StorageUnavailableError(
                    "remote storage is unavailable; skipping store"
                )
            slots = self._staging.take(1)
            if slots is None:
                raise MemoryError(
                    "remote staging buffer is full; skipping new cache admission"
                )
            slot = slots[0]
            return RemoteAllocation(self._staging.view(slot)[:size], slot, size)

    def free(self, handle: RemoteAllocation) -> None:
        """Return an unsubmitted allocation; a submitted one is owned by batch_store."""
        with self._lock:
            if not isinstance(handle, RemoteAllocation) or handle.state != "writing":
                return
            handle.state = "freed"
            if self._staging is not None:
                self._staging.give(handle.slot)

    def has_local(self, key: str) -> bool:
        """Whether this handler stored (or found) ``key`` in the current server run."""
        with self._lock:
            return self.connected and key in self._stored

    def batch_store(
        self, keys: list[str], handles: list[RemoteAllocation]
    ) -> list[bool]:
        """Publish written staging slots under ``keys``; takes ownership of every slot.

        Returns:
            One bool per key: True if the key is in the pool afterwards (this
            call created it or it already existed), False otherwise.

        Raises:
            ValueError: on malformed input (nothing is consumed).
        """
        if len(keys) != len(handles):
            raise ValueError("keys and handles must have the same length")
        if not keys:
            return []
        with self._lock:
            self._ensure()
            if len({id(h) for h in handles}) != len(handles):
                raise ValueError(
                    "A remote allocation can only be committed once per batch"
                )
            for key, h in zip(keys, handles, strict=True):
                if not isinstance(key, str) or not key or len(key) > 4096:
                    raise ValueError("Invalid remote key")
                if not isinstance(h, RemoteAllocation) or h.state != "writing":
                    raise ValueError(
                        "Allocation is not an unsubmitted remote allocation"
                    )
            for h in handles:
                h.state = "submitted"
            try:
                results = self._store_locked(keys, handles)
            finally:
                assert self._staging is not None
                for h in handles:
                    h.state = "freed"
                    if h.slot not in self._quarantined_slots:
                        self._staging.give(h.slot)
            self.counters["stores" if all(results) else "stores_failed"] += 1
            return results

    def batch_exists(self, keys: list[str]) -> list[bool]:
        """Report which keys are in the pool (all False while the server is down)."""
        with self._lock:
            self._ensure(data=False)
            if not keys:
                return []
            if not self._ready():
                return [False] * len(keys)
            try:
                return list(self._client.exists([self._scope(k) for k in keys]))
            except Exception as exc:
                self._fail(exc, "exists")
                return [False] * len(keys)

    def batch_retrieve(self, keys: list[str]) -> list[RemoteReadLease | None]:
        """Copy the keys' bytes into staging slots and return read leases.

        Returns:
            One lease per found key, None per missing key.

        Raises:
            StorageUnavailableError: if the server or the transfer fails.
            MemoryError: if the staging buffer cannot hold every found key.
        """
        with self._lock:
            self._ensure()
            self._reap()
            if not keys:
                return []
            if not self._ready():
                raise StorageUnavailableError("remote storage is unavailable")
            return self._retrieve_locked(keys)

    def ping(self) -> bool:
        """True if the remote server answers now."""
        with self._lock:
            if not self.connected or not self._ready():
                return False
            try:
                self._client.ping()
                return True
            except Exception as exc:
                self._fail(exc, "ping")
                return False

    def stats(self) -> dict[str, Any]:
        """Local counters, staging occupancy and (if reachable) server counters."""
        with self._lock:
            out: dict[str, Any] = {
                "counters": dict(self.counters),
                "stored_keys": len(self._stored),
                "outstanding_leases": self._reads,
                "quarantined_slots": len(self._quarantined_slots),
                "available": self.connected and self._clock() >= self._retry_at,
            }
            if self._staging is not None:
                out["staging_slots"] = self._staging.count
                out["staging_free"] = self._staging.free_count()
            if self.connected and self._ready():
                try:
                    out["server"] = self._client.stats()
                except Exception as exc:
                    self._fail(exc, "stats")
            return out

    # ---- connection -------------------------------------------------------------

    def _connect_control_only(self) -> bool:
        """Connect a control-only client (scheduler-side existence checks)."""
        client = self._client_factory(
            self.config.remote_url,
            None,
            client_id=self._client_id(),
            timeout_ms=self.config.timeout_ms,
        )
        try:
            client.connect()
        except Exception as exc:
            client.close()
            raise StorageUnavailableError(
                f"remote server {self.config.remote_url} is unreachable: {exc}"
            ) from exc
        self._client = client
        self.connected = True
        return True

    def _connect_data(self) -> bool:
        """Create the staging buffer and NIXL agent, then say hello."""
        try:
            transport = self._transport_factory(
                f"maru-remote-client-{self.config.instance_id}",
                self.config.remote_ucx_device,
            )
        except ImportError as exc:
            raise StorageError(
                "remote storage needs NIXL; install it with pip install 'maru[remote]'"
            ) from exc
        staging = None
        client = None
        try:
            staging = _Staging(self.config.pool_size, self.config.chunk_size_bytes)
            transport.register(staging.address, staging.nbytes, "maru-remote-staging")
            client = self._client_factory(
                self.config.remote_url,
                transport,
                client_id=self._client_id(),
                timeout_ms=self.config.timeout_ms,
            )
            hello = client.connect()
            self._check_hello(hello)
        except StorageError:
            self._discard(client, transport, staging)
            raise
        except Exception as exc:
            self._discard(client, transport, staging)
            raise StorageUnavailableError(
                f"remote server {self.config.remote_url} is unreachable: {exc}"
            ) from exc
        self._client, self._transport, self._staging = client, transport, staging
        self.connected = True
        logger.info(
            "remote storage connected to %s (pool %s, page %d B, %d staging slots of %d B)",
            self.config.remote_url,
            hello.get("pool_id"),
            int(hello["page_bytes"]),
            staging.count,
            staging.slot_bytes,
        )
        return True

    def _check_hello(self, hello: dict[str, Any]) -> None:
        """Reject a server whose layout this handler cannot use."""
        from maru_remote.protocol import PROTOCOL_VERSION

        if hello.get("protocol") != PROTOCOL_VERSION:
            raise StorageError(
                f"remote server speaks protocol {hello.get('protocol')}, "
                f"this handler {PROTOCOL_VERSION}"
            )
        page = int(hello["page_bytes"])
        if page < self.config.chunk_size_bytes:
            raise StorageError(
                f"remote pool pages ({page} B) are smaller than one KV object "
                f"({self.config.chunk_size_bytes} B); raise --page-bytes"
            )
        ttl = float(hello.get("reservation_ttl_s", 0))
        if ttl < 2 * self.config.remote_transfer_timeout_s:
            raise StorageError(
                f"remote reservation lifetime {ttl}s must be at least twice the "
                f"transfer timeout {self.config.remote_transfer_timeout_s}s"
            )

    @staticmethod
    def _discard(client: Any, transport: Any, staging: _Staging | None) -> None:
        """Tear down a half-built connection."""
        for close in (
            client.close if client is not None else None,
            transport.close if transport is not None else None,
            staging.close if staging is not None else None,
        ):
            if close is None:
                continue
            try:
                close()
            except Exception as exc:
                logger.warning("remote connect cleanup failed: %s", exc)

    def _client_id(self) -> str:
        return f"maru-{self.config.instance_id}"

    def _scope(self, key: str) -> str:
        """Key as stored on the server: the sharing namespace, then the chunk key."""
        return f"{self._namespace}/{key}"

    # ---- availability -------------------------------------------------------------

    def _ready(self) -> bool:
        """True if the server may be called now; reconnects when one is due."""
        if self._clock() < self._retry_at:
            return False
        if not self._reconnect:
            return True
        old = getattr(self._client, "generation", "")
        try:
            hello = self._client.connect()
            if not self.config.metadata_only:
                self._check_hello(hello)
        except StorageError as exc:
            logger.error("remote server layout changed and cannot be used: %s", exc)
            self._trip(exc)
            return False
        except Exception as exc:
            self._trip(exc)
            return False
        self._reconnect = False
        if self._client.generation != old:
            logger.warning(
                "remote server restarted (generation %s -> %s); forgetting stored keys",
                old,
                self._client.generation,
            )
            self._stored.clear()
        else:
            logger.info("remote server reachable again")
        return True

    def _trip(self, exc: BaseException) -> None:
        """Stop calling the server for ``remote_retry_s`` after a failure."""
        if self._clock() >= self._retry_at:
            logger.warning(
                "remote storage unavailable for %.0fs: %s",
                self.config.remote_retry_s,
                exc,
            )
        self._retry_at = self._clock() + self.config.remote_retry_s
        self._reconnect = True

    def _fail(self, exc: BaseException, op: str) -> None:
        """Classify a failed control call."""
        from maru_remote.client import RemoteRestarted, RemoteTimeout, RemoteUnreachable

        if isinstance(exc, RemoteRestarted):
            logger.warning("remote %s: %s", op, exc)
            self._reconnect = True  # reconnect on the next call, no cool-down
        elif isinstance(exc, RemoteTimeout | RemoteUnreachable):
            self._trip(exc)
        else:
            logger.warning("remote %s failed: %s", op, exc)

    # ---- store / retrieve ---------------------------------------------------------

    def _store_locked(
        self, keys: list[str], handles: list[RemoteAllocation]
    ) -> list[bool]:
        """Reserve, WRITE and publish (lock held; slots are returned by the caller)."""
        from maru_remote.transport import TransferTimeout

        self._reap()
        if not self._ready():
            return [False] * len(keys)
        assert self._staging is not None
        first: dict[str, int] = {}
        for i, key in enumerate(keys):
            first.setdefault(key, i)
        order = list(first.values())
        t0 = time.perf_counter()
        try:
            pages = self._client.reserve([handles[i].size for i in order])
        except Exception as exc:
            self._fail(exc, "reserve")
            return [False] * len(keys)
        t1 = time.perf_counter()
        tickets = [p["ticket"] for p in pages]
        pairs = [
            (
                self._staging.addr(handles[i].slot),
                int(p["base"]) + int(p["offset"]),
                handles[i].size,
            )
            for i, p in zip(order, pages, strict=True)
        ]
        try:
            self._transport.write(
                self._client.peer,
                pairs,
                timeout_s=self.config.remote_transfer_timeout_s,
            )
        except TransferTimeout as exc:
            self._isolate(exc.pending, [handles[i].slot for i in order], tickets)
            self._trip(exc)
            return [False] * len(keys)
        except Exception as exc:  # the transfer ended in an error state
            self._abandon_quietly(tickets)
            self._trip(exc)
            return [False] * len(keys)
        t2 = time.perf_counter()
        try:
            statuses = self._client.publish(
                [(t, self._scope(keys[i])) for t, i in zip(tickets, order, strict=True)]
            )
        except Exception as exc:  # unpublished pages expire on the server
            self._fail(exc, "publish")
            return [False] * len(keys)
        t3 = time.perf_counter()
        ok_by_key: dict[str, bool] = {}
        for i, status in zip(order, statuses, strict=True):
            ok = status in (_CREATED, _ALREADY_PRESENT)
            ok_by_key[keys[i]] = ok
            if ok:
                self._stored.add(keys[i])
        nbytes = sum(handles[i].size for i in order)
        self.counters["store_bytes"] += nbytes
        self.counters["write_seconds"] += t2 - t1
        logger.debug(
            "remote store: %d keys, %d bytes, reserve %.2f ms, write %.2f ms, publish %.2f ms",
            len(order),
            nbytes,
            (t1 - t0) * 1e3,
            (t2 - t1) * 1e3,
            (t3 - t2) * 1e3,
        )
        return [ok_by_key[k] for k in keys]

    def _retrieve_locked(self, keys: list[str]) -> list[RemoteReadLease | None]:
        """Lookup with protection, READ into staging, unpin (lock held)."""
        from maru_remote.transport import TransferTimeout

        assert self._staging is not None
        ticket_id = f"{self._client_id()}-{uuid.uuid4().hex}"
        t0 = time.perf_counter()
        try:
            entries = self._client.lookup(
                [self._scope(k) for k in keys], ticket_id, protect=True
            )
        except Exception as exc:
            self._fail(exc, "lookup")
            self.counters["loads_failed"] += 1
            raise StorageUnavailableError(f"remote lookup failed: {exc}") from exc
        t1 = time.perf_counter()
        found = [i for i, e in enumerate(entries) if e is not None]
        if not found:
            return [None] * len(keys)
        lengths = [int(entries[i]["length"]) for i in found]
        if max(lengths) > self._staging.slot_bytes:
            self._release_quietly(ticket_id)
            raise StorageError("a remote object is larger than one staging slot")
        slots = self._staging.take(len(found))
        if slots is None:
            self._release_quietly(ticket_id)
            self.counters["loads_failed"] += 1
            raise MemoryError(
                f"remote staging buffer cannot hold {len(found)} objects "
                f"({self._staging.free_count()} free slots)"
            )
        pairs = [
            (
                self._staging.addr(s),
                int(entries[i]["base"]) + int(entries[i]["offset"]),
                n,
            )
            for s, i, n in zip(slots, found, lengths, strict=True)
        ]
        try:
            self._transport.read(
                self._client.peer,
                pairs,
                timeout_s=self.config.remote_transfer_timeout_s,
            )
        except TransferTimeout as exc:
            self._release_quietly(ticket_id)  # a late READ only lands in isolated slots
            self._isolate(exc.pending, slots, [])
            self._trip(exc)
            self.counters["loads_failed"] += 1
            raise StorageUnavailableError("remote READ timed out") from exc
        except Exception as exc:
            for s in slots:
                self._staging.give(s)
            self._release_quietly(ticket_id)
            self._trip(exc)
            self.counters["loads_failed"] += 1
            raise StorageUnavailableError(f"remote READ failed: {exc}") from exc
        t2 = time.perf_counter()
        self._release_quietly(ticket_id)  # the bytes are local now
        t3 = time.perf_counter()
        leases: list[RemoteReadLease | None] = [None] * len(keys)
        for s, i, n in zip(slots, found, lengths, strict=True):
            leases[i] = RemoteReadLease(
                self._staging.view(s)[:n], keys[i], functools.partial(self._end_read, s)
            )
        self._reads += len(found)
        nbytes = sum(lengths)
        self.counters["read_seconds"] += t2 - t1
        self.counters["loads"] += 1
        self.counters["load_bytes"] += nbytes
        logger.debug(
            "remote load: %d/%d keys, %d bytes, lookup %.2f ms, read %.2f ms, release %.2f ms",
            len(found),
            len(keys),
            nbytes,
            (t1 - t0) * 1e3,
            (t2 - t1) * 1e3,
            (t3 - t2) * 1e3,
        )
        return leases

    def _end_read(self, slot: int) -> None:
        """Lease release: return its slot."""
        with self._lock:
            self._reads -= 1
            if self._staging is not None:
                self._staging.give(slot)

    # ---- quarantine -----------------------------------------------------------------

    def _isolate(self, pending: Any, slots: list[int], tickets: list[str]) -> None:
        """Keep the buffers of a timed-out transfer out of circulation."""
        self._quarantined_slots.update(slots)
        self._quarantine.append(_Quarantined(pending, list(slots), list(tickets)))
        self.counters["quarantined_transfers"] += 1
        if tickets:
            try:
                self._client.quarantine(tickets)
            except Exception as exc:  # the pages stay reserved until their TTL
                logger.warning("remote quarantine request failed: %s", exc)
        logger.warning(
            "remote %s timed out; isolating %d staging slots and %d pool pages",
            pending.op,
            len(slots),
            len(tickets),
        )

    def _reap(self) -> None:
        """Return the buffers of quarantined transfers that have ended."""
        if not self._quarantine:
            return
        keep: list[_Quarantined] = []
        for q in self._quarantine:
            if not q.slots and not q.tickets:
                continue
            if q.slots:
                if not q.pending.poll():
                    keep.append(q)
                    continue
                for s in q.slots:
                    self._quarantined_slots.discard(s)
                    if self._staging is not None:
                        self._staging.give(s)
                q.slots = []
            if q.tickets:
                try:
                    self._client.abandon(q.tickets)
                    q.tickets = []
                except Exception as exc:  # retried on the next reap
                    logger.debug("remote abandon of quarantined pages failed: %s", exc)
                    keep.append(q)
        self._quarantine = keep

    def _abandon_quietly(self, tickets: list[str]) -> None:
        try:
            self._client.abandon(tickets)
        except Exception as exc:  # the reservations expire on the server
            logger.debug("remote abandon failed: %s", exc)

    def _release_quietly(self, ticket_id: str) -> None:
        try:
            self._client.release(ticket_id)
        except Exception as exc:  # the server unpins at ticket expiry
            self._fail(exc, "release")
