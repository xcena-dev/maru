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

The staging buffer is anonymous memory registered with NIXL. Callers must
finish with a slot before handing it back: the vLLM connector's lease path
copies with blocking ``copy_``/``.to(non_blocking=False)`` calls, so a lease
released after those calls return is no longer read by the GPU.
"""

from __future__ import annotations

import functools
import logging
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from maru_common.storage_types import StorageError, StorageUnavailableError

from .remote_buffers import (
    QuarantinedTransfer,
    RemoteAllocation,
    RemoteReadLease,
    StagingBuffer,
    release_view,
)

logger = logging.getLogger(__name__)

_CREATED = "CREATED"
_ALREADY_PRESENT = "ALREADY_PRESENT"
# Period of the maintenance thread: one ping of its own probe client, then a
# reconnect (server restarted or back) or an eviction sync when needed.
_MAINTAIN_INTERVAL_S = 1.0
# Reply deadline of a probe ping (capped by the control timeout); a ping
# needs well under a millisecond, so a short deadline detects outages fast.
_PROBE_TIMEOUT_MS = 500
# Consecutive failed probes that stop calls. The server serves one request
# at a time, so a long request (a region mapped while reserving pages takes
# up to ~2 s) must not read as an outage.
_PROBE_FAILURES_TO_STOP = 2


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
        self._lock = threading.RLock()  # control requests and RDMA transfers
        # Slot bookkeeping only. alloc, free and lease release take this lock
        # alone, so the engine thread never waits for a transfer or a control
        # request another thread is running under ``_lock``. Lock order:
        # ``_lock`` before ``_slot_lock``.
        self._slot_lock = threading.Lock()
        self._clock = clock
        self._transport_factory = transport_factory or _default_transport
        self._client_factory = client_factory or _default_client
        self._namespace = config.cache_namespace
        self._client: Any = None
        self._transport: Any = None
        self._staging: StagingBuffer | None = None
        # Keys this handler stored or read in the current server run. A hint
        # for skipping stores: dropped when the server evicts them, all
        # forgotten on a restart.
        self._stored: set[str] = set()
        self._evictions_seen = 0
        # Keys the server refused to publish, with the time to try them again.
        self._rejected: dict[str, float] = {}
        self._retry_at = 0.0
        self._store_retry_at = 0.0
        self._reconnect = False
        self._quarantine: list[QuarantinedTransfer] = []
        self._quarantined_slots: set[int] = set()
        self._reads = 0
        self._closed = False
        self._maintainer: threading.Thread | None = None
        self._stop_maintainer = threading.Event()
        # Control-only client of the maintenance thread: its probes never
        # hold the I/O lock or the main client's socket.
        self._probe: Any = None
        # Serializes eviction syncs of the I/O threads and the maintenance thread.
        self._evict_lock = threading.Lock()
        self._probe_failures = 0
        # The server answered but cannot serve this handler (layout changed):
        # reconnect only after the retry period, not on every probe.
        self._layout_error = False
        self.counters = {
            "stores": 0,
            "store_bytes": 0,
            "stores_failed": 0,
            "store_skipped_present": 0,
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
            self._stop_maintainer.set()
            for q in self._quarantine:
                if q.pending.poll() and q.tickets:
                    self._abandon_quietly(q.tickets)  # the WRITE ended: free its pages
            self._quarantine.clear()
            if self._client is not None:
                self._client.close()
            if self._transport is not None:
                self._transport.close()
            with self._slot_lock:
                if self._staging is not None:
                    self._staging.close()
                self._client = self._transport = self._staging = None
            self._stored.clear()
            self._rejected.clear()
            maintainer, self._maintainer = self._maintainer, None
        if maintainer is not None:
            maintainer.join(timeout=self.config.timeout_ms / 1000 + 1.0)
        if self._probe is not None:
            self._probe.close()
            self._probe = None

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
        # Return slots of ended timed-out transfers when nobody holds the I/O
        # lock; never wait for it.
        if self._quarantine and self._lock.acquire(blocking=False):
            try:
                self._reap_slots()
            finally:
                self._lock.release()
        with self._slot_lock:
            self._ensure()
            staging = self._staging
            assert staging is not None
            if type(size) is not int or not 0 < size <= staging.slot_bytes:
                raise ValueError("remote allocation must fit in one staging slot")
            if self._clock() < max(self._retry_at, self._store_retry_at):
                raise StorageUnavailableError(
                    "remote storage is unavailable or full; skipping store"
                )
            slots = staging.take(1)
            if slots is None:
                raise MemoryError(
                    "remote staging buffer is full; skipping new cache admission"
                )
            slot = slots[0]
            return RemoteAllocation(staging.view(slot)[:size], slot, size)

    def free(self, handle: RemoteAllocation) -> None:
        """Return an unsubmitted allocation; a submitted one is owned by batch_store."""
        with self._slot_lock:
            if not isinstance(handle, RemoteAllocation) or handle.state != "writing":
                return
            handle.state = "freed"
            release_view(handle.buf)
            if self._staging is not None:
                self._staging.give(handle.slot)

    def has_local(self, key: str) -> bool:
        """Whether this handler stored (or found) ``key`` in the current server run.

        Answers from memory and never calls the server, so the engine thread
        never waits on the network. The remembered keys belong to one server
        run: the maintenance thread checks the run every round and forgets
        them on a restart, and it or the next load or store drops the keys
        the server evicted. Until then a remembered key may already be gone.
        """
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
                    release_view(h.buf)
                    if h.slot not in self._quarantined_slots:
                        self._staging.give(h.slot)
            self.counters["stores" if all(results) else "stores_failed"] += 1
            return results

    @property
    def gpu_accessible(self) -> bool:
        """Whether GPU kernels and async copies may read and write the slots."""
        return self._staging is not None and self._staging.cuda_registered

    def retrieve_capacity(self) -> int:
        """Free staging slots: the most objects one batch_retrieve can hold now."""
        with self._slot_lock:
            return self._staging.free_count() if self._staging is not None else 0

    def batch_exists(self, keys: list[str]) -> list[bool]:
        """Report which keys are in the pool (all False while the server is down).

        Callers sit on vLLM's scheduler path, so a reconnect that is due is
        left to the maintenance thread; until it succeeds every key misses.
        """
        with self._lock:
            self._ensure(data=False)
            if not keys:
                return []
            if not self._ready(connect=False):
                return [False] * len(keys)
            try:
                scoped = [self._scope(k) for k in keys]
                found = list(self._client.exists(scoped))
                return found
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
            if not self._ready(connect=False):
                raise StorageUnavailableError("remote storage is unavailable")
            return self._retrieve_locked(keys)

    def ping(self) -> bool:
        """True if the remote server answers now."""
        with self._lock:
            if not self.connected or not self._ready(connect=False):
                return False
            try:
                self._client.ping()
                return True
            except Exception as exc:
                self._fail(exc, "ping")
                return False

    def _start_maintainer(self) -> None:
        """Start the maintenance thread and its probe client once."""
        if self._maintainer is not None:
            return
        self._probe = self._client_factory(
            self.config.remote_url,
            None,
            client_id=f"{self._client_id()}-probe",
            timeout_ms=min(self.config.timeout_ms, _PROBE_TIMEOUT_MS),
        )
        self._stop_maintainer.clear()
        self._maintainer = threading.Thread(
            target=self._maintain_loop, name="maru-remote-maintain", daemon=True
        )
        self._maintainer.start()

    def _maintain_loop(self) -> None:
        while not self._stop_maintainer.wait(_MAINTAIN_INTERVAL_S):
            try:
                self.maintain()
            except Exception:  # keep the thread alive; the next round retries
                logger.warning("remote storage maintenance failed", exc_info=True)

    def maintain(self) -> None:
        """One round of background upkeep, off the engine and scheduler paths.

        The only place that reconnects after an outage or a restart. Pings
        the server with the probe client without holding the I/O lock, also
        while calls are stopped: a failed ping keeps calls stopped, and the
        first answer after an outage (or a changed server run) reconnects
        right away under the I/O lock. Otherwise drops remembered keys the
        server reported evicted.
        """
        from maru_remote.client import RemoteTimeout, RemoteUnreachable

        probe = self._probe
        if probe is None or not self.connected or self._closed:
            return
        try:
            probe.connect()  # a ping: learns the generation and eviction count
        except (RemoteTimeout, RemoteUnreachable) as exc:
            self._probe_failures += 1
            if self._reconnect or self._probe_failures >= _PROBE_FAILURES_TO_STOP:
                self._trip(exc)
            return
        except Exception as exc:  # the server answered with an error
            logger.warning("remote probe failed: %s", exc)
            return
        self._probe_failures = 0
        if self._layout_error and self._clock() < self._retry_at:
            return
        if self._reconnect or probe.generation != getattr(
            self._client, "generation", ""
        ):
            with self._lock:
                if self.connected and not self._closed:
                    self._reconnect = True
                    self._retry_at = 0.0  # the server answered just now
                    self._ready()
            return
        if self._staging is not None:
            with self._evict_lock:
                self._sync_evictions(probe)

    def _sync_from_io(self) -> None:
        """Eviction sync from a load or store (I/O lock held); never waits.

        When the maintenance thread is syncing, it covers the same evictions.
        """
        if self._evict_lock.acquire(blocking=False):
            try:
                self._sync_evictions(self._client)
            finally:
                self._evict_lock.release()

    def _sync_evictions(self, client: Any) -> None:
        """Drop remembered keys the server has evicted (eviction lock held).

        Costs a control call only when ``client``'s last reply reported new
        evictions. If the server no longer lists all of them, every
        remembered key is forgotten. A failed call leaves the keys for the
        next attempt.
        """
        evictions = getattr(client, "evictions", 0)
        if evictions == self._evictions_seen:
            return
        try:
            keys, complete = client.evicted_since(self._evictions_seen)
        except Exception as exc:
            if client is self._client:
                self._fail(exc, "evicted_since")
            return
        if complete:
            prefix = f"{self._namespace}/"
            for key in keys:
                if key.startswith(prefix):
                    self._stored.discard(key[len(prefix) :])
        else:
            self._stored.clear()
        self._evictions_seen = evictions

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
            if self.connected and self._ready(connect=False):
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
        self._start_maintainer()
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
        try:  # local resources only once the server is known to be usable
            staging = StagingBuffer(self.config.pool_size, self.config.chunk_size_bytes)
            # Page-locked for CUDA first so the GPU kernels and async copies
            # can use the slots directly; NIXL registers the same memory.
            staging.cuda_register()
            transport.register(staging.address, staging.nbytes, "maru-remote-staging")
        except Exception as exc:
            self._discard(client, transport, staging)
            raise StorageError(f"remote staging buffer setup failed: {exc}") from exc
        self._client, self._transport, self._staging = client, transport, staging
        self.connected = True
        self._evictions_seen = getattr(client, "evictions", 0)
        self._start_maintainer()
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
        timeout = self.config.remote_transfer_timeout_s
        for name, what in (
            ("reservation_ttl_s", "reservation"),
            ("ticket_ttl_s", "read protection"),
        ):
            ttl = float(hello.get(name, 0))
            if ttl < 2 * timeout:
                raise StorageError(
                    f"remote {what} lifetime {ttl}s must be at least twice the "
                    f"transfer timeout {timeout}s"
                )

    @staticmethod
    def _discard(client: Any, transport: Any, staging: StagingBuffer | None) -> None:
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

    def _ready(self, connect: bool = True) -> bool:
        """True if the server may be called now; reconnects when one is due.

        Args:
            connect: Reconnect here when one is due; False reports the server
                unavailable instead (for callers that must not wait).
        """
        if self._clock() < self._retry_at:
            return False
        if not self._reconnect:
            return True
        if not connect:
            return False
        old = getattr(self._client, "generation", "")
        try:
            hello = self._client.connect()
            if not self.config.metadata_only:
                self._check_hello(hello)
        except StorageError as exc:
            logger.error("remote server layout changed and cannot be used: %s", exc)
            self._trip(exc)
            self._layout_error = True
            return False
        except Exception as exc:
            self._trip(exc)
            return False
        self._reconnect = False
        self._layout_error = False
        if self._client.generation != old:
            logger.warning(
                "remote server restarted (generation %s -> %s); forgetting stored keys",
                old,
                self._client.generation,
            )
            with self._evict_lock:
                self._stored.clear()
                self._rejected.clear()
                self._evictions_seen = getattr(self._client, "evictions", 0)
        else:
            logger.info("remote server reachable again")
        return True

    def _after_restart(self, call: Callable[[], Any]) -> Any:
        """Run ``call``; if the server restarted, reconnect and run it once more.

        Only for calls that start an operation (exists, reserve, lookup):
        nothing from the earlier server run is carried into the retry.
        """
        from maru_remote.client import RemoteRestarted

        try:
            result = call()
        except RemoteRestarted as exc:
            self._fail(exc, "call")
            if not self._ready():
                raise
            result = call()
        return result

    def _trip(self, exc: BaseException) -> None:
        """Stop calling the server for ``remote_retry_s`` after a failure."""
        if self._clock() >= self._retry_at and not self._reconnect:
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
        if not self._ready(connect=False):
            return [False] * len(keys)
        assert self._staging is not None
        self._sync_from_io()
        now = self._clock()
        first: dict[str, int] = {}
        for i, key in enumerate(keys):
            if self._rejected.get(key, 0.0) > now:
                continue  # refused recently: do not spend another WRITE on it
            first.setdefault(key, i)
        order = list(first.values())
        if not order:
            return [False] * len(keys)
        t0 = time.perf_counter()
        try:  # a key another writer stored, or one forgotten after an eviction
            present = self._after_restart(
                lambda: self._client.exists([self._scope(keys[i]) for i in order])
            )
        except Exception as exc:
            self._fail(exc, "exists")
            return [False] * len(keys)
        ok_by_key: dict[str, bool] = {}
        for i, found in zip(order, present, strict=True):
            if found:
                ok_by_key[keys[i]] = True
                self._stored.add(keys[i])
                self._rejected.pop(keys[i], None)
        order = [i for i, found in zip(order, present, strict=True) if not found]
        self.counters["store_skipped_present"] += len(ok_by_key)
        if ok_by_key:
            logger.debug(
                "remote store: %d keys already present, not written", len(ok_by_key)
            )
        if not order:
            return [ok_by_key.get(k, False) for k in keys]
        sizes = [handles[i].size for i in order]
        try:
            pages = self._after_restart(lambda: self._client.reserve(sizes))
        except Exception as exc:
            if getattr(exc, "code", None) == "POOL_FULL":
                if self._clock() >= self._store_retry_at:
                    logger.warning(
                        "remote pool is full; skipping stores for %.0fs",
                        self.config.remote_retry_s,
                    )
                self._store_retry_at = self._clock() + self.config.remote_retry_s
            else:
                self._fail(exc, "reserve")
            return [ok_by_key.get(k, False) for k in keys]
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
            return [ok_by_key.get(k, False) for k in keys]
        except Exception as exc:  # the transfer ended in an error state
            self._abandon_quietly(tickets)
            self._trip(exc)
            return [ok_by_key.get(k, False) for k in keys]
        t2 = time.perf_counter()
        try:
            statuses = self._client.publish(
                [(t, self._scope(keys[i])) for t, i in zip(tickets, order, strict=True)]
            )
        except Exception as exc:
            from maru_remote.client import RemoteTimeout, RemoteUnreachable

            self._fail(exc, "publish")
            if not isinstance(exc, RemoteTimeout | RemoteUnreachable):
                self._abandon_quietly(tickets)  # unknown tickets are skipped
            return [ok_by_key.get(k, False) for k in keys]
        t3 = time.perf_counter()
        for i, status in zip(order, statuses, strict=True):
            ok = status in (_CREATED, _ALREADY_PRESENT)
            ok_by_key[keys[i]] = ok
            if ok:
                self._stored.add(keys[i])
                self._rejected.pop(keys[i], None)
            else:
                self._rejected[keys[i]] = self._clock() + self.config.remote_retry_s
        self._sync_from_io()  # this reservation may have evicted keys
        nbytes = sum(handles[i].size for i in order)
        self.counters["store_bytes"] += nbytes
        self.counters["write_seconds"] += t2 - t1
        logger.debug(
            "remote store: %d keys, %d bytes, check+reserve %.2f ms, write %.2f ms, publish %.2f ms",
            len(order),
            nbytes,
            (t1 - t0) * 1e3,
            (t2 - t1) * 1e3,
            (t3 - t2) * 1e3,
        )
        return [ok_by_key.get(k, False) for k in keys]

    def _retrieve_locked(self, keys: list[str]) -> list[RemoteReadLease | None]:
        """Lookup with protection, READ into staging, unpin (lock held)."""
        from maru_remote.transport import TransferTimeout

        assert self._staging is not None
        t0 = time.perf_counter()
        scoped = [self._scope(k) for k in keys]
        ticket_id = ""

        def lookup() -> list[dict[str, Any] | None]:
            nonlocal ticket_id
            ticket_id = f"{self._client_id()}-{uuid.uuid4().hex}"  # fresh per attempt
            return list(self._client.lookup(scoped, ticket_id, protect=True))

        try:
            entries = self._after_restart(lookup)
        except Exception as exc:
            self._fail(exc, "lookup")
            self.counters["loads_failed"] += 1
            raise StorageUnavailableError(f"remote lookup failed: {exc}") from exc
        t1 = time.perf_counter()
        self._sync_from_io()
        found = [i for i, e in enumerate(entries) if e is not None]
        for i, e in enumerate(entries):
            if e is None:
                self._stored.discard(keys[i])
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
            self._stored.add(keys[i])  # present in this run: no need to store it
            leases[i] = RemoteReadLease(
                self._staging.view(s)[:n], keys[i], functools.partial(self._end_read, s)
            )
        with self._slot_lock:
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
        with self._slot_lock:
            self._reads -= 1
            if self._staging is not None:
                self._staging.give(slot)

    # ---- quarantine -----------------------------------------------------------------

    def _isolate(self, pending: Any, slots: list[int], tickets: list[str]) -> None:
        """Keep the buffers of a timed-out transfer out of circulation."""
        self._quarantined_slots.update(slots)
        self._quarantine.append(
            QuarantinedTransfer(pending, list(slots), list(tickets))
        )
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

    def _reap_slots(self) -> None:
        """Return the staging slots of quarantined transfers that have ended.

        Local work only (no control request), so ``alloc`` may run it.
        """
        for q in self._quarantine:
            if q.slots and q.pending.poll():
                for s in q.slots:
                    self._quarantined_slots.discard(s)
                    if self._staging is not None:
                        self._staging.give(s)
                q.slots = []

    def _reap(self) -> None:
        """Return the buffers of quarantined transfers that have ended."""
        if not self._quarantine:
            return
        self._reap_slots()
        keep: list[QuarantinedTransfer] = []
        for q in self._quarantine:
            if not q.slots and not q.tickets:
                continue
            if q.slots:  # still in flight
                keep.append(q)
                continue
            if q.tickets:
                if self._clock() < self._retry_at:
                    keep.append(q)  # the server is considered down: ask later
                    continue
                from maru_remote.client import RemoteRestarted

                try:
                    self._client.abandon(q.tickets)
                    q.tickets = []
                except RemoteRestarted:
                    q.tickets = []  # an earlier server run's pages are gone with it
                except Exception as exc:  # retried after the cool-down
                    self._fail(exc, "abandon")
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
