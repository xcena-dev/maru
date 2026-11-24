# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruShmClient — shared memory client for the Maru Resource Manager.

Communicates with the resource manager over a persistent TCP connection
using the binary IPC protocol defined in maru_shm.ipc.
"""

import errno
import logging
import mmap as mmap_module
import os
import re
import socket
import threading
import time

from .constants import DEFAULT_ADDRESS
from .device_scanner import read_device_uuid, scan_dax_devices
from .ipc import (
    HEADER_SIZE,
    AllocReq,
    AllocResp,
    ErrorResp,
    FreeReq,
    FreeResp,
    GetAccessReq,
    GetAccessResp,
    HoldReq,
    MapHoldReq,
    MsgHeader,
    MsgType,
    NodeAllocReq,
    NodeClaimResp,
    NodeFreeReq,
    NodeHoldReq,
    NodeHoldsResp,
    NodeReleaseResp,
    NodeRenewResp,
    NodeReq,
    ResetHoldsReq,
    ResetHoldsResp,
    StatsReq,
    StatsResp,
    UnholdReq,
)
from .types import (
    MaruHandle,
    MaruPoolInfo,
    NodeClaim,
    NodeHostConflictError,
    NodeLeaseExpiredError,
    NodeOwnershipUnsupportedError,
    NodeRegion,
    RetryableNodeError,
    StaleNodeEpochError,
)
from .uds_helpers import read_full, write_full

logger = logging.getLogger(__name__)


class HoldRefusedError(RuntimeError):
    """The resource manager refused a hold request; it recorded nothing.

    A bad token, a region that is not allocated (or is draining), or a
    malformed holder name. Other failures leave the outcome unknown.
    """


class HoldsUnsupportedError(HoldRefusedError):
    """The resource manager predates region holds (restart it with this Maru)."""


# Refusals: sending the same request again gets the same answer.
# How long a resource manager without MAP_HOLD is not asked again.
_MAP_HOLD_RETRY_S = 60

_NOT_RETRYABLE = (
    0,
    -errno.ENOSYS,
    -errno.EINVAL,
    -errno.EACCES,
    -errno.EPERM,
    -errno.ENOENT,
)


def _make_client_id() -> str:
    """Build a client_id string: 'hostname:pid'."""
    import platform

    return f"{platform.node()}:{os.getpid()}"


def _process_identity() -> tuple[str, int, int]:
    """This process as the resource manager can recognize it on its host.

    Returns:
        The boot ID, the process start time (field 22 of /proc/self/stat) and
        the inode of its pid namespace; "" or 0 for what cannot be read, which
        the resource manager takes as unknown.
    """
    boot_id, pid_start, pid_ns = "", 0, 0
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            boot_id = f.read().strip()
    except OSError:
        pass
    try:
        with open("/proc/self/stat") as f:
            # The command (field 2) may hold spaces and parentheses.
            pid_start = int(f.read().rsplit(")", 1)[1].split()[19])
    except (OSError, IndexError, ValueError):
        pass
    try:
        pid_ns = os.stat("/proc/self/ns/pid").st_ino
    except OSError:
        pass
    return boot_id, pid_start, pid_ns


# Module-level request ID counter shared across all MaruShmClient instances.
# Prevents idempotency cache collisions when multiple instances in the same
# process have the same client_id (hostname:pid). Seeded at random, so that a
# later process with the same hostname:pid (PID 1 in a container) does not
# reuse an earlier one's IDs while the RM still caches their replies.
_request_id_counter = int.from_bytes(os.urandom(8), "little")
_request_id_lock = threading.Lock()
_MAX_REQUEST_ID = (1 << 64) - 1


def _next_request_id() -> int:
    """Next request ID: a nonzero u64, increasing, wrapping (process-global)."""
    global _request_id_counter
    with _request_id_lock:
        _request_id_counter = _request_id_counter % _MAX_REQUEST_ID + 1
        return _request_id_counter


class MaruShmClient:
    """Client for the Maru Resource Manager.

    Maintains a persistent TCP connection to the resource manager.
    Multiple RPC calls reuse the same connection. If the connection
    drops, it is transparently re-established on the next call.

    Device paths received from alloc/get_access are cached by region_id.
    mmap() opens the device path directly to create Python mmap objects.

    Each region this client maps is held with the resource manager until it is
    unmapped: the region's owner may free it, or be reclaimed, while this
    process can still write through the mapping, and the resource manager
    hands the range to nobody else before the hold is dropped. A hold that is
    not dropped here (the mapping could not be closed, or the resource manager
    could not be reached) is dropped by the resource manager once it sees this
    process gone, which it can for a process on its own host, in its pid
    namespace; otherwise an operator drops it (``maru-rm-ctl drop-holds``).
    """

    def __init__(
        self,
        address: str | None = None,
        device_table: dict[str, str] | None = None,
    ):
        self._address = address or DEFAULT_ADDRESS
        self._device_table = dict(device_table or {})  # uuid -> local_dax_path
        # A client given a table (even an empty one) resolves devices by UUID.
        self._resolve_by_uuid = device_table is not None
        self._path_cache: dict[int, str] = {}  # region_id -> device path
        self._mmap_cache: dict[int, mmap_module.mmap] = {}  # region_id -> mmap
        self._lock = threading.Lock()
        self._client_id = _make_client_id()
        # This client's holds of the regions it maps: one name per client, so
        # that two clients of one process hold (and unhold) independently.
        self._map_holder = "mmap:{}:{}".format(
            re.sub(r"[^\x21-\x7e]", "_", self._client_id)[:96], os.urandom(6).hex()
        )
        self._held: dict[int, MaruHandle] = {}  # region_id -> handle held
        # Regions with a mapping that could not be closed (exported buffers):
        # this process can still write there, so their hold is never dropped.
        self._unclosed: set[int] = set()
        # Serializes hold-then-map against unmap-then-unhold, so that an
        # unhold never lands after another thread mapped the region again.
        self._hold_lock = threading.RLock()
        # A forked child shares the name but must not drop the parent's holds.
        self._pid = os.getpid()
        # Holds to drop that the RM has not confirmed dropped yet: sent again
        # after the next answer of the RM, and in close().
        self._unhold_pending: dict[int, MaruHandle] = {}
        self._flushing = False  # Only read and set with _hold_lock held.
        # No unhold is tried before this time (the RM was unreachable), other
        # than after an answer of the RM.
        self._unhold_after = 0.0
        # Whether an answer of the RM ends that wait: yes if it was
        # unreachable, no if it answered that it could not drop a hold.
        self._unhold_forceable = True
        # False once the resource manager turned out to predate MAP_HOLD. It
        # is asked again from _map_holds_retry_at on (it may be upgraded), and
        # the regions mapped without a hold meanwhile are then held too.
        self._map_holds = True
        self._map_holds_retry_at = 0.0
        self._map_holds_warned = False
        self._unheld: dict[int, MaruHandle] = {}  # region_id -> mapped, not held
        self._sock: socket.socket | None = None
        self._conn_lock = threading.Lock()

    @staticmethod
    def _parse_address(address: str) -> tuple[str, int]:
        """Parse 'host:port' string."""
        host, _, port_str = address.rpartition(":")
        if not host:
            host = "127.0.0.1"
        return host, int(port_str)

    def is_running(self) -> bool:
        """Check if the resource manager is reachable."""
        host, port = self._parse_address(self._address)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
            sock.close()
            return True
        except OSError:
            sock.close()
            return False

    def _ensure_conn(self) -> socket.socket:
        """Return the persistent connection, creating it if needed.

        Must be called with _conn_lock held.
        """
        if self._sock is not None:
            return self._sock

        host, port = self._parse_address(self._address)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(5.0)  # 5s connect timeout
            sock.connect((host, port))
            sock.settimeout(
                10.0
            )  # 10s RPC timeout (prevents infinite block on server hang)
        except OSError as e:
            sock.close()
            raise ConnectionError(
                f"Resource manager is not running "
                f"(address: {self._address}).\n"
                f"Start it first: maru-resource-manager "
                f"--host {host} --port {port}"
            ) from e

        # Warn when connecting to a remote host over plaintext TCP
        if host not in ("127.0.0.1", "localhost", "::1"):
            logger.warning(
                "Connecting to remote host %s over PLAINTEXT TCP. "
                "Auth tokens will be transmitted without encryption. "
                "Use an encrypted tunnel for production deployments.",
                host,
            )

        # Disable Nagle for low-latency RPC
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock = sock
        return self._sock

    def _close_conn(self) -> None:
        """Close the persistent connection."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def _rpc(self, msg_type: MsgType, payload: bytes) -> tuple[MsgHeader, bytes]:
        """Execute a single RPC: send request, receive response.

        Thread-safe: the entire send+recv cycle is serialized by _conn_lock.
        On connection error, closes and retries once with a fresh connection.
        Idempotency for alloc/free is guaranteed by request_id in the payload;
        the server deduplicates by request_id and returns cached responses.

        Returns:
            (response_header, response_payload)
        """
        result = self._rpc_once(msg_type, payload)
        if self._unhold_pending:
            # The RM answers: drop the holds it could not be told to drop.
            # Not while another thread maps or unmaps: that one flushes.
            if self._hold_lock.acquire(blocking=False):
                try:
                    self._flush_unholds(force=self._unhold_forceable)
                finally:
                    self._hold_lock.release()
        return result

    def _rpc_once(self, msg_type: MsgType, payload: bytes) -> tuple[MsgHeader, bytes]:
        for attempt in range(2):
            with self._conn_lock:
                sock = self._ensure_conn()
                try:
                    # Send
                    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
                    write_full(sock, hdr.pack())
                    if payload:
                        write_full(sock, payload)

                    # Receive
                    resp_data = read_full(sock, HEADER_SIZE)
                    resp_hdr = MsgHeader.unpack(resp_data)
                    if not resp_hdr.validate():
                        raise ConnectionError(
                            f"Invalid response header: magic=0x{resp_hdr.magic:08X}"
                        )

                    resp_payload = b""
                    if resp_hdr.payload_len > 0:
                        resp_payload = read_full(sock, resp_hdr.payload_len)

                    return resp_hdr, resp_payload

                except (ConnectionError, OSError):
                    self._close_conn()
                    if attempt == 1:
                        raise

    def _check_error(self, hdr: MsgHeader, payload: bytes, context: str) -> None:
        """Raise RuntimeError if response is an ERROR_RESP."""
        if hdr.msg_type == MsgType.ERROR_RESP:
            err = ErrorResp.unpack(payload)
            raise RuntimeError(f"{context} ({err.status}): {err.message}")

    # =========================================================================
    # Public API
    # =========================================================================

    def stats(self) -> list[MaruPoolInfo]:
        """Query pool statistics from the resource manager."""
        hdr, payload = self._rpc(MsgType.STATS_REQ, StatsReq().pack())
        self._check_error(hdr, payload, "Stats failed")
        resp = StatsResp.unpack(payload)
        return resp.pools or []

    def alloc(self, size: int, dax_path: str = "") -> MaruHandle:
        """Allocate shared memory from the resource manager.

        Args:
            size: Requested allocation size in bytes.
            dax_path: DAX device path (e.g. "/dev/dax0.0"), or "" for any pool.

        Returns:
            Handle for the allocation.

        Raises:
            RuntimeError: On allocation failure.
        """
        req = AllocReq(
            size=size,
            dax_path=dax_path,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.ALLOC_REQ, req.pack())
        self._check_error(hdr, payload, "Alloc failed")

        resp = AllocResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"Alloc failed with status {resp.status}")

        handle = resp.handle
        self._cache_path(handle, resp.dax_path, resp.device_uuid, "alloc")

        logger.debug(
            "alloc(size=%d, dax_path=%s) -> region_id=%d path=%s",
            size,
            dax_path,
            handle.region_id,
            resp.dax_path,
        )
        return handle

    def free(self, handle: MaruHandle) -> None:
        """Free a previously allocated handle.

        Args:
            handle: Handle from a previous alloc() call.
        """
        req = FreeReq(
            handle=handle,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.FREE_REQ, req.pack())
        self._check_error(hdr, payload, "Free failed")

        resp = FreeResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"Free failed with status {resp.status}")

        with self._hold_lock:
            with self._lock:
                held = self._close_region_locked(handle.region_id)
            if held is not None:
                self._unhold_mapping(held)
        logger.debug("free(region_id=%d)", handle.region_id)

    def get_access(self, handle: MaruHandle) -> GetAccessResp:
        """Check a handle with the resource manager and get its device.

        Always asks the resource manager (``GET_ACCESS``), unlike :meth:`mmap`,
        which reuses a device path it saw before for the same region.

        Args:
            handle: Handle of the region.

        Returns:
            The device path, device UUID, offset and length of the region.

        Raises:
            RuntimeError: if the resource manager refuses the handle.
        """
        return self._request_access(handle)

    def hold_access(
        self, handle: MaruHandle, holder: str, instance: str
    ) -> GetAccessResp:
        """Check a handle as :meth:`get_access` does and hold the region.

        While ``holder`` holds the region, the resource manager keeps its
        range out of use after it is freed, until :meth:`unhold`.

        Args:
            handle: Handle of the region.
            holder: Who registers the region (a remote CXL agent's node).
            instance: The holder's process instance; :meth:`reset_holds` drops
                the holds of the others.

        Returns:
            The device path, device UUID, offset and length of the region.

        Raises:
            HoldsUnsupportedError: if the resource manager predates holds.
            RuntimeError: if the resource manager refuses the handle.
        """
        req = HoldReq(
            handle=handle,
            holder=holder,
            instance=instance,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.HOLD_REQ, req.pack())
        self._check_hold_error(hdr, payload, "Hold failed")
        resp = GetAccessResp.unpack(payload)
        if resp.status in (-errno.ENOENT, -errno.EINVAL):
            raise HoldRefusedError(f"Hold failed with status {resp.status}")
        if resp.status != 0:
            raise RuntimeError(f"Hold failed with status {resp.status}")
        return resp

    def unhold(self, handle: MaruHandle, holder: str, instance: str) -> None:
        """Tell the resource manager that ``holder`` released a region.

        Unholding a region that is not held, or no longer allocated, succeeds.

        Raises:
            HoldsUnsupportedError: if the resource manager predates holds.
            RuntimeError: if the resource manager refuses the request.
        """
        req = UnholdReq(
            handle=handle,
            holder=holder,
            instance=instance,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        status = self._unhold_status(req)
        if status == -errno.EINVAL:
            raise HoldRefusedError(f"Unhold failed with status {status}")
        if status != 0:
            raise RuntimeError(f"Unhold failed with status {status}")

    def _unhold_status(self, req: UnholdReq) -> int:
        """Send an unhold; returns the status the resource manager answers."""
        hdr, payload = self._rpc(MsgType.UNHOLD_REQ, req.pack())
        self._check_hold_error(hdr, payload, "Unhold failed")
        return FreeResp.unpack(payload).status

    def reset_holds(self, holder: str, instance: str) -> int:
        """Drop the holds of ``holder``'s other instances; they have ended.

        Returns:
            How many holds were dropped.

        Raises:
            HoldsUnsupportedError: if the resource manager predates holds.
            RuntimeError: if the resource manager refuses the request.
        """
        req = ResetHoldsReq(
            holder=holder,
            instance=instance,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.RESET_HOLDS_REQ, req.pack())
        self._check_hold_error(hdr, payload, "Reset holds failed")
        resp = ResetHoldsResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"Reset holds failed with status {resp.status}")
        return resp.dropped

    def _check_hold_error(self, hdr: MsgHeader, payload: bytes, context: str) -> None:
        if hdr.msg_type == MsgType.ERROR_RESP:
            err = ErrorResp.unpack(payload)
            if err.status == -errno.ENOSYS:
                raise HoldsUnsupportedError(
                    "the resource manager does not support region holds; "
                    "restart it with this Maru version"
                )
            if err.status == -errno.EACCES:
                raise HoldRefusedError(f"{context} ({err.status}): {err.message}")
            raise RuntimeError(f"{context} ({err.status}): {err.message}")

    def _hold_mapping(self, handle: MaruHandle) -> GetAccessResp | None:
        """Hold ``handle`` as a region this process is about to map.

        Returns:
            The region's device, as :meth:`get_access` returns it, or ``None``
            if the resource manager predates these holds (the region is then
            mapped without one, as before).

        Raises:
            RuntimeError: if the resource manager refuses the handle, or could
                not make the hold durable. The region must not be mapped.
        """
        boot_id, pid_start, pid_ns = _process_identity()
        req = MapHoldReq(
            handle=handle,
            holder=self._map_holder,
            boot_id=boot_id,
            pid=os.getpid(),
            pid_start=pid_start,
            pid_ns=pid_ns,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        # The hold asked for here is the one a pending unhold would drop.
        stale = self._unhold_pending.pop(handle.region_id, None)
        try:
            hdr, payload = self._rpc(MsgType.MAP_HOLD_REQ, req.pack())
        except BaseException:
            if stale is not None:
                self._unhold_pending[handle.region_id] = stale
            raise
        if hdr.msg_type == MsgType.ERROR_RESP:
            if stale is not None:
                self._unhold_pending[handle.region_id] = stale
            err = ErrorResp.unpack(payload)
            if err.status == -errno.ENOSYS:
                self._map_holds = False
                self._map_holds_retry_at = time.monotonic() + _MAP_HOLD_RETRY_S
                if not self._map_holds_warned:
                    self._map_holds_warned = True
                    logger.warning(
                        "the resource manager at %s does not hold mapped "
                        "regions (it predates this Maru version): a region "
                        "freed while this process maps it can be handed to "
                        "another owner. Restart the resource manager with "
                        "this Maru version; this client asks again at most "
                        "every %d s when it maps a region, and then holds "
                        "the regions it maps. A client that maps nothing "
                        "more stays unprotected until it is restarted",
                        self._address,
                        _MAP_HOLD_RETRY_S,
                    )
                return None
            raise RuntimeError(f"Map hold failed ({err.status}): {err.message}")
        resp = GetAccessResp.unpack(payload)
        if resp.status != 0:
            if stale is not None:
                self._unhold_pending[handle.region_id] = stale
            if resp.status not in _NOT_RETRYABLE:
                # Recorded, but not durable yet: not to be relied on.
                self._unhold_mapping(handle)
            raise RuntimeError(f"Map hold failed with status {resp.status}")
        return resp

    def _unhold_mapping(self, handle: MaruHandle, flush: bool = True) -> None:
        """Drop this client's hold of a region it no longer maps (best effort).

        Call with ``_hold_lock`` held. Nothing is sent for a region that an
        unclosed mapping of this client still covers, or from a forked child.
        A hold the resource manager could not be told to drop stays pending
        and is dropped after its next answer, or in :meth:`close`.
        """
        if handle.region_id in self._unclosed or os.getpid() != self._pid:
            return
        self._unhold_pending[handle.region_id] = handle
        if flush:
            self._flush_unholds()

    def _flush_unholds(self, force: bool = False) -> None:
        """Send the pending unholds (``_hold_lock`` held).

        Stops at the first one the resource manager does not answer, so that
        its timeouts are not waited out once per region; ``force`` tries also
        within the back-off that follows.
        """
        if self._flushing or not self._unhold_pending or os.getpid() != self._pid:
            return
        if not force and time.monotonic() < self._unhold_after:
            logger.debug(
                "%d unholds pending: RM unreachable", len(self._unhold_pending)
            )
            return
        self._flushing = True
        try:
            for region_id, handle in list(self._unhold_pending.items()):
                if region_id in self._unclosed:
                    del self._unhold_pending[region_id]
                    continue
                req = UnholdReq(
                    handle=handle,
                    holder=self._map_holder,
                    instance=self._map_holder,
                    client_id=self._client_id,
                    request_id=_next_request_id(),
                )
                try:
                    status = self._unhold_status(req)
                except OSError as exc:
                    self._unhold_after = time.monotonic() + 30.0
                    self._unhold_forceable = True
                    logger.warning(
                        "could not drop the hold of region %d (%s); tried "
                        "again after the next answer of the resource manager, "
                        "which keeps the range out of use until then",
                        region_id,
                        exc,
                    )
                    return
                except RuntimeError as exc:
                    # Refused as a request: asking again changes nothing.
                    logger.warning(
                        "could not drop the hold of region %d (%s); the "
                        "resource manager keeps its range out of use until "
                        "this process exits",
                        region_id,
                        exc,
                    )
                    status = 0
                if status not in _NOT_RETRYABLE:
                    # The RM kept the hold (e.g. it could not save the drop).
                    self._unhold_after = time.monotonic() + 5.0
                    self._unhold_forceable = False
                    logger.warning(
                        "could not drop the hold of region %d (status %d); "
                        "tried again later and in close()",
                        region_id,
                        status,
                    )
                    return
                if status != 0:
                    logger.warning(
                        "could not drop the hold of region %d (status %d); "
                        "the resource manager keeps its range out of use "
                        "until this process exits",
                        region_id,
                        status,
                    )
                self._unhold_pending.pop(region_id, None)
        finally:
            self._flushing = False

    def _cache_path(
        self, handle: MaruHandle, rm_path: str, device_uuid: str, context: str
    ) -> None:
        """Remember the local device path of a region the RM just reported."""
        if not rm_path:
            return
        # The region stands either way: an unknown device fails at mmap time
        # instead, where nothing is lost.
        try:
            path = self._resolve_device_path(rm_path, device_uuid)
        except RuntimeError as exc:
            logger.warning("%s(region_id=%d): %s", context, handle.region_id, exc)
        else:
            with self._lock:
                self._path_cache[handle.region_id] = path

    # =========================================================================
    # Node ownership: a StorageService owns regions as its node. Each run has
    # its own node_epoch; requests of any other run fail with
    # StaleNodeEpochError.
    # =========================================================================

    def node_claim(self, node_id: str, node_epoch: str) -> NodeClaim:
        """Make ``node_epoch`` the node's current run and take over its regions.

        Re-sending the same epoch returns the same result while it is the
        node's run. A run that has ended (another run claimed the node, it was
        released or its lease ran out) is refused: the RM keeps the node's
        last 64 ended runs, so every run takes a new epoch.

        Raises:
            StaleNodeEpochError: if the run has ended.
            NodeHostConflictError: if the node is bound to another host.
            NodeOwnershipUnsupportedError: if the RM predates node ownership.
            RetryableNodeError: if the RM could not complete it now (e.g. the
                claim is not durable yet); send it again.
        """
        req = NodeReq(
            node_id=node_id,
            node_epoch=node_epoch,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.NODE_CLAIM_REQ, req.pack())
        self._check_node_error(hdr, payload, "Node claim failed")
        resp = NodeClaimResp.unpack(payload)
        self._check_node_status(resp.status, "Node claim failed", retryable=True)
        regions = resp.regions or []
        for r in regions:
            self._cache_path(r.handle, r.dax_path, r.device_uuid, "node_claim")
        return NodeClaim(
            rm_domain=resp.rm_domain, lease_s=resp.lease_ms / 1000, regions=regions
        )

    def node_alloc(
        self, node_id: str, node_epoch: str, size: int, dax_path: str = ""
    ) -> NodeRegion:
        """Allocate a region owned by the node, as :meth:`alloc` does."""
        req = NodeAllocReq(
            size=size,
            node_id=node_id,
            node_epoch=node_epoch,
            dax_path=dax_path,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.NODE_ALLOC_REQ, req.pack())
        self._check_node_error(hdr, payload, "Node alloc failed")
        resp = AllocResp.unpack(payload)
        self._check_node_status(resp.status, "Node alloc failed")
        if resp.handle is None:
            raise RuntimeError("Node alloc failed: the RM sent no handle")
        self._cache_path(resp.handle, resp.dax_path, resp.device_uuid, "node_alloc")
        return NodeRegion(
            handle=resp.handle,
            requested_size=resp.requested_size,
            dax_path=resp.dax_path,
            device_uuid=resp.device_uuid,
        )

    def node_free(self, node_id: str, node_epoch: str, handle: MaruHandle) -> None:
        """Free a region of the node."""
        req = NodeFreeReq(
            handle=handle,
            node_id=node_id,
            node_epoch=node_epoch,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.NODE_FREE_REQ, req.pack())
        self._check_node_error(hdr, payload, "Node free failed")
        self._check_node_status(FreeResp.unpack(payload).status, "Node free failed")
        with self._hold_lock:
            with self._lock:
                held = self._close_region_locked(handle.region_id)
            if held is not None:
                self._unhold_mapping(held)

    def node_renew(self, node_id: str, node_epoch: str) -> float:
        """Start the node's lease again. Returns the lease in seconds.

        Raises:
            NodeLeaseExpiredError: if the run's lease ran out, or the node
                record is gone (released), and no other run claimed the node
                since.
            StaleNodeEpochError: if another run claimed the node.
        """
        req = NodeReq(
            node_id=node_id,
            node_epoch=node_epoch,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.NODE_RENEW_REQ, req.pack())
        self._check_node_error(hdr, payload, "Node renew failed")
        resp = NodeRenewResp.unpack(payload)
        if resp.status == -errno.EKEYEXPIRED:
            raise NodeLeaseExpiredError(
                "Node renew failed: the lease of this run ran out"
            )
        self._check_node_status(resp.status, "Node renew failed", retryable=True)
        return resp.lease_ms / 1000

    def node_holds(self, node_id: str, node_epoch: str) -> list[NodeRegion]:
        """The node's regions with their holds now. Read only: the lease is
        not renewed.

        Raises:
            StaleNodeEpochError: if ``node_epoch`` is not the node's current,
                unfenced run.
        """
        req = NodeReq(
            node_id=node_id,
            node_epoch=node_epoch,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.NODE_HOLDS_REQ, req.pack())
        self._check_node_error(hdr, payload, "Node holds failed")
        resp = NodeHoldsResp.unpack(payload)
        self._check_node_status(resp.status, "Node holds failed", retryable=True)
        return resp.regions or []

    def node_release(self, node_id: str, node_epoch: str) -> int:
        """Free every region of the node and forget it (``leave``).

        A release that fails part way is finished by the RM; one that fails
        before it starts leaves the run as it was.

        Returns:
            How many regions were freed (held ones drain).
        """
        req = NodeReq(
            node_id=node_id,
            node_epoch=node_epoch,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.NODE_RELEASE_REQ, req.pack())
        self._check_node_error(hdr, payload, "Node release failed")
        resp = NodeReleaseResp.unpack(payload)
        self._check_node_status(resp.status, "Node release failed", retryable=True)
        return resp.released

    def node_hold(
        self,
        node_id: str,
        node_epoch: str,
        handle: MaruHandle,
        holder: str,
        *,
        boot_id: str,
        pid: int,
        pid_start: int,
        pid_ns: int,
    ) -> GetAccessResp:
        """Hold a region of the node that this process maps, as :meth:`hold_access`.

        ``holder`` names this process's hold in this run (unique per process
        and run); drop it with ``unhold(handle, holder, holder)``. The RM keeps the region
        out of reuse until every holder drops its hold, and drops the hold
        itself once it sees the process gone (same host, boot and pid
        namespace): ``pid_start`` is field 22 of /proc/<pid>/stat, ``pid_ns``
        the inode of /proc/<pid>/ns/pid.

        Returns:
            The device path, device UUID, offset and length of the region.

        Raises:
            StaleNodeEpochError: if ``node_epoch`` is not the node's current run.
            RetryableNodeError: if the RM could not make the hold durable now.
            RuntimeError: on any other refusal (bad token, not the node's
                region, region freed).
        """
        req = NodeHoldReq(
            handle=handle,
            node_id=node_id,
            node_epoch=node_epoch,
            holder=holder,
            boot_id=boot_id,
            pid=pid,
            pid_start=pid_start,
            pid_ns=pid_ns,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.NODE_HOLD_REQ, req.pack())
        self._check_node_error(hdr, payload, "Node hold failed")
        resp = GetAccessResp.unpack(payload)
        self._check_node_status(resp.status, "Node hold failed", retryable=True)
        return resp

    def _check_node_error(self, hdr: MsgHeader, payload: bytes, context: str) -> None:
        if hdr.msg_type == MsgType.ERROR_RESP:
            err = ErrorResp.unpack(payload)
            if err.status == -errno.ENOSYS:
                raise NodeOwnershipUnsupportedError(
                    "the resource manager does not support node ownership; "
                    "restart it with this Maru version"
                )
            raise RuntimeError(f"{context} ({err.status}): {err.message}")

    @staticmethod
    def _check_node_status(status: int, context: str, retryable: bool = False) -> None:
        if status == -errno.ESTALE:
            raise StaleNodeEpochError(f"{context}: not the node's current run")
        if status == -errno.EEXIST:
            raise NodeHostConflictError(f"{context}: node is bound to another host")
        if retryable and status not in _NOT_RETRYABLE:
            raise RetryableNodeError(f"{context} with status {status}")
        if status != 0:
            raise RuntimeError(f"{context} with status {status}")

    def _resolve_device_path(self, rm_path: str, device_uuid: str) -> str:
        """Map the RM's device (path, UUID) to this host's device path.

        The RM's path can name a different device on this host, so a client
        built with a device table never opens it unless the UUID matches.

        Raises:
            RuntimeError: If the UUID cannot be found on this host.
        """
        if not device_uuid or not self._resolve_by_uuid:
            return rm_path
        with self._lock:
            local = self._device_table.get(device_uuid)
        if local:
            return local

        # Unknown UUID: the table may predate the device, so rescan once.
        # Merge, so a device whose header read fails now keeps its entry.
        table = dict(scan_dax_devices())
        with self._lock:
            self._device_table.update(table)
        local = table.get(device_uuid)
        if local:
            return local

        # Sysfs may be invisible (e.g. a single-host setup in a container).
        found = read_device_uuid(rm_path)
        if found == device_uuid:
            return rm_path
        raise RuntimeError(
            f"device UUID {device_uuid} (RM path {rm_path}) not found on this "
            f"host; header at {rm_path} has UUID {found}"
        )

    def _request_access(self, handle: MaruHandle) -> GetAccessResp:
        """Request access info from the resource manager via GET_ACCESS_REQ."""
        req = GetAccessReq(handle=handle, client_id=self._client_id)
        hdr, payload = self._rpc(MsgType.GET_ACCESS_REQ, req.pack())
        self._check_error(hdr, payload, "GetAccess failed")

        resp = GetAccessResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"GetAccess failed with status {resp.status}")
        return resp

    def mmap(self, handle: MaruHandle, prot: int) -> mmap_module.mmap:
        """Memory-map a handle into the calling process.

        Opens the device path directly and creates an mmap.

        Args:
            handle: Handle from alloc() or lookup.
            prot: Protection flags (PROT_READ | PROT_WRITE).

        Returns:
            Python mmap object with buffer protocol support.
        """
        # Fast path: check cache
        with self._lock:
            if handle.region_id in self._mmap_cache:
                return self._mmap_cache[handle.region_id]
            path = self._path_cache.get(handle.region_id)

        # Slow path: network RPC outside _lock. The hold comes before the
        # mapping, so that the range is never mapped without one.
        if os.getpid() != self._pid:
            # Its hold could never be dropped: this client's holds are named
            # after, and dropped by, the process that created it.
            raise RuntimeError(
                "this MaruShmClient was created before fork(): create a new "
                "one in the child to map a region"
            )
        with self._hold_lock:
            mm = self._mmap_held(handle, prot, path)
            if self._map_holds and self._unheld:
                self._hold_unheld()
            return mm

    def _mmap_held(
        self, handle: MaruHandle, prot: int, path: str | None
    ) -> mmap_module.mmap:
        """Hold and map a region that is not mapped yet (``_hold_lock`` held)."""
        with self._lock:
            if handle.region_id in self._mmap_cache:  # Mapped meanwhile.
                return self._mmap_cache[handle.region_id]
        if not self._map_holds and time.monotonic() >= self._map_holds_retry_at:
            self._map_holds = True  # Ask again: the RM may be a newer one now.
        access_resp = self._hold_mapping(handle) if self._map_holds else None
        held = access_resp is not None
        try:
            if access_resp is None and path is None:
                access_resp = self._request_access(handle)
            if access_resp is not None:
                path = self._resolve_device_path(
                    access_resp.dax_path, access_resp.device_uuid
                )

            # Create mmap and update cache
            with self._lock:
                access = mmap_module.ACCESS_READ
                if prot & 0x2:  # PROT_WRITE
                    access = mmap_module.ACCESS_WRITE

                fd = os.open(path, os.O_RDWR)
                try:
                    mm = mmap_module.mmap(
                        fd,
                        handle.length,
                        access=access,
                        offset=handle.offset,
                    )
                finally:
                    os.close(fd)

                self._path_cache[handle.region_id] = path
                self._mmap_cache[handle.region_id] = mm
                if held:
                    self._held[handle.region_id] = handle
                else:
                    self._unheld[handle.region_id] = handle
        except BaseException:
            if held:
                self._unhold_mapping(handle)
            raise

        logger.debug(
            "mmap(region_id=%d, length=%d, offset=%d, path=%s)",
            handle.region_id,
            handle.length,
            handle.offset,
            path,
        )
        return mm

    def _hold_unheld(self) -> None:
        """Hold the regions mapped while the RM had no MAP_HOLD (best effort).

        Call with ``_hold_lock`` held, once the resource manager answers
        MAP_HOLD after all.
        """
        for region_id, handle in list(self._unheld.items()):
            try:
                if self._hold_mapping(handle) is None:
                    return  # Not supported after all.
            except (RuntimeError, OSError) as exc:
                logger.warning(
                    "region %d stays mapped without a hold (%s)", region_id, exc
                )
                if isinstance(exc, OSError):
                    return
                self._unheld.pop(region_id, None)
                continue
            with self._lock:
                self._unheld.pop(region_id, None)
                self._held[region_id] = handle
        logger.info(
            "the resource manager at %s now holds the regions this client maps",
            self._address,
        )

    def munmap(self, handle: MaruHandle) -> None:
        """Unmap a previously mapped handle and drop its hold.

        Raises:
            BufferError: if buffers exported from the mapping still exist. The
                mapping then lives until they are released, and its hold
                until this process exits.
        """
        with self._hold_lock:
            with self._lock:
                held = self._close_region_locked(handle.region_id, keep_path=True)
            if held is not None:
                self._unhold_mapping(held)
        logger.debug("munmap(region_id=%d)", handle.region_id)

    def get_dax_path(self, region_id: int) -> str | None:
        """Return the DAX device path backing ``region_id``, or ``None``.

        Served from the local cache populated by ``alloc()`` / ``mmap()`` — every
        region this client allocated is present. A cache miss returns ``None``;
        callers treat that as an unresolved device, not an error.
        """
        with self._lock:
            return self._path_cache.get(region_id)

    def _close_region_locked(
        self, region_id: int, keep_path: bool = False
    ) -> MaruHandle | None:
        """Close mmap and path cache for a region (must hold self._lock).

        Returns:
            The handle whose hold the caller drops (:meth:`_unhold_mapping`,
            without self._lock), or ``None`` if the region was not held.

        Raises:
            BufferError: if the mapping cannot be closed yet. Its hold then
                stays for good, also if the region is mapped again later.
        """
        mm = self._mmap_cache.pop(region_id, None)
        held = self._held.pop(region_id, None)
        self._unheld.pop(region_id, None)
        if mm is not None:
            try:
                mm.close()
            except BufferError:
                if held is not None:
                    self._unclosed.add(region_id)
                raise
        if not keep_path:
            self._path_cache.pop(region_id, None)
        return held

    def close(self) -> None:
        """Close all cached mmaps, drop their holds, and close the connection.

        Raises:
            BufferError: if buffers exported from a mapping still exist, after
                everything else was closed. That mapping lives until they are
                released, and its hold until this process exits.
        """
        unmapped: list[MaruHandle] = []
        unclosed: BufferError | None = None
        with self._hold_lock:
            try:
                with self._lock:
                    num_mmaps = len(self._mmap_cache)
                    for region_id in list(self._mmap_cache.keys()):
                        try:
                            held = self._close_region_locked(region_id)
                        except BufferError as exc:
                            # Its hold stays; the others are still dropped.
                            unclosed = unclosed or exc
                            continue
                        if held is not None:
                            unmapped.append(held)
                    self._mmap_cache.clear()
                    self._path_cache.clear()
            finally:
                try:
                    for handle in unmapped:
                        self._unhold_mapping(handle, flush=False)
                    # One more try also for the holds left pending earlier.
                    self._flush_unholds(force=True)
                except Exception:
                    # close() also runs from __del__, where nothing may
                    # escape (e.g. at interpreter shutdown).
                    logger.debug("unhold failed in close()", exc_info=True)
                with self._conn_lock:
                    self._close_conn()
        logger.debug("close(): released %d mmaps", num_mmaps)
        if unclosed is not None:
            raise unclosed

    def __del__(self) -> None:
        try:
            self.close()
        except BufferError:
            pass  # Exported buffers outlive the client; nothing to report to.
