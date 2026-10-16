# SPDX-License-Identifier: Apache-2.0
"""Region lifecycle of a remote CXL storage session: getting, mapping, setting
aside and giving back regions, evicting keys, and ending stray transfers.

:class:`RegionLifecycle` is the half of
:class:`~maru_handler.storage.remote_cxl.client.RemoteCxlStorageClient` that manages
regions; the client's stores and loads use it. Methods that change regions
or keys expect the client lock to be held unless they say otherwise.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import asdict
from typing import Any

from maru_common.storage_types import (
    MAX_STORAGE_BATCH,
    StorageError,
    StorageLocation,
    StorageUnavailableError,
)

from .datapath import RdmaDataPath, Release
from .region import IsolatedTransfer, RegionAccess, RemoteCxlRegion, pick_mapper

logger = logging.getLogger(__name__)

EVICT_BATCH = 32
SETTLE_POLL_S = 0.01
CALL_ATTEMPTS = 3


class RegionLifecycle:
    """Regions of one storage session; mixed into the remote CXL storage client."""

    config: Any
    regions: dict[str, RemoteCxlRegion]
    connected: bool
    _data: RdmaDataPath
    _mappers: list[str]
    _keys: dict[str, StorageLocation]
    _removing: list[dict]
    _dropping: list[dict]
    _isolated: list[IsolatedTransfer]
    _expand_op: str | None
    _credentials: dict
    _lock: threading.RLock

    def _call(self, action: str, **payload: Any) -> dict:
        raise NotImplementedError

    # ------------------------------------------------------------ mapping

    def _add_region(self, info: dict) -> RemoteCxlRegion:
        """Track a region the session got and map it now.

        A region whose mapper cannot map it is kept aside; new pages then
        come from another region, a new one if needed.
        """
        region = RemoteCxlRegion(
            info["pool_id"],
            info["capacity"],
            self.config.chunk_size_bytes,
            info["handle"],
        )
        self.regions[region.pool_id] = region
        try:
            self._map_own(region)
        except StorageUnavailableError as exc:
            region.reachable = False
            logger.warning(
                "Remote CXL region %d is not reachable: %s", region.region_id, exc
            )
        return region

    def _region(self, pool_id: str, handle: dict, own: bool = False) -> RegionAccess:
        return self._data.region(pool_id, handle, self._mappers, own=own)

    def _map_own(self, region: RemoteCxlRegion) -> RegionAccess:
        """Access to a region of this session, mapping it if needed."""
        access = self._region(region.pool_id, region.handle, own=True)
        self._adopt(region, access)
        return access

    def _adopt(self, region: RemoteCxlRegion, access: RegionAccess) -> None:
        """Note a map of an own region; a rebooted memory node voids its keys.

        A WRITE the memory node's NIC completed may have sat in that node's
        CPU cache; a reboot lost it while the key stayed committed. The keys
        of the region are removed before it is used again.
        """
        if region.boot and access.boot != region.boot:
            logger.warning(
                "memory node of remote CXL region %d rebooted; removing its keys",
                region.region_id,
            )
            self._drop_keys(region)
        region.boot = access.boot
        region.mapped = True

    def _retry_unreachable(self) -> None:
        """Map regions set aside earlier; one that maps is used again.

        Runs without the client lock: reaching a node that is down can take
        the connect timeout, and stores must not wait for it.
        """
        for region in list(self.regions.values()):
            if region.reachable or region.retired:
                continue
            try:
                access = self._region(region.pool_id, region.handle, own=True)
            except StorageUnavailableError:
                continue
            with self._lock:
                if region.retired:
                    # It is going back and was mapped meanwhile: undo the
                    # registration. If that fails, whoever gives the region
                    # back sees it mapped and releases it first.
                    region.mapped = True
                    self._data.release(region.pool_id, region.handle, self._mappers)
                    continue
                self._adopt(region, access)
                region.reachable = True
            logger.info("Remote CXL region %d is reachable again", region.region_id)

    def _check_boots(self) -> None:
        """Remove the keys of own regions whose memory node rebooted.

        Asks every region mapper of a mapped own region for its boot id, so a
        reboot is noticed even for a region this worker no longer writes.
        Runs without the client lock until there is something to change.
        """
        owned: dict[str, list[RemoteCxlRegion]] = {}
        for region in list(self.regions.values()):
            if region.boot and not region.retired:
                owned.setdefault(
                    pick_mapper(self._mappers, region.region_id), []
                ).append(region)
        if not owned:
            return
        boots = self._data.boots(list(owned))
        with self._lock:
            for address, boot in boots.items():
                for region in owned[address]:
                    if region.retired or not boot or boot == region.boot:
                        continue
                    logger.warning(
                        "memory node of remote CXL region %d rebooted; removing its keys",
                        region.region_id,
                    )
                    self._drop_keys(region)
                    self._data.forget(region.pool_id)
                    region.boot = boot

    # ---------------------------------------------------------- give back

    def _return_regions(self) -> None:
        """Give every region back in the order that keeps late WRITEs out.

        1. Wait (bounded) for the WRITEs sent to the regions; this only
           spares them an error.
        2. Have each region's mapper deregister it (``release``). From the
           reply on, an RDMA to the region fails at the memory node's NIC.
           A region for which no map request ever reached a mapper needs no
           release: no RDMA was ever sent to it. After a region mapper does
           not reply, its other regions are not tried again.
        3. Only then give the regions back. If all are released the session
           closes drained; otherwise each released region goes back alone
           (``return_region``) and MaruServer keeps the others.
        """
        if not self.regions:
            return
        for region in self.regions.values():
            region.retire()
        if not self._settle_writes(self.config.rdma_timeout_s):
            logger.warning(
                "RDMA WRITEs are still unconfirmed; releasing their regions ends them"
            )
        released: dict[str, bool] = {}
        silent: set[str] = set()
        for pool_id, region in self.regions.items():
            if not region.mapped and not self._data.map_attempted(pool_id):
                released[pool_id] = True
                continue
            address = pick_mapper(self._mappers, region.region_id)
            if address in silent:
                released[pool_id] = False
                continue
            outcome = self._data.release(pool_id, region.handle, self._mappers)
            if outcome is Release.NO_REPLY:
                silent.add(address)
            released[pool_id] = outcome is Release.DONE
        if all(released.values()):
            self._close_session_quietly()
        else:
            logger.error(
                "%d remote CXL region(s) could not be released at their region mapper; "
                "MaruServer keeps them",
                list(released.values()).count(False),
            )
            for pool_id, ok in released.items():
                if ok:
                    self._call_retrying("return_region", pool_id=pool_id)
        self.regions.clear()

    def _close_session_quietly(self) -> None:
        if self._credentials and not self.config.metadata_only:
            self._call_retrying("close", drained=True)

    def _call_retrying(self, action: str, **payload: Any) -> bool:
        """Send an idempotent request a few times; True once it succeeded."""
        for _ in range(CALL_ATTEMPTS):
            try:
                self._call(action, **payload)
                return True
            except StorageError as exc:
                logger.warning("Remote CXL %s refused: %s", action, exc)
                return False
            except Exception:
                continue
        logger.warning("Remote CXL %s could not be confirmed", action)
        return False

    # --------------------------------------------------------- more space

    def _may_expand(self) -> bool:
        """Whether to ask for one more region: not while every region mapper
        of the pool is down, since no new region could be mapped."""
        return self.config.auto_expand and not self._data.all_down(self._mappers)

    def _expand(self) -> bool:
        """One more region from MaruServer, as a cxl Handler's auto_expand.

        The request carries an ``op_id``; when its reply is lost the next
        attempt resends the same id and gets the region MaruServer gave.
        """
        op_id = self._expand_op = self._expand_op or uuid.uuid4().hex
        try:
            result = self._call("expand", op_id=op_id)
        except StorageError as exc:
            self._expand_op = None
            logger.info("Remote CXL expand refused (%s); evicting instead", exc)
            return False
        except Exception:
            logger.warning(
                "Remote CXL expand outcome unknown; resending it later", exc_info=True
            )
            return False
        self._expand_op = None
        region = self.regions.get(result["pool"]["pool_id"])
        if region is None:
            region = self._add_region(result["pool"])
        if not region.mapped:
            self._give_up(region)
            return False
        return True

    def _give_up(self, region: RemoteCxlRegion) -> None:
        """Give back at once a new region whose mapper cannot map it.

        No RDMA was ever sent to it. It still needs a release first if a map
        request may have reached its mapper (no reply came, or the
        background retry mapped it meanwhile). The next expand gets a new
        region number, which may fall on another memory node. A region that
        cannot go back now stays until the close.
        """
        region.retire()
        if region.mapped or self._data.map_attempted(region.pool_id):
            region.mapped = True
            outcome = self._data.release(region.pool_id, region.handle, self._mappers)
            if outcome is not Release.DONE:
                return
        if self._call_retrying("return_region", pool_id=region.pool_id):
            self.regions.pop(region.pool_id, None)

    def _evict(self) -> bool:
        """Remove this session's least recently used, unleased keys."""
        if self._retry_removes():
            return True
        pages = sum(r.capacity // r.page_size for r in self.regions.values())
        count = max(1, min(EVICT_BATCH, pages // 16))
        try:
            victims = self._call("victims", count=count)["entries"]
        except Exception:
            logger.warning("Remote CXL eviction failed", exc_info=True)
            return False
        if not victims:
            return False
        for entry in victims:
            self._keys.pop(entry["key"], None)
        try:
            statuses = self._call("remove", entries=victims)["statuses"]
        except Exception:
            # The server may have removed them already; a retry tells.
            self._removing.extend(victims)
            logger.warning("Remote CXL eviction outcome unknown; retrying later")
            return False
        return self._reclaim(victims, statuses)

    def _reclaim(self, entries: list[dict], statuses: list[str]) -> bool:
        """Reuse the pages of removed keys; True if any page came back."""
        removed = False
        for entry, status in zip(entries, statuses, strict=True):
            if status in ("REMOVED", "ABSENT"):
                location = StorageLocation(**entry["location"])
                region = self.regions.get(location.pool_id)
                if region is not None:
                    region.give_page(location)
                    removed = True
        return removed

    def _retry_removes(self) -> bool:
        """Resend removes whose reply was lost; True if a page came back."""
        if not self._removing or not self.regions:
            return False
        entries = self._removing[:MAX_STORAGE_BATCH]
        try:
            statuses = self._call("remove", entries=entries)["statuses"]
        except Exception:
            logger.warning("Remote CXL eviction retry failed", exc_info=True)
            return False
        del self._removing[: len(entries)]
        return self._reclaim(entries, statuses)

    # ------------------------------------------------------ keys that go

    def _set_aside(self, region: RemoteCxlRegion) -> None:
        """Stop using a region its mapper cannot map, and remove its keys.

        Without the removal, probes keep reporting the keys as hits and every
        request with that prefix reads, fails and recomputes. A background
        retry maps the region again later (:meth:`_retry_unreachable`).
        """
        region.reachable = False
        self._drop_keys(region)

    def _drop_keys(self, region: RemoteCxlRegion) -> None:
        """Remove every key of a region from MaruServer.

        A key a reader holds under a lease is kept by MaruServer; it is
        removed by a later retry (:meth:`_retry_drops`).
        """
        entries = [
            {"key": key, "location": asdict(location)}
            for key, location in self._keys.items()
            if location.pool_id == region.pool_id
        ]
        for entry in entries:
            self._keys.pop(entry["key"], None)
        self._dropping.extend(entries)
        self._retry_drops()

    def _retry_drops(self) -> None:
        """Resend removes of keys that must go until MaruServer drops them."""
        pending, self._dropping = self._dropping, []
        for i in range(0, len(pending), MAX_STORAGE_BATCH):
            batch = pending[i : i + MAX_STORAGE_BATCH]
            try:
                statuses = self._call("remove", entries=batch)["statuses"]
            except Exception:
                self._dropping.extend(batch)
                logger.warning("removing keys of a remote CXL region failed; retrying")
                continue
            self._reclaim(batch, statuses)
            for entry, status in zip(batch, statuses, strict=True):
                region = self.regions.get(entry["location"]["pool_id"])
                if status == "KEPT" and region is not None and not region.retired:
                    self._dropping.append(entry)  # leased now; try again later

    # ---------------------------------------------- unconfirmed transfers

    def _reap_isolated(self) -> None:
        """Give back the pages and slots of transfers NIXL has released."""
        kept = []
        for entry in self._isolated:
            if entry.pending.finished or entry.pending.poll():
                # Pages go back to the region they came from; a retired
                # region ignores them.
                if entry.region is not None:
                    for location in entry.locations:
                        entry.region.unquarantine(location)
                        entry.region.give_page(location)
                if self._data.staging is not None:
                    for slot in entry.slots:
                        self._data.staging.give(slot)
            else:
                kept.append(entry)
        self._isolated = kept

    def _settle_writes(self, timeout_s: float) -> bool:
        """Wait (bounded) for the unconfirmed WRITEs to end.

        This spares those WRITEs an error when the regions are released; it
        is not what keeps them out of a returned region (the release is).

        Returns:
            True if no WRITE is unconfirmed.
        """
        deadline = time.monotonic() + timeout_s
        while True:
            self._reap_isolated()
            if not any(e.op == "WRITE" for e in self._isolated):
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(SETTLE_POLL_S)
