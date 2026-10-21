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
from maru_remote.region_map import pick_agent

from .datapath import RdmaDataPath, Release
from .region import IsolatedTransfer, RegionAccess, RemoteCxlRegion

logger = logging.getLogger(__name__)

EVICT_BATCH = 32
SETTLE_POLL_S = 0.01
CALL_ATTEMPTS = 3
# A region set aside keeps its keys this long, as MaruServer keeps those of a
# region it took over: a remote CXL agent that restarts maps it again in time.
UNREACHABLE_KEYS_S = 30.0


class RegionLifecycle:
    """Regions of one storage session; mixed into the remote CXL storage client."""

    config: Any
    regions: dict[str, RemoteCxlRegion]
    connected: bool
    _data: RdmaDataPath
    _agents: list[str]
    _unreturned: list[RemoteCxlRegion]
    _keys: dict[str, StorageLocation]
    _removing: list[dict]
    _dropping: list[dict]
    _isolated: list[IsolatedTransfer]
    # Pages of late-completed WRITEs waiting for the access policy
    _late_pages: list[tuple[RemoteCxlRegion, list[StorageLocation]]]
    _expand_op: str | None
    _credentials: dict
    _lock: threading.RLock

    def _call(self, action: str, **payload: Any) -> dict:
        raise NotImplementedError

    def _heartbeat(self) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------ mapping

    def _add_region(self, info: dict) -> RemoteCxlRegion:
        """Track a region the session got and map it now.

        A region whose agent cannot map it is kept aside; new pages then
        come from another region, a new one if needed.
        """
        region = RemoteCxlRegion(
            info["pool_id"],
            info["capacity"],
            self.config.chunk_size_bytes,
            info["handle"],
        )
        policy = getattr(self, "_policy", None)
        if policy is not None:
            region.reuse_guard = policy.reusable
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
        return self._data.region(pool_id, handle, self._agents, own=own)

    def _map_own(self, region: RemoteCxlRegion) -> RegionAccess:
        """Access to a region of this session, mapping it if needed."""
        access = self._region(region.pool_id, region.handle, own=True)
        self._adopt(region, access)
        return access

    def _adopt(self, region: RemoteCxlRegion, access: RegionAccess) -> None:
        """Note a map of an own region; one on a rebooted memory node goes back.

        The caller checks ``region.retired`` before it writes to the region.
        """
        region.mapped = True
        if region.boot and access.boot != region.boot:
            self._give_back_rebooted(region)
            return
        first = not region.boot
        region.boot = access.boot
        if first and access.boot:
            # MaruServer learns the boot as the region starts to be used: if
            # this worker dies, MaruServer can tell whether the memory node
            # rebooted before it took the region over. Until a report reaches
            # it, a region left behind goes back with its keys.
            try:
                self._heartbeat()
            except Exception:
                logger.warning(
                    "reporting the boot of remote CXL region %d failed; the "
                    "next heartbeat sends it",
                    region.region_id,
                )

    def _give_back_rebooted(self, region: RemoteCxlRegion) -> None:
        """Give back an own region whose memory node rebooted.

        A WRITE the memory node's NIC completed may have sat in that node's
        CPU cache; a reboot lost it while the key stayed committed. Giving
        the region back (released first) makes MaruServer stop finding its
        keys at once and end the leases on them, so a reader holding one
        drops what it read; removing the keys one by one would leave a
        leased key found. The next store gets a new region. A region that
        cannot go back now has its keys removed meanwhile and goes back at
        the close; it keeps the boot it was written under, so MaruServer
        also sees the reboot if it takes the region over.
        """
        logger.warning(
            "memory node of remote CXL region %d rebooted; giving the region back",
            region.region_id,
        )
        entries = self._forget_keys(region)
        region.retire()
        outcome = self._data.release(region.pool_id, region.handle, self._agents)
        if outcome is Release.DONE and self._call_retrying(
            "return_region", pool_id=region.pool_id
        ):
            self.regions.pop(region.pool_id, None)
            return
        self._unreturned.append(region)
        self._dropping.extend(entries)
        self._retry_drops()

    def _retry_returns(self) -> None:
        """Give back again the rebooted regions that could not go back."""
        pending, self._unreturned = self._unreturned, []
        for region in pending:
            if self.regions.get(region.pool_id) is not region:
                continue  # gone with the close
            outcome = self._data.release(region.pool_id, region.handle, self._agents)
            if outcome is Release.DONE and self._call_retrying(
                "return_region", pool_id=region.pool_id
            ):
                self.regions.pop(region.pool_id, None)
                self._dropping = [
                    e
                    for e in self._dropping
                    if e["location"]["pool_id"] != region.pool_id
                ]
            else:
                self._unreturned.append(region)

    def _retry_unreachable(self) -> None:
        """Map regions set aside earlier; one that maps is used again.

        A region that still cannot be mapped :data:`UNREACHABLE_KEYS_S` after
        it was set aside has its keys removed. Runs without the client lock:
        reaching a node that is down can take the connect timeout, and stores
        must not wait for it.
        """
        for region in list(self.regions.values()):
            if region.reachable or region.retired:
                continue
            try:
                access = self._region(region.pool_id, region.handle, own=True)
            except StorageUnavailableError:
                since = region.unreachable_since
                if since is not None and time.monotonic() - since >= UNREACHABLE_KEYS_S:
                    with self._lock:
                        if not region.reachable and not region.retired:
                            region.unreachable_since = None
                            logger.warning(
                                "Remote CXL region %d stayed unreachable; removing "
                                "its keys",
                                region.region_id,
                            )
                            self._drop_keys(region)
                continue
            with self._lock:
                if region.retired:
                    # It is going back and was mapped meanwhile: undo the
                    # registration. If that fails, whoever gives the region
                    # back sees it mapped and releases it first.
                    region.mapped = True
                    self._data.release(region.pool_id, region.handle, self._agents)
                    continue
                self._adopt(region, access)
                region.reachable = True
                region.unreachable_since = None
            logger.info("Remote CXL region %d is reachable again", region.region_id)

    def _check_boots(self) -> None:
        """Give back own regions whose memory node rebooted.

        Asks every remote CXL agent of a mapped own region for its boot id, so a
        reboot is noticed even for a region this worker no longer writes.
        Runs without the client lock until there is something to change.
        """
        owned: dict[str, list[RemoteCxlRegion]] = {}
        for region in list(self.regions.values()):
            if region.boot and not region.retired:
                owned.setdefault(pick_agent(self._agents, region.region_id), []).append(
                    region
                )
        if not owned:
            return
        boots = self._data.boots(list(owned))
        with self._lock:
            for address, boot in boots.items():
                for region in owned[address]:
                    if region.retired or not boot or boot == region.boot:
                        continue
                    self._give_back_rebooted(region)

    # ---------------------------------------------------------- give back

    def _return_regions(self) -> None:
        """Give every region back in the order that keeps late WRITEs out.

        Used when MaruServer cannot take the regions over: it restarted (a
        new epoch) or never granted the session.

        1. Wait (bounded) for the WRITEs sent to the regions; this only
           spares them an error.
        2. Have each region's agent deregister it (``release``). From the
           reply on, an RDMA to the region fails at the memory node's NIC.
           A region for which no map request ever reached a agent needs no
           release: no RDMA was ever sent to it. After a remote CXL agent does
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
            address = pick_agent(self._agents, region.region_id)
            if address in silent:
                released[pool_id] = False
                continue
            outcome = self._data.release(pool_id, region.handle, self._agents)
            if outcome is Release.NO_REPLY:
                silent.add(address)
            released[pool_id] = outcome is Release.DONE
        if all(released.values()):
            self._close_session_quietly()
        else:
            logger.error(
                "%d remote CXL region(s) could not be released at their remote CXL agent; "
                "MaruServer keeps them",
                list(released.values()).count(False),
            )
            for pool_id, ok in released.items():
                if ok:
                    self._call_retrying("return_region", pool_id=pool_id)
        self.regions.clear()

    def _leave(self) -> None:
        """Hand the regions over to MaruServer: the worker is done with them.

        A cxl Handler's regions stay while keys refer to them after it
        closes, and other Handlers keep reading those keys. Here MaruServer
        takes the regions over the same way: it keeps the ones with keys,
        holding their registration for readers, and releases the others
        before it frees them. So the owner releases nothing:

        1. Wait (bounded) for the WRITEs sent to the regions. A WRITE that
           is never confirmed lands, if at all, on a page no key refers to.
        2. Give back at once the regions no remote CXL agent ever mapped for
           it: no RDMA was ever sent to them, so they need no release.
        3. Close the session drained.
        4. Drop this worker's own holds at the remote CXL agents.

        If MaruServer does not confirm the close, it takes the regions over
        when the session expires. A session that holds no region (its
        only region went back) still closes.
        """
        for region in self.regions.values():
            region.retire()
        if self.regions and not self._settle_writes(self.config.rdma_timeout_s):
            logger.warning("RDMA WRITEs are still unconfirmed; their pages hold no key")
        for pool_id, region in list(self.regions.items()):
            if not region.mapped and not self._data.map_attempted(pool_id):
                if self._call_retrying("return_region", pool_id=pool_id):
                    del self.regions[pool_id]
        if not self._close_session_quietly():
            logger.warning(
                "MaruServer did not confirm the close; it takes the regions over "
                "when the session expires"
            )
        for pool_id, region in self.regions.items():
            self._data.unmap(pool_id, region.handle, self._agents)
        self.regions.clear()

    def _close_session_quietly(self) -> bool:
        if self._credentials and not self.config.metadata_only:
            # The boot of each region's memory node goes along, so MaruServer
            # can tell a reboot after it takes a region over.
            boots = [
                {"pool_id": region.pool_id, "boot": region.boot}
                for region in self.regions.values()
                if region.boot
            ]
            return self._call_retrying("close", drained=True, pools=boots)
        return True

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
        """Whether to ask for one more region: not while no remote CXL agent of
        the pool answers, since no new region could be mapped and a store
        would wait for the map's timeout. The heartbeat's boot check and the
        retry of set-aside regions notice when one answers again."""
        return (
            (self.config.auto_expand or not self.regions)
            and any(self._data.answering(m) for m in self._agents)
            and not self._data.all_down(self._agents)
        )

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
        """Give back at once a new region whose agent cannot map it.

        No RDMA was ever sent to it. It still needs a release first if a map
        request may have reached its agent (no reply came, or the
        background retry mapped it meanwhile). The next expand gets a new
        region number, which may fall on another memory node. A region that
        cannot go back now stays until the close.
        """
        region.retire()
        if region.mapped or self._data.map_attempted(region.pool_id):
            region.mapped = True
            outcome = self._data.release(region.pool_id, region.handle, self._agents)
            if outcome is not Release.DONE:
                return
        if self._call_retrying("return_region", pool_id=region.pool_id):
            self.regions.pop(region.pool_id, None)

    def _writable(self, region: RemoteCxlRegion) -> bool:
        """Whether a WRITE can reach a region now: it is in use and its
        remote CXL agent answered since its last unanswered request."""
        return (
            region.reachable
            and not region.retired
            and self._data.answering(pick_agent(self._agents, region.region_id))
        )

    def _evict(self) -> bool:
        """Remove this session's least recently used, unleased keys.

        Only keys in regions a WRITE can reach now go: a page freed in a
        region whose memory node does not answer could not be written, and
        the key's bytes there stay readable once the node answers again.
        """
        if self._retry_removes():
            return True
        usable = [r.pool_id for r in self.regions.values() if self._writable(r)]
        if not usable:
            return False
        pages = sum(r.capacity // r.page_size for r in self.regions.values())
        count = max(1, min(EVICT_BATCH, pages // 16))
        try:
            victims = self._call("victims", count=count, pools=usable)["entries"]
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
        self._track_kept(victims, statuses)
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
        self._track_kept(entries, statuses)
        return self._reclaim(entries, statuses)

    def _track_kept(self, entries: list[dict], statuses: list[str]) -> None:
        """Track again the evicted keys MaruServer kept.

        A victim was unleased when MaruServer picked it, but another session
        may lease it before the remove: MaruServer then keeps the key, and
        this session must still remove it when the region's keys go. Keys of
        a region that already went back, or whose keys were removed, stay
        with the drops that handle that region.
        """
        for entry, status in zip(entries, statuses, strict=True):
            if status != "KEPT":
                continue
            region = self.regions.get(entry["location"]["pool_id"])
            if region is None or region.retired:
                continue  # MaruServer drops the keys of a region given back
            if not region.reachable and region.unreachable_since is None:
                self._dropping.append(entry)  # its region's keys were removed
            else:
                self._keys.setdefault(
                    entry["key"], StorageLocation(**entry["location"])
                )

    # ------------------------------------------------------ keys that go

    def _set_aside(self, region: RemoteCxlRegion) -> None:
        """Stop using a region its agent cannot map.

        A background retry maps it again (:meth:`_retry_unreachable`); one
        that maps within :data:`UNREACHABLE_KEYS_S` keeps its keys, as after a
        remote CXL agent restart. Later the keys are removed: otherwise probes
        keep reporting them as hits and every request with that prefix reads,
        fails and recomputes.
        """
        region.reachable = False
        if region.unreachable_since is None:
            region.unreachable_since = time.monotonic()

    def _drop_keys(self, region: RemoteCxlRegion) -> None:
        """Remove every key of a region from MaruServer.

        A key a reader holds under a lease is kept by MaruServer; it is
        removed by a later retry (:meth:`_retry_drops`).
        """
        self._dropping.extend(self._forget_keys(region))
        self._retry_drops()

    def _forget_keys(self, region: RemoteCxlRegion) -> list[dict]:
        """Stop tracking a region's keys; returns them as remove entries."""
        entries = [
            {"key": key, "location": asdict(location)}
            for key, location in self._keys.items()
            if location.pool_id == region.pool_id
        ]
        for entry in entries:
            self._keys.pop(entry["key"], None)
        return entries

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
        """Give back the pages and slots of transfers NIXL has released.

        The slots go back once NIXL released the transfer. A WRITE's remote
        pages go back only if it completed: one released after an error may
        still be in the network, so its pages stay out of use while this
        worker owns the region, as after a WRITE that fails at once.
        """
        policy = getattr(self, "_policy", None)
        kept = []
        for entry in self._isolated:
            if entry.pending.finished or entry.pending.poll():
                # Pages go back to the region they came from; a retired
                # region ignores them.
                if entry.region is not None and entry.pending.completed:
                    if policy is not None and not entry.region.retired:
                        # The access policy prepared these pages for the WRITE
                        # that turned out complete: they wait for it to take
                        # them (_settle_late_pages). The slots do not.
                        self._late_pages.append((entry.region, list(entry.locations)))
                    else:
                        for location in entry.locations:
                            entry.region.unquarantine(location)
                            entry.region.give_page(location)
                if self._data.staging is not None:
                    for slot in entry.slots:
                        self._data.staging.give(slot)
            else:
                kept.append(entry)
        self._isolated = kept
        self._settle_late_pages()

    def _settle_late_pages(self) -> None:
        """Hand the pages of late-completed WRITEs to the access policy, then
        back to their region; a page the policy did not take waits for the
        next housekeeping pass."""
        policy = getattr(self, "_policy", None)
        waiting = []
        for region, locations in self._late_pages:
            if region.retired:
                continue  # its pages went back with it
            try:
                policy.completed_write(locations)
            except Exception as exc:
                logger.warning("access policy did not take a late WRITE: %s", exc)
                waiting.append((region, locations))
                continue
            for location in locations:
                region.unquarantine(location)
                region.give_page(location)
        self._late_pages = waiting

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
