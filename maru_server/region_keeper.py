# SPDX-License-Identifier: Apache-2.0
"""MaruServer's side of the regions whose owner left.

On a CXL-attached node, MaruServer keeps a cxl Handler's region while keys
refer to it after the Handler closed, and other Handlers keep reading it
through their own mmap. A remote CXL region is read through one
registration at its remote CXL agent instead, which someone must hold while
readers read and remove before the region's memory is reused. Once the
owner left, MaruServer is the one that decides when the region goes back,
so it does both, from this thread:

* A kept region (its keys are still found) is mapped at its remote CXL agent
  with the region's handle, so the registration stays while readers read.
  A remote CXL agent that restarted is asked again; one whose memory node
  rebooted, or that refuses the handle, has the region given back.
* A region to give back is released at its remote CXL agent (the NIC refuses
  RDMA to it from then on) and only then freed at the resource manager.

The remote CXL agent calls run here, never in the request thread, so a memory
node that does not answer delays no heartbeat.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from maru_remote.region_map import (
    RegionMapClient,
    RegionMapError,
    RegionMapUnavailable,
    pick_agent,
)

logger = logging.getLogger(__name__)

INTERVAL_S = 1.0
# How often held regions' memory nodes are asked for their boot
CHECK_S = 10.0
# A remote CXL agent that did not answer is not asked again for this long
DOWN_S = 5.0
CONNECT_TIMEOUT_S = 3.0
# A reply that takes longer counts as no answer. Mapping registers a whole
# region with the NIC, which takes well under a second; one slow node must
# not hold up the others for long, since the keeper is one thread.
REPLY_TIMEOUT_S = 10.0
# A kept region whose remote CXL agent does not answer this long stops being
# found, as a live owner removes the keys of a region it cannot reach: its
# keys would keep being hits that fail to read.
UNREACHABLE_S = 30.0


class RegionKeeper:
    """Holds the kept regions' mappings and releases the regions to give back.

    The directory, the remote CXL agent list and the client factory are passed
    in so tests can use fakes.
    """

    def __init__(
        self,
        directory: Any,
        agents: list[str],
        key: bytes | None,
        *,
        client_factory: Callable[[str], Any] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create an idle keeper; :meth:`start` runs it.

        Args:
            directory: The ``ReplicaDirectory`` of this MaruServer.
            agents: Remote CXL agent addresses of the pool, as handed to workers.
            key: Remote CXL agent key.
            client_factory: ``address -> RegionMapClient``.
            clock: Monotonic clock.
        """
        self._directory = directory
        self._agents = list(agents)
        self._key = key
        self._factory = client_factory or self._default_client
        self._clock = clock
        self._clients: dict[str, Any] = {}
        # pool_id -> (address, connection generation, boot) of the hold
        self._held: dict[str, tuple[str, int, str]] = {}
        self._down: dict[str, float] = {}
        # Kept region -> when its remote CXL agent first failed to answer
        self._unreachable: dict[str, float] = {}
        self._next_check = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Run the keeper on a daemon thread."""
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._run, daemon=True, name="maru-region-keeper"
            )
            self._thread.start()

    def stop(self) -> None:
        """Give back the regions MaruServer took over, then stop.

        The resource manager reclaims every region of a MaruServer that goes
        away. A region this keeper holds is released at its remote CXL agent
        first, so a reader's hold does not keep it registered after the
        resource manager hands it out again. A region whose remote CXL agent
        does not answer is left to the resource manager, as when MaruServer
        dies.
        """
        self._stop.set()
        self._directory.keeper_wakeup.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None
        try:
            self._give_back_all()
        except Exception:
            logger.warning("giving back the kept regions failed", exc_info=True)
        for client in self._clients.values():
            client.close()
        self._clients.clear()

    def _give_back_all(self) -> None:
        for pool_id, _handle, fate, _boot in self._directory.departed():
            if fate == "keep":
                self._directory.give_back(pool_id, "maru-server is stopping")
        for pool_id, handle, _fate, _boot in self._directory.departed():
            address = pick_agent(self._agents, int(handle["region_id"]))
            if not self._is_down(address):
                self._release(pool_id, handle, address)

    def run_once(self) -> None:
        """Do what the departed regions need now."""
        self._directory.make_room()
        work = self._directory.departed()
        current = {item[0] for item in work}
        for pool_id in set(self._held) - current:
            del self._held[pool_id]
        for pool_id in set(self._unreachable) - current:
            del self._unreachable[pool_id]
        now = self._clock()
        check = now >= self._next_check
        if check:
            self._next_check = now + CHECK_S
        for pool_id, handle, fate, boot in work:
            address = pick_agent(self._agents, int(handle["region_id"]))
            if self._is_down(address):
                if fate == "keep":
                    self._note_unreachable(pool_id, now)
                continue
            if fate == "return":
                self._release(pool_id, handle, address)
            else:
                self._keep(pool_id, handle, boot, address, check)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._directory.keeper_wakeup.wait(INTERVAL_S)
            self._directory.keeper_wakeup.clear()
            if self._stop.is_set():
                return
            try:
                self.run_once()
            except Exception:
                logger.warning("region keeper pass failed", exc_info=True)

    def _keep(
        self, pool_id: str, handle: dict, boot: str, address: str, check: bool
    ) -> None:
        """Hold a kept region; give it back if its bytes may be stale.

        A held region is mapped again at every check: the map shares the
        registration if it is still there, maps the region again after the
        remote CXL agent restarted (the connection opens anew), and is refused
        if someone released the region meanwhile.
        """
        client = self._client(address)
        held = self._held.get(pool_id)
        if held is not None and held[1] == client.generation and not check:
            return
        try:
            now_boot = client.map(handle).boot
        except RegionMapUnavailable as exc:
            self._held.pop(pool_id, None)
            self._mark_down(address, exc)
            self._note_unreachable(pool_id, self._clock())
            return
        except RegionMapError as exc:
            self._held.pop(pool_id, None)
            if exc.code == "denied":
                # Released meanwhile (by an owner that only seemed gone), or
                # the agent has another region under this id: the keys
                # cannot be read any more.
                self._directory.give_back(
                    pool_id, f"its remote CXL agent refused: {exc}"
                )
            else:
                logger.warning("holding region %s failed: %s", handle["region_id"], exc)
            return
        self._unreachable.pop(pool_id, None)
        known = boot or (held[2] if held is not None else "")
        if known and now_boot and now_boot != known:
            # The memory node rebooted: bytes its CPU cache held are gone.
            self._directory.give_back(pool_id, "its memory node rebooted")
            return
        self._held[pool_id] = (address, client.generation, known or now_boot)

    def _note_unreachable(self, pool_id: str, now: float) -> None:
        """Give a kept region back once its remote CXL agent is gone too long."""
        since = self._unreachable.setdefault(pool_id, now)
        if now - since >= UNREACHABLE_S:
            del self._unreachable[pool_id]
            self._held.pop(pool_id, None)
            self._directory.give_back(
                pool_id,
                f"its remote CXL agent did not answer for {UNREACHABLE_S:.0f} s",
            )

    def _release(self, pool_id: str, handle: dict, address: str) -> None:
        """Release a region at its remote CXL agent, then free it."""
        try:
            self._client(address).release(handle)
        except RegionMapUnavailable as exc:
            self._mark_down(address, exc)
            return
        except RegionMapError as exc:
            if exc.code != "denied":
                # The deregistration failed: the region stays registered.
                logger.warning(
                    "releasing region %s failed: %s", handle["region_id"], exc
                )
                return
            # denied: the agent maps another region under this id, so this
            # one is not registered there.
        self._held.pop(pool_id, None)
        self._directory.given_back(pool_id)

    def _client(self, address: str) -> Any:
        client = self._clients.get(address)
        if client is None:
            client = self._clients[address] = self._factory(address)
        return client

    def _default_client(self, address: str) -> RegionMapClient:
        return RegionMapClient(
            address,
            timeout_s=REPLY_TIMEOUT_S,
            connect_timeout_s=CONNECT_TIMEOUT_S,
            key=self._key,
        )

    def _is_down(self, address: str) -> bool:
        until = self._down.get(address)
        return until is not None and self._clock() < until

    def _mark_down(self, address: str, exc: Exception) -> None:
        logger.warning("remote CXL agent %s did not answer: %s", address, exc)
        self._down[address] = self._clock() + DOWN_S
