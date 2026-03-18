"""Region state machine for auth-controlled allocation.

States:
    CREATING      → Region being created (perm_set_default + perm_grant in progress)
    CHOWN_PENDING → ALLOC_OK sent, waiting for owner to CHOWN + re-grant GRANT to server
    ACTIVE        → Owner confirmed GRANT_READY, region fully operational

ACCESS_REQUESTs arriving during CHOWN_PENDING are queued and processed
when GRANT_READY transitions the region to ACTIVE.
"""

import enum
import logging
import threading
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

CHOWN_TIMEOUT_SEC = 30.0


class RegionState(enum.Enum):
    CREATING = "creating"
    CHOWN_PENDING = "chown_pending"
    ACTIVE = "active"
    ERROR = "error"


@dataclass
class QueuedAccessRequest:
    """Pending ACCESS_REQUEST waiting for GRANT_READY."""

    region_id: int
    node_id: int
    pid: int
    role: str
    event: threading.Event = field(default_factory=threading.Event)
    granted_perms: int = 0
    error: str = ""


@dataclass
class RegionInfo:
    state: RegionState
    owner_spiffe_id: str
    created_at: float = field(default_factory=time.time)
    queued_requests: list[QueuedAccessRequest] = field(default_factory=list)


class RegionStateManager:
    """Thread-safe region state machine with request queuing."""

    def __init__(self, chown_timeout: float = CHOWN_TIMEOUT_SEC):
        self._regions: dict[int, RegionInfo] = {}
        self._lock = threading.Lock()
        self._chown_timeout = chown_timeout

    def create(self, region_id: int, owner_spiffe_id: str) -> None:
        with self._lock:
            if region_id in self._regions:
                raise ValueError(f"Region {region_id} already exists")
            self._regions[region_id] = RegionInfo(
                state=RegionState.CREATING,
                owner_spiffe_id=owner_spiffe_id,
            )
            logger.debug("region %d: CREATING", region_id)

    def transition_to_chown_pending(self, region_id: int) -> None:
        with self._lock:
            info = self._get_region(region_id)
            if info.state != RegionState.CREATING:
                raise ValueError(
                    f"Region {region_id}: cannot transition from {info.state} to CHOWN_PENDING"
                )
            info.state = RegionState.CHOWN_PENDING
            logger.debug("region %d: CREATING → CHOWN_PENDING", region_id)

    def grant_ready(
        self, region_id: int, caller_spiffe_id: str
    ) -> list[QueuedAccessRequest]:
        """Transition to ACTIVE and return queued requests for processing.

        Only the region owner can call GRANT_READY.
        """
        with self._lock:
            info = self._get_region(region_id)
            if info.state != RegionState.CHOWN_PENDING:
                raise ValueError(
                    f"Region {region_id}: cannot transition from {info.state} to ACTIVE"
                )
            if info.owner_spiffe_id != caller_spiffe_id:
                raise PermissionError(
                    f"Region {region_id}: GRANT_READY from non-owner "
                    f"(expected {info.owner_spiffe_id}, got {caller_spiffe_id})"
                )
            info.state = RegionState.ACTIVE
            queued = list(info.queued_requests)
            info.queued_requests.clear()
            logger.debug(
                "region %d: CHOWN_PENDING → ACTIVE (%d queued requests)",
                region_id,
                len(queued),
            )
            return queued

    def queue_access_request(
        self, region_id: int, node_id: int, pid: int, role: str
    ) -> QueuedAccessRequest:
        """Queue an ACCESS_REQUEST for a CHOWN_PENDING region.

        Returns a QueuedAccessRequest whose event will be set when processed.
        """
        with self._lock:
            info = self._get_region(region_id)
            if info.state != RegionState.CHOWN_PENDING:
                raise ValueError(
                    f"Region {region_id}: not in CHOWN_PENDING state"
                )
            req = QueuedAccessRequest(
                region_id=region_id,
                node_id=node_id,
                pid=pid,
                role=role,
            )
            info.queued_requests.append(req)
            logger.debug(
                "region %d: queued ACCESS_REQUEST (node=%d, pid=%d, role=%s)",
                region_id,
                node_id,
                pid,
                role,
            )
            return req

    def get_state(self, region_id: int) -> RegionState:
        with self._lock:
            return self._get_region(region_id).state

    def get_owner(self, region_id: int) -> str:
        with self._lock:
            return self._get_region(region_id).owner_spiffe_id

    def remove(self, region_id: int) -> None:
        with self._lock:
            self._regions.pop(region_id, None)

    def _get_region(self, region_id: int) -> RegionInfo:
        info = self._regions.get(region_id)
        if info is None:
            raise KeyError(f"Region {region_id} not found")
        return info
