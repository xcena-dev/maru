# SPDX-License-Identifier: Apache-2.0
"""The public NIXL native API needed by Maru's DRAM-only transport.

NIXL is imported only when constructing an adapter. Transfer handles have no
finalizer: NixlTransport owns their explicit release and uncertain DMA lifetime.
NIXL 1.x public API capabilities are checked together with its CUDA wheel.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from typing import Any, cast

_REQUIRED = (
    "nixlAgent",
    "nixlAgentConfig",
    "nixlRegDList",
    "nixlXferDList",
    "DRAM_SEG",
    "NIXL_READ",
    "NIXL_WRITE",
    "NIXL_SUCCESS",
    "NIXL_IN_PROG",
    "NIXL_THREAD_SYNC_NONE",
)
_METHODS = (
    "createBackend",
    "getBackendParams",
    "registerMem",
    "deregisterMem",
    "getLocalMD",
    "getLocalPartialMD",
    "loadRemoteMD",
    "invalidateRemoteMD",
    "genNotif",
    "getNotifs",
    "createXferReq",
    "postXferReq",
    "getXferStatus",
    "releaseXferReq",
)


def _load_bindings() -> Any:
    requirement = (
        "Maru native transport requires nixl>=1.2.0,<2 with public native APIs "
        "and a matching nixl-cu wheel"
    )
    try:
        api = import_module("nixl")
        missing = [name for name in _REQUIRED if not hasattr(api, name)]
        if missing:
            raise ImportError(
                f"{requirement}; public exports missing: {', '.join(missing)}"
            )
        missing = [name for name in _METHODS if not hasattr(api.nixlAgent, name)]
        if missing:
            raise ImportError(f"{requirement}; native agent methods missing: {missing}")
        wheel = api.nixlAgent.__module__.split(".", 1)[0].replace("_", "-")
        if not re.fullmatch(r"nixl-cu\d+", wheel):
            raise ImportError(f"{requirement}; unknown native binding origin {wheel!r}")
        versions = {name: version(name) for name in ("nixl", wheel)}
        for found in versions.values():
            release = re.fullmatch(r"1\.(\d+)\.\d+(?:[.+-].*)?", found)
            if release is None or int(release[1]) < 2:
                raise ImportError(f"{requirement}; found {versions}")
        if len(set(versions.values())) != 1:
            raise ImportError(f"{requirement}; wheel versions differ: {versions}")
    except (ModuleNotFoundError, PackageNotFoundError) as exc:
        raise ImportError(f"{requirement}; install maru[rdma]") from exc
    return api


@dataclass(eq=False, slots=True)
class NativeTransfer:
    """Opaque request retaining its native agent until explicit release.

    This object deliberately has no destructor, cancellation, or automatic free.
    """

    owner: Any = field(repr=False)
    value: int = 0
    released: bool = False


class NativeNixlAgent:
    """Provide only the native-backed operations NixlTransport uses.

    Registration and transfer inputs are DRAM tuple descriptors, not tensors or
    NumPy arrays. Descriptor order is preserved to keep paired ranges aligned.
    NixlTransport serializes calls; UCX MULTI protects its own progress thread.
    """

    def __init__(
        self, name: str, *, progress_thread: bool = True, event_timeout_us: int = 1000
    ) -> None:
        """Create an agent without a backend or registered memory.

        Args:
            name: Unique local agent name.
            progress_thread: Run NIXL's event-driven UCX progress thread.
            event_timeout_us: Periodic event-wait fallback; UCX rounds up to ms.

        Raises:
            ImportError: NIXL or its required public native API is unavailable.
            ValueError: The timeout cannot be represented by NIXL 1.2's int path.
        """
        if (
            not isinstance(event_timeout_us, int)
            or not 0 <= event_timeout_us <= 2147483647
        ):
            raise ValueError("event_timeout_us must be an integer in [0, INT_MAX]")
        self._api = _load_bindings()
        config = self._api.nixlAgentConfig()
        config.useProgThread = progress_thread
        config.useListenThread = False
        config.syncMode = self._api.NIXL_THREAD_SYNC_NONE
        config.pthrDelay = event_timeout_us
        config.lthrDelay = 100000
        config.captureTelemetry = False
        self.name = name
        self._native = self._api.nixlAgent(name, config)

    def create_backend(self, backend: str, params: dict[str, str]) -> None:
        """Initialize UCX, propagating native plugin/configuration errors."""
        if backend != "UCX":
            raise ValueError("Maru native transport supports only UCX")
        handle = self._native.createBackend(backend, dict(params))
        # Match the wrapper's initialization check without retaining raw pointers.
        self._native.getBackendParams(handle)

    def register_memory(self, descs: Any, mem_type: str = "DRAM") -> Any:
        """Register four-tuple DRAM ranges and return their native descriptor list."""
        self._require_dram(mem_type)
        if isinstance(descs, self._api.nixlXferDList):
            raise TypeError("registration requires RegDList, not XferDList")
        if not isinstance(descs, self._api.nixlRegDList):
            descs = self._api.nixlRegDList(self._api.DRAM_SEG, list(descs))
        self._native.registerMem(descs, [])
        return descs

    def deregister_memory(self, registration: Any) -> None:
        """Deregister exactly the descriptor list returned by register_memory."""
        self._native.deregisterMem(registration, [])

    def get_agent_metadata(self) -> bytes:
        """Return full native metadata without changing its bytes."""
        return cast(bytes, self._native.getLocalMD())

    def get_partial_agent_metadata(
        self, registration: Any, inc_conn_info: bool = False
    ) -> bytes:
        """Return metadata for only registration, with optional connection info."""
        return cast(
            bytes, self._native.getLocalPartialMD(registration, inc_conn_info, [])
        )

    def add_remote_agent(self, metadata: bytes) -> bytes:
        """Load metadata and preserve the native peer-name/error result."""
        return cast(bytes, self._native.loadRemoteMD(metadata))

    def remove_remote_agent(self, name: str) -> None:
        """Invalidate a peer; callers must first drain its active requests."""
        self._native.invalidateRemoteMD(name)

    def send_notif(self, name: str, message: bytes) -> None:
        """Send an out-of-band notification through the native UCX backend."""
        self._native.genNotif(name, message, [])

    def get_new_notifs(self) -> dict[str, list[bytes]]:
        """Return only new notifications, as the installed wrapper does."""
        return cast(dict[str, list[bytes]], self._native.getNotifs({}, []))

    def get_xfer_descs(
        self, descs: Iterable[tuple[int, int, int]], mem_type: str
    ) -> Any:
        """Materialize ordered DRAM descriptors before any request is created."""
        self._require_dram(mem_type)
        if isinstance(descs, self._api.nixlRegDList):
            raise TypeError("transfer requires XferDList, not RegDList")
        if isinstance(descs, self._api.nixlXferDList):
            return descs
        return self._api.nixlXferDList(self._api.DRAM_SEG, list(descs))

    def initialize_xfer(
        self, operation: str, local: Any, remote: Any, peer: str
    ) -> NativeTransfer:
        """Prepare a request without posting, releasing, or hiding native errors."""
        ops = {"READ": self._api.NIXL_READ, "WRITE": self._api.NIXL_WRITE}
        handle = NativeTransfer(self._native)
        handle.value = self._native.createXferReq(
            ops[operation], local, remote, peer, b"", []
        )
        return handle

    def transfer(self, handle: NativeTransfer) -> str:
        """Post the request; native exceptions leave its ownership with Transport."""
        value = self._request_value(handle)
        return self._state(self._native.postXferReq(value, b""))

    def check_xfer_state(self, handle: NativeTransfer) -> str:
        """Query state; native exceptions remain exceptions, never synthetic ERR."""
        return self._state(self._native.getXferStatus(self._request_value(handle)))

    def release_xfer_handle(self, handle: NativeTransfer) -> None:
        """Release once; a native release exception leaves the handle unreleased."""
        if handle.owner is not self._native:
            raise ValueError("transfer belongs to another native agent")
        if not handle.released:
            self._native.releaseXferReq(handle.value)
            handle.released = True

    @staticmethod
    def _require_dram(mem_type: str) -> None:
        if mem_type != "DRAM":
            raise ValueError("Maru native transport supports only DRAM descriptors")

    def _request_value(self, handle: NativeTransfer) -> int:
        if handle.owner is not self._native or handle.released:
            raise ValueError("transfer belongs to another agent or was released")
        return handle.value

    def _state(self, status: Any) -> str:
        # Binding throws on negative statuses, exactly as in the Python wrapper.
        if status == self._api.NIXL_SUCCESS:
            return "DONE"
        if status == self._api.NIXL_IN_PROG:
            return "PROC"
        # A new/invalid raw status is not a confirmed terminal error. Preserve
        # its handle through Transport's uncertainty path instead of freeing it.
        raise RuntimeError(f"unexpected NIXL native transfer status {status!r}")
