# SPDX-License-Identifier: Apache-2.0
"""Thin NIXL wrapper: register host memory, exchange metadata, batch READ/WRITE.

The pool node registers its mapped CXL regions; a worker registers its host
staging buffer. Metadata bytes travel over the control channel (ZMQ), not
through NIXL's own side channels, so the transport never opens sockets.

A transfer that misses its deadline is not released: the NIC may still be
moving bytes into or out of the buffers it names. :class:`TransferTimeout`
carries a :class:`PendingTransfer` so the caller can keep those buffers out of
circulation until :meth:`PendingTransfer.poll` reports that the transfer ended.
"""

from __future__ import annotations

import ctypes
import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_POLL_SLEEP_S = 0.0002


def buffer_address(buf: Any) -> int:
    """Return the start address of a host buffer.

    Accepts memoryview, bytearray, numpy arrays and CPU torch tensors.

    Args:
        buf: The buffer object.

    Returns:
        The address of the first byte.
    """
    if hasattr(buf, "data_ptr"):
        return int(buf.data_ptr())
    if hasattr(buf, "ctypes"):
        return int(buf.ctypes.data)
    return ctypes.addressof(ctypes.c_char.from_buffer(buf))


def _default_agent(agent_name: str, ucx_device: str) -> Any:
    """Create a real NIXL agent with only the UCX backend.

    Args:
        agent_name: Unique agent name.
        ucx_device: UCX device string (e.g. ``"mlx5_0:1"``); empty for default.

    Returns:
        A ``nixl_agent`` instance.

    Raises:
        ImportError: if NIXL is not installed (``pip install 'maru[remote]'``).
    """
    from nixl._api import nixl_agent, nixl_agent_config

    agent = nixl_agent(agent_name, nixl_agent_config(backends=[]))
    params = {"ucx_devices": ucx_device} if ucx_device else {}
    agent.create_backend("UCX", params)
    return agent


class PendingTransfer:
    """A transfer that missed its deadline and still owns its buffers."""

    def __init__(self, transport: NixlTransport, op: str, peer: str, handle: Any):
        """Wrap a NIXL transfer handle that was not released.

        Args:
            transport: The transport that created the handle.
            op: ``"READ"`` or ``"WRITE"``.
            peer: Peer name.
            handle: Handle from ``initialize_xfer``.
        """
        self.op = op
        self.peer = peer
        self._transport = transport
        self._handle = handle
        self.finished = False

    def poll(self) -> bool:
        """Check whether the transfer has ended; release the handle if so.

        Returns:
            True once NIXL reports the transfer done or failed. Its buffers
            may then be reused. False while it is still in progress.
        """
        if self.finished:
            return True
        try:
            state = self._transport.check_state(self._handle)
        except Exception as exc:  # a broken agent cannot touch buffers again
            logger.warning(
                "NIXL %s to %s: state check failed: %s", self.op, self.peer, exc
            )
            state = "ERR"
        if state == "PROC":
            return False
        self._transport.release_handle(self.op, self.peer, self._handle)
        self.finished = True
        return True


class TransferTimeout(TimeoutError):  # noqa: N818 (mirrors TimeoutError)
    """A transfer missed its deadline; ``pending`` still owns its buffers."""

    def __init__(self, message: str, pending: PendingTransfer):
        """Create the error.

        Args:
            message: Human-readable description.
            pending: The unreleased transfer.
        """
        super().__init__(message)
        self.pending = pending


class NixlTransport:
    """One NIXL agent with UCX only, exposing byte-range READ/WRITE.

    The agent is created without internal locking, so every call into it
    holds one lock. A transfer takes the lock for each agent call and drops
    it while sleeping between completion polls, so a WRITE can be posted
    while a READ from another thread is still in flight.
    """

    def __init__(
        self, agent_name: str, *, ucx_device: str = "", agent: Any | None = None
    ) -> None:
        """Create the transport.

        Args:
            agent_name: Unique agent name.
            ucx_device: UCX device string; empty for the UCX default.
            agent: Pre-built agent (tests inject a fake); a real one is
                created when omitted.
        """
        if agent is None:
            agent = _default_agent(agent_name, ucx_device)
        elif ucx_device:
            agent.create_backend("UCX", {"ucx_devices": ucx_device})
        self._agent = agent
        self._registrations: list[Any] = []
        self._peers: set[str] = set()
        self._lock = threading.Lock()

    def register(self, addr: int, nbytes: int, tag: str = "") -> Any:
        """Register ``[addr, addr+nbytes)`` as DRAM.

        Args:
            addr: Start address.
            nbytes: Length in bytes.
            tag: Optional label.

        Returns:
            A handle for :meth:`deregister`.

        Raises:
            RuntimeError: if NIXL refuses the registration.
        """
        with self._lock:
            reg = self._agent.register_memory([(addr, nbytes, 0, tag)], "DRAM")
            if reg is None:
                raise RuntimeError(f"NIXL refused to register {addr:#x}+{nbytes}")
            self._registrations.append(reg)
        return reg

    def deregister(self, handle: Any) -> None:
        """Undo :meth:`register`.

        Args:
            handle: Value returned by :meth:`register`.
        """
        with self._lock:
            self._registrations.remove(handle)
            self._agent.deregister_memory(handle)

    def metadata(self) -> bytes:
        """Return current agent metadata (connection info + registered regions)."""
        with self._lock:
            return bytes(self._agent.get_agent_metadata())

    def add_peer(self, md: bytes) -> str:
        """Load (or reload) a peer's metadata.

        Args:
            md: Metadata bytes from the peer's :meth:`metadata`.

        Returns:
            The peer name as ``str``.
        """
        with self._lock:
            name = self._agent.add_remote_agent(md)
            name = name.decode() if isinstance(name, bytes) else str(name)
            self._peers.add(name)
        return name

    def remove_peer(self, name: str) -> None:
        """Forget a peer.

        Args:
            name: Peer name returned by :meth:`add_peer`.
        """
        with self._lock:
            self._peers.discard(name)
            self._agent.remove_remote_agent(name)

    def read(
        self,
        peer: str,
        pairs: list[tuple[int, int, int]],
        *,
        timeout_s: float = 30.0,
    ) -> float:
        """RDMA READ each ``(local, remote, nbytes)`` pair in one request.

        Args:
            peer: Peer name.
            pairs: ``(local_addr, remote_addr, nbytes)`` tuples.
            timeout_s: Completion deadline in seconds.

        Returns:
            Seconds spent transferring.

        Raises:
            TransferTimeout: if the transfer does not finish in time; the
                local and remote buffers stay owned by ``exc.pending``.
            RuntimeError: if the transfer ends in an error state.
        """
        return self._xfer("READ", peer, pairs, timeout_s)

    def write(
        self,
        peer: str,
        pairs: list[tuple[int, int, int]],
        *,
        timeout_s: float = 30.0,
    ) -> float:
        """RDMA WRITE each ``(local, remote, nbytes)`` pair in one request.

        Args:
            peer: Peer name.
            pairs: ``(local_addr, remote_addr, nbytes)`` tuples.
            timeout_s: Completion deadline in seconds.

        Returns:
            Seconds spent transferring.

        Raises:
            TransferTimeout: if the transfer does not finish in time; the
                local and remote buffers stay owned by ``exc.pending``.
            RuntimeError: if the transfer ends in an error state.
        """
        return self._xfer("WRITE", peer, pairs, timeout_s)

    def check_state(self, handle: Any) -> str:
        """Return the NIXL state string of a transfer handle.

        Args:
            handle: Handle from ``initialize_xfer``.

        Returns:
            ``"PROC"``, ``"DONE"`` or ``"ERR"``.
        """
        with self._lock:
            return str(self._agent.check_xfer_state(handle))

    def release_handle(self, op: str, peer: str, handle: Any) -> None:
        """Release a transfer handle, logging a failure instead of raising.

        Args:
            op: ``"READ"`` or ``"WRITE"``.
            peer: Peer name.
            handle: Handle from ``initialize_xfer``.
        """
        try:
            with self._lock:
                self._agent.release_xfer_handle(handle)
        except Exception as exc:  # the caller's own outcome matters more
            logger.warning("releasing NIXL %s handle to %s failed: %s", op, peer, exc)

    def close(self) -> None:
        """Remove peers and deregister everything."""
        with self._lock:  # snapshot only; each call below takes the lock itself
            peers = list(self._peers)
            registrations = list(self._registrations)
        for name in peers:
            try:
                self.remove_peer(name)
            except Exception as exc:  # keep tearing down the rest
                logger.warning("removing NIXL peer %s failed: %s", name, exc)
        for reg in registrations:
            try:
                self.deregister(reg)
            except Exception as exc:
                logger.warning("deregistering NIXL memory failed: %s", exc)

    def _xfer(
        self,
        op: str,
        peer: str,
        pairs: list[tuple[int, int, int]],
        timeout_s: float,
    ) -> float:
        """Run one batched transfer and poll it to completion.

        Args:
            op: ``"READ"`` or ``"WRITE"``.
            peer: Peer name.
            pairs: ``(local_addr, remote_addr, nbytes)`` tuples.
            timeout_s: Completion deadline in seconds.

        Returns:
            Seconds spent transferring.

        Raises:
            TransferTimeout: if the transfer does not finish in time.
            RuntimeError: if the transfer ends in an error state.
        """
        if not pairs:
            return 0.0
        with self._lock:
            local = self._agent.get_xfer_descs(
                [(la, n, 0) for la, _ra, n in pairs], "DRAM"
            )
            remote = self._agent.get_xfer_descs(
                [(ra, n, 0) for _la, ra, n in pairs], "DRAM"
            )
            handle = self._agent.initialize_xfer(op, local, remote, peer)
        t0 = time.perf_counter()
        release = True
        try:
            with self._lock:
                state = self._agent.transfer(handle)
            deadline = t0 + timeout_s
            while state == "PROC":
                if time.perf_counter() > deadline:
                    release = False  # the NIC may still touch these buffers
                    raise TransferTimeout(
                        f"NIXL {op} to {peer} exceeded {timeout_s}s",
                        PendingTransfer(self, op, peer, handle),
                    )
                time.sleep(_POLL_SLEEP_S)  # lock released: other threads may post
                with self._lock:
                    state = self._agent.check_xfer_state(handle)
            if state != "DONE":
                raise RuntimeError(f"NIXL {op} to {peer} ended in state {state}")
            return time.perf_counter() - t0
        finally:
            if release:
                self.release_handle(op, peer, handle)
