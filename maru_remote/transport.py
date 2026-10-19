# SPDX-License-Identifier: Apache-2.0
"""Thin NIXL wrapper: register host memory, exchange metadata, batch READ/WRITE.

A memory node's region mapper registers mapped CXL regions; a worker registers
its host staging buffer. Metadata bytes travel in region mapper replies, not
through NIXL's own side channels, so the transport never opens sockets.

A timed-out transfer or failed cancellation can leave the NIC accessing its
buffers. :class:`TransferTimeout` and :class:`TransferUncertainError` carry a
:class:`PendingTransfer` so the caller can retain those buffers until
:meth:`PendingTransfer.poll` confirms handle release.
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
        ImportError: if NIXL is not installed (``pip install nixl``).
    """
    from nixl._api import nixl_agent, nixl_agent_config

    agent = nixl_agent(agent_name, nixl_agent_config(backends=[]))
    params = {"ucx_devices": ucx_device} if ucx_device else {}
    agent.create_backend("UCX", params)
    return agent


class PendingTransfer:
    """A transfer whose handle and buffers have not been released."""

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
        self.handle = handle
        self.finished = False
        # The posting thread polls the transfer itself until it hands the
        # handle over (a timeout or an unconfirmed failure); until then
        # poll() leaves the handle alone so it is never released under it.
        self.in_flight = True
        # Pools of one worker share the transport: two clients may poll the
        # same transfer, and its handle must be released once.
        self._poll_lock = threading.Lock()

    def hand_over(self, finished: bool = False) -> None:
        """Called by the posting thread once it stops using the handle.

        Args:
            finished: Whether that thread already released the handle.
        """
        with self._poll_lock:
            self.in_flight = False
            self.finished = self.finished or finished

    def poll(self) -> bool:
        """Check whether the transfer has ended; release the handle if so.

        Returns:
            True only after NIXL releases the handle, confirming completion
            or cancellation. False while progress or cleanup is uncertain,
            including while the posting thread still polls it.
        """
        with self._poll_lock:
            if self.finished:
                return True
            if self.in_flight:
                return False
            try:
                state = self._transport.check_state(self.handle)
            except Exception as exc:
                logger.warning(
                    "NIXL %s to %s: state check failed: %s", self.op, self.peer, exc
                )
                state = None  # Try cancellation; a failed query proves nothing.
            if state == "PROC":
                return False
            self.finished = self._transport.release_handle(
                self.op, self.peer, self.handle
            )
            return self.finished


class TransferUncertainError(RuntimeError):
    """A transfer failed without confirmed cancellation; keep its buffers."""

    def __init__(self, message: str, pending: PendingTransfer):
        """Keep the unreleased handle available for polling and cleanup."""
        super().__init__(message)
        self.pending = pending


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
        # Peers a transfer to ended in an error (their connection is in
        # the error state); a timeout is left to the isolation of its pages
        self._tainted: set[str] = set()
        self._pending: dict[int, PendingTransfer] = {}
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
            self._agent.deregister_memory(handle)
            self._registrations.remove(handle)

    def metadata(self) -> bytes:
        """Return current agent metadata (connection info + registered regions)."""
        with self._lock:
            return bytes(self._agent.get_agent_metadata())

    def partial_metadata(self, registration: Any) -> bytes:
        """Metadata naming one registration only, with connection info.

        A peer that loads it can reach this registration and nothing else
        this agent registered; loading it into a peer that already knows this
        agent adds the range to what it knows.

        Args:
            registration: Value returned by :meth:`register`.

        Returns:
            Metadata bytes for the peer's :meth:`add_peer`.
        """
        with self._lock:
            return bytes(
                self._agent.get_partial_agent_metadata(registration, inc_conn_info=True)
            )

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
            if any(p.peer == name for p in self._pending.values()):
                raise RuntimeError(f"NIXL peer {name} still has unreleased transfers")
            self._agent.remove_remote_agent(name)
            self._peers.discard(name)
            self._tainted.discard(name)

    def has_peer(self, name: str) -> bool:
        """Whether a peer is loaded (added by :meth:`add_peer`, not removed)."""
        with self._lock:
            return name in self._peers

    def peer_usable(self, name: str) -> bool:
        """Whether NIXL's connection to a loaded peer has not failed.

        UCX fails a connection on its own when the peer stops answering, and
        NIXL then closes it but keeps it in the peer; loading the peer's
        metadata again crashes the process. A peer a transfer to ended in an
        error is unusable. Otherwise an empty notification tells the two states apart
        without touching a closed connection: NIXL refuses to send it then.
        The answer holds for the moment of the check; UCX may fail the
        connection right after it. The peer drops the notifications with
        :meth:`discard_notifs`.

        Args:
            name: Peer name returned by :meth:`add_peer`.

        Returns:
            False if a transfer to the peer ended in an error or NIXL refused
            to send to it.
        """
        with self._lock:
            if name in self._tainted:
                return False
            try:
                self._agent.send_notif(name, b"")
            except Exception:
                return False
        return True

    def discard_notifs(self) -> None:
        """Drop the notifications peers sent (their :meth:`peer_usable` checks)."""
        with self._lock:
            self._agent.get_new_notifs()

    def pending_peers(self) -> set[str]:
        """Peers that still have a transfer whose handle is not released."""
        with self._lock:
            return {p.peer for p in self._pending.values() if not p.finished}

    def disconnect(self, name: str) -> bool:
        """Drop the connection to a peer, as after a transfer error.

        A reliable connection that saw an error fails every later transfer;
        removing the peer lets the next :meth:`add_peer` connect anew.
        Transfers to the peer whose handle NIXL has not released stay
        pending: removing the peer does not prove that the NIC stopped using
        their buffers, so their owners keep them until :meth:`PendingTransfer.poll`
        confirms the release. Transfers another thread still polls are left
        to that thread.

        Args:
            name: Peer name returned by :meth:`add_peer`.

        Returns:
            True if the peer is gone; False if NIXL refused to remove it.
        """
        with self._lock:
            pending = [p for p in self._pending.values() if p.peer == name]
        for p in pending:
            p.poll()  # releases the handle if NIXL lets it go now
        try:
            with self._lock:
                self._agent.remove_remote_agent(name)
                self._peers.discard(name)
                self._tainted.discard(name)
        except Exception as exc:
            logger.error("closing the NIXL connection to %s failed: %s", name, exc)
            return False
        return True

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
                caller must retain buffers until ``exc.pending.poll()`` succeeds.
            TransferUncertainError: if completion or cleanup fails and handle
                release cannot be confirmed; retain buffers as for timeout.
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
                caller must retain buffers until ``exc.pending.poll()`` succeeds.
            TransferUncertainError: if completion or cleanup fails and handle
                release cannot be confirmed; retain buffers as for timeout.
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

    def release_handle(self, op: str, peer: str, handle: Any) -> bool:
        """Release a transfer handle; False keeps its buffers owned.

        Args:
            op: ``"READ"`` or ``"WRITE"``.
            peer: Peer name.
            handle: Handle from ``initialize_xfer``.

        Returns:
            True if NIXL confirmed release, False if cleanup must be retried.
        """
        try:
            with self._lock:
                self._agent.release_xfer_handle(handle)
                self._pending.pop(id(handle), None)
            return True
        except Exception as exc:
            logger.warning("releasing NIXL %s handle to %s failed: %s", op, peer, exc)
            return False

    def close(self) -> None:
        """Remove peers and deregister memory after all handles are released.

        Raises:
            RuntimeError: if transfers still own buffers.
            Exception: if NIXL cleanup fails. Keep the buffers alive and retry.
        """
        with self._lock:  # snapshot only; each call below takes the lock itself
            if self._pending:
                raise RuntimeError("NIXL transfers still own registered memory")
            peers = list(self._peers)
            registrations = list(self._registrations)
        for name in peers:
            self.remove_peer(name)
        for reg in registrations:
            self.deregister(reg)

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
            pending = PendingTransfer(self, op, peer, handle)
            self._pending[id(handle)] = pending
        t0 = time.perf_counter()
        try:
            with self._lock:
                state = self._agent.transfer(handle)
            deadline = t0 + timeout_s
            while state == "PROC":
                if time.perf_counter() > deadline:
                    raise TransferTimeout(
                        f"NIXL {op} to {peer} exceeded {timeout_s}s",
                        pending,
                    )
                time.sleep(_POLL_SLEEP_S)  # lock released: other threads may post
                with self._lock:
                    state = self._agent.check_xfer_state(handle)
            if state != "DONE":
                raise RuntimeError(f"NIXL {op} to {peer} ended in state {state}")
        except TransferTimeout:
            pending.hand_over()
            raise
        except Exception as exc:
            with self._lock:
                self._tainted.add(peer)
            released = self.release_handle(op, peer, handle)
            pending.hand_over(finished=released)
            if not released:
                raise TransferUncertainError(
                    f"NIXL {op} to {peer} failed without confirmed cancellation",
                    pending,
                ) from exc
            raise
        released = self.release_handle(op, peer, handle)
        pending.hand_over(finished=released)
        if not released:
            raise TransferUncertainError(
                f"NIXL {op} to {peer} completed but its handle is unreleased", pending
            )
        return time.perf_counter() - t0
