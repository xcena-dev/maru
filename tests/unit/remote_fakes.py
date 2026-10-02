# SPDX-License-Identifier: Apache-2.0
"""Test doubles for remote storage: an in-process NIXL agent and a fake clock."""

from __future__ import annotations

import ctypes


class FakeNixlAgent:
    """Stands in for ``nixl._api.nixl_agent``.

    Every agent registers itself in a class-level registry keyed by name so
    that ``add_remote_agent`` can resolve metadata to a peer. Transfers copy
    bytes with ``ctypes.memmove`` and refuse addresses outside registered
    ranges, which is the failure the real UCX backend reports.

    ``stall`` keeps every new transfer in progress (no bytes move) until
    :meth:`finish_stalled` completes them, which is how a test reproduces a
    transfer that misses its deadline and lands later.
    """

    registry: dict[str, FakeNixlAgent] = {}

    def __init__(self, name: str, *args, **kwargs) -> None:
        self.name = name
        self.registered: list[tuple[int, int]] = []
        self.peers: set[str] = set()
        self.backends: list[tuple[str, dict]] = []
        self.stall = False
        self.fail_next = False
        self.released: list[tuple] = []
        self._stalled: list[list] = []
        FakeNixlAgent.registry[name] = self

    def create_backend(self, name: str, params: dict) -> None:
        self.backends.append((name, dict(params)))

    def get_xfer_descs(self, descs, mem_type=None):
        assert mem_type == "DRAM", mem_type
        return list(descs)

    def register_memory(self, descs, mem_type=None):
        assert mem_type == "DRAM", mem_type
        reg = [(addr, size, dev) for (addr, size, dev, _tag) in descs]
        for addr, size, _dev in reg:
            self.registered.append((addr, size))
        return reg

    def deregister_memory(self, reg) -> None:
        for addr, size, _dev in reg:
            self.registered.remove((addr, size))

    def get_agent_metadata(self) -> bytes:
        return self.name.encode()

    def add_remote_agent(self, md: bytes) -> bytes:
        name = md.decode()
        assert name in FakeNixlAgent.registry, name
        self.peers.add(name)
        return md  # the real binding returns bytes too

    def remove_remote_agent(self, name: str) -> None:
        self.peers.discard(name)

    @staticmethod
    def _covered(agent: FakeNixlAgent, addr: int, size: int) -> bool:
        return any(a <= addr and addr + size <= a + s for a, s in agent.registered)

    def initialize_xfer(self, op, local, remote, peer, notif=b""):
        peer_name = peer.decode() if isinstance(peer, bytes) else peer
        assert peer_name in self.peers, f"unknown peer {peer_name}"
        target = FakeNixlAgent.registry[peer_name]
        for (la, ln, _), (ra, rn, _) in zip(local, remote, strict=True):
            assert ln == rn, "descriptor length mismatch"
            assert self._covered(self, la, ln), f"local {la:#x}+{ln} unregistered"
            assert self._covered(target, ra, rn), f"remote {ra:#x}+{rn} unregistered"
        return [op, list(local), list(remote), "NEW"]

    @staticmethod
    def _move(handle) -> None:
        op, local, remote, _ = handle
        for (la, ln, _), (ra, _rn, _) in zip(local, remote, strict=True):
            if op == "READ":
                ctypes.memmove(la, ra, ln)
            else:
                ctypes.memmove(ra, la, ln)

    def transfer(self, handle, notif=b"") -> str:
        if self.fail_next:
            self.fail_next = False
            handle[3] = "ERR"
            return "ERR"
        if self.stall:
            handle[3] = "PROC"
            self._stalled.append(handle)
            return "PROC"
        self._move(handle)
        handle[3] = "DONE"
        return "DONE"

    def finish_stalled(self) -> int:
        """Complete every stalled transfer (moving its bytes now)."""
        n = 0
        for handle in self._stalled:
            self._move(handle)
            handle[3] = "DONE"
            n += 1
        self._stalled.clear()
        return n

    def check_xfer_state(self, handle) -> str:
        return handle[3]

    def release_xfer_handle(self, handle) -> None:
        self.released.append(tuple(handle[:1]))


class FakeClock:
    """Manually advanced monotonic clock."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def reset_fake_agents() -> None:
    """Forget every fake agent (call from fixtures between tests)."""
    FakeNixlAgent.registry.clear()
