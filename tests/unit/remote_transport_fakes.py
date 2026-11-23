# SPDX-License-Identifier: Apache-2.0
"""FakeTransport: the real NixlTransport over an in-process fake NIC."""

from __future__ import annotations

import mmap

from maru_remote.transport import NixlTransport, buffer_address
from tests.unit.remote_fakes import FakeNixlAgent


def mapped(nbytes: int, fill: int = 0) -> tuple[mmap.mmap, int]:
    """An anonymous mapping filled with ``fill``, and its address."""
    view = mmap.mmap(-1, nbytes)
    if fill:
        view[:] = bytes([fill]) * nbytes
    return view, buffer_address(view)


class FakeTransport(NixlTransport):
    """A NixlTransport whose NIXL agent is a :class:`FakeNixlAgent`.

    Bytes move by memcpy between registered ranges (e.g. :func:`mapped`). The
    same range registered again, by any agent, gets the same key. Faults are
    injected into the transfers this transport posts: :meth:`stall` keeps them
    in flight, :meth:`fail_next` ends the next one in ``ERR`` with nothing
    moved, and :meth:`fail_stalled` ends the stalled ones' handles in ``ERR``
    while, with ``late``, their packets still land at :meth:`deliver_late`.
    Handle release, peer removal, reconnection and the source's end never
    stop a late packet; the destination's deregistration or its agent's end
    (:meth:`kill`, :meth:`close`) does.
    """

    def __init__(self, name: str, *, ucx_device: str = "") -> None:
        """Create the transport and its fake agent.

        Args:
            name: Agent name; peers load its metadata under it.
            ucx_device: Recorded on the fake agent's UCX backend.
        """
        self.agent = FakeNixlAgent(name)
        super().__init__(name, ucx_device=ucx_device, agent=self.agent)

    def stall(self, on: bool = True) -> None:
        """Keep every transfer posted from now on in progress."""
        self.agent.stall = on

    def fail_next(self) -> None:
        """End the next posted transfer in ``ERR`` at once; nothing moves."""
        self.agent.fail_next = True

    def finish_stalled(self) -> int:
        """Complete the stalled transfers; returns how many moved their bytes."""
        return self.agent.finish_stalled()

    def fail_stalled(self, late: bool = False) -> int:
        """End the stalled transfers' handles in ``ERR``, packets late if asked."""
        return self.agent.fail_stalled(late)

    def deliver_late(self) -> int:
        """Land late packets whose destination is still registered."""
        return self.agent.deliver_late()

    def kill(self) -> None:
        """End the agent as a process exit does: no deregistration, no peer
        removal; its registrations and connections are gone all the same."""
        self.agent.close()

    def close(self) -> None:
        """Close the transport, then end its agent (connections included)."""
        super().close()
        self.agent.close()
