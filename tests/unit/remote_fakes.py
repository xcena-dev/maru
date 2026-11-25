# SPDX-License-Identifier: Apache-2.0
"""Test doubles for remote storage: an in-process NIXL agent and a fake clock."""

from __future__ import annotations

import ctypes


class LoadIntoFailedEndpoint(BaseException):
    """Metadata was loaded into a peer whose UCX endpoint had failed.

    NIXL 1.2.0 closes a failed endpoint in its error callback but keeps the
    pointer, and loading metadata into the same peer again unpacks the keys
    with it: the process crashes (seen as a segfault in
    ``ucp_ep_rkey_unpack``). A BaseException, so no handler swallows it.
    """


class FakeNixlAgent:
    """Stands in for ``nixl._api.nixl_agent``.

    Every agent registers itself in a class-level registry keyed by name so
    that ``add_remote_agent`` can resolve metadata to a peer. Transfers copy
    bytes with ``ctypes.memmove``.

    A registration of an address range gets the key the same range had
    before, as the RoCE NIC this was checked on does (design note 5.4); a new
    range gets a new key. As with the real agent, a
    transfer can only name remote ranges whose metadata was loaded: full
    metadata (``get_agent_metadata``) covers every current registration of
    the peer, partial metadata only the registrations it lists (with their
    keys), and partial metadata without connection info cannot introduce a
    new peer. A transfer carries the key it was set up with; if the peer no
    longer holds that registration when the transfer reaches it, the
    transfer ends in ``ERR`` and moves nothing, even if the same range was
    registered again under a new key. As a reliable connection does after a
    remote access error, the connection breaks: every later transfer to that
    peer ends in ``ERR`` until the peer is removed and loaded again.

    ``stall`` keeps every new transfer in progress (no bytes move) until
    :meth:`finish_stalled` completes them, which is how a test reproduces a
    transfer that misses its deadline and lands later.

    A transfer's handle and its packets end apart. :meth:`fail_stalled` ends
    the stalled handles in ``ERR``; with ``late`` their packets, already read
    from the source, stay in the network, and neither releasing the handle,
    removing the peer, nor ending the source's registration or agent stops
    them: :meth:`deliver_late` lands each one whose destination agent is open
    and still holds the range under the key the transfer was set up with
    (the remote side for a WRITE, the local side for a READ). A transfer uses
    the connection (QP) to the peer agent whose metadata was loaded, so a new
    agent of the same name, or one registering the same range under the same
    key, is not reached through it. :meth:`close` ends the agent as a process
    exit does: its registrations and connections go, a transfer it posted
    that has not yet read its source moves nothing, and no late packet lands
    in its memory.

    :meth:`fail_endpoint` is what UCX does when a peer stops answering: the
    connection fails without any transfer seeing it. Transfers to the peer
    then end in ``ERR``, notifications to it are refused, and loading its
    metadata again before removing it raises :class:`LoadIntoFailedEndpoint`,
    as NIXL crashes there. Notifications travel the same connection as
    transfers: one to an agent that has ended fails the connection, and none
    reaches a later agent of the same name. Notifications sent to an agent
    stay in its ``notifs`` until it calls ``get_new_notifs``.
    """

    registry: dict[str, FakeNixlAgent] = {}
    # Every agent by its incarnation: metadata names the one it came from
    incarnations: dict[int, FakeNixlAgent] = {}
    _next_incarnation = 1

    def __init__(self, name: str, *args, **kwargs) -> None:
        self.name = name
        self.incarnation = FakeNixlAgent._next_incarnation
        FakeNixlAgent._next_incarnation += 1
        FakeNixlAgent.incarnations[self.incarnation] = self
        self.regs: list[tuple[int, int, int]] = []  # (addr, size, key)
        self.peers: set[str] = set()
        # peer -> registrations loaded from its metadata (addr, size, key);
        # None = every current registration (full metadata)
        self.known: dict[str, set[tuple[int, int, int]] | None] = {}
        self.broken: set[str] = set()
        self.failed: set[str] = set()  # peers whose endpoint UCX closed
        # Peers NIXL invalidated after their connection failed (the peer's
        # process died and the RC retries ran out)
        self.invalidated: set[str] = set()
        self.notifs: list[tuple[str, bytes]] = []  # (sender, message) received
        self.backends: list[tuple[str, dict]] = []
        self.stall = False
        self.fail_next = False
        self.released: list[tuple] = []
        self.closed = False
        # peer name -> the agent its loaded metadata connected to
        self.conns: dict[str, FakeNixlAgent] = {}
        self._stalled: list[list] = []
        # ERR handles whose packets still fly, with the bytes they carry
        self._late: list[tuple[list, list[bytes]]] = []
        FakeNixlAgent.registry[name] = self

    def create_backend(self, name: str, params: dict) -> None:
        self.backends.append((name, dict(params)))

    def get_xfer_descs(self, descs, mem_type=None):
        assert mem_type == "DRAM", mem_type
        return list(descs)

    _next_key = 1

    @property
    def registered(self) -> list[tuple[int, int]]:
        return [(addr, size) for addr, size, _key in self.regs]

    _keys_by_range: dict[tuple[int, int], int] = {}

    def register_memory(self, descs, mem_type=None):
        assert mem_type == "DRAM", mem_type
        if self.closed:
            raise RuntimeError("NIXL agent closed")
        reg = []
        for addr, size, _dev, _tag in descs:
            key = FakeNixlAgent._keys_by_range.get((addr, size))
            if key is None:
                key = FakeNixlAgent._next_key
                FakeNixlAgent._next_key += 1
                FakeNixlAgent._keys_by_range[(addr, size)] = key
            self.regs.append((addr, size, key))
            reg.append((addr, size, key))
        return reg

    def deregister_memory(self, reg) -> None:
        if self.closed:
            return  # the agent's end took its registrations
        for entry in reg:
            self.regs.remove(tuple(entry))

    def key_of(self, addr: int, size: int) -> int | None:
        """Key of this agent's registration of exactly ``[addr, addr+size)``."""
        for a, n, key in self.regs:
            if (a, n) == (addr, size):
                return key
        return None

    def close(self) -> None:
        """End the agent: registrations and connections go without a word to
        peers, as when its process exits."""
        self.closed = True
        self.regs.clear()

    def get_agent_metadata(self) -> bytes:
        return f"{self.name}|1|*|{self.incarnation}".encode()

    def check_remote_metadata(self, name: str) -> bool:
        return name in self.peers

    def get_partial_agent_metadata(self, descs, inc_conn_info=False, backends=()):
        ranges = ";".join(f"{addr}:{size}:{key}" for addr, size, key in descs)
        conn = int(bool(inc_conn_info))
        return f"{self.name}|{conn}|{ranges}|{self.incarnation}".encode()

    def add_remote_agent(self, md: bytes) -> bytes:
        """Metadata carries its agent's connection address (its
        incarnation): stale metadata connects to the agent it came from,
        never to a later one of the same name."""
        name, conn, ranges, incarnation = md.decode().split("|")
        target = FakeNixlAgent.incarnations[int(incarnation)]
        assert target.name == name, name
        if conn != "1" and name not in self.peers:
            raise RuntimeError(f"no connection info for new agent {name}")
        if name in self.failed:
            raise LoadIntoFailedEndpoint(name)
        self.peers.add(name)
        if conn == "1" or name not in self.conns:
            self.conns[name] = target
        if ranges == "*":
            self.known[name] = None
        else:
            loaded = {
                (int(a), int(n), int(k))
                for a, n, k in (r.split(":") for r in ranges.split(";") if r)
            }
            if self.known.get(name, set()) is not None:
                self.known[name] = self.known.get(name, set()) | loaded
        return name.encode()  # the real binding returns bytes too

    def remove_remote_agent(self, name: str) -> None:
        if name in self.invalidated:  # NIXL dropped its metadata already
            self.invalidated.discard(name)
            self.peers.discard(name)
            self.conns.pop(name, None)
            self.known.pop(name, None)
            raise RuntimeError("NIXL_ERR_NOT_FOUND")
        self.peers.discard(name)
        self.conns.pop(name, None)
        self.known.pop(name, None)
        self.broken.discard(name)
        self.failed.discard(name)

    def send_notif(self, remote_agent_name, notif_msg, backend=None) -> None:
        name = remote_agent_name
        if isinstance(name, bytes):
            name = name.decode()
        if self.closed:
            raise RuntimeError("NIXL agent closed")
        if name not in self.peers:
            raise RuntimeError("NIXL_ERR_NOT_FOUND")
        target = self.conns[name]
        if target.closed:  # the peer's end failed the connection
            self.failed.add(name)
            self.broken.add(name)
        if name in self.failed or name in self.broken:  # an RC error is final
            raise RuntimeError("NIXL_ERR_REMOTE_DISCONNECT")
        target.notifs.append((self.name, bytes(notif_msg)))

    def get_new_notifs(self, backends=()) -> dict[str, list[bytes]]:
        out: dict[str, list[bytes]] = {}
        for sender, msg in self.notifs:
            out.setdefault(sender, []).append(msg)
        self.notifs.clear()
        return out

    def fail_endpoint(self, name: str) -> None:
        """Fail the connection to a known peer, as UCX does on its own."""
        assert name in self.peers, name
        self.failed.add(name)
        self.broken.add(name)

    @staticmethod
    def _covered(agent: FakeNixlAgent, addr: int, size: int) -> bool:
        return any(a <= addr and addr + size <= a + s for a, s in agent.registered)

    @staticmethod
    def _key_for(regs, addr: int, size: int) -> int | None:
        for a, n, key in regs:
            if a <= addr and addr + size <= a + n:
                return key
        return None

    def initialize_xfer(self, op, local, remote, peer, notif=b""):
        if self.closed:
            raise RuntimeError("NIXL agent closed")
        peer_name = peer.decode() if isinstance(peer, bytes) else peer
        assert peer_name in self.peers, f"unknown peer {peer_name}"
        known = self.known.get(peer_name)
        target = self.conns[peer_name]
        keys, local_keys = [], []
        for (la, ln, _), (ra, rn, _) in zip(local, remote, strict=True):
            assert ln == rn, "descriptor length mismatch"
            assert self._covered(self, la, ln), f"local {la:#x}+{ln} unregistered"
            local_keys.append(self._key_for(self.regs, la, ln))
            regs = target.regs if known is None else known
            key = self._key_for(regs, ra, rn)
            if key is None and known is not None:
                raise RuntimeError("NIXL_ERR_NOT_FOUND")  # no loaded descriptor
            keys.append(key)
        # [op, local, remote, state, peer, keys, target agent, local keys]
        handle = [op, list(local), list(remote), "NEW", peer_name, keys]
        return [*handle, target, local_keys]

    @staticmethod
    def _held(agent: FakeNixlAgent, desc, key) -> bool:
        """Whether an open agent still holds the range under its key."""
        ra, rn, _ = desc
        return (
            not agent.closed
            and key is not None
            and any(k == key and a <= ra and ra + rn <= a + n for a, n, k in agent.regs)
        )

    def _sides(self, handle) -> tuple[list[bool], list[bool]]:
        """Per range, whether its local and its remote end are still held."""
        local = [
            self._held(self, d, k) for d, k in zip(handle[1], handle[7], strict=True)
        ]
        remote = [
            self._held(handle[6], d, k)
            for d, k in zip(handle[2], handle[5], strict=True)
        ]
        return local, remote

    def _complete(self, handle) -> None:
        """All ranges held: DONE. Otherwise ERR, and the connection breaks;
        the ranges whose ends are both held may still have moved."""
        if handle[4] in self.broken:  # the connection failed before
            handle[3] = "ERR"
            return
        local, remote = self._sides(handle)
        ok = [a and b for a, b in zip(local, remote, strict=True)]
        self._move(handle, ok)
        if all(ok):
            handle[3] = "DONE"
            return
        self.broken.add(handle[4])  # an access error breaks the connection
        handle[3] = "ERR"

    @staticmethod
    def _move(handle, which: list[bool]) -> None:
        op, local, remote = handle[:3]
        for (la, ln, _), (ra, _rn, _), go in zip(local, remote, which, strict=True):
            if not go:
                continue
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
        self._complete(handle)
        return handle[3]

    def finish_stalled(self) -> int:
        """Complete every stalled transfer now; returns how many moved bytes.

        A stalled transfer whose remote range was deregistered meanwhile ends
        in ``ERR`` and moves nothing, as on a NIC.
        """
        n = 0
        for handle in self._stalled:
            self._complete(handle)
            n += handle[3] == "DONE"
        self._stalled.clear()
        return n

    def fail_stalled(self, late: bool = False) -> int:
        """End every stalled transfer's handle in ``ERR`` now.

        The connection breaks, as after any transfer error. Without ``late``
        the packets are lost; with it the NIC has read the source (if the
        source is still held) and the packets stay in the network until
        :meth:`deliver_late`. Returns how many transfers failed.
        """
        for handle in self._stalled:
            handle[3] = "ERR"
            self.broken.add(handle[4])
            if late:
                payload = self._read_source(handle)
                if any(data is not None for data in payload):
                    self._late.append((handle, payload))
        n = len(self._stalled)
        self._stalled.clear()
        return n

    def deliver_late(self) -> int:
        """Land the late packets of failed transfers; returns how many landed.

        A packet lands only while its destination agent is open and holds the
        range under the transfer's key: the destination's deregistration or
        end drops it. Handle release, peer removal and the source's
        deregistration or end do not.
        """
        n = 0
        for handle, payload in self._late:
            local, remote = self._sides(handle)
            held = local if handle[0] == "READ" else remote
            dest = handle[1] if handle[0] == "READ" else handle[2]
            landed = False
            for (addr, size, _), data, ok in zip(dest, payload, held, strict=True):
                if data is not None and ok:
                    ctypes.memmove(addr, data, size)
                    landed = True
            n += landed
        self._late.clear()
        return n

    def _read_source(self, handle) -> list[bytes | None]:
        """Per range, the bytes the NIC read, or None if the source was gone."""
        local, remote = self._sides(handle)
        held = remote if handle[0] == "READ" else local
        source = handle[2] if handle[0] == "READ" else handle[1]
        return [
            ctypes.string_at(addr, size) if ok else None
            for (addr, size, _), ok in zip(source, held, strict=True)
        ]

    def check_xfer_state(self, handle) -> str:
        """A transfer still in progress whose peer agent ended raises
        NIXL_ERR_REMOTE_DISCONNECT from then on, and one whose peer NIXL
        invalidated raises NIXL_ERR_NOT_FOUND, as NIXL does; it never ends
        on its own."""
        if handle[3] == "PROC" and handle[4] in self.invalidated:
            raise RuntimeError("NIXL_ERR_NOT_FOUND")
        if handle[3] == "PROC" and handle[6].closed:
            raise RuntimeError("NIXL_ERR_REMOTE_DISCONNECT")
        return handle[3]

    def release_xfer_handle(self, handle) -> None:
        self.released.append(tuple(handle[:1]))


def reset_fake_agents() -> None:
    """Forget every fake agent and NIC key (call from fixtures between tests)."""
    FakeNixlAgent.registry.clear()
    FakeNixlAgent._keys_by_range.clear()
    FakeNixlAgent._next_key = 1


class FakeStorageControl:
    """Worker control replies; publication waits for explicit COMMIT completion."""

    closed = False

    def __init__(self, credit_response):
        self.credit_response = credit_response
        self.requests = []
        self.writes = []
        self.published = set()

    def submit(self, code, request):
        from concurrent.futures import Future

        from maru_common.node_protocol import NodeMessageType

        self.requests.append((code, request))
        future = Future()
        if code == NodeMessageType.CREDIT:
            future.set_result(self.credit_response)
        elif code == NodeMessageType.WRITE_DONE:
            self.writes.append((request, future))
        else:
            raise AssertionError(code)
        return future

    def close(self):
        self.closed = True

    def commit(self):
        from maru_common.node_protocol import WriteDoneResponse
        from maru_common.remote_protocol import TransferEnd
        from maru_common.storage_types import ItemOutcome, ItemResult, Status

        request, future = self.writes.pop(0)
        items = []
        for item in request.items:
            if item.end == TransferEnd.DONE:
                self.published.add(item.key)
                items.append(ItemOutcome(ItemResult.ADDED))
            else:
                items.append(ItemOutcome(ItemResult.OK))
        future.set_result(WriteDoneResponse(Status.OK, items=items))
