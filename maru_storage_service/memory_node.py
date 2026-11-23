# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Shared read registration and independently revoked write registrations."""

from __future__ import annotations

import mmap
import os
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from maru_common.remote_protocol import AgentsRequest
from maru_common.storage_types import PoolInfo, RemotePool, WriteAgent, WriteDomain
from maru_remote.auth import load_key
from maru_remote.checked import require_checked_transport
from maru_remote.exporter import NixlExporter
from maru_remote.progress import ConnectProgress
from maru_remote.transport import NixlTransport

from .peer_ops import peer_node
from .remote_server import TcpRemoteServer

AGENTS_PER_NODE = 64
WRITE_DOMAINS_PER_NODE = 64
MAX_AGENT_NAME = 256
MAX_AGENT_METADATA = 1 << 20


def agent_name(node_id: str, node_epoch: str) -> str:
    return f"maru-mem-{node_id}-{node_epoch}"


class RunOps:
    def __init__(self) -> None:
        self.target: Any = None

    def __getattr__(self, name: str) -> Any:
        if self.target is None:
            raise RuntimeError("memory node not ready")
        return getattr(self.target, name)


@dataclass
class _Writer:
    transport: NixlTransport
    exporter: NixlExporter
    progress: ConnectProgress
    name: str


class RemoteParts:
    """Mappings stay alive until every registration is revoked."""

    def __init__(
        self,
        name: str,
        make_transport: Callable[[str], NixlTransport],
        *,
        agents_max: int = AGENTS_PER_NODE,
        domains_max: int = WRITE_DOMAINS_PER_NODE,
    ):
        self.name, self._make_transport = name, make_transport
        self.agents_max, self.domains_max = agents_max, domains_max
        self.transport = make_transport(name)
        self.exporter = NixlExporter(self.transport)
        self.progress = ConnectProgress(self.transport)
        self.server: TcpRemoteServer | None = None
        self._lock = threading.Lock()
        self._maps: dict[str, mmap.mmap] = {}
        self._infos: list[PoolInfo] = []
        self.pool_registrations: dict[str, int] = {}
        self._writers: dict[WriteDomain, _Writer] = {}
        self._closed_domains: set[WriteDomain] = set()
        self._agents: dict[str, tuple[str, bytes]] = {}
        self._closed = False
        self._agents_failed = False

    def attach(self, info: PoolInfo, memory: mmap.mmap) -> None:
        self._maps[info.pool_id] = memory
        self._infos.append(info)
        self.exporter.attach(info.pool_id, memory)

    def _pools(self, exporter: NixlExporter) -> list[RemotePool]:
        pools = []
        for info in self._infos:
            export = exporter.export(info.pool_id)
            if export is not None:
                pools.append(
                    RemotePool(
                        info.pool_id,
                        info.kind,
                        info.pool_generation,
                        info.capacity,
                        export.base,
                        export.metadata,
                        self.pool_registrations[info.pool_id],
                    )
                )
        return pools

    def remote_pools(self) -> list[RemotePool]:
        return self._pools(self.exporter)

    def writer(self, domain: WriteDomain) -> WriteAgent:
        with self._lock:
            if self._closed or self._agents_failed or domain in self._closed_domains:
                raise ValueError("write domain closed")
            entry = self._writers.get(domain)
            if entry is None:
                count = sum(d.node_id == domain.node_id for d in self._writers)
                if count >= self.domains_max:
                    raise ValueError("write domain limit")
                name = f"{self.name}-write-{uuid.uuid4().hex}"
                transport = self._make_transport(name)
                entry = _Writer(
                    transport, NixlExporter(transport), ConnectProgress(transport), name
                )
                self._writers[domain] = entry
                for pool_id, memory in self._maps.items():
                    entry.exporter.attach(pool_id, memory)
                for worker, (_, metadata) in self._agents.items():
                    self._add(entry.transport, entry.progress, worker, metadata)
            return WriteAgent(
                domain,
                entry.name,
                entry.transport.metadata(),
                self._pools(entry.exporter),
            )

    def fence(self, domain: WriteDomain) -> None:
        with self._lock:
            self._closed_domains.add(domain)
            entry = self._writers.get(domain)
            if entry is None:
                return
            for pool_id in self._maps:
                entry.exporter.detach(pool_id)
            entry.progress.close()
            entry.transport.close()
            del self._writers[domain]

    @staticmethod
    def _add(transport, progress, name, metadata) -> None:
        if transport.add_peer(metadata) != name:
            raise ValueError("agent name mismatch")
        progress.expect(name)

    def _targets(self):
        return [
            (self.transport, self.progress),
            *[(w.transport, w.progress) for w in self._writers.values()],
        ]

    def agents(self, origin: str, request: AgentsRequest) -> None:
        with self._lock:
            if self._closed or self._agents_failed:
                raise ValueError("agent updates closed")
            add = {a.agent_name: a.agent_metadata for a in request.add}
            if (
                len(add) != len(request.add)
                or len(add) > self.agents_max
                or len(request.remove) > self.agents_max
            ):
                raise ValueError("agent update limit")
            for name in request.remove:
                if name in self._agents and self._agents[name][0] != origin:
                    raise ValueError("agent belongs to another run")
            for name, metadata in add.items():
                if (
                    not name
                    or len(name) > MAX_AGENT_NAME
                    or not metadata
                    or len(metadata) > MAX_AGENT_METADATA
                ):
                    raise ValueError("invalid agent metadata")
                existing = self._agents.get(name)
                if existing is not None and existing != (origin, metadata):
                    raise ValueError("agent metadata changed")
            kept = {n: a for n, a in self._agents.items() if n not in request.remove}
            kept.update({n: (origin, md) for n, md in add.items()})
            if (
                sum(peer_node(o) == peer_node(origin) for o, _ in kept.values())
                > self.agents_max
            ):
                raise ValueError("agent limit")
            try:
                for name in request.remove:
                    if name in self._agents:
                        for transport, _ in self._targets():
                            transport.remove_peer(name)
                        del self._agents[name]
                for name, metadata in add.items():
                    self._agents[name] = origin, metadata
                    for transport, progress in self._targets():
                        self._add(transport, progress, name, metadata)
            except Exception:
                self._agents_failed = True
                raise

    def forget_agents(self, origin: str) -> None:
        if self._agents_failed:
            return
        names = [n for n, (o, _) in self._agents.items() if o == origin]
        self.agents(origin, AgentsRequest(remove=names))

    def close(self) -> None:
        with self._lock:
            self._closed = True
            for entry in self._writers.values():
                entry.progress.close()
                for pool_id in self._maps:
                    entry.exporter.detach(pool_id)
                entry.transport.close()
            self.progress.close()
            for pool_id in self._maps:
                self.exporter.detach(pool_id)
            self.transport.close()
            for memory in self._maps.values():
                memory.close()
            self._maps.clear()
            self._writers.clear()


def make_remote_parts(run) -> RemoteParts:
    config = run.config
    rs = config.remote_server
    assert rs is not None
    key = load_key(rs.key_file)
    if run.make_transport is None:
        require_checked_transport(os.environ)

    def make(name):
        if run.make_transport is not None:
            return run.make_transport(name, rs.rdma_device)
        require_checked_transport(os.environ)
        return NixlTransport(name, ucx_device=rs.rdma_device)

    parts = RemoteParts(agent_name(config.node_id, run.node_epoch), make)
    parts.server = TcpRemoteServer(
        rs.listen,
        key,
        node_id=config.node_id,
        node_epoch=run.node_epoch,
        capabilities=run.capabilities(),
        page_size=config.page_size,
        agent_name=parts.name,
        progress=parts.progress,
        ops=run.role.remote_ops,
    )
    return parts
