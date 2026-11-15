# SPDX-License-Identifier: Apache-2.0
"""A GPU node [dram, remote, ssd] and a memory node against the real
MaruServer process, with a worker in its own process and the fake NIC: DRAM
demotes to the memory node and to the SSD what the memory node cannot take
(it credits a peer 2 pages at a time), and each key lands in one of them."""

import shutil
import tempfile

import pytest

from maru_common.storage_types import PoolKind
from maru_storage_service.pool_manager import PageState
from tests.integration.test_ms_s2_real_server import RealMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    Agent,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_demotions_applied,
    wait_until,
)
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.test_ms_remote_demotion import memory_config
from tests.unit.test_ms_remote_ssd import gpu_config
from tests.unit.test_ms_remote_tier import fake_transport, free_port

pytestmark = pytest.mark.integration


@pytest.fixture
def workdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def server(workdir):
    server = RealMaruServer(workdir, 6.0, 0.3)
    yield server
    server.close()


@pytest.fixture
def nodes(server, workdir):
    reset_fake_agents()
    ssd_dir = tempfile.mkdtemp(prefix="maru-l2-", dir="/var/tmp")
    port = free_port()
    started = []
    try:
        started.append(
            start_node(
                memory_config(server.url, port, pages=64),
                rpc_timeout_s=0.5,
                make_transport=fake_transport,
            )
        )
        started.append(
            start_node(
                gpu_config(server.url, workdir, port, ssd_dir),
                rpc_timeout_s=0.5,
                make_transport=fake_transport,
            )
        )
        memory, gpu = started
        wait_until(lambda: gpu.tier("remote").ready("mem1"), what="remote ready")
        yield gpu, memory
    finally:
        for node in reversed(started):
            node.stop()
        shutil.rmtree(ssd_dir, ignore_errors=True)
        reset_fake_agents()


def ready_keys(node, kind: PoolKind | None = None) -> set[str]:
    """The keys the node's ledger has READY (in step with the Catalog)."""
    out = set()
    for pool_id in node.pools.pool_ids():
        for ref in node.pools.pages_in(pool_id, PageState.READY):
            page = node.pools.page(ref)
            if page is None or page.key is None or page.location is None:
                continue
            if kind is None or page.location.kind == kind:
                out.add(page.key.key)
    return out


def test_dram_demotes_to_the_memory_node_and_the_ssd_takes_the_rest(nodes, workdir):
    gpu, memory = nodes
    agent = Agent(gpu.config.socket, engine="writer", log_dir=workdir)
    try:
        agent.connect()
        for i in range(24):
            agent.call("wait_credit", bytes=PAGE)
            assert agent.call("store", key=f"k{i}", value=(f"k{i}" * 64).encode().hex())
        m = gpu.manager
        wait_until(
            lambda: (
                m.moved_bytes["move_dram_to_remote"] > 0
                and m.moved_bytes["move_dram_to_ssd"] > 0
            ),
            what="both tiers took pages",
        )
        wait_demotions_applied(m)
        on_memory, on_ssd = ready_keys(memory), ready_keys(gpu, PoolKind.SSD)
        assert on_memory and on_ssd and not on_memory & on_ssd
        gpu.pools.check()
        memory.pools.check()
    finally:
        agent.stop()
