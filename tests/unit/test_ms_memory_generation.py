# SPDX-License-Identifier: Apache-2.0
"""Memory resets fence registrations and invalidate Catalog locations."""

import dataclasses
import itertools
import threading

import pytest

from maru_common.storage_types import CloseMode, ItemResult, Status
from maru_remote.transport import TransferState
from maru_storage_service.ports import ServerStopped
from maru_storage_service.region_generation import (
    MemoryGeneration,
    MemoryGenerationError,
)
from maru_storage_service.service import Node, StorageService
from tests.unit.ms_node_helpers import anon_region, wait_until
from tests.unit.remote_transport_fakes import FakeTransport, mapped
from tests.unit.test_ms_region_extension import FakeExtension
from tests.unit.test_ms_region_extension import extension_env as extension_env


class ResettableExtension(FakeExtension):
    def __init__(self):
        super().__init__()
        self.generation = "g1"
        self.generation_error = None

    def memory_generation(self):
        if self.generation_error is not None:
            raise self.generation_error
        return self.generation


@pytest.fixture
def reset_env(extension_env):
    return extension_env(ResettableExtension())


def reset(env, generation="g2"):
    env.extension.generation = generation
    assert env.node.role.check_generation() is False
    assert env.node.wait_finished(5)
    assert env.node.teardown_failed is None


def test_the_boot_carries_memory_generation(reset_env, extension_env):
    env = reset_env
    _, hello = env.connect()
    assert hello.boot == f"{env.node.role.generation.boot_id}:g1"
    assert env.node.server.host_boot_id == hello.boot
    plain = extension_env(FakeExtension())
    _, hello = plain.connect()
    assert hello.boot == plain.node.role.generation.boot_id


def test_a_reset_deregisters_every_read_and_write_region(reset_env):
    env = reset_env
    env.reserve()
    read = env.parts.transport
    writers = list(env.parts._writers.values())
    assert read.agent.regs and all(w.transport.agent.regs for w in writers)
    reset(env)
    assert not read.agent.regs and all(not w.transport.agent.regs for w in writers)
    assert env.node.role.generation.boot.endswith(":g2")


def test_a_reset_removes_old_catalog_keys_and_starts_fresh_regions(reset_env):
    env = reset_env
    env.write(env.reserve().credits)
    assert env.lookup().result == ItemResult.HIT
    old_ids = set(env.node.pools.pool_ids())
    reset(env)
    assert env.lookup().result == ItemResult.MISS
    assert env.rm.released
    replacement = env.start_cxl()
    assert old_ids.isdisjoint(replacement.pools.pool_ids())
    assert not replacement.adopted
    assert all(env.extension.fresh.values())
    assert (
        env.write(env.reserve().credits, key="new").items[0].result == ItemResult.ADDED
    )
    assert env.lookup("new0").result == ItemResult.HIT


def test_a_store_after_a_reset_cannot_commit_old_credits(reset_env):
    env = reset_env
    credit = env.reserve().credits
    env.extension.generation = "g2"
    assert env.write(credit).status == Status.BUSY
    assert env.node.wait_finished(5)
    assert env.lookup().result == ItemResult.MISS
    env.start_cxl()
    assert (
        env.write(env.reserve().credits, key="new").items[0].result == ItemResult.ADDED
    )


def test_a_reader_loads_a_miss_after_reset_even_with_old_metadata(reset_env):
    env = reset_env
    credit = env.reserve().credits[0]
    env.write([credit])
    _, hello = env.connect()
    worker = FakeTransport("reader")
    buffer, address = mapped(credit.length)
    worker.register(address, credit.length)
    peer = worker.add_peer(hello.pools[0].metadata)
    try:
        assert (
            worker.post(
                "READ",
                peer,
                [(address, hello.pools[0].base + credit.offset, credit.length)],
            ).poll_state()
            == TransferState.DONE
        )
        reset(env)
        assert (
            worker.post(
                "READ",
                peer,
                [(address, hello.pools[0].base + credit.offset, credit.length)],
            ).poll_state()
            == TransferState.ERR
        )
        env.start_cxl()
        assert env.lookup().result == ItemResult.MISS
    finally:
        worker.close()
        buffer.close()


def test_generation_failure_within_grace_keeps_regions(reset_env):
    env = reset_env
    env.write(env.reserve().credits)
    env.extension.generation_error = RuntimeError("device unavailable")
    assert env.node.role.check_generation()
    assert env.node.role.generation.boot.endswith(":g1")
    assert env.parts.exporter.exports()
    assert env.lookup().result == ItemResult.HIT


def test_unknown_generation_at_start_refuses_service(extension_env):
    ext = ResettableExtension()
    ext.generation_error = RuntimeError("device unavailable")
    env = extension_env(ext, start=False)
    with pytest.raises(MemoryGenerationError, match="device unavailable"):
        env.start_cxl()
    assert not env.nodes[-1].role.parts and env.rm.calls == []
    assert ext.closed


def test_unknown_past_grace_fences_and_recovery_has_a_new_boot(reset_env):
    env = reset_env
    env.write(env.reserve().credits)
    generation = env.node.role.generation
    now = [0.0]
    generation._clock = lambda: now[0]
    env.extension.generation_error = RuntimeError("device unavailable")
    assert generation.check()
    now[0] = 3.0
    assert not env.node.role.check_generation()
    assert generation.boot == f"{generation.boot_id}:unknown"
    assert env.node.wait_finished(5)
    assert env.lookup().result == ItemResult.MISS
    with pytest.raises(MemoryGenerationError):
        env.start_cxl()
    env.extension.generation_error = None
    env.start_cxl()
    _, hello = env.connect()
    assert hello.boot == f"{generation.boot_id}:g1~1"
    assert not env.node.adopted
    env.node.role.generation._grace_s = 0.0
    env.extension.generation_error = RuntimeError("device unavailable")
    assert not env.node.role.check_generation()
    assert env.node.wait_finished(5)
    env.extension.generation_error = None
    env.start_cxl()
    assert env.node.server.host_boot_id == f"{generation.boot_id}:g1~2"


def test_generation_thread_detects_reset_without_requests(reset_env):
    env = reset_env
    generation = env.node.role.generation
    generation.stop()
    generation._stop.clear()
    generation.start(0.01)
    env.extension.generation = "g2"
    assert env.node.wait_finished(5)
    assert env.node.new_run and env.node.ended
    assert not env.parts.exporter.exports()


def test_concurrent_generation_reads_never_set_an_older_one(tmp_path):
    ext = ResettableExtension()
    state = MemoryGeneration(
        ext,
        "boot",
        grace_s=1,
        state_path=tmp_path / "generation",
        on_change=lambda: None,
    )
    count = itertools.count(2)
    ext.memory_generation = lambda: f"g{next(count)}"
    state._read = ext.memory_generation
    threads = [
        threading.Thread(target=lambda: [state.check() for _ in range(50)])
        for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
        assert not thread.is_alive()
    assert state.boot == f"boot:g{next(count) - 1}"


def test_a_mapping_reset_before_its_reply_never_opens_catalog(extension_env):
    class ResetDuringMap(ResettableExtension):
        def mapped(self, *args, **kwargs):
            super().mapped(*args, **kwargs)
            self.generation = "g2"

    env = extension_env(ResetDuringMap(), start=False)
    with pytest.raises(ServerStopped, match="reset while region was mapped"):
        env.start_cxl()
    failed = env.nodes[-1]
    assert failed.wait_finished(5)
    assert not failed.role.parts.exporter.exports()
    assert env.config.node_id not in env.metadata.server.ms_service.registry.nodes
    env.start_cxl()
    assert env.node.server.host_boot_id.endswith(":g2")


def test_generation_unknown_between_check_and_mapping_never_exports(extension_env):
    ext = ResettableExtension()
    env = extension_env(ext, start=False)
    original = ext.validate_access

    def turn_unknown(handle, access):
        original(handle, access)
        ext.generation_error = RuntimeError("device unavailable")
        env.nodes[-1].role.generation._grace_s = 0.0

    ext.validate_access = turn_unknown
    with pytest.raises(ServerStopped):
        env.start_cxl()
    failed = env.nodes[-1]
    assert failed.wait_finished(5)
    assert failed.role.generation.boot.endswith(":unknown")
    assert not failed.role.parts.exporter.exports()
    assert env.config.node_id not in env.metadata.server.ms_service.registry.nodes


def test_failed_reset_revocation_keeps_mappings_holds_and_stops_restart(
    reset_env, monkeypatch
):
    env = reset_env
    env.write(env.reserve().credits)
    with monkeypatch.context() as patch:
        patch.setattr(
            env.parts.transport,
            "deregister",
            lambda _: (_ for _ in ()).throw(RuntimeError("NIC busy")),
        )
        env.extension.generation = "g2"
        assert not env.node.role.check_generation()
        assert env.node.wait_finished(5)
        assert "revocation failed" in env.node.teardown_failed
        assert env.parts._maps and all(
            not memory.closed for memory in env.parts._maps.values()
        )
        assert not env.rm.released


def test_node_automatically_restarts_after_reset_and_waits_for_known_generation(
    reset_env,
):
    env = reset_env
    env.write(env.reserve().credits)
    env.node.stop(CloseMode.RESTART)
    created = []

    def make(config):
        service = StorageService(
            config,
            region_source=env.rm,
            map_region=anon_region,
            make_transport=lambda name, device: FakeTransport(name),
            rpc_timeout_s=0.15,
        )
        created.append(service)
        env.nodes.append(service)
        return service

    config = dataclasses.replace(
        env.config,
        region_extension=dataclasses.replace(
            env.config.region_extension, generation_poll_s=0.01, generation_grace_s=0.01
        ),
    )
    node = Node(config, make_service=make, pause_s=0.01, max_pause_s=0.01)
    result = []
    thread = threading.Thread(target=lambda: result.append(node.run()))
    thread.start()
    try:
        wait_until(lambda: created and created[0].role.ops is not None)
        assert created[0].adopted
        env.extension.generation_error = RuntimeError("device unavailable")
        wait_until(lambda: len(created) >= 2)
        assert created[1].server is None
        assert not created[0].role.parts.exporter.exports()
        assert env.lookup().result == ItemResult.MISS
        env.extension.generation_error = None
        wait_until(lambda: created[1].role.ops is not None)
        assert not created[1].adopted and created[1].server.host_boot_id.endswith(
            ":g1~1"
        )
    finally:
        node.stop()
        thread.join(10)
        assert not thread.is_alive()
    assert result == [True] and node.failed is None


def test_hello_detects_reset_and_never_returns_old_pools(reset_env, monkeypatch):
    env = reset_env
    ended = []
    monkeypatch.setattr(
        env.node, "_end_run", lambda *args, **kwargs: ended.append(args)
    )
    env.extension.generation = "g2"
    _, hello = env.connect()
    assert hello.status == Status.BUSY and hello.pools == []
    assert hello.boot.endswith(":g2") and ended
    assert not env.parts.exporter.exports()
