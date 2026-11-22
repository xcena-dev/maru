# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Single-store nodes and worker processes against the real MaruServer."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from maru_common.ms_rpc import MsRpcClient
from maru_common.protocol import (
    MESSAGE_CLASSES,
    MS_MESSAGE_CLASSES,
    HandshakeRequest,
    MessageType,
)
from maru_common.storage_types import CloseMode, ItemResult
from maru_storage_service.cli import config_to_dict
from maru_storage_service.pool_manager import PageState
from tests.integration.ms_l1_helpers import STORES, command_argv, l1_config
from tests.unit.ms_node_helpers import (
    PAGE,
    WORKTREE,
    Agent,
    AgentError,
    RawWorker,
    listening,
    publish,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)

pytestmark = pytest.mark.integration


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def subprocess_env() -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = WORKTREE + os.pathsep + env.get("PYTHONPATH", "")
    return env


class RealMaruServer:
    """A metadata server on a private loopback port."""

    def __init__(
        self,
        workdir: str,
        ttl_s: float,
        heartbeat_s: float,
        policy=None,
        *,
        port: int | None = None,
    ):
        for _ in range(2):
            chosen = port or free_port()
            self.url = f"tcp://127.0.0.1:{chosen}"
            self.log_path = os.path.join(
                workdir, f"maru-node-server-{chosen}-{os.urandom(3).hex()}.log"
            )
            self._log = open(self.log_path, "wb")
            args = [sys.executable, "-m", "maru_server.node_server"]
            args += ["--host", "127.0.0.1", "--port", str(chosen)]
            args += ["--session-ttl-s", str(ttl_s), "--heartbeat-s", str(heartbeat_s)]
            if policy is not None:
                args += ["--policy-file", policy]
            self.proc = subprocess.Popen(
                args,
                env=subprocess_env(),
                cwd=WORKTREE,
                stdout=self._log,
                stderr=subprocess.STDOUT,
            )
            started = f"started on {self.url}"
            if (
                wait_until(
                    lambda started=started: (
                        self.proc.poll() is not None or started in self.log()
                    ),
                    timeout=30,
                    what="MaruServer start",
                )
                and self.proc.poll() is None
            ):
                break
            self._log.close()  # it died: the port was taken; try another
        else:
            raise AssertionError(f"MaruServer did not start: {self.log()}")
        # Our process holds the port (it logged the bind), so it answers
        self.server_epoch = wait_until(self.handshake, timeout=30, what="handshake")
        assert self.proc.poll() is None

    def log(self) -> str:
        with open(self.log_path, "rb") as f:
            return f.read().decode(errors="replace")

    def handshake(self) -> str | None:
        rpc = MsRpcClient(
            self.url,
            timeout_s=0.5,
            message_classes=MESSAGE_CLASSES,
            strict_codes=MS_MESSAGE_CLASSES,
        )
        try:
            return rpc.call(MessageType.HANDSHAKE, HandshakeRequest()).server_epoch
        except Exception:
            return None
        finally:
            rpc.close()

    def kill(self) -> None:
        """SIGKILL: every state is lost."""
        self.proc.kill()
        self.proc.wait(10)

    @property
    def port(self) -> int:
        return int(self.url.rsplit(":", 1)[1])

    def pause(self) -> None:
        self.proc.send_signal(signal.SIGSTOP)

    def resume(self) -> None:
        self.proc.send_signal(signal.SIGCONT)

    def admin_delete(self, namespace: str, keys: list[str]) -> dict[str, str]:
        out = subprocess.run(
            [sys.executable, "-m", "maru_server.ms_admin", "delete"]
            + ["--server", self.url, "--namespace", namespace, *keys],
            env=subprocess_env(),
            cwd=WORKTREE,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        return dict(line.split("\t", 1) for line in out.stdout.splitlines())

    def close(self) -> None:
        if self.proc.poll() is None:
            self.resume()
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(10)
        self._log.close()


@pytest.fixture
def workdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def servers(workdir):
    made = []

    def make(ttl_s: float = 6.0, heartbeat_s: float = 0.5, policy=None):
        server = RealMaruServer(workdir, ttl_s, heartbeat_s, policy)
        made.append(server)
        return server

    yield make
    for server in made:
        server.close()


@pytest.fixture(params=STORES)
def nodes(workdir, request):
    made = []

    def make(server: RealMaruServer, **kwargs):
        options = {
            k: kwargs.pop(k) for k in list(kwargs) if k in ("gpu_drain_grace_s",)
        }
        node = start_node(
            l1_config(
                server.url, f"{workdir}/node.sock", store=request.param, **kwargs
            ),
            rpc_timeout_s=0.5,
            **options,
        )
        made.append(node)
        return node

    yield make
    for node in made:
        node.stop(CloseMode.LEAVE)


@pytest.fixture
def agents(workdir):
    made = []

    def make(socket_path: str, engine: str = "eng", **kwargs):
        agent = Agent(socket_path, engine=engine, log_dir=workdir, **kwargs)
        made.append(agent)
        agent.connect()
        return agent

    yield make
    for agent in made:
        agent.stop()


def value(key: str) -> str:
    return (key * 4).encode().hex()


def store(agent, key: str) -> bool:
    agent.call("wait_credit", bytes=PAGE)
    return agent.call("store", key=key, value=value(key))


def readable(agent, key: str) -> bool:
    got = agent.call("retrieve", key=key, hold=False)
    assert got in (None, value(key))
    return got is not None


def test_two_processes_share_one_pool(servers, nodes, agents):
    server = servers()
    node = nodes(server, pages=32)
    a = agents(node.config.socket, "engine-a")
    b = agents(node.config.socket, "engine-b")
    for i in range(18):  # more than half of the pool: no split in advance
        assert store(a, f"a{i}")
    for i in range(4):
        assert store(b, f"b{i}")
    assert node.pools.bytes_in(PageState.READY) == 22 * PAGE
    assert a.call("state")["pools"] == b.call("state")["pools"] == node.pools.pool_ids()
    assert readable(b, "a0") and readable(b, "a17") and readable(a, "b3")
    node.pools.check()


def test_kill_9_of_a_writer(servers, nodes, agents):
    server = servers()
    node = nodes(server, gpu_drain_grace_s=1.0)
    w = agents(node.config.socket, "writer")
    for key in ("w0", "w1", "w2"):
        assert store(w, key)
    assert w.call("retrieve", key="w0") == value("w0")  # a pin it holds
    w.call("wait_credit", bytes=PAGE)
    w.call("alloc_hold", name="half", size=64)  # an unfinished write
    w.call("wait_credit", bytes=PAGE)
    assert node.pools.held_bytes(w.incarnation) >= 2 * PAGE
    w.kill()
    wait_until(
        lambda: (
            node.pools.held_bytes(w.incarnation) == 0
            and node.pools.bytes_in(PageState.QUARANTINED) > 0
        ),
        what="quarantine",
    )
    wait_until(
        lambda: node.pools.bytes_in(PageState.QUARANTINED) == 0, what="grace over"
    )
    wait_until(lambda: node.manager.pending_exits() == 0, what="WORKER_EXIT sent")
    r = agents(node.config.socket, "reader", read_only=True)
    for key in ("w0", "w1", "w2"):
        assert readable(r, key)  # its committed KV stays
    # WORKER_EXIT released its pin at MaruServer: the delete goes through
    assert server.admin_delete("ns", ["w0"]) == {"w0": "OK"}
    wait_until(lambda: not readable(r, "w0"), what="w0 retired")
    node.pools.check()


def test_full_gpu_store_skips_and_keeps_the_pinned_key(servers, nodes, agents):
    server = servers()
    node = nodes(
        server,
        pages=16,
        budgets={
            "credit_max_bytes": 2 * PAGE,
            "credit_total_max_bytes": 4 * PAGE,
            "credit_refill_low": PAGE,
            "credit_refill_high": 2 * PAGE,
            "commit_pending_max_bytes": 2 * PAGE,
        },
    )
    w = agents(node.config.socket, "writer")
    r = agents(node.config.socket, "reader", read_only=True)
    assert store(w, "k0")
    assert r.call("retrieve", key="k0") == value("k0")  # held: a pin
    capacity = sum(node.pools.pool_info(p).capacity for p in node.pools.pool_ids())
    count = capacity // PAGE - 1
    for i in range(1, count):
        assert store(w, f"k{i}")
    with pytest.raises(AgentError, match="no credit"):
        w.call("wait_credit", bytes=PAGE, wait_s=0.5)
    assert node.pools.bytes_in(PageState.READY) == count * PAGE
    assert not node.manager.retire_results
    for i in range(count):
        assert readable(w, f"k{i}")
    r.call("release", key="k0")
    assert server.admin_delete("ns", ["k1"]) == {"k1": "OK"}
    wait_until(lambda: not readable(w, "k1"), what="deleted page returned")
    assert store(w, "after")
    assert readable(w, "after")
    node.pools.check()


def test_maruserver_stopped_then_back(servers, nodes, agents):
    server = servers(ttl_s=60.0, heartbeat_s=0.5)  # no expiry while paused
    node = nodes(
        server,
        budgets={
            "credit_max_bytes": 4 * PAGE,
            "credit_total_max_bytes": 8 * PAGE,
            "credit_refill_low": PAGE,
            "credit_refill_high": 4 * PAGE,
            "commit_pending_max_bytes": 2 * PAGE,
        },
    )
    w = agents(node.config.socket, "writer", store_timeout_s=0.5)
    w.call("wait_credit", bytes=4 * PAGE)
    server.pause()
    try:
        stored, skipped = [], 0
        for i in range(8):
            t0 = time.monotonic()
            try:
                result = w.call("store", key=f"p{i}", value=value(f"p{i}"))
            except AgentError:
                skipped += 1  # no credit: the store is skipped
                continue
            assert result is False and time.monotonic() - t0 < 3.0
            stored.append(f"p{i}")
        cap = node.config.budgets.commit_pending_max_bytes
        assert node.pools.bytes_in(PageState.PENDING) == len(stored) * PAGE >= cap
        assert skipped > 0
        raw = RawWorker(node.config.socket)
        raw.attach()
        assert raw.credit(PAGE).credits == []  # no credit at the cap
        raw.close()
    finally:
        server.resume()
    wait_until(lambda: node.pools.bytes_in(PageState.PENDING) == 0, what="published")
    # The worker looks keys up again once a heartbeat is answered
    wait_until(lambda: readable(w, stored[0]), what="MaruServer answers the worker")
    for key in stored:
        assert readable(w, key)
    assert store(w, "after")
    assert node.server.renewals == 0 and node.manager.ended is None
    node.pools.check()


def test_node_session_expiry_is_renewed(servers, nodes, agents):
    server = servers(ttl_s=2.0, heartbeat_s=0.4)
    node = nodes(server)
    w = agents(node.config.socket, "writer")
    assert store(w, "before")
    epoch_seq = node.server.epoch_seq
    wait_until(lambda: node.server.last_heartbeat_ok > 0, what="a heartbeat")
    server.pause()
    # past the TTL since the last heartbeat MaruServer answered
    wait_until(
        lambda: time.monotonic() - node.server.last_heartbeat_ok > 2.0 + 0.5,
        timeout=30,
        what="session TTL passed",
    )
    server.resume()
    wait_until(lambda: node.server.renewals >= 1, timeout=15, what="renewal")
    assert node.server.epoch_seq == epoch_seq  # the same run
    assert node.manager.ended is None
    assert readable(w, "before")  # the worker reopens its expired session
    assert store(w, "after")
    assert readable(w, "after")


@pytest.mark.parametrize("store_kind", STORES)
def test_storage_service_restart(servers, workdir, agents, store_kind):
    server = servers()
    config = l1_config(server.url, f"{workdir}/node.sock", store=store_kind)
    path = os.path.join(workdir, "node.json")
    with open(path, "w") as f:
        json.dump(_raw_config(config), f)
    log_path = os.path.join(workdir, "maru-storage.log")
    log = open(log_path, "wb")
    first = subprocess.Popen(
        command_argv("maru-storage", "--config", path),
        env=subprocess_env(),
        cwd=WORKTREE,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    second = None
    try:
        wait_until(lambda: listening(config.socket), timeout=30, what="node listening")
        x = agents(config.socket, "stays")
        y = agents(config.socket, "ends")
        old_epoch = x.call("state")["node_epoch"]
        assert store(x, "x0")
        if store_kind == "dram":
            x.call("wait_credit", bytes=PAGE)
            x.call("alloc_hold", name="copy", size=64)
        first.kill()  # the StorageService crashes
        first.wait(10)
        y.kill()  # and a worker ends while it is down
        second = start_node(config, rpc_timeout_s=0.5)
        assert second.node_epoch != old_epoch
        # The worker attaches to the new run before returning the old DRAM credit.
        wait_until(lambda: x.call("state")["node_epoch"] == second.node_epoch)
        assert readable(x, "x0") is (store_kind == "local_cxl")
        assert store(x, "x1")  # reopened on the new run
        assert readable(x, "x1")
        if store_kind == "dram":
            x.call("free_held", name="copy")
        second.pools.check()
    except BaseException:
        log.flush()
        with open(log_path, "rb") as f:
            print("maru-storage output:\n" + f.read().decode(errors="replace"))
        raise
    finally:
        if first.poll() is None:
            first.kill()
            first.wait(10)
        log.close()
        if second is not None:
            second.stop(CloseMode.LEAVE)


def _raw_config(config) -> dict:
    return config_to_dict(config)


def test_admin_delete_is_retired_by_the_node(servers, nodes, agents):
    server = servers()
    node = nodes(server)
    w = agents(node.config.socket, "writer")
    r = agents(node.config.socket, "reader")
    for key in ("d0", "d1"):
        assert store(w, key)
    assert r.call("retrieve", key="d1") == value("d1")  # pinned
    assert server.admin_delete("ns", ["d0", "d1", "none"]) == {
        "d0": "OK",
        "d1": "OK",
        "none": "MISS (absent)",
    }
    wait_until(lambda: not readable(w, "d0"), what="d0 retired")
    wait_until(lambda: node.manager.retire_results["KEPT:PINNED"] >= 1, what="d1 tried")
    assert readable(w, "d1")  # KEPT(PINNED): the block waits for the pin
    r.call("release", key="d1")
    wait_until(lambda: not readable(w, "d1"), what="d1 retired")
    wait_until(lambda: node.pools.bytes_in(PageState.READY) == 0, what="pages back")
    node.pools.check()


def test_preserved_key_is_kept(servers, nodes, agents, workdir):
    policy = os.path.join(workdir, "policy.json")
    with open(policy, "w") as f:
        json.dump({"preserve": [{"namespace": "ns", "key": "keep"}]}, f)
    server = servers(policy=policy)
    node = nodes(server)
    w = agents(node.config.socket, "writer")
    assert store(w, "keep")
    assert store(w, "drop")
    out = server.admin_delete("ns", ["keep", "drop"])
    assert out["keep"].startswith("KEPT") and out["drop"] == "OK"
    wait_until(lambda: not readable(w, "drop"), what="drop retired")
    # Explicit retirement of a preserved key returns KEPT(PINNED).
    from maru_common.storage_types import Key, KeyLocation

    ref = next(
        r
        for pool in node.pools.pool_ids()
        for r in node.pools.pages_in(pool, PageState.READY)
        if node.pools.page(r).key == Key("ns", "keep")
    )
    page = node.pools.page(ref)
    node.manager.retire([ref], [KeyLocation(page.key, page.location)])
    assert node.manager.retire_results["KEPT:PINNED"] >= 1
    assert readable(w, "keep")


@pytest.mark.parametrize("store_kind", STORES)
def test_memory_node_lru_keeps_preserved_key(servers, workdir, store_kind):
    policy = os.path.join(workdir, "memory-policy.json")
    with open(policy, "w") as stream:
        json.dump({"preserve": [{"namespace": "ns", "key": "k0"}]}, stream)
    server = servers(policy=policy)
    config = l1_config(
        server.url,
        f"{workdir}/memory.sock",
        store=store_kind,
        role="memory",
        high=0.75,
        low=0.5,
    )
    node = start_node(config, rpc_timeout_s=0.5)
    try:
        capacity = sum(node.pools.pool_info(p).capacity for p in node.pools.pool_ids())
        count = int(capacity // PAGE * 0.75) + 1
        for i in range(count):
            assert publish(node, f"k{i}").result is ItemResult.ADDED
        wait_until(lambda: node.manager.retire_results["RETIRED"] > 0)
        wait_until(lambda: node.manager.retire_results["KEPT:PINNED"] > 0)
        assert server.admin_delete("ns", ["k0"])["k0"].startswith("KEPT")
        assert publish(node, "after").result is ItemResult.ADDED
        assert node.local is None
        node.pools.check()
    finally:
        node.stop(CloseMode.LEAVE)


@pytest.mark.parametrize("store_kind", STORES)
def test_memory_node_command_restart(servers, workdir, store_kind):
    server = servers()
    config = l1_config(
        server.url, f"{workdir}/memory.sock", store=store_kind, role="memory"
    )
    raw = _raw_config(config)
    assert "role" not in raw and "socket" not in raw
    path = Path(workdir, "memory.json")
    path.write_text(json.dumps(raw))
    for run in range(2):
        log = Path(workdir, f"memory-{run}.log")
        with log.open("wb") as out:
            proc = subprocess.Popen(
                command_argv("maru-memory-node", "--config", str(path)),
                env=subprocess_env(),
                cwd=WORKTREE,
                stdout=out,
                stderr=subprocess.STDOUT,
            )
        try:
            wait_until(
                lambda log=log, proc=proc: (
                    "serving memory node" in log.read_text() or proc.poll() is not None
                ),
                timeout=30,
                what="memory node start",
            )
            assert proc.poll() is None, log.read_text()
            assert Path(config.state_dir, f"{config.node_id}.lock").exists()
            assert not Path(workdir, "memory.sock").exists()
            proc.terminate()
            assert proc.wait(30) == 0, log.read_text()
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(10)
    # SIGTERM retains CXL ownership for a same-boot restart.
    if store_kind == "local_cxl":
        node = start_node(config, rpc_timeout_s=0.5)
        node.stop(CloseMode.LEAVE)
