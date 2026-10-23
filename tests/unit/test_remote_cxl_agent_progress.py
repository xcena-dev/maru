# SPDX-License-Identifier: Apache-2.0
"""Remote CXL agent: UCX is driven while a worker connects, and only then."""

import logging
import sys
import time
import types

import pytest

from maru_remote import transport as transport_mod
from maru_remote.transport import NixlTransport
from maru_remote_cxl_agent.progress import ConnectProgress
from maru_remote_cxl_agent.server import MAX_NIXL_AGENT_NAME, handle_request
from maru_remote_cxl_agent.service import RegionMapService
from tests.unit.remote_fakes import (
    FakeCxl,
    FakeDeviceMapper,
    FakeNixlAgent,
    reset_fake_agents,
)


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def agent_logs(caplog, monkeypatch):
    # The package logger has its own handler and does not propagate once
    # configured; let its records reach caplog.
    monkeypatch.setattr(logging.getLogger("maru_remote_cxl_agent"), "propagate", True)
    return caplog


def wait_for(condition, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached")
        time.sleep(0.005)


class Node:
    """A region map service with fake RM, device and NIXL."""

    def __init__(self):
        self.cxl = FakeCxl()
        self.agent = FakeNixlAgent("mapper")
        self.transport = NixlTransport("mapper", agent=self.agent)
        self.service = RegionMapService(
            self.cxl,
            FakeDeviceMapper(self.cxl),
            self.transport,
            node_id="mem1",
            epoch="e1",
            boot_id="b1",
        )

    def region(self, size=4096):
        return self.cxl.reserve("owner", size).to_dict()


@pytest.fixture
def node():
    n = Node()
    yield n
    n.service.close()


def worker(name="w1"):
    return NixlTransport(name, agent=FakeNixlAgent(name))


# ------------------------------------------------------------ the transport


def test_the_nixl_agent_has_no_progress_thread(monkeypatch):
    made = {}

    class Agent:
        def __init__(self, name, config):
            made["name"], made["config"] = name, config

        def create_backend(self, backend, params):
            made["backend"] = (backend, params)

    api = types.ModuleType("nixl._api")
    api.nixl_agent = Agent
    api.nixl_agent_config = lambda **kwargs: kwargs
    monkeypatch.setitem(sys.modules, "nixl", types.ModuleType("nixl"))
    monkeypatch.setitem(sys.modules, "nixl._api", api)
    transport_mod._default_agent("w1", "mlx5_0:1")
    assert made["config"]["enable_prog_thread"] is False
    assert made["backend"] == ("UCX", {"ucx_devices": "mlx5_0:1"})


def test_an_announcement_reaches_the_peer_and_take_notifs_returns_it():
    a, b = worker("a"), worker("b")
    peer = a.add_peer(b.metadata())
    assert a.announce(peer)
    assert b.take_notifs() == {"a": [b""]}
    assert b.take_notifs() == {}


def test_an_announcement_over_a_failed_connection_is_refused():
    a, b = worker("a"), worker("b")
    peer = a.add_peer(b.metadata())
    a._agent.fail_endpoint(peer)
    assert not a.announce(peer)
    assert b.take_notifs() == {}


# --------------------------------------------------------- connect progress


def test_no_thread_runs_while_no_worker_connects():
    progress = ConnectProgress(worker("mapper"))
    progress.drain()
    assert not progress.running() and progress.expecting() == set()


def test_the_wait_for_a_worker_ends_when_it_announces_itself():
    agent = worker("mapper")
    progress = ConnectProgress(agent)
    progress.expect("w1")
    assert progress.running() and progress.expecting() == {"w1"}
    w = worker("w1")
    w.announce(w.add_peer(agent.metadata()))
    wait_for(lambda: not progress.running())
    assert progress.expecting() == set()


def test_a_worker_that_never_announces_itself_waits_only_for_the_window(agent_logs):
    progress = ConnectProgress(worker("mapper"), window_s=0.05)
    with agent_logs.at_level(logging.INFO, logger="maru_remote_cxl_agent.progress"):
        progress.expect("ghost")
        wait_for(lambda: not progress.running(), timeout=2.0)
    assert progress.expecting() == set()
    assert "did not announce itself" in agent_logs.text


def test_an_announcement_drained_by_another_call_ends_the_wait():
    agent = worker("mapper")
    progress = ConnectProgress(agent, poll_s=0.5)
    progress.expect("w1")
    w = worker("w1")
    w.announce(w.add_peer(agent.metadata()))
    progress.drain()  # a hello or a sweep took it
    assert progress.expecting() == set()
    wait_for(lambda: not progress.running())


def test_waits_for_several_workers_end_one_by_one():
    agent = worker("mapper")
    progress = ConnectProgress(agent, poll_s=0.5)
    progress.expect("w1")
    progress.expect("w2")
    w1 = worker("w1")
    w1.announce(w1.add_peer(agent.metadata()))
    progress.drain()
    assert progress.expecting() == {"w2"} and progress.running()
    w2 = worker("w2")
    w2.announce(w2.add_peer(agent.metadata()))
    wait_for(lambda: not progress.running())


def test_a_closed_progress_waits_for_no_one():
    progress = ConnectProgress(worker("mapper"), poll_s=0.01)
    progress.expect("w1")
    progress.close()
    wait_for(lambda: not progress.running())
    progress.expect("w2")
    assert not progress.running() and progress.expecting() == set()


def test_a_failing_drain_keeps_the_wait_going_until_the_window(agent_logs):
    class Broken:
        def take_notifs(self):
            raise RuntimeError("NIXL failed")

    progress = ConnectProgress(Broken(), window_s=0.05)
    with agent_logs.at_level(logging.WARNING, logger="maru_remote_cxl_agent.progress"):
        progress.expect("w1")
        wait_for(lambda: not progress.running(), timeout=2.0)
    assert agent_logs.text.count("driving UCX for connecting workers failed") == 1


# ------------------------------------------------------------------ service


def test_a_map_naming_a_nixl_agent_drives_ucx_until_it_announces_itself(node):
    reply = node.service.map(1, node.region(), "w1")
    assert node.service._progress.expecting() == {"w1"}
    w = worker("w1")
    w.announce(w.add_peer(reply["metadata"]))
    wait_for(lambda: not node.service._progress.running())


def test_a_map_naming_no_nixl_agent_drives_nothing(node):
    node.service.map(1, node.region())
    assert not node.service._progress.running()


def test_a_refused_map_waits_for_no_one(node):
    handle = {**node.region(), "auth_token": 1}
    with pytest.raises(RuntimeError):  # the resource manager refused it
        node.service.map(1, handle, "w1")
    assert node.service._progress.expecting() == set()


def test_a_sweep_drops_the_notifications_workers_sent(node):
    reply = node.service.map(1, node.region())
    w = worker("w1")
    assert w.peer_usable(w.add_peer(reply["metadata"]))  # a connection check
    assert node.agent.notifs
    node.service.sweep()
    assert node.agent.notifs == []


# ---------------------------------------------------------------- front end


def test_a_map_request_carries_the_nixl_agent_to_the_service(node):
    reply = handle_request(
        node.service, 1, {"op": "map", "handle": node.region(), "nixl_agent": "w1"}
    )
    assert reply["ok"]
    assert node.service._progress.expecting() == {"w1"}


@pytest.mark.parametrize("name", [5, b"w1", "", "w" * (MAX_NIXL_AGENT_NAME + 1)])
def test_a_map_request_with_a_malformed_nixl_agent_is_invalid(node, name):
    handle = node.region()
    reply = handle_request(
        node.service, 1, {"op": "map", "handle": handle, "nixl_agent": name}
    )
    assert reply["code"] == "invalid"
    assert node.service.holders() == {}
    assert node.service._progress.expecting() == set()
