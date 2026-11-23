# SPDX-License-Identifier: Apache-2.0
"""Shared progress during worker connection setup."""

import logging
import time

import pytest

from maru_remote.progress import ConnectProgress
from maru_remote.transport import NixlTransport
from tests.unit.remote_fakes import FakeNixlAgent, reset_fake_agents


@pytest.fixture(autouse=True)
def fresh():
    reset_fake_agents()
    yield
    reset_fake_agents()


def worker(name):
    return NixlTransport(name, agent=FakeNixlAgent(name))


def wait_for(condition, timeout=5):
    end = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < end
        time.sleep(0.005)


@pytest.fixture
def agent_logs(caplog, monkeypatch):
    logger = logging.getLogger("maru_remote.progress")
    monkeypatch.setattr(logger, "propagate", False)
    logger.addHandler(caplog.handler)
    yield caplog
    logger.removeHandler(caplog.handler)


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
    with agent_logs.at_level(logging.INFO, logger="maru_remote.progress"):
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
    with agent_logs.at_level(logging.WARNING, logger="maru_remote.progress"):
        progress.expect("w1")
        wait_for(lambda: not progress.running(), timeout=2.0)
    assert agent_logs.text.count("driving UCX for connecting workers failed") == 1


# ------------------------------------------------------------------ service
