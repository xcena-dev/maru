# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Process evidence: alive, ended, or unknown."""

import errno
import os
import subprocess
import threading

import pytest

from maru_common.node_protocol import process_start_time
from maru_storage_service.gpu import peer_watcher
from maru_storage_service.gpu.peer_watcher import PeerWatcher


@pytest.fixture
def watcher():
    watcher = PeerWatcher()
    yield watcher
    watcher.stop()


def test_watching_a_live_process_is_alive(watcher):
    assert watcher.watch("self", os.getpid(), process_start_time(os.getpid())) is True
    assert watcher.watching("self")


def test_a_reused_pid_is_ended(watcher):
    assert (
        watcher.watch("reused", os.getpid(), process_start_time(os.getpid()) + 1)
        is False
    )
    assert not watcher.watching("reused")


def test_pidfd_process_lookup_error_is_ended(watcher, monkeypatch):
    def gone(pid):
        raise ProcessLookupError(errno.ESRCH, "gone")

    monkeypatch.setattr(os, "pidfd_open", gone)
    assert watcher.watch("gone", 42, 7) is False


@pytest.mark.parametrize("error", [errno.EPERM, errno.EMFILE, errno.EIO])
def test_an_orphan_without_evidence_of_its_end_is_kept(watcher, monkeypatch, error):
    def unknown(pid):
        raise OSError(error, "unknown")

    monkeypatch.setattr(os, "pidfd_open", unknown)
    assert watcher.watch("orphan", 42, 7) is None
    assert not watcher.watching("orphan")


@pytest.mark.parametrize(
    "error", [PermissionError(), FileNotFoundError(), ValueError(), IndexError()]
)
def test_proc_read_errors_are_unknown_and_close_the_pidfd(watcher, monkeypatch, error):
    opened = []
    pidfd_open = os.pidfd_open

    def record(pid):
        fd = pidfd_open(pid)
        opened.append(fd)
        return fd

    def unknown(pid):
        raise error

    monkeypatch.setattr(os, "pidfd_open", record)
    monkeypatch.setattr(peer_watcher, "process_start_time", unknown)
    assert watcher.watch("self", os.getpid(), 7) is None
    assert not watcher.watching("self")
    with pytest.raises(OSError, match="Bad file descriptor"):
        os.fstat(opened[0])


def test_unknown_rewatch_preserves_the_existing_watch(watcher, monkeypatch):
    pid = os.getpid()
    start = process_start_time(pid)
    assert watcher.watch("self", pid, start) is True

    def unknown(pid):
        raise PermissionError()

    monkeypatch.setattr(peer_watcher, "process_start_time", unknown)
    assert watcher.watch("self", pid, start) is None
    assert watcher.watching("self")


def test_pidfd_exit_reports_the_watched_incarnation_once(watcher):
    child = subprocess.Popen(["sleep", "30"])
    exited = threading.Event()
    reports = []

    def report(key):
        reports.append(key)
        exited.set()

    watcher.start(report)
    try:
        assert (
            watcher.watch("incarnation", child.pid, process_start_time(child.pid))
            is True
        )
        child.kill()
        child.wait(5)
        assert exited.wait(5)
        assert reports == ["incarnation"]
        assert not watcher.watching("incarnation")
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(5)
