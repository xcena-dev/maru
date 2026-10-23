# SPDX-License-Identifier: Apache-2.0
"""Incomplete RDMA setup/cleanup stays unusable until cleanup succeeds.

Only owned anonymous probe memory is mapped. Staging, registration, and transport
cleanup are fakes; these tests do not import NIXL or call CUDA/CXL/NIC APIs.
"""

from types import SimpleNamespace

import pytest

from maru_common.storage_types import StorageUnavailableError
from maru_handler.storage.remote_cxl import datapath


class Staging:
    def __init__(self, size, page):
        self.count = size // page
        self.address = 1000
        self.nbytes = size
        self.closed = False

    def cuda_register(self):
        return False

    def close(self):
        self.closed = True


class Transport:
    def __init__(self, *, fail_probe=False, fail_close=False):
        self.fail_probe = fail_probe
        self.fail_close = fail_close
        self.partial_cleanup = False
        self.registration_calls = []
        self.registrations = []
        self.close_calls = 0

    def register(self, address, length, tag):
        self.registration_calls.append(tag)
        if tag == "connect-probe" and self.fail_probe:
            raise RuntimeError("probe registration failed")
        self.registrations.append(tag)
        return tag

    def close(self):
        self.close_calls += 1
        if self.partial_cleanup and self.registrations:
            self.registrations.pop(0)
        if self.fail_close:
            raise RuntimeError("deregistration failed")
        self.registrations.clear()


def make_data(monkeypatch, first):
    monkeypatch.setattr(datapath, "StagingBuffer", Staging)
    made = []

    def make_transport(name):
        transport = first if not made else Transport()
        made.append(transport)
        return transport

    config = SimpleNamespace(
        chunk_size_bytes=4096,
        rdma_staging_size=32768,
        rdma_load_reserve=0.25,
        engine_id="audit",
    )
    return (
        datapath.RdmaDataPath(
            config,
            transport_factory=make_transport,
            agent_factory=lambda address: None,
        ),
        made,
    )


def test_failed_setup_cannot_become_ready_on_start_retry(monkeypatch):
    first = Transport(fail_probe=True, fail_close=True)
    data, made = make_data(monkeypatch, first)
    try:
        with pytest.raises(RuntimeError, match="probe registration failed"):
            data.start()
        staging, probe = data.staging, data._probe
        with pytest.raises(StorageUnavailableError, match="cleanup is pending"):
            data.start()
        assert made == [first]
        assert first.registration_calls == ["staging", "connect-probe"]
        assert first.close_calls == 1
        assert data.transport is first and data.staging is staging
        assert not staging.closed and not probe.closed

        first.fail_close = False
        data.close()
        assert staging.closed and probe.closed
        data.start()
        assert len(made) == 2 and data.transport is made[1]
        assert made[1].registrations == ["staging", "connect-probe"]
        data.start()  # Successfully initialized start remains idempotent.
        assert len(made) == 2
        assert made[1].registration_calls == ["staging", "connect-probe"]
    finally:
        first.fail_close = False
        data.close()


def test_partial_cleanup_cannot_resume_with_a_missing_registration(monkeypatch):
    first = Transport()
    data, made = make_data(monkeypatch, first)
    try:
        data.start()
        staging, probe = data.staging, data._probe
        first.partial_cleanup = first.fail_close = True
        data.close()
        assert first.registrations == ["connect-probe"]
        with pytest.raises(StorageUnavailableError, match="cleanup is pending"):
            data.start()
        assert made == [first]
        assert data.transport is first and data.staging is staging
        assert not staging.closed and not probe.closed

        first.fail_close = False
        data.close()
        assert staging.closed and probe.closed
        data.start()
        assert len(made) == 2
        assert made[1].registrations == ["staging", "connect-probe"]
    finally:
        first.fail_close = False
        data.close()


def test_failed_setup_with_successful_cleanup_can_start_fresh(monkeypatch):
    first = Transport(fail_probe=True)
    data, made = make_data(monkeypatch, first)
    try:
        with pytest.raises(RuntimeError, match="probe registration failed"):
            data.start()
        assert data.transport is None and data.staging is None
        data.start()
        assert len(made) == 2
        assert made[1].registrations == ["staging", "connect-probe"]
    finally:
        data.close()
