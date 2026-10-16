# SPDX-License-Identifier: Apache-2.0
"""The region mapper's system service: unit, settings file and install."""

import pytest

from maru_region_mapper import installer


def _install(tmp_path, monkeypatch, nixl=True):
    monkeypatch.setattr(installer, "nixl_available", lambda: nixl)
    systemd = tmp_path / "systemd"
    systemd.mkdir(exist_ok=True)
    env = tmp_path / "etc" / "maru" / "region-mapper.env"
    calls = []
    ok = installer.install(
        python="/opt/venv/bin/python",
        systemd_dir=systemd,
        env_file=env,
        systemctl=lambda *a: calls.append(a),
    )
    return ok, systemd / installer.SERVICE_NAME, env, calls


def test_the_service_starts_and_stops_with_the_resource_manager():
    unit = installer.render_unit("/opt/venv/bin/python")
    assert "ExecStart=/opt/venv/bin/python -m maru_region_mapper.cli" in unit
    # Started by the RM's start, stopped and restarted with it; the RM's own
    # unit needs no change.
    assert "WantedBy=maru-resource-manager.service" in unit
    assert "PartOf=maru-resource-manager.service" in unit
    assert "After=network-online.target maru-resource-manager.service" in unit
    assert f"EnvironmentFile=-{installer.ENV_FILE}" in unit
    # NIC registration pins memory; the node lock lives in /run/maru.
    assert "LimitMEMLOCK=infinity" in unit
    assert "RuntimeDirectory=maru" in unit
    # A node without an RDMA NIC fails every start: stop retrying.
    assert "StartLimitIntervalSec=60" in unit
    assert "StartLimitBurst=5" in unit


def test_a_standalone_service_starts_at_boot_without_a_resource_manager():
    # A memory node of a shared pool that runs no resource manager.
    unit = installer.render_unit("/opt/venv/bin/python", standalone=True)
    assert "WantedBy=multi-user.target" in unit
    assert "maru-resource-manager.service" not in unit
    assert "After=network-online.target\n" in unit
    assert "ExecStart=/opt/venv/bin/python -m maru_region_mapper.cli" in unit
    assert "LimitMEMLOCK=infinity" in unit


def test_install_standalone_writes_the_standalone_unit(tmp_path, monkeypatch):
    monkeypatch.setattr(installer, "nixl_available", lambda: True)
    systemd = tmp_path / "systemd"
    systemd.mkdir()
    assert installer.install(
        python="/opt/venv/bin/python",
        systemd_dir=systemd,
        env_file=tmp_path / "region-mapper.env",
        systemctl=lambda *a: None,
        standalone=True,
    )
    unit = (systemd / installer.SERVICE_NAME).read_text()
    assert unit == installer.render_unit("/opt/venv/bin/python", standalone=True)
    # This node runs no resource manager: the operator names the pool's.
    settings = (tmp_path / "region-mapper.env").read_text()
    assert settings == installer.STANDALONE_ENV
    assert "\nMARU_RM_ADDRESS=\n" in settings
    assert "starts and stops with maru-resource-manager" not in settings


def test_install_writes_the_unit_and_settings_and_enables_it(tmp_path, monkeypatch):
    ok, unit, env, calls = _install(tmp_path, monkeypatch)
    assert ok
    assert unit.read_text() == installer.render_unit("/opt/venv/bin/python")
    assert env.read_text() == installer.DEFAULT_ENV
    assert calls == [("daemon-reload",), ("enable", installer.SERVICE_NAME)]


def test_default_settings_accept_workers_of_other_nodes():
    settings = dict(
        line.split("=", 1)
        for line in installer.DEFAULT_ENV.splitlines()
        if line and not line.startswith("#")
    )
    assert settings == {
        "MARU_RM_ADDRESS": "127.0.0.1:9850",
        "MARU_REGION_MAPPER_HOST": "0.0.0.0",
        "MARU_REGION_MAPPER_PORT": "9860",
        "MARU_REGION_MAPPER_UCX_DEVICE": "",
    }


def test_a_reinstall_keeps_the_settings(tmp_path, monkeypatch):
    _, _, env, _ = _install(tmp_path, monkeypatch)
    env.write_text("MARU_REGION_MAPPER_UCX_DEVICE=mlx5_1:1\n")
    ok, unit, env, _ = _install(tmp_path, monkeypatch)
    assert ok and unit.exists()
    assert env.read_text() == "MARU_REGION_MAPPER_UCX_DEVICE=mlx5_1:1\n"


def test_no_service_without_nixl(tmp_path, monkeypatch):
    ok, unit, env, calls = _install(tmp_path, monkeypatch, nixl=False)
    assert not ok
    assert not unit.exists() and not env.exists() and not calls


def test_no_service_without_systemd(tmp_path, monkeypatch):
    monkeypatch.setattr(installer, "nixl_available", lambda: True)
    assert not installer.install(
        systemd_dir=tmp_path / "absent",
        env_file=tmp_path / "env",
        systemctl=lambda *a: None,
    )


def test_uninstall_removes_the_unit_and_keeps_the_settings(tmp_path, monkeypatch):
    _, unit, env, _ = _install(tmp_path, monkeypatch)
    calls = []
    assert installer.uninstall(
        systemd_dir=unit.parent, systemctl=lambda *a: calls.append(a)
    )
    assert not unit.exists() and env.exists()
    assert calls[:2] == [
        ("stop", installer.SERVICE_NAME),
        ("disable", installer.SERVICE_NAME),
    ]
    assert not installer.uninstall(systemd_dir=unit.parent, systemctl=lambda *a: None)


def test_install_needs_root(monkeypatch, capsys):
    monkeypatch.setattr(installer.os, "getuid", lambda: 1000)
    assert installer.main([]) == 1
    assert "root privileges required" in capsys.readouterr().err


@pytest.mark.parametrize(
    "argv,called",
    [
        ([], ("install", False)),
        (["--standalone"], ("install", True)),
        (["--uninstall"], ("uninstall",)),
    ],
)
def test_main_installs_or_removes(monkeypatch, argv, called):
    calls = []
    monkeypatch.setattr(installer.os, "getuid", lambda: 0)
    monkeypatch.setattr(
        installer,
        "install",
        lambda standalone=False: calls.append(("install", standalone)) or True,
    )
    monkeypatch.setattr(installer, "uninstall", lambda: calls.append(("uninstall",)))
    assert installer.main(argv) == 0
    assert calls == [called]
