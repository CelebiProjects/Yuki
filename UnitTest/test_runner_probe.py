"""Tests for runner capability probing and test-runner endpoints."""
import json
import os
import subprocess
import tempfile
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.server import runner_probe
from Yuki.server.routes import runner as runner_routes


def _app(bp):
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app


def _temp_config(monkeypatch):
    tmp = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmp, ".Yuki"), exist_ok=True)
    config_obj = mock.MagicMock()
    config_obj.config_path = os.path.join(tmp, ".Yuki", "config.json")
    config_obj.get_config_file.return_value = ConfigFile(config_obj.config_path)
    monkeypatch.setattr(runner_routes, "config", config_obj)
    return config_obj


def _write_runner(config_obj, name="local", backend="native", settings=None):
    runner_id = "r-uuid"
    data = {
        "runners": [name],
        "runners_id": {name: runner_id},
        "urls": {runner_id: ""},
        "tokens": {runner_id: ""},
        "backend_types": {runner_id: backend},
    }
    if settings:
        data["runner_settings"] = {runner_id: settings}
    with open(config_obj.config_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return runner_id


def test_probe_native_all_ok(monkeypatch, tmp_path):
    """A native runner with all tools present passes every check."""
    monkeypatch.setattr(runner_probe.shutil, "which",
                        lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(runner_probe.subprocess, "run",
                        lambda *a, **k: subprocess.CompletedProcess(a, 0, "8.1.0\n", ""))
    checks = runner_probe.probe_native({"workdir": str(tmp_path)})
    assert checks["snakemake"]["ok"] is True
    assert checks["snakemake"]["version"] == "8.1.0"
    assert checks["conda"]["ok"] is True
    assert checks["workdir_writable"]["ok"] is True


def test_probe_native_missing_tools(monkeypatch, tmp_path):
    """A native runner without tools reports not-found errors."""
    monkeypatch.setattr(runner_probe.shutil, "which", lambda name: None)
    checks = runner_probe.probe_native({"workdir": str(tmp_path)})
    assert checks["snakemake"]["ok"] is False
    assert "not found" in checks["snakemake"]["error"]
    assert checks["conda"]["ok"] is False
    assert checks["workdir_writable"]["ok"] is True


def test_test_runner_native_persists_health(monkeypatch, tmp_path):
    """A passing native probe persists runner_health status ok."""
    config_obj = _temp_config(monkeypatch)
    runner_id = _write_runner(config_obj, settings={"workdir": str(tmp_path)})
    monkeypatch.setattr(runner_probe.shutil, "which",
                        lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(runner_probe.subprocess, "run",
                        lambda *a, **k: subprocess.CompletedProcess(a, 0, "1.0\n", ""))

    r = _app(runner_routes.bp).test_client().get("/test-runner/local")
    assert r.status_code == 200
    body = r.get_json()
    assert body["status"] == "ok"
    assert "checked_at" in body

    with open(config_obj.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    assert cfg["runner_health"][runner_id]["status"] == "ok"


def test_test_runner_ssh_failure_marks_failed(monkeypatch):
    """An ssh connect failure persists runner health as failed."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj, name="cluster", backend="ssh",
                  settings={"ssh_host": "h", "ssh_user": "u"})
    with mock.patch("paramiko.SSHClient") as ssh_cls:
        ssh_cls.return_value.connect.side_effect = Exception("no route")
        r = _app(runner_routes.bp).test_client().get("/test-runner/cluster")
    body = r.get_json()
    assert body["status"] == "failed"
    assert body["checks"]["connectivity"]["ok"] is False


def test_probe_ihep_requires_step_cluster_submission(monkeypatch):  # pylint: disable=unused-argument
    """IHEP probing verifies Snakemake can dispatch individual rule jobs."""
    mock_client = mock.MagicMock()

    def exec_command(command, timeout=None):  # pylint: disable=unused-argument
        if command.endswith("snakemake --help"):
            output = "--executor --cluster-generic-submit-cmd VALUE"
        elif "--version" in command:
            output = "9.0.0"
        elif command.startswith("command -v"):
            output = "/afs/ihep/bin/" + command.rsplit(" ", 1)[-1]
        else:
            output = ""
        stdout = mock.MagicMock()
        stdout.read.return_value = output.encode()
        stderr = mock.MagicMock()
        stderr.read.return_value = b""
        return None, stdout, stderr

    mock_client.exec_command.side_effect = exec_command
    with mock.patch("paramiko.SSHClient") as ssh_cls:
        ssh_cls.return_value = mock_client
        checks = runner_probe.probe_ssh(
            {"host": "lxlogin.ihep.ac.cn", "user": "alice"},
            extra_tools=(("hep_sub", "hep_sub_path"),
                         ("hep_q", "hep_q_path"),
                         ("hep_rm", "hep_rm_path")),
            require_cluster_executor=True)

    assert checks["hep_sub"]["ok"] is True
    assert checks["snakemake_cluster"] == {
        "ok": True, "mode": "cluster-generic"}


def test_test_runner_ssh_mid_probe_failure_marks_failed(monkeypatch):
    """A probe failing mid-run still marks the runner failed with details."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj, name="cluster", backend="ssh",
                  settings={"ssh_host": "h", "ssh_user": "u"})
    with mock.patch("paramiko.SSHClient") as ssh_cls:
        client = ssh_cls.return_value
        stdout = mock.MagicMock()
        stdout.read.return_value = b"8.1.0\n"
        stderr = mock.MagicMock()
        stderr.read.return_value = b""
        client.exec_command.side_effect = [
            (None, stdout, stderr),      # snakemake probe succeeds
            Exception("channel closed"),  # conda probe blows up mid-run
        ]
        r = _app(runner_routes.bp).test_client().get("/test-runner/cluster")
    body = r.get_json()
    assert body["status"] == "failed"
    assert body["checks"]["connectivity"]["ok"] is True
    assert body["checks"]["snakemake"]["ok"] is True
    assert body["checks"]["conda"]["ok"] is False
    assert "channel closed" in body["checks"]["conda"]["error"]
    assert body["checks"]["workdir_writable"]["ok"] is False

    with open(config_obj.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    assert cfg["runner_health"]["r-uuid"]["status"] == "failed"


def test_test_runner_unknown_404(monkeypatch):
    """An unknown runner name gives 404 from test-runner."""
    _temp_config(monkeypatch)
    r = _app(runner_routes.bp).test_client().get("/test-runner/ghost")
    assert r.status_code == 404


def test_runner_health_untested_and_persisted(monkeypatch, tmp_path):
    """Runner health is untested until a probe persists its status."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj)
    client = _app(runner_routes.bp).test_client()
    assert client.get("/runner-health/local").get_json() == {"status": "untested"}

    monkeypatch.setattr(runner_probe.shutil, "which", lambda name: None)
    client.get("/test-runner/local")
    assert client.get("/runner-health/local").get_json()["status"] == "failed"


def test_probe_ssh_abort_error_names_exception_type(monkeypatch):  # pylint: disable=unused-argument
    """Exceptions with empty str() must still produce a readable error."""
    class EmptyStrException(Exception):
        """An exception whose str() is empty."""

        def __str__(self):
            return ""

    mock_client = mock.MagicMock()

    def exec_side_effect(cmd, timeout=None):  # pylint: disable=unused-argument
        if "snakemake" in cmd:
            return (mock.MagicMock(),
                    mock.MagicMock(**{"read.return_value": b"9.0"}),
                    mock.MagicMock(**{"read.return_value": b""}))
        raise EmptyStrException()

    mock_client.exec_command.side_effect = exec_side_effect
    with mock.patch("paramiko.SSHClient") as ssh_cls:
        ssh_cls.return_value = mock_client
        checks = runner_probe.probe_ssh({"host": "h", "user": "u"})
    assert checks["connectivity"]["ok"] is True
    assert checks["snakemake"]["ok"] is True
    assert checks["conda"]["ok"] is False
    assert "EmptyStrException" in checks["conda"]["error"]
