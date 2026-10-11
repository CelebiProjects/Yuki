"""Tests for listing conda environments on runners."""
import json
import os
import subprocess
import tempfile
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.server import runner_probe
from Yuki.server.routes import runner as runner_routes
from Yuki.server import tasks


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


CONDA_ENV_LIST = """# conda environments:
#
base                  *  /opt/miniconda3
celebi                   /opt/miniconda3/envs/celebi
                         /home/user/.conda/envs/unnamed
"""


def test_parse_conda_env_list():
    """conda env -l output parses into name/path/active rows."""
    envs = runner_probe.parse_conda_env_list(CONDA_ENV_LIST)
    assert envs == [
        {"name": "base", "path": "/opt/miniconda3", "active": True},
        {"name": "celebi", "path": "/opt/miniconda3/envs/celebi", "active": False},
        {"name": "", "path": "/home/user/.conda/envs/unnamed", "active": False},
    ]


def test_runner_envs_native(monkeypatch):
    """A native runner's conda envs are listed via subprocess."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj)
    monkeypatch.setattr(runner_probe.shutil, "which", lambda name: "/usr/bin/conda")
    monkeypatch.setattr(
        runner_probe.subprocess, "run",
        lambda *a, **k: subprocess.CompletedProcess(a, 0, CONDA_ENV_LIST, ""))

    r = _app(runner_routes.bp).test_client().get("/runner-envs/local")
    assert r.status_code == 200
    body = r.get_json()
    assert body["error"] is None
    assert [e["name"] for e in body["envs"]] == ["base", "celebi", ""]


def test_runner_envs_native_no_conda(monkeypatch):
    """A runner without conda reports an empty env list and error."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj)
    monkeypatch.setattr(runner_probe.shutil, "which", lambda name: None)
    body = _app(runner_routes.bp).test_client().get("/runner-envs/local").get_json()
    assert body["envs"] == []
    assert "conda not found" in body["error"]


def test_runner_envs_ssh(monkeypatch):
    """An ssh runner's conda envs are listed over the ssh channel."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj, name="cluster", backend="ssh",
                  settings={"ssh_host": "h", "ssh_user": "u"})
    mock_client = mock.MagicMock()
    mock_client.exec_command.return_value = (
        mock.MagicMock(),
        mock.MagicMock(**{"read.return_value": CONDA_ENV_LIST.encode()}),
        mock.MagicMock(**{"read.return_value": b""}),
    )
    with mock.patch("paramiko.SSHClient") as ssh_cls:
        ssh_cls.return_value = mock_client
        body = _app(runner_routes.bp).test_client().get("/runner-envs/cluster").get_json()
    assert body["error"] is None
    assert len(body["envs"]) == 3
    assert body["envs"][0]["active"] is True


def test_runner_envs_ssh_connect_failure(monkeypatch):
    """An ssh connect failure surfaces as an error body."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj, name="cluster", backend="ssh",
                  settings={"ssh_host": "h", "ssh_user": "u"})
    with mock.patch("paramiko.SSHClient") as ssh_cls:
        ssh_cls.return_value.connect.side_effect = Exception("no route")
        body = _app(runner_routes.bp).test_client().get("/runner-envs/cluster").get_json()
    assert body["envs"] == []
    assert "no route" in body["error"]


def test_runner_envs_reana_rejected(monkeypatch):
    """REANA runners do not support env listing."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj, name="cern", backend="reana")
    body = _app(runner_routes.bp).test_client().get("/runner-envs/cern").get_json()
    assert body["envs"] == []
    assert "reana" in body["error"]


def test_runner_envs_unknown_404(monkeypatch):
    """An unknown runner name gives 404."""
    _temp_config(monkeypatch)
    r = _app(runner_routes.bp).test_client().get("/runner-envs/ghost")
    assert r.status_code == 404
    assert "error" in r.get_json()


def test_runner_envs_project_scope(monkeypatch):
    """A project query lists only that project's managed prefixes."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj, name="cluster", backend="ihep",
                  settings={"ssh_host": "h", "ssh_user": "u",
                            "remote_workdir": "/work"})
    listing = {"envs": [{"name": "analysis", "path": "/work/env", "active": False}],
               "error": None, "scope": "project", "project_uuid": "project-1"}
    monkeypatch.setattr(
        runner_routes.project_environments, "list_project_environments",
        lambda settings, project_uuid: listing)

    response = _app(runner_routes.bp).test_client().get(
        "/runner-envs/cluster?project_uuid=project-1")

    assert response.status_code == 200
    assert response.get_json() == listing


def test_runner_envs_native_project_scope(monkeypatch, tmp_path):
    """Native project queries list only installed managed prefixes."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj, settings={"workdir": str(tmp_path)})
    (tmp_path / "environments" / "project-1" / "analysis" /
     "conda-meta").mkdir(parents=True)

    response = _app(runner_routes.bp).test_client().get(
        "/runner-envs/local?project_uuid=project-1")

    assert response.status_code == 200
    body = response.get_json()
    assert body["scope"] == "project"
    assert [entry["name"] for entry in body["envs"]] == ["analysis"]


def test_install_environment_starts_background_job(monkeypatch, tmp_path):
    """The install endpoint validates input, persists state, and dispatches work."""
    config_obj = _temp_config(monkeypatch)
    _write_runner(config_obj, name="cluster", backend="ssh",
                  settings={"ssh_host": "h", "ssh_user": "u"})
    monkeypatch.setenv("YUKIDIR", str(tmp_path / ".Yuki"))
    dispatch = mock.Mock()
    monkeypatch.setattr(tasks.task_install_environment, "apply_async", dispatch)

    response = _app(runner_routes.bp).test_client().post(
        "/runner-environments/cluster/install",
        json={"project_uuid": "project-1", "name": "analysis",
              "yaml": "dependencies:\n  - python\n"})

    assert response.status_code == 202
    job_id = response.get_json()["job_id"]
    dispatch.assert_called_once_with(
        args=[job_id, "r-uuid", "project-1", "analysis",
              "dependencies:\n  - python\n"])
    state = _app(runner_routes.bp).test_client().get(
        f"/environment-jobs/{job_id}").get_json()
    assert state["status"] == "queued"
    assert state["environment"] == "analysis"
