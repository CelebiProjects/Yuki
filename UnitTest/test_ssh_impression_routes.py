"""Route coverage for scoped SSH impression sharing."""

import json
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile
from flask import Flask

from Yuki.kernel.storage import remote as remote_data_ops
from Yuki.server.routes import impression as impression_routes
from Yuki.services.ssh_impression_transfer import JOBS_DIR_NAME


def _app():
    app = Flask(__name__)
    app.register_blueprint(impression_routes.bp)
    return app


def _configure(monkeypatch, tmp_path, backend="ssh"):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    config_obj = mock.MagicMock()
    config_obj.config_path = str(tmp_path / "config.json")
    config_obj.get_config_file.return_value = ConfigFile(config_obj.config_path)
    monkeypatch.setattr(impression_routes, "config", config_obj)
    with open(config_obj.config_path, "w", encoding="utf-8") as stream:
        json.dump({"runners": ["cluster"],
                   "runners_id": {"cluster": "runner-id"},
                   "backend_types": {"runner-id": backend}}, stream)


def test_export_starts_persisted_job(monkeypatch, tmp_path):
    """A valid export request persists state and dispatches Celery."""
    _configure(monkeypatch, tmp_path)
    with mock.patch.object(
            impression_routes, "task_share_ssh_impressions") as task:
        response = _app().test_client().post("/share-ssh-impressions", json={
            "runner": "cluster", "project_uuid": "project", "operation": "export",
            "impressions": ["a" * 32], "force": False,
        })
    assert response.status_code == 202
    job_id = response.get_json()["job_id"]
    task.apply_async.assert_called_once()
    state = remote_data_ops.read_job_state(
        str(tmp_path), job_id, jobs_dir_name=JOBS_DIR_NAME)
    assert state["status"] == "queued"
    assert state["impressions"] == ["a" * 32]

    status = _app().test_client().get(
        f"/share-ssh-impressions/{job_id}")
    assert status.status_code == 200
    assert status.get_json()["project_uuid"] == "project"


def test_route_rejects_non_ssh_runner_and_bad_operation(monkeypatch, tmp_path):
    """Only registered SSH runners and supported operations are accepted."""
    _configure(monkeypatch, tmp_path, backend="native")
    client = _app().test_client()
    response = client.post("/share-ssh-impressions", json={
        "project_uuid": "project", "operation": "import",
        "runner": "cluster", "impressions": ["a" * 32],
    })
    assert response.status_code == 400
    assert "ssh runner" in response.get_json()["error"]

    response = client.post("/share-ssh-impressions", json={
        "runner": "cluster", "project_uuid": "project", "operation": "copy",
        "impressions": ["a" * 32],
    })
    assert response.status_code == 400
    assert "import or export" in response.get_json()["error"]


def test_route_requires_nonempty_impression_selection(monkeypatch, tmp_path):
    """The server never broadens an empty client-side scope."""
    _configure(monkeypatch, tmp_path)
    response = _app().test_client().post("/share-ssh-impressions", json={
        "project_uuid": "project", "operation": "import",
        "runner": "cluster", "impressions": [],
    })
    assert response.status_code == 400
    assert "non-empty list" in response.get_json()["error"]
