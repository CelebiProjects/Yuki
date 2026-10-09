"""Durable submission API and impression status tests."""
from unittest import mock

from flask import Flask

from Yuki.kernel.execution.submissions import SubmissionStore
from Yuki.server.routes import execution, status as status_route


def test_submission_endpoint_returns_persisted_record(tmp_path, monkeypatch):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    store, _ = SubmissionStore.create(
        "proj", ["imp"], "runner", submission_id="sub-1")
    store.update(status="scheduled", workflow_id="wf-1")
    app = Flask(__name__)
    app.register_blueprint(execution.bp)

    response = app.test_client().get("/submissions/proj/sub-1")

    assert response.status_code == 200
    assert response.get_json()["workflow_id"] == "wf-1"


def test_terminal_submission_cannot_be_rewritten(tmp_path):
    store, _ = SubmissionStore.create(
        "proj", ["imp"], "runner", yuki_dir=tmp_path,
        submission_id="sub-1")
    store.update(status="blocked", error="overlap")

    record = store.update(status="scheduled", workflow_id="wf-late")

    assert record["status"] == "blocked"
    assert record["workflow_id"] == ""


def test_impression_status_surfaces_blocked_submission(tmp_path, monkeypatch):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    store, _ = SubmissionStore.create(
        "proj", ["imp"], "runner-id", submission_id="sub-1")
    store.update(
        status="blocked", error="shared dependency is active",
        conflicts={"upstream": "wf-existing"})
    app = Flask(__name__)
    app.register_blueprint(status_route.bp)

    job = mock.Mock()
    job.workflow_id.return_value = ""
    job.submission_id.return_value = "sub-1"
    server_config = mock.Mock()
    server_config.get_job_path.return_value = "/jobs/proj/imp"
    server_config.get_job_config_path.return_value = "/jobs/proj/imp/config.json"
    server_config.get_config_file.return_value.read_variable.side_effect = (
        lambda key, default=None: ["runner"] if key == "runners"
        else {"runner": "runner-id"} if key == "runners_id" else default)
    job_config = mock.Mock()
    job_config.read_variable.side_effect = (
        lambda key, default=None: "task" if key == "object_type" else default)

    with mock.patch.object(status_route, "config", server_config), \
         mock.patch.object(status_route, "ConfigFile", return_value=job_config), \
         mock.patch.object(status_route, "VJob", return_value=job):
        response = app.test_client().get("/status/proj/imp")

    payload = response.get_json()
    assert payload["status_legacy"] == "failed"
    assert payload["submission"]["status"] == "blocked"
    assert payload["detailed_status"] == "shared dependency is active"
