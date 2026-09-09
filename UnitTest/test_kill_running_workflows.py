"""Bulk cancellation respects preview scope and current live references."""
import json
from unittest import mock

import pytest

from Yuki.kernel import workflow_kill
from Yuki.server.routes import workflow as routes


@pytest.fixture
def setup_workflows(tmp_path, monkeypatch):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    instances = {}

    def add(name, status="running", runner="r1", project="proj"):
        path = tmp_path / "Workflows" / project / name
        path.mkdir(parents=True)
        (path / "config.json").write_text(json.dumps({"machine_id": runner}))
        workflow = mock.MagicMock()
        workflow.status.return_value = status
        workflow.jobs = []
        instances[name] = workflow
        return workflow

    monkeypatch.setattr(workflow_kill.VWorkflow, "create",
                        lambda project, jobs, name: instances[name])
    monkeypatch.setattr(workflow_kill.workflow_purge, "_project_live_workflows",
                        lambda project, root: {"live"})
    return add


def test_preview_filters_scope_without_killing(setup_workflows):
    add = setup_workflows
    stale = add("stale")
    add("live")
    add("finished", status="finished")
    add("elsewhere", runner="r2")
    add("other-project", project="other")
    result = workflow_kill.kill_running_workflows("r1", "proj")
    assert result["selected"] == [{"project": "proj", "workflow": "stale"}]
    assert result["killed"] == []
    stale.force_kill.assert_not_called()
    stale.delete_workspace.assert_not_called()


def test_execution_never_expands_preview_or_touches_reruns(setup_workflows):
    stale = setup_workflows("stale")
    new = setup_workflows("new")
    owned, reassigned = mock.MagicMock(), mock.MagicMock()
    for job, name in [(owned, "stale"), (reassigned, "new")]:
        job.is_input = False
        job.job_type.return_value = "task"
        job.workflow_id.return_value = name
    stale.jobs = [owned, reassigned]
    result = workflow_kill.kill_running_workflows(
        "r1", "proj", dry_run=False, workflows=["stale"])
    assert result["killed"] == [{"project": "proj", "workflow": "stale"}]
    assert stale.jobs == [owned]
    stale.force_kill.assert_called_once_with()
    new.force_kill.assert_not_called()
    stale.delete_workspace.assert_not_called()


@pytest.mark.parametrize("live", [None, {"stale"}])
def test_rechecks_live_set_after_preview(setup_workflows, monkeypatch, live):
    stale = setup_workflows("stale")
    plan = workflow_kill.kill_running_workflows("r1", "proj")
    assert len(plan["selected"]) == 1
    monkeypatch.setattr(workflow_kill.workflow_purge, "_project_live_workflows",
                        lambda project, root: live)
    result = workflow_kill.kill_running_workflows(
        "r1", "proj", dry_run=False, workflows=["stale"])
    assert result["killed"] == []
    stale.force_kill.assert_not_called()


def test_one_failure_does_not_stop_batch(setup_workflows):
    bad = setup_workflows("bad")
    bad.force_kill.side_effect = OSError("runner unreachable")
    good = setup_workflows("good")
    result = workflow_kill.kill_running_workflows(
        "r1", "proj", dry_run=False, workflows=["bad", "good"])
    assert [entry["workflow"] for entry in result["failed"]] == ["bad"]
    assert [entry["workflow"] for entry in result["killed"]] == ["good"]
    good.force_kill.assert_called_once()


def test_ssh_batch_requires_strict_kill(setup_workflows):
    stale = setup_workflows("stale")
    stale.backend_type.return_value = "ssh"
    workflow_kill.kill_running_workflows(
        "r1", "proj", dry_run=False, workflows=["stale"])
    stale.force_kill.assert_called_once_with(strict=True)


def test_execution_requires_preview_ids():
    with pytest.raises(ValueError, match="preview"):
        workflow_kill.kill_running_workflows("r1", "proj", dry_run=False)


@pytest.fixture
def client():
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(routes.bp)
    return app.test_client()


@pytest.mark.parametrize("body", [
    {}, {"runner": "farm", "project": "../bad"},
    {"runner": "farm", "project": "proj", "dry_run": "false"},
    {"runner": "farm", "project": "proj", "dry_run": False},
    {"runner": "farm", "project": "proj", "workflows": ["../bad"]},
])
def test_route_rejects_invalid_or_unbounded_requests(client, body):
    with mock.patch.object(routes.workflow_kill, "kill_running_workflows") as kill:
        response = client.post("/kill-running-workflows", json=body)
    assert response.status_code == 400
    kill.assert_not_called()


def test_route_forwards_exact_selection(client):
    with mock.patch.object(routes.config, "get_config_file") as config, \
            mock.patch.object(routes.workflow_kill, "kill_running_workflows") as kill:
        config.return_value.read_variable.return_value = {"farm": "r1"}
        kill.return_value = {"killed": []}
        response = client.post("/kill-running-workflows", json={
            "runner": "farm", "project": "proj", "dry_run": False,
            "workflows": ["old"],
        })
    assert response.status_code == 200
    kill.assert_called_once_with("r1", "proj", dry_run=False, workflows=["old"])
