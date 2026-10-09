"""Tests for workflow force-kill (kill-workflow)."""
import json
import os
from unittest import mock

import pytest


def test_vworkflow_force_kill_not_implemented():
    """The base workflow has no generic force-kill."""
    from Yuki.kernel.workflows.base import VWorkflow

    class _ConcreteVWorkflow(VWorkflow):
        """Concrete subclass so the abstract base can be instantiated."""

        def _execute_backend(self):
            return None

        def _sync_external_job_status(self, job):
            return None

        def update_workflow_status(self):
            return None

    workflow = _ConcreteVWorkflow.__new__(_ConcreteVWorkflow)
    with pytest.raises(NotImplementedError):
        workflow.force_kill()


def _fake_job(status="in movement"):
    job = mock.MagicMock()
    job.uuid = "job1"
    job.is_input = False
    job.job_type.return_value = "task"
    job.status.return_value = status
    return job


def _ssh_workflow(tmp_path):
    from Yuki.kernel.workflows.ssh import SshWorkflow
    workflow = SshWorkflow.__new__(SshWorkflow)
    workflow.uuid = tmp_path.name
    workflow.project_uuid = "proj"
    workflow.machine_id = "runner"
    workflow.lease_token = ""
    workflow.remote_exec_path = "/remote/workflows/proj/wf1"
    workflow.path = str(tmp_path / "mirror")
    os.makedirs(workflow.path, exist_ok=True)
    workflow.jobs = [_fake_job()]
    workflow.logger = lambda msg: None
    return workflow


def test_ssh_force_kill_escalates_to_kill9(tmp_path):
    """TERM, then KILL when the process survives, then pkill + exit file."""
    from Yuki.kernel.workflows.ssh import SshWorkflow
    workflow = _ssh_workflow(tmp_path)

    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False

    alive_checks = iter([0, 0, 1])

    def exec_side_effect(command, timeout=300):
        if command.startswith("cat /remote"):
            return "1234", "", 0
        if command.startswith("kill -0"):
            return "", "", next(alive_checks)
        if command.startswith("readlink"):
            return workflow.remote_exec_path, "", 0
        if command.startswith("tr "):
            return "snakemake --snakefile Snakefile", "", 0
        if command.startswith("pkill"):
            return "", "", 1
        return "", "", 0

    ssh.exec.side_effect = exec_side_effect
    with mock.patch.object(SshWorkflow, "_ssh", return_value=ssh), \
            mock.patch("Yuki.kernel.workflows.ssh.time.sleep"):
        workflow.force_kill()

    commands = [c for c in ssh.exec.call_args_list]
    flattened = [c[0][0] for c in commands]
    assert any("kill -TERM -- 1234" in c for c in flattened)
    assert any("kill -KILL -- 1234" in c for c in flattened)
    assert any("pkill -KILL -f '[/]remote/workflows/proj/wf1'" in c
               for c in flattened)
    assert any("printf '137\\n' > "
               "/remote/workflows/proj/wf1/yuki.exit.tmp.kill" in c
               for c in flattened)
    results = json.load(open(os.path.join(workflow.path, "results.json")))
    assert results["results"]["status"] == "stopped"
    workflow.jobs[0].set_status.assert_called_once()
    assert workflow.jobs[0].set_status.call_args[0][0] == "stopped"


def test_ssh_force_kill_without_identity_is_noop(tmp_path):
    """No live PID means no signal and no false stopped state."""
    from Yuki.kernel.workflows.ssh import SshWorkflow
    workflow = _ssh_workflow(tmp_path)

    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False
    ssh.exec.return_value = ("", "", 1)  # cat pid fails
    with mock.patch.object(SshWorkflow, "_ssh", return_value=ssh), \
            mock.patch("Yuki.kernel.workflows.ssh.time.sleep"):
        assert workflow.force_kill() is False

    flattened = [c[0][0] for c in ssh.exec.call_args_list]
    assert not any("kill -TERM" in c for c in flattened)
    assert not any("kill -9" in c for c in flattened)
    assert not os.path.exists(os.path.join(workflow.path, "results.json"))


def test_native_force_kill_marks_stopped(tmp_path):
    """The native cancellation request uses the valid stopped state."""
    from Yuki.kernel.workflows.native import NativeWorkflow
    workflow = NativeWorkflow.__new__(NativeWorkflow)
    workflow.uuid = tmp_path.name
    workflow.project_uuid = "proj"
    workflow.machine_id = "runner"
    workflow.lease_token = ""
    workflow.path = str(tmp_path / "mirror")
    os.makedirs(workflow.path, exist_ok=True)
    workflow.jobs = [_fake_job()]
    workflow.logger = lambda msg: None

    workflow.force_kill()

    results = json.load(open(os.path.join(workflow.path, "results.json")))
    assert results["results"]["status"] == "stopped"
    assert workflow.jobs[0].set_status.call_args[0][0] == "stopped"


def test_force_kill_preserves_finished_job(tmp_path):
    """Stopping an active workflow never rewrites a completed job."""
    from Yuki.kernel.workflows.ssh import SshWorkflow
    workflow = _ssh_workflow(tmp_path)
    finished = _fake_job("finished")
    running = _fake_job("in movement")
    running.uuid = "job2"
    workflow.jobs = [finished, running]
    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False
    ssh.exec.return_value = ("", "", 0)

    with mock.patch.object(SshWorkflow, "_ssh", return_value=ssh), \
            mock.patch.object(workflow, "_verified_remote_target",
                              return_value=(1234, "1234")), \
            mock.patch.object(workflow, "_remote_pid_alive", return_value=False), \
            mock.patch("Yuki.kernel.workflows.ssh.time.sleep"):
        assert workflow.force_kill() is True

    finished.set_status.assert_not_called()
    running.set_status.assert_called_once_with(
        "stopped", "Workflow force-stopped by user")


def test_force_kill_finished_workflow_is_noop(tmp_path):
    """A completed workflow is immutable and receives no remote signal."""
    from Yuki.kernel.workflows.ssh import SshWorkflow
    workflow = _ssh_workflow(tmp_path)
    workflow.set_workflow_status("finished")
    with mock.patch.object(SshWorkflow, "_ssh") as connect:
        assert workflow.force_kill() is False
    connect.assert_not_called()
    workflow.jobs[0].set_status.assert_not_called()
    results = json.loads((tmp_path / "mirror" / "results.json").read_text())
    assert results["results"]["status"] == "finished"


def test_legacy_killed_status_is_terminal():
    """Historical invalid kill records are treated as stopped, not running."""
    from Yuki.kernel.execution.status import (
        STOPPED, is_terminal_status, translate_to_musical)
    assert translate_to_musical("killed") == STOPPED
    assert is_terminal_status("killed")


def test_remote_refresh_cannot_resurrect_stopped_workflow(tmp_path):
    """A concurrent backend refresh cannot replace stopped with running."""
    workflow = _ssh_workflow(tmp_path)
    workflow.set_workflow_status("stopped")

    assert workflow._update_results_if_active(  # pylint: disable=protected-access
        {"status": "running"}, replace=True) is False

    results = json.loads((tmp_path / "mirror" / "results.json").read_text())
    assert results["results"]["status"] == "stopped"


def test_native_monitor_cannot_resurrect_stopped_workflow(tmp_path):
    """Host progress updates preserve an already stopped workflow."""
    from Yuki.kernel.execution.monitor import SnakemakeMonitor
    workflow_path = tmp_path / "workflow"
    workflow_path.mkdir()
    results_path = workflow_path / "results.json"
    results_path.write_text(
        json.dumps({"results": {"status": "stopped"}}), encoding="utf-8")
    monitor = SnakemakeMonitor(
        str(workflow_path), str(tmp_path / "execution"), "proj", "wf1")

    monitor._update_results("running", 2, 1, {})  # pylint: disable=protected-access

    assert json.loads(results_path.read_text())["results"]["status"] == "stopped"


def test_direct_impression_kill_requires_explicit_workflow():
    """The legacy ambiguous impression-level mutation is disabled."""
    from Yuki.kernel.storage import impressions as impression_storage
    storage = impression_storage.ImpressionStorage.__new__(
        impression_storage.ImpressionStorage)
    with pytest.raises(ValueError, match="explicit workflow ID"):
        storage.kill()


def test_impression_kill_plan_deduplicates_and_shows_full_scope():
    """Preview exposes all jobs and one shared workflow only once."""
    from Yuki.kernel.storage import impressions as impression_storage
    storage = impression_storage.ImpressionStorage.__new__(
        impression_storage.ImpressionStorage)
    storage.impression = "requested"
    workflow = mock.MagicMock()
    workflow.uuid = "wf1"
    workflow.backend_type.return_value = "ssh"
    workflow.status.return_value = "running"
    job1, job2 = _fake_job("finished"), _fake_job("in movement")
    job1.uuid, job2.uuid = "job1", "job2"
    workflow.execution_jobs.return_value = [job1, job2]
    storage._get_runner_contexts = lambda: [
        ("runner-a", mock.Mock(), workflow),
        ("runner-b", mock.Mock(), workflow),
    ]

    plan = storage.kill_plan()

    assert plan["status"] == "confirmation_required"
    assert len(plan["workflows"]) == 1
    assert plan["workflows"][0]["workflow"] == "wf1"
    assert plan["workflows"][0]["runners"] == ["runner-a", "runner-b"]
    assert plan["workflows"][0]["jobs"] == [
        {"impression": "job1", "status": "finished"},
        {"impression": "job2", "status": "in movement"},
    ]


def test_impression_kill_executes_only_confirmed_workflow():
    """Execution requires an exact workflow ID from the preview."""
    from Yuki.kernel.storage import impressions as impression_storage
    storage = impression_storage.ImpressionStorage.__new__(
        impression_storage.ImpressionStorage)
    storage.impression = "requested"
    workflow = mock.MagicMock()
    workflow.uuid = "wf1"
    workflow.kill.return_value = True
    workflow.backend_type.return_value = "ssh"
    storage._get_runner_contexts = lambda: [
        ("runner", mock.Mock(), workflow)]

    with pytest.raises(ValueError, match="not referenced"):
        storage.kill_workflow("other")
    result = storage.kill_workflow("wf1")

    assert result["status"] == "stopped"
    workflow.kill.assert_called_once_with()


def test_strict_ssh_kill_preserves_state_on_connection_failure(tmp_path):
    workflow = _ssh_workflow(tmp_path)
    workflow.set_workflow_status = mock.Mock()
    with mock.patch.object(workflow, "_ssh", side_effect=OSError("offline")):
        with pytest.raises(OSError, match="offline"):
            workflow.force_kill(strict=True)
    workflow.set_workflow_status.assert_not_called()
    workflow.jobs[0].set_status.assert_not_called()


def test_strict_ssh_kill_rejects_reused_pid(tmp_path):
    workflow = _ssh_workflow(tmp_path)
    workflow.set_workflow_status = mock.Mock()
    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.exec.return_value = ("/unrelated/workflow", "", 0)
    with mock.patch.object(workflow, "_ssh", return_value=ssh), \
            mock.patch.object(workflow, "_read_remote_started", return_value=(123, 124)), \
            mock.patch.object(workflow, "_remote_pid_alive", return_value=True):
        with pytest.raises(RuntimeError, match="cannot be verified"):
            workflow.force_kill(strict=True)
    assert ssh.exec.call_args_list == [mock.call("readlink -f /proc/123/cwd")]
    workflow.set_workflow_status.assert_not_called()


def test_ssh_kill_rejects_mismatched_process_command(tmp_path):
    """A stale PID marker cannot signal an unrelated process in the same cwd."""
    workflow = _ssh_workflow(tmp_path)
    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.exec.side_effect = [
        (workflow.remote_exec_path, "", 0),
        ("python unrelated.py", "", 0),
    ]
    with mock.patch.object(workflow, "_ssh", return_value=ssh), \
            mock.patch.object(workflow, "_read_remote_started",
                              return_value=(123, 124)), \
            mock.patch.object(workflow, "_remote_pid_alive", return_value=True):
        with pytest.raises(RuntimeError, match="command does not match"):
            workflow.kill()
    workflow.jobs[0].set_status.assert_not_called()
    assert not (tmp_path / "mirror" / "results.json").exists()


def test_strict_ssh_kill_clears_missing_pid_without_matching_own_shell(tmp_path):
    workflow = _ssh_workflow(tmp_path)
    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.exec.return_value = ("", "", 0)
    with mock.patch.object(workflow, "_ssh", return_value=ssh), \
            mock.patch.object(workflow, "_read_remote_started", return_value=None), \
            mock.patch.object(workflow, "_read_remote_int", return_value=None):
        assert workflow.force_kill(strict=True) is False
    commands = [call.args[0] for call in ssh.exec.call_args_list]
    assert commands == []
    assert not (tmp_path / "mirror" / "results.json").exists()


def test_reana_force_kill_stops_with_force(tmp_path):
    """stop_workflow gets force=True and the status is marked stopped."""
    from Yuki.kernel.workflows import reana as reana_workflow
    workflow = reana_workflow.ReanaWorkflow.__new__(
        reana_workflow.ReanaWorkflow)
    workflow.machine_id = "r1"
    workflow.uuid = "wf-reana"
    workflow.project_uuid = "proj"
    workflow.lease_token = ""
    workflow.jobs = []
    workflow.path = str(tmp_path / "mirror")
    workflow.get_name = mock.MagicMock(return_value="w-proj-wf1")
    workflow.get_access_token = mock.MagicMock(return_value="tok")
    workflow.set_environment = mock.MagicMock()

    with mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
            mock.patch.object(reana_workflow, "client") as client:
        workflow.force_kill()

    client.stop_workflow.assert_called_once_with(
        "w-proj-wf1", True, "tok")
    results = json.loads((tmp_path / "mirror" / "results.json").read_text())
    assert results["results"]["status"] == "stopped"


def _app(bp):
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app


def test_kill_workflow_route(monkeypatch, tmp_path):
    """/kill-workflow force-kills and reports."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    mirror = tmp_path / ".Yuki" / "Workflows" / "proj" / "wf1"
    mirror.mkdir(parents=True)
    wf = mock.MagicMock()
    wf.backend_type.return_value = "ssh"
    with mock.patch.object(workflow_routes, "VWorkflow") as vwf:
        vwf.create.return_value = wf
        r = _app(workflow_routes.bp).test_client().get(
            "/kill-workflow/proj/wf1")
    assert r.status_code == 200
    body = r.get_json()
    assert body["status"] == "stopped"
    assert body["workflow"] == "wf1"
    wf.force_kill.assert_called_once_with()


def test_kill_workflow_route_reports_terminal_noop(monkeypatch, tmp_path):
    """The route reports that an already terminal workflow was unchanged."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    mirror = tmp_path / ".Yuki" / "Workflows" / "proj" / "wf1"
    mirror.mkdir(parents=True)
    wf = mock.MagicMock()
    wf.force_kill.return_value = False
    wf.backend_type.return_value = "ssh"
    with mock.patch.object(workflow_routes, "VWorkflow") as vwf:
        vwf.create.return_value = wf
        response = _app(workflow_routes.bp).test_client().get(
            "/kill-workflow/proj/wf1")
    assert response.status_code == 200
    assert response.get_json()["status"] == "unchanged"


def test_impression_kill_route_previews_before_mutation():
    """GET is read-only and POST requires the previewed workflow ID."""
    from Yuki.server.routes import workflow as workflow_routes
    storage = mock.MagicMock()
    storage.kill_plan.return_value = {
        "status": "confirmation_required", "workflows": [{"workflow": "wf1"}]}
    storage.kill_workflow.return_value = {
        "status": "stopped", "workflow": "wf1"}
    with mock.patch.object(workflow_routes, "ImpressionStorage",
                           return_value=storage):
        client = _app(workflow_routes.bp).test_client()
        preview = client.get("/kill/proj/imp1")
        missing = client.post("/kill/proj/imp1", json={})
        stopped = client.post("/kill/proj/imp1", json={"workflow": "wf1"})

    assert preview.get_json()["status"] == "confirmation_required"
    assert missing.status_code == 400
    assert stopped.get_json()["status"] == "stopped"
    storage.kill_workflow.assert_called_once_with("wf1")


def test_kill_workflow_route_404(monkeypatch, tmp_path):
    """Unknown workflows get a 404."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    r = _app(workflow_routes.bp).test_client().get(
        "/kill-workflow/proj/nope")
    assert r.status_code == 404
