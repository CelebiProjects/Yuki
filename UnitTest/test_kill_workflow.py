"""Tests for workflow force-kill (kill-workflow)."""
import json
import os
from unittest import mock

import pytest


def test_vworkflow_force_kill_not_implemented():
    """The base workflow has no generic force-kill."""
    from Yuki.kernel.vworkflow import VWorkflow

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


def _fake_job():
    job = mock.MagicMock()
    job.is_input = False
    job.job_type.return_value = "task"
    return job


def _ssh_workflow(tmp_path):
    from Yuki.kernel.ssh_workflow import SshWorkflow
    workflow = SshWorkflow.__new__(SshWorkflow)
    workflow.remote_exec_path = "/remote/workflows/proj/wf1"
    workflow.path = str(tmp_path / "mirror")
    os.makedirs(workflow.path, exist_ok=True)
    workflow.jobs = [_fake_job()]
    workflow.logger = lambda msg: None
    return workflow


def test_ssh_force_kill_escalates_to_kill9(tmp_path):
    """TERM, then KILL when the process survives, then pkill + exit file."""
    from Yuki.kernel.ssh_workflow import SshWorkflow
    workflow = _ssh_workflow(tmp_path)

    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False

    def exec_side_effect(command, timeout=300):
        if command.startswith("cat /remote"):
            return "1234", "", 0
        if command.startswith("kill -0"):
            return "", "", 0  # still alive
        return "", "", 0

    ssh.exec.side_effect = exec_side_effect
    with mock.patch.object(SshWorkflow, "_ssh", return_value=ssh), \
            mock.patch("Yuki.kernel.ssh_workflow.time.sleep"):
        workflow.force_kill()

    commands = [c for c in ssh.exec.call_args_list]
    flattened = [c[0][0] for c in commands]
    assert any("kill -TERM -- 1234" in c for c in flattened)
    assert any("kill -KILL -- 1234" in c for c in flattened)
    assert any("pkill -f /remote/workflows/proj/wf1" in c
               for c in flattened)
    assert any("printf '137\\n' > "
               "/remote/workflows/proj/wf1/yuki.exit.tmp.kill" in c
               for c in flattened)
    results = json.load(open(os.path.join(workflow.path, "results.json")))
    assert results["results"]["status"] == "killed"
    workflow.jobs[0].set_status.assert_called_once()
    assert workflow.jobs[0].set_status.call_args[0][0] == "stopped"


def test_ssh_force_kill_zombie_still_marks_killed(tmp_path):
    """No pid file: nothing to kill, but the stale running clears."""
    from Yuki.kernel.ssh_workflow import SshWorkflow
    workflow = _ssh_workflow(tmp_path)

    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False
    ssh.exec.return_value = ("", "", 1)  # cat pid fails
    with mock.patch.object(SshWorkflow, "_ssh", return_value=ssh), \
            mock.patch("Yuki.kernel.ssh_workflow.time.sleep"):
        workflow.force_kill()

    flattened = [c[0][0] for c in ssh.exec.call_args_list]
    assert not any("kill -TERM" in c for c in flattened)
    assert not any("kill -9" in c for c in flattened)
    results = json.load(open(os.path.join(workflow.path, "results.json")))
    assert results["results"]["status"] == "killed"


def test_native_force_kill_marks_killed(tmp_path):
    """The untracked local process cannot be killed; status is marked."""
    from Yuki.kernel.native_workflow import NativeWorkflow
    workflow = NativeWorkflow.__new__(NativeWorkflow)
    workflow.path = str(tmp_path / "mirror")
    os.makedirs(workflow.path, exist_ok=True)
    workflow.jobs = [_fake_job()]
    workflow.logger = lambda msg: None

    workflow.force_kill()

    results = json.load(open(os.path.join(workflow.path, "results.json")))
    assert results["results"]["status"] == "killed"
    assert workflow.jobs[0].set_status.call_args[0][0] == "failed"


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


def test_strict_ssh_kill_clears_missing_pid_without_matching_own_shell(tmp_path):
    workflow = _ssh_workflow(tmp_path)
    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.exec.return_value = ("", "", 0)
    with mock.patch.object(workflow, "_ssh", return_value=ssh), \
            mock.patch.object(workflow, "_read_remote_started", return_value=None), \
            mock.patch.object(workflow, "_read_remote_int", return_value=None):
        workflow.force_kill(strict=True)
    commands = [call.args[0] for call in ssh.exec.call_args_list]
    assert commands[0] == "pkill -KILL -f '[/]remote/workflows/proj/wf1'"
    results = json.loads((tmp_path / "mirror" / "results.json").read_text())
    assert results["results"]["status"] == "killed"


def test_reana_force_kill_stops_with_force():
    """stop_workflow gets force=True and the status is marked killed."""
    from Yuki.kernel import reana_workflow
    workflow = reana_workflow.ReanaWorkflow.__new__(
        reana_workflow.ReanaWorkflow)
    workflow.machine_id = "r1"
    workflow.path = "/tmp/fake-mirror"  # set_workflow_status writes there
    workflow.get_name = mock.MagicMock(return_value="w-proj-wf1")
    workflow.get_access_token = mock.MagicMock(return_value="tok")
    workflow.set_environment = mock.MagicMock()
    workflow.set_workflow_status = mock.MagicMock()

    with mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
            mock.patch.object(reana_workflow, "client") as client:
        workflow.force_kill()

    client.stop_workflow.assert_called_once_with(
        "w-proj-wf1", True, "tok")
    workflow.set_workflow_status.assert_called_once_with("killed")


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
    assert body["status"] == "killed"
    assert body["workflow"] == "wf1"
    wf.force_kill.assert_called_once_with()


def test_kill_workflow_route_404(monkeypatch, tmp_path):
    """Unknown workflows get a 404."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    r = _app(workflow_routes.bp).test_client().get(
        "/kill-workflow/proj/nope")
    assert r.status_code == 404
