"""Tests for workflow workspace deletion (delete_workspace + routes)."""
# pylint: disable=protected-access
import os
import json
from unittest import mock

import pytest


def test_vworkflow_delete_workspace_not_implemented():
    """The base workflow has no generic way to delete a workspace."""
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
        workflow.delete_workspace()


def test_ssh_delete_workspace_removes_remote_dir():
    """The remote workspace is deleted with a quoted rm -rf command."""
    from Yuki.kernel.workflows.ssh import SshWorkflow
    workflow = SshWorkflow.__new__(SshWorkflow)
    workflow.remote_exec_path = "/remote/workflows/proj/wf one"
    workflow.logger = lambda msg: None

    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False
    ssh.exec.return_value = ("", "", 0)
    workflow._ssh = mock.MagicMock(return_value=ssh)

    workflow.delete_workspace()

    ssh.exec.assert_called_once()
    cmd = ssh.exec.call_args[0][0]
    assert cmd == "rm -rf '/remote/workflows/proj/wf one'"
    assert ssh.exec.call_args[1]["timeout"] == 3600


def test_ssh_delete_workspace_failure_raises():
    """A nonzero remote exit code surfaces as a RuntimeError."""
    from Yuki.kernel.workflows.ssh import SshWorkflow
    workflow = SshWorkflow.__new__(SshWorkflow)
    workflow.remote_exec_path = "/remote/workflows/proj/wf1"
    workflow.logger = lambda msg: None

    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False
    ssh.exec.return_value = ("", "no such file", 1)
    workflow._ssh = mock.MagicMock(return_value=ssh)

    with pytest.raises(RuntimeError):
        workflow.delete_workspace()


def test_native_delete_workspace_removes_local_dir(tmp_path):
    """The local execution workspace is removed."""
    from Yuki.kernel.workflows.native import NativeWorkflow
    workflow = NativeWorkflow.__new__(NativeWorkflow)
    workflow.local_exec_path = str(tmp_path / "wf1")
    workflow.logger = lambda msg: None
    os.makedirs(workflow.local_exec_path)
    with open(os.path.join(workflow.local_exec_path, "a.done"), "w",
              encoding="utf-8") as f:
        f.write("x")

    workflow.delete_workspace()

    assert not os.path.exists(workflow.local_exec_path)


def test_native_delete_workspace_missing_dir_no_raise(tmp_path):
    """Deleting an already-gone workspace does not raise."""
    from Yuki.kernel.workflows.native import NativeWorkflow
    workflow = NativeWorkflow.__new__(NativeWorkflow)
    workflow.local_exec_path = str(tmp_path / "gone")
    workflow.logger = lambda msg: None

    workflow.delete_workspace()  # no raise


def test_reana_delete_workspace_calls_client():
    """The online workflow is deleted with workspace + all-runs flags."""
    from Yuki.kernel.workflows import reana as reana_workflow
    workflow = reana_workflow.ReanaWorkflow.__new__(
        reana_workflow.ReanaWorkflow)
    workflow.machine_id = "r1"
    workflow.get_name = mock.MagicMock(return_value="w-proj-wf1")
    workflow.get_access_token = mock.MagicMock(return_value="tok")
    workflow.set_environment = mock.MagicMock()

    with mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
            mock.patch.object(reana_workflow, "client") as client:
        workflow.delete_workspace()

    client.delete_workflow.assert_called_once_with(
        "w-proj-wf1", True, True, "tok")


def _app(bp):
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app


def _mirror(tmp_path, project, workflow, status):
    """Create the workflow mirror dir with a results.json status."""
    wf_dir = tmp_path / ".Yuki" / "Workflows" / project / workflow
    wf_dir.mkdir(parents=True)
    with open(wf_dir / "results.json", "w", encoding="utf-8") as f:
        json.dump({"results": {"status": status}}, f)
    return wf_dir


def _mock_workflow(status="finished"):
    workflow = mock.MagicMock()
    workflow.status.return_value = status
    workflow.backend_type.return_value = "ssh"
    return workflow


def test_delete_workflow_deletes_and_reports(monkeypatch, tmp_path):
    """A terminal workflow's workspace is deleted with a success payload."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    wf_dir = _mirror(tmp_path, "proj", "wf1", "finished")
    wf = _mock_workflow("finished")
    wf.path = str(wf_dir)

    with mock.patch.object(workflow_routes, "VWorkflow") as vwf:
        vwf.create.return_value = wf
        r = _app(workflow_routes.bp).test_client().get(
            "/delete-workflow/proj/wf1")

    assert r.status_code == 200
    body = r.get_json()
    assert body["status"] == "deleted"
    assert body["project_uuid"] == "proj"
    assert body["workflow"] == "wf1"
    assert body["backend_type"] == "ssh"
    wf.delete_workspace.assert_called_once_with()
    # The purge record lands in the real mirror, not a mock-derived path.
    cfg = json.load(open(wf_dir / "config.json", encoding="utf-8"))
    assert "workspace_purged_at" in cfg


def test_delete_workflow_unknown_workflow_404(monkeypatch, tmp_path):
    """A workflow without a mirror directory gets a 404."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    r = _app(workflow_routes.bp).test_client().get(
        "/delete-workflow/proj/nope")
    assert r.status_code == 404


def test_delete_workflow_running_409(monkeypatch, tmp_path):
    """A running workflow is refused with a 409."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    _mirror(tmp_path, "proj", "wf1", "in movement")
    wf = _mock_workflow("in movement")

    with mock.patch.object(workflow_routes, "VWorkflow") as vwf:
        vwf.create.return_value = wf
        r = _app(workflow_routes.bp).test_client().get(
            "/delete-workflow/proj/wf1")

    assert r.status_code == 409
    assert "running" in r.get_json()["error"]
    wf.delete_workspace.assert_not_called()


def test_delete_workflow_backend_failure_500(monkeypatch, tmp_path):
    """A backend failure surfaces as a 500 with the error message."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    _mirror(tmp_path, "proj", "wf1", "finished")
    wf = _mock_workflow("finished")
    wf.delete_workspace.side_effect = OSError("ssh down")

    with mock.patch.object(workflow_routes, "VWorkflow") as vwf:
        vwf.create.return_value = wf
        r = _app(workflow_routes.bp).test_client().get(
            "/delete-workflow/proj/wf1")

    assert r.status_code == 500
    assert "ssh down" in r.get_json()["error"]


def test_homekeep_is_deprecated():
    """/homekeep returns 410 and points at delete-workflow."""
    from Yuki.server.routes import status as status_routes
    r = _app(status_routes.bp).test_client().get("/homekeep/proj")
    assert r.status_code == 410
    assert "delete-workflow" in r.get_json()["error"]


def test_delete_workflow_create_failure_returns_json_500(monkeypatch,
                                                         tmp_path):
    """A workflow-construction failure returns the JSON 500 contract."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    _mirror(tmp_path, "proj", "wf1", "finished")

    with mock.patch.object(workflow_routes, "VWorkflow") as vwf:
        vwf.create.side_effect = ImportError("reana_client is not available")
        r = _app(workflow_routes.bp).test_client().get(
            "/delete-workflow/proj/wf1")

    assert r.status_code == 500
    assert "reana_client" in r.get_json()["error"]


def test_delete_workflow_backend_type_failure_returns_json_500(
        monkeypatch, tmp_path):
    """A payload-read failure after deletion still returns JSON 500."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    wf_dir = _mirror(tmp_path, "proj", "wf1", "finished")
    wf = _mock_workflow("finished")
    wf.path = str(wf_dir)
    wf.backend_type.side_effect = OSError("corrupt config")

    with mock.patch.object(workflow_routes, "VWorkflow") as vwf:
        vwf.create.return_value = wf
        r = _app(workflow_routes.bp).test_client().get(
            "/delete-workflow/proj/wf1")

    assert r.status_code == 500
    assert "corrupt config" in r.get_json()["error"]
    wf.delete_workspace.assert_called_once_with()


def test_delete_workflow_records_purge(monkeypatch, tmp_path):
    """The single-workflow delete records workspace_purged_at."""
    from Yuki.server.routes import workflow as workflow_routes
    monkeypatch.setenv("HOME", str(tmp_path))
    _mirror(tmp_path, "proj", "wf1", "finished")
    wf = _mock_workflow("finished")
    wf.path = str(tmp_path / ".Yuki" / "Workflows" / "proj" / "wf1")

    with mock.patch.object(workflow_routes, "VWorkflow") as vwf, \
            mock.patch.object(workflow_routes.workflow_purge,
                              "record_workspace_purged") as record:
        vwf.create.return_value = wf
        r = _app(workflow_routes.bp).test_client().get(
            "/delete-workflow/proj/wf1")

    assert r.status_code == 200
    record.assert_called_once_with(wf)
