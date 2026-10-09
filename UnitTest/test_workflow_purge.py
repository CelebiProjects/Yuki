"""Tests for stale-workflow workspace purging."""
import json
import os
from unittest import mock

from Yuki.kernel.storage import liveness
from Yuki.services import workflow_purge


def _workflow_mirror(tmp_path, project, workflow, machine_id,
                     status="finished"):
    """A Workflows mirror dir with config.json machine_id and results.json."""
    wf_dir = tmp_path / "Workflows" / project / workflow
    wf_dir.mkdir(parents=True)
    with open(wf_dir / "config.json", "w", encoding="utf-8") as f:
        json.dump({"machine_id": machine_id}, f)
    with open(wf_dir / "results.json", "w", encoding="utf-8") as f:
        json.dump({"results": {"status": status}}, f)
    return wf_dir


def test_purge_stale_workflows_deletes_non_live(monkeypatch, tmp_path):
    """Non-live workflows of the runner are deleted; others are skipped."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    # live impression a -> wf-live; run config per machine.
    run_dir = tmp_path / "Storage" / "proj" / ("a" * 32) / "r1"
    run_dir.mkdir(parents=True)
    with open(run_dir / "config.json", "w", encoding="utf-8") as f:
        json.dump({"workflow": "wf-live"}, f)
    liveness.save_live_set("proj", ["a" * 32], [])

    _workflow_mirror(tmp_path, "proj", "wf-live", "r1")
    _workflow_mirror(tmp_path, "proj", "wf-stale", "r1")
    _workflow_mirror(tmp_path, "proj", "wf-other-runner", "r9")

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "finished"
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows("r1")

    assert summary["purged"] == [
        {"project": "proj", "workflow": "wf-stale"}]
    skipped = {(s["workflow"], s["reason"]) for s in summary["skipped"]}
    assert ("wf-live", "live") in skipped
    assert ("wf-other-runner", None) not in skipped  # filtered by runner
    fake_workflow.delete_workspace.assert_called_once_with()


def test_purge_stale_workflows_skips_running(monkeypatch, tmp_path):
    """Running workflows are skipped, never deleted."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    liveness.save_live_set("proj", [], [])
    _workflow_mirror(tmp_path, "proj", "wf-stale", "r1", status="running")

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "running"
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows("r1")

    assert summary["purged"] == []
    assert summary["skipped"][0]["reason"] == "running"
    fake_workflow.delete_workspace.assert_not_called()


def test_purge_stale_workflows_without_live_set_skips_all(monkeypatch,
                                                          tmp_path):
    """Projects without a synced set are unknown: nothing is purged."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _workflow_mirror(tmp_path, "proj", "wf-1", "r1")

    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        summary = workflow_purge.purge_stale_workflows("r1")

    assert summary["purged"] == []
    assert summary["skipped"][0]["reason"] == \
        "no live set synced"
    vwf.create.assert_not_called()


def test_purge_stale_workflows_dry_run(monkeypatch, tmp_path):
    """Dry-run lists what would go and deletes nothing."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    liveness.save_live_set("proj", [], [])
    _workflow_mirror(tmp_path, "proj", "wf-stale", "r1")

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "finished"
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows("r1", dry_run=True)

    assert summary["purged"] == [
        {"project": "proj", "workflow": "wf-stale"}]
    assert summary["dry_run"] is True
    fake_workflow.delete_workspace.assert_not_called()


def test_purge_stale_workflows_delete_failure_is_skip(monkeypatch,
                                                      tmp_path):
    """A delete failure becomes a skip entry, not an abort."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    liveness.save_live_set("proj", [], [])
    _workflow_mirror(tmp_path, "proj", "wf-stale", "r1")

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "finished"
    fake_workflow.delete_workspace.side_effect = OSError("ssh down")
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows("r1")

    assert summary["purged"] == []
    assert "purge failed" in summary["skipped"][0]["reason"]


def test_purge_stale_workflows_corrupt_mirror_is_skip(monkeypatch,
                                                      tmp_path):
    """A corrupt mirror config is a per-entry skip, never a sweep abort."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    liveness.save_live_set("proj", [], [])
    bad_dir = tmp_path / "Workflows" / "proj" / "wf-bad"
    bad_dir.mkdir(parents=True)
    with open(bad_dir / "config.json", "w", encoding="utf-8") as f:
        f.write("{not json")
    _workflow_mirror(tmp_path, "proj", "wf-good", "r1")

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "finished"
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows("r1")

    bad_skip = next(s for s in summary["skipped"]
                    if s["workflow"] == "wf-bad")
    assert "purge failed" in bad_skip["reason"]
    assert summary["purged"] == [
        {"project": "proj", "workflow": "wf-good"}]


def test_purge_stale_workflows_derives_liveness_at_purge_time(
        monkeypatch, tmp_path):
    """Workflows from re-runs after the last sync are still live."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    run_dir = tmp_path / "Storage" / "proj" / ("a" * 32) / "r1"
    run_dir.mkdir(parents=True)
    with open(run_dir / "config.json", "w", encoding="utf-8") as f:
        json.dump({"workflow": "wf-old"}, f)
    liveness.save_live_set("proj", ["a" * 32], [])
    # A re-run after the sync points the live impression at a new workflow.
    with open(run_dir / "config.json", "w", encoding="utf-8") as f:
        json.dump({"workflow": "wf-new"}, f)

    _workflow_mirror(tmp_path, "proj", "wf-new", "r1")
    _workflow_mirror(tmp_path, "proj", "wf-old", "r1")

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "finished"
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows("r1")

    skipped = {(s["workflow"], s["reason"]) for s in summary["skipped"]}
    assert ("wf-new", "live") in skipped
    assert summary["purged"] == [
        {"project": "proj", "workflow": "wf-old"}]


def _purge_app(monkeypatch, config_vars):
    from Yuki.server.routes import workflow as workflow_routes
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(workflow_routes.bp)
    config_obj = mock.MagicMock()
    from CelebiChrono.utils.metadata import ConfigFile
    import tempfile
    tmp = tempfile.mkdtemp()
    config_obj.config_path = os.path.join(tmp, "config.json")
    config_obj.get_config_file.return_value = ConfigFile(
        config_obj.config_path)
    with open(config_obj.config_path, "w", encoding="utf-8") as f:
        json.dump(config_vars, f)
    monkeypatch.setattr(workflow_routes, "config", config_obj)
    return app


def test_purge_runner_workflows_returns_summary(monkeypatch):
    """/purge-runner-workflows delegates to the kernel purge."""
    from Yuki.server.routes import workflow as workflow_routes
    app = _purge_app(monkeypatch, {
        "runners_id": {"pkufarm": "r1"},
        "backend_types": {"r1": "ssh"},
    })
    summary = {"purged": [], "skipped": [], "dry_run": True}
    with mock.patch.object(workflow_routes, "workflow_purge") as purge:
        purge.purge_stale_workflows.return_value = summary
        r = app.test_client().post(
            "/purge-runner-workflows",
            json={"runner": "pkufarm", "dry_run": True,
                  "project_uuid": "proj"})
    assert r.status_code == 200
    assert r.get_json()["dry_run"] is True
    purge.purge_stale_workflows.assert_called_once_with(
        "r1", True, project_uuid="proj")


def test_purge_runner_workflows_unknown_runner_404(monkeypatch):
    app = _purge_app(monkeypatch, {
        "runners_id": {"pkufarm": "r1"},
        "backend_types": {"r1": "ssh"},
    })
    r = app.test_client().post(
        "/purge-runner-workflows", json={"runner": "nope"})
    assert r.status_code == 404


def test_purge_runner_workflows_missing_runner_400(monkeypatch):
    app = _purge_app(monkeypatch, {
        "runners_id": {"pkufarm": "r1"},
        "backend_types": {"r1": "ssh"},
    })
    r = app.test_client().post(
        "/purge-runner-workflows", json={})
    assert r.status_code == 400


def test_purge_runner_workflows_unsupported_backend_400(monkeypatch):
    """A runner whose backend type is not purgeable returns 400."""
    app = _purge_app(monkeypatch, {
        "runners_id": {"pkufarm": "r1"},
        "backend_types": {"r1": "docker"},
    })
    r = app.test_client().post(
        "/purge-runner-workflows", json={"runner": "pkufarm"})
    assert r.status_code == 400
    assert "backend" in r.get_json()["error"]


def test_purge_runner_workflows_kernel_error_500(monkeypatch):
    """A kernel purge failure surfaces as a 500 with the error."""
    from Yuki.server.routes import workflow as workflow_routes
    app = _purge_app(monkeypatch, {
        "runners_id": {"pkufarm": "r1"},
        "backend_types": {"r1": "ssh"},
    })
    with mock.patch.object(workflow_routes, "workflow_purge") as purge:
        purge.purge_stale_workflows.side_effect = RuntimeError("x")
        r = app.test_client().post(
            "/purge-runner-workflows", json={"runner": "pkufarm"})
    assert r.status_code == 500
    assert r.get_json() == {"error": "x"}


def test_purge_stale_workflows_project_scope(monkeypatch, tmp_path):
    """With project_uuid given, only that project's workflows are scanned."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    liveness.save_live_set("proj", [], [])
    _workflow_mirror(tmp_path, "proj", "wf-stale", "r1")
    _workflow_mirror(tmp_path, "other", "wf-other", "r1")

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "finished"
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows(
            "r1", project_uuid="proj")

    assert summary["purged"] == [
        {"project": "proj", "workflow": "wf-stale"}]
    assert all(s["project"] == "proj" for s in summary["skipped"])
    fake_workflow.delete_workspace.assert_called_once_with()


def _runner_config(tmp_path, backend="reana", remote_workdir="/remote",
                  workdir=None):
    """Write a global config with the runner's backend and settings."""
    with open(tmp_path / "config.json", "w", encoding="utf-8") as f:
        json.dump({
            "runners_id": {"farm": "r1"},
            "backend_types": {"r1": backend},
            "runner_settings": {"r1": dict(
                {"remote_workdir": remote_workdir},
                **({"workdir": workdir} if workdir else {}))},
        }, f)


def _record_purged(wf_dir, timestamp="2026-08-29T12:00:00+00:00"):
    """Write workspace_purged_at into a mirror config.json."""
    cfg = json.load(open(os.path.join(wf_dir, "config.json")))
    cfg["workspace_purged_at"] = timestamp
    with open(os.path.join(wf_dir, "config.json"), "w") as f:
        json.dump(cfg, f)


def test_purge_stale_workflows_skips_recorded_purged(monkeypatch, tmp_path):
    """Mirrors already recording a purge are skipped and summarized."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    liveness.save_live_set("proj", [], [])
    wf_dir = _workflow_mirror(tmp_path, "proj", "wf-gone", "r1")
    _record_purged(wf_dir)

    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        summary = workflow_purge.purge_stale_workflows("r1")

    assert summary["purged"] == []
    assert summary["already_gone"] == 1
    vwf.create.assert_not_called()


def test_purge_stale_workflows_records_after_delete(monkeypatch, tmp_path):
    """A successful delete records workspace_purged_at in the mirror."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    liveness.save_live_set("proj", [], [])
    wf_dir = _workflow_mirror(tmp_path, "proj", "wf-stale", "r1")

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "finished"
    fake_workflow.path = str(wf_dir)
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows("r1")

    assert summary["purged"] == [{"project": "proj",
                                  "workflow": "wf-stale"}]
    cfg = json.load(open(os.path.join(wf_dir, "config.json")))
    assert "workspace_purged_at" in cfg


def test_purge_stale_workflows_heals_missing_ssh_workspaces(monkeypatch,
                                                            tmp_path):
    """Missing remote dirs get the record retroactively and are skipped."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _runner_config(tmp_path, backend="ssh", remote_workdir="/remote")
    liveness.save_live_set("proj", [], [])
    gone = _workflow_mirror(tmp_path, "proj", "wf-gone", "r1")
    stays = _workflow_mirror(tmp_path, "proj", "wf-stays", "r1")

    fake_ssh = mock.MagicMock()
    fake_ssh.__enter__.return_value = fake_ssh
    fake_ssh.__exit__.return_value = False
    # The batch existence command echoes only the dirs that exist.
    fake_ssh.exec.return_value = (
        "/remote/workflows/proj/wf-stays", "", 0)

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "finished"
    fake_workflow.path = str(stays)
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf, \
            mock.patch.object(workflow_purge, "_SshConnection",
                              return_value=fake_ssh):
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows("r1")

    assert summary["purged"] == [{"project": "proj",
                                  "workflow": "wf-stays"}]
    assert summary["already_gone"] == 1
    cfg = json.load(open(os.path.join(gone, "config.json")))
    assert "workspace_purged_at" in cfg
    fake_workflow.delete_workspace.assert_called_once_with()


def test_purge_stale_workflows_heals_missing_native_workspaces(
        monkeypatch, tmp_path):
    """Native candidates whose dir is gone are healed, not listed."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _runner_config(tmp_path, backend="native",
                   workdir=str(tmp_path / "localwf"))
    liveness.save_live_set("proj", [], [])
    gone = _workflow_mirror(tmp_path, "proj", "wf-gone", "r1")
    stays = _workflow_mirror(tmp_path, "proj", "wf-stays", "r1")
    (tmp_path / "localwf" / "wf-stays").mkdir(parents=True)

    fake_workflow = mock.MagicMock()
    fake_workflow.status.return_value = "finished"
    fake_workflow.path = str(stays)
    with mock.patch.object(workflow_purge, "VWorkflow") as vwf:
        vwf.create.return_value = fake_workflow
        summary = workflow_purge.purge_stale_workflows("r1")

    assert summary["purged"] == [{"project": "proj",
                                  "workflow": "wf-stays"}]
    assert summary["already_gone"] == 1
    cfg = json.load(open(os.path.join(gone, "config.json")))
    assert "workspace_purged_at" in cfg
