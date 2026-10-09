"""Shared-storage native agent behavior."""
import json
from unittest import mock

from click.testing import CliRunner

from Yuki import native_runner
from Yuki.kernel.execution.local import execute_workflow


def _prepared(tmp_path, status="ready_for_local_execution"):
    root = tmp_path / ".Yuki"
    path = root / "Workflows" / "project" / "workflow"
    path.mkdir(parents=True)
    (path / "config.json").write_text(
        json.dumps({"backend_type": "native", "machine_id": "machine"}))
    (path / "results.json").write_text(json.dumps({"results": {"status": status}}))
    (root / "LocalWorkflows" / "workflow").mkdir(parents=True)
    (root / "LocalWorkflows" / "workflow" / "Snakefile").write_text("rule all:\n")
    (root / "config.json").write_text("{}")
    return root, path


def _status(path):
    return json.loads((path / "results.json").read_text())["results"]["status"]


def test_agent_claims_ready_workflow_once(monkeypatch, tmp_path):
    root, path = _prepared(tmp_path)
    monkeypatch.setenv("YUKIDIR", str(root))

    def complete(_uuid, logger):
        assert _status(path) == "running"
        native_runner._set_state(str(path), "finished")
        return 0

    with mock.patch.object(native_runner, "execute_workflow", side_effect=complete) as run:
        assert native_runner._run_ready(str(root))
        assert not native_runner._run_ready(str(root))
    run.assert_called_once()
    assert _status(path) == "finished"


def test_agent_reports_launch_error_and_keeps_processing(monkeypatch, tmp_path):
    root, path = _prepared(tmp_path)
    monkeypatch.setenv("YUKIDIR", str(root))
    with mock.patch.object(native_runner, "execute_workflow",
                           side_effect=FileNotFoundError("snakemake missing")):
        assert native_runner._run_ready(str(root))
    data = json.loads((path / "results.json").read_text())["results"]
    assert data["status"] == "failed"
    assert "snakemake missing" in data["error"]


def test_agent_ignores_unprepared_workflow(monkeypatch, tmp_path):
    root, _path = _prepared(tmp_path, "failed")
    monkeypatch.setenv("YUKIDIR", str(root))
    with mock.patch.object(native_runner, "execute_workflow") as run:
        assert not native_runner._run_ready(str(root))
    run.assert_not_called()


def test_agent_rejects_missing_success_metadata(monkeypatch, tmp_path):
    root, path = _prepared(tmp_path)
    monkeypatch.setenv("YUKIDIR", str(root))
    with mock.patch.object(native_runner, "execute_workflow", return_value=0):
        assert native_runner._run_ready(str(root))
    assert _status(path) == "failed"


def test_restart_marks_abandoned_claim_failed(monkeypatch, tmp_path):
    root, path = _prepared(tmp_path, "running")
    monkeypatch.setenv("YUKIDIR", str(root))
    (path / "native-runner.claim").write_text("1234")
    assert not native_runner._recover_interrupted(str(root))
    assert _status(path) == "failed"
    assert not (path / "native-runner.claim").exists()


def test_restart_waits_for_orphaned_process(monkeypatch, tmp_path):
    root, path = _prepared(tmp_path, "running")
    monkeypatch.setenv("YUKIDIR", str(root))
    (path / "native-runner.claim").write_text("1234")
    pid_file = root / "LocalWorkflows" / "workflow" / "native-runner.snakemake.pid"
    pid_file.write_text("4321")
    with mock.patch.object(native_runner, "_pid_alive", return_value=True):
        assert native_runner._recover_interrupted(str(root))
    assert _status(path) == "running"
    assert (path / "native-runner.claim").exists()


def test_runner_foreground_and_status(monkeypatch, tmp_path):
    root, _path = _prepared(tmp_path, "finished")
    monkeypatch.setenv("YUKIDIR", str(root))
    cli = CliRunner()
    assert cli.invoke(native_runner.cli, ["status"]).output.strip() == "stopped"
    with mock.patch.object(native_runner, "_run_ready", return_value=False):
        native_runner._serve(str(root), once=True)
    assert cli.invoke(native_runner.cli, ["status"]).output.strip() == "stopped"


def test_output_collection_failure_is_not_success(monkeypatch, tmp_path):
    root, path = _prepared(tmp_path)
    monkeypatch.setenv("YUKIDIR", str(root))
    with mock.patch("Yuki.kernel.storage.staging.FileStager") as stager, \
            mock.patch("Yuki.kernel.execution.monitor.SnakemakeMonitor") as monitor:
        stager.return_value.stage_in.return_value = True
        stager.return_value.stage_out.return_value = False
        monitor.return_value.execute_snakemake.return_value = 0
        assert execute_workflow("workflow") == 1
    monitor.return_value._finalize_results.assert_not_called()
    monitor.return_value._update_results.assert_called_once()


def test_cancelled_before_snakemake_does_not_launch(monkeypatch, tmp_path):
    root, _path = _prepared(tmp_path)
    monkeypatch.setenv("YUKIDIR", str(root))
    (root / "LocalWorkflows" / "workflow" / "native-runner.cancel").write_text("cancel\n")
    with mock.patch("Yuki.kernel.storage.staging.FileStager") as stager, \
            mock.patch("Yuki.kernel.execution.monitor.SnakemakeMonitor") as monitor:
        stager.return_value.stage_in.return_value = True
        assert execute_workflow("workflow") == 1
    monitor.return_value.execute_snakemake.assert_not_called()
    monitor.return_value._update_results.assert_called_once()


def test_native_status_refresh_preserves_host_failure(monkeypatch, tmp_path):
    root, path = _prepared(tmp_path, "failed")
    monkeypatch.setenv("YUKIDIR", str(root))
    from Yuki.kernel.workflows.native import NativeWorkflow
    workflow = NativeWorkflow("project", [], "workflow")
    workflow.update_workflow_status()
    assert _status(path) == "failed"


def test_host_agent_stages_runs_and_collects(monkeypatch, tmp_path):
    """Exercise the whole handoff with a tiny Snakemake-shaped executable."""
    root, path = _prepared(tmp_path)
    monkeypatch.setenv("YUKIDIR", str(root))
    job = "j" * 32
    (path / "config.json").write_text(json.dumps({
        "backend_type": "native", "machine_id": "machine",
        "jobs_info": {job: {"is_input": False, "job_type": "task"}},
    }))
    source = root / "Storage" / "project" / job / "rawdata"
    source.mkdir(parents=True)
    (source / "input.txt").write_text("input")
    fake = tmp_path / "fake-snakemake"
    fake.write_text("#!/bin/sh\n"
                    "test -f impjjjjjjj/stageout/input.txt || exit 4\n"
                    "printf output > impjjjjjjj/stageout/result.txt\n"
                    "touch jjjjjjj.done\n")
    fake.chmod(0o755)
    (root / "config.json").write_text(json.dumps({
        "runner_settings": {"machine": {"snakemake_path": str(fake)}}}))
    assert native_runner._run_ready(str(root))
    assert _status(path) == "coda"
    assert (root / "Storage" / "project" / job / "machine" / "stageout" /
            "result.txt").read_text() == "output"
