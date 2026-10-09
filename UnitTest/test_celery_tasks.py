"""Tests for Yuki Celery tasks."""
import logging
from unittest import mock

import pytest


def test_task_exec_impression_logs_submit_finished():
    """The submit task logs a completion line once the workflow is handed off."""
    from Yuki.server import tasks
    workflow = mock.Mock()
    workflow.uuid = "wf-1"

    records = []

    def capture(record):
        records.append(record.getMessage())

    handler = logging.Handler()
    handler.emit = capture
    kernel_logger = logging.getLogger("Yuki.kernel")
    kernel_logger.addHandler(handler)
    old_level = kernel_logger.level
    kernel_logger.setLevel(logging.DEBUG)
    try:
        with mock.patch.object(tasks, "metadata") as meta, \
             mock.patch.object(tasks, "VJob"), \
             mock.patch.object(tasks, "VWorkflow") as vwf, \
             mock.patch.object(tasks, "_validate_remote_data_binding",
                               return_value=[]):
            meta.ConfigFile.return_value.read_variable.return_value = {}
            vwf.create.return_value = workflow
            tasks.task_exec_impression("proj", "imp1", "runner-1")
    finally:
        kernel_logger.removeHandler(handler)
        kernel_logger.setLevel(old_level)

    workflow.run.assert_called_once_with()
    assert any("submit finished" in msg and "wf-1" in msg
               for msg in records)


def test_duplicate_submission_returns_existing_workflow():
    """An identical active job set is idempotent instead of launching again."""
    from Yuki.server import tasks
    from Yuki.kernel.execution.lease import WorkflowAlreadyActive
    workflow = mock.Mock()
    workflow.uuid = "wf-new"
    workflow.run.side_effect = WorkflowAlreadyActive(
        "wf-existing", {"imp1": {"workflow_id": "wf-existing"}})
    with mock.patch.object(tasks, "metadata") as meta, \
         mock.patch.object(tasks, "VJob"), \
         mock.patch.object(tasks, "VWorkflow") as factory, \
         mock.patch.object(tasks, "_validate_remote_data_binding", return_value=[]):
        meta.ConfigFile.return_value.read_variable.return_value = {}
        factory.create.return_value = workflow
        result = tasks.task_exec_impression("proj", "imp1", "runner-1")

    assert result == {"workflow_id": "wf-existing", "deduplicated": True}
    workflow.set_workflow_status.assert_called_once_with("stopped")


def test_partial_overlap_returns_conflict_without_backend_retry():
    """A partial overlap is reported and not retried as another workflow."""
    from Yuki.server import tasks
    from Yuki.kernel.execution.lease import WorkflowLeaseConflict
    workflow = mock.Mock()
    workflow.uuid = "wf-new"
    workflow.run.side_effect = WorkflowLeaseConflict(
        {"imp1": {"workflow_id": "wf-existing"}})
    with mock.patch.object(tasks, "metadata") as meta, \
         mock.patch.object(tasks, "VJob"), \
         mock.patch.object(tasks, "VWorkflow") as factory, \
         mock.patch.object(tasks, "_validate_remote_data_binding", return_value=[]):
        meta.ConfigFile.return_value.read_variable.return_value = {}
        factory.create.return_value = workflow
        result = tasks.task_exec_impression("proj", "imp1 imp2", "runner-1")

    assert result["conflicts"] == {"imp1": "wf-existing"}
    assert result["deduplicated"] is False
    workflow.set_workflow_status.assert_called_once_with("failed")


def test_partial_overlap_is_persisted_as_blocked(tmp_path, monkeypatch):
    """A rejected DAG remains visible after the Celery result disappears."""
    from Yuki.server import tasks
    from Yuki.kernel.execution.lease import WorkflowLeaseConflict
    from Yuki.kernel.execution.submissions import SubmissionStore
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    store, _ = SubmissionStore.create(
        "proj", ["imp1", "imp2"], "runner-1", submission_id="sub-1")
    workflow = mock.Mock(uuid="wf-new")
    workflow.run.side_effect = WorkflowLeaseConflict(
        {"imp1": {"workflow_id": "wf-existing"}})

    with mock.patch.object(tasks, "metadata") as meta, \
         mock.patch.object(tasks, "VJob"), \
         mock.patch.object(tasks, "VWorkflow") as factory, \
         mock.patch.object(tasks, "_validate_remote_data_binding", return_value=[]):
        meta.ConfigFile.return_value.read_variable.return_value = {}
        factory.create.return_value = workflow
        tasks.task_exec_impression(
            "proj", "imp1 imp2", "runner-1", submission_id="sub-1")

    record = store.read()
    assert record["status"] == "blocked"
    assert record["workflow_id"] == ""
    assert record["candidate_workflow_id"] == "wf-new"
    assert record["retryable"] is True
    assert record["conflicts"] == {"imp1": "wf-existing"}


def test_unexpected_build_failure_is_persisted(tmp_path, monkeypatch):
    """Construction exceptions cannot leave an accepted submission silent."""
    from Yuki.server import tasks
    from Yuki.kernel.execution.submissions import SubmissionStore
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    store, _ = SubmissionStore.create(
        "proj", ["imp1"], "runner-1", submission_id="sub-1")

    with mock.patch.object(tasks, "metadata") as meta, \
         mock.patch.object(tasks, "VJob"), \
         mock.patch.object(tasks, "VWorkflow") as factory:
        meta.ConfigFile.return_value.read_variable.return_value = {}
        factory.create.side_effect = RuntimeError("broken graph")
        with pytest.raises(RuntimeError, match="broken graph"):
            tasks.task_exec_impression(
                "proj", "imp1", "runner-1", submission_id="sub-1")

    record = store.read()
    assert record["status"] == "failed"
    assert record["error"] == "RuntimeError: broken graph"


def test_task_transfer_results_calls_run_transfer():
    """task_transfer_results delegates to result_transfer.run_transfer."""
    from Yuki.server.tasks import task_transfer_results
    with mock.patch("Yuki.server.tasks.result_transfer") as rt:
        rt.run_transfer.return_value = {"transferred": ["a.txt"]}
        result = task_transfer_results("job1", "proj", "imp",
                                       "runner:pkufarm", "yuki",
                                       None, False)
        rt.run_transfer.assert_called_once_with(
            "job1", "proj", "imp",
            "runner:pkufarm", "yuki",
            pattern=None, force=False)
        assert result == {"transferred": ["a.txt"]}


def test_task_update_workflow_status_delegates_to_workflow(tmp_path, monkeypatch):
    """The task refreshes status via the workflow's own status write.

    Distribution refresh on the terminal transition happens inside
    update_workflow_status, not separately in the task.
    """
    from Yuki.server import tasks
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    workflow = mock.Mock()

    with mock.patch.object(tasks, "VWorkflow") as vwf:
        vwf.create.return_value = workflow
        tasks.task_update_workflow_status("proj", "wf-1")

    vwf.create.assert_called_once_with("proj", [], "wf-1")
    workflow.update_workflow_status.assert_called_once_with()


def test_task_update_workflow_status_skips_stopped(tmp_path, monkeypatch):
    """A queued refresh cannot revive a workflow stopped after dispatch."""
    from Yuki.server import tasks
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    workflow = mock.Mock()
    workflow.status.return_value = "stopped"

    with mock.patch.object(tasks, "VWorkflow") as vwf:
        vwf.create.return_value = workflow
        tasks.task_update_workflow_status("proj", "wf-1")

    workflow.update_workflow_status.assert_not_called()


@pytest.mark.parametrize("status", ["finished", "coda", "failed"])
def test_refresh_workflow_distributions_terminal(status):
    """Terminal workflows refresh every non-algorithm job's registry."""
    from Yuki.kernel.storage.impressions import refresh_workflow_distributions
    task_job = mock.Mock()
    task_job.job_type.return_value = "task"
    task_job.uuid = "imp1"
    task_job.is_input = False
    algo_job = mock.Mock()
    algo_job.job_type.return_value = "algorithm"
    algo_job.uuid = "imp2"
    workflow = mock.Mock()
    workflow.jobs = [task_job, algo_job]
    workflow.machine_id = "runner-1"

    with mock.patch("Yuki.kernel.storage.impressions.ImpressionStorage") as ims:
        refresh_workflow_distributions("proj", workflow, status)

    ims.assert_called_once_with("proj", "imp1")
    ims.return_value.update_distribution.assert_called_once_with(
        refresh_cache=True, cache_runner_id="runner-1")


def test_refresh_workflow_distributions_no_refresh_while_running():
    """A non-terminal workflow status leaves the registry alone."""
    from Yuki.kernel.storage.impressions import refresh_workflow_distributions
    workflow = mock.Mock()
    workflow.jobs = []

    with mock.patch("Yuki.kernel.storage.impressions.ImpressionStorage") as ims:
        refresh_workflow_distributions("proj", workflow, "running")

    ims.assert_not_called()


def test_refresh_workflow_distributions_survives_failure():
    """A failing refresh never fails the status update."""
    from Yuki.kernel.storage.impressions import refresh_workflow_distributions
    task_job = mock.Mock()
    task_job.job_type.return_value = "task"
    task_job.uuid = "imp1"
    task_job.is_input = False
    workflow = mock.Mock()
    workflow.jobs = [task_job]
    workflow.machine_id = "runner-1"

    with mock.patch("Yuki.kernel.storage.impressions.ImpressionStorage") as ims:
        ims.return_value.update_distribution.side_effect = OSError("boom")
        refresh_workflow_distributions("proj", workflow, "failed")  # no raise
