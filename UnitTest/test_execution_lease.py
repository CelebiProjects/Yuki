"""Cross-process execution ownership regression tests."""
# pylint: disable=protected-access
import json
import multiprocessing
import os
from unittest import mock

import pytest

from Yuki.kernel import execution_lease


def _claim_worker(root, workflow_id, ready, start, output):
    """Compete for one job set from an independent process."""
    os.environ["YUKIDIR"] = root
    ready.put(workflow_id)
    start.wait()
    try:
        claim = execution_lease.claim_many(
            "project", "runner", workflow_id, ["job-a", "job-b"])
        output.put(("claimed", workflow_id, claim.token))
    except execution_lease.WorkflowAlreadyActive as exc:
        output.put(("duplicate", workflow_id, exc.workflow_id))
    except execution_lease.WorkflowLeaseConflict as exc:
        output.put(("conflict", workflow_id, sorted(exc.conflicts)))


def _write_status(root, workflow_id, status):
    """Write a minimal workflow result used by lease liveness checks."""
    directory = root / "Workflows" / "project" / workflow_id
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "results.json").write_text(
        json.dumps({"results": {"status": status}}), encoding="utf-8")


def test_identical_claim_returns_existing_workflow(tmp_path, monkeypatch):
    """An identical claim resolves to the first workflow."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    first = execution_lease.claim_many(
        "project", "runner", "wf-1", ["job-b", "job-a"])

    with pytest.raises(execution_lease.WorkflowAlreadyActive) as error:
        execution_lease.claim_many(
            "project", "runner", "wf-2", ["job-a", "job-b"])

    assert error.value.workflow_id == "wf-1"
    assert execution_lease.validate_many(
        "project", "runner", "wf-1", first.token, ["job-a", "job-b"])


def test_partial_overlap_claims_nothing(tmp_path, monkeypatch):
    """A conflicting batch is rejected atomically."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    execution_lease.claim_many(
        "project", "runner", "wf-1", ["job-a", "job-b"])

    with pytest.raises(execution_lease.WorkflowLeaseConflict) as error:
        execution_lease.claim_many(
            "project", "runner", "wf-2", ["job-b", "job-c"])

    assert set(error.value.conflicts) == {"job-b"}
    # Atomicity: job-c was not retained by the rejected wf-2 claim.
    claim = execution_lease.claim_many(
        "project", "runner", "wf-3", ["job-c"])
    assert execution_lease.validate_many(
        "project", "runner", "wf-3", claim.token, ["job-c"])


def test_same_impression_conflicts_across_runners(tmp_path, monkeypatch):
    """Runner choice does not create a second ownership namespace."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    execution_lease.claim_many(
        "project", "runner-a", "wf-1", ["job-a"])

    with pytest.raises(execution_lease.WorkflowLeaseConflict):
        execution_lease.claim_many(
            "project", "runner-b", "wf-2", ["job-a"])


def test_terminal_owner_can_be_replaced_but_old_token_cannot_write(
        tmp_path, monkeypatch):
    """A rerun supersedes terminal ownership using a fresh token."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    old = execution_lease.claim_many(
        "project", "runner", "wf-old", ["job-a"])
    _write_status(tmp_path, "wf-old", "failed")

    new = execution_lease.claim_many(
        "project", "runner", "wf-new", ["job-a"])

    assert not execution_lease.owns(
        "project", "runner", "wf-old", old.token, "job-a")
    assert execution_lease.owns(
        "project", "runner", "wf-new", new.token, "job-a")


def test_stale_workflow_skips_shared_job_status_write(tmp_path, monkeypatch):
    """A late completion from an old workflow cannot overwrite its rerun."""
    from Yuki.kernel.native_workflow import NativeWorkflow
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    old = execution_lease.claim_many(
        "project", "runner", "wf-old", ["job-a"])
    _write_status(tmp_path, "wf-old", "failed")
    execution_lease.claim_many(
        "project", "runner", "wf-new", ["job-a"])

    workflow = NativeWorkflow.__new__(NativeWorkflow)
    workflow.project_uuid = "project"
    workflow.machine_id = "runner"
    workflow.uuid = "wf-old"
    workflow.lease_token = old.token
    workflow.logger = mock.Mock()
    job = mock.Mock()
    job.uuid = "job-a"
    job.is_input = False
    job.job_type.return_value = "task"

    assert not workflow._set_owned_job_status(job, "failed", "late")
    job.set_status.assert_not_called()


def test_legacy_workflow_pointer_also_blocks_late_status_write():
    """Pre-lease workflows cannot overwrite a job already reassigned."""
    from Yuki.kernel.native_workflow import NativeWorkflow
    workflow = NativeWorkflow.__new__(NativeWorkflow)
    workflow.uuid = "wf-old"
    workflow.lease_token = ""
    workflow.logger = mock.Mock()
    job = mock.Mock()
    job.uuid = "job-a"
    job.is_input = False
    job.job_type.return_value = "task"
    job.workflow_id.return_value = "wf-new"

    assert not workflow._set_owned_job_status(job, "failed", "late")
    job.set_status.assert_not_called()


def test_flock_allows_only_one_process_to_claim_same_set(tmp_path):
    """Filesystem locking coordinates separate worker processes."""
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    output = context.Queue()
    start = context.Event()
    workers = [
        context.Process(target=_claim_worker,
                        args=(str(tmp_path), f"wf-{index}", ready, start, output))
        for index in range(2)
    ]
    for worker in workers:
        worker.start()
    for _worker in workers:
        ready.get(timeout=10)
    start.set()
    results = [output.get(timeout=10) for _worker in workers]
    for worker in workers:
        worker.join(timeout=10)
        assert worker.exitcode == 0

    assert [result[0] for result in results].count("claimed") == 1
    assert [result[0] for result in results].count("duplicate") == 1
