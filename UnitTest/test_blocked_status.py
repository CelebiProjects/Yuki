"""Structured, immutable blocked-attempt status metadata."""
import json
from Yuki.kernel.execution.status import PRELUDE
from Yuki.kernel.jobs.base import VJob
from Yuki.server.routes.status import _job_status_response


def _bare_job(path):
    job = object.__new__(VJob)
    job.path = str(path)
    return job


def test_set_blocked_records_snapshot_and_new_status_clears_it(tmp_path):
    """A blocker is historical metadata for one attempt only."""
    job = _bare_job(tmp_path / "job")
    blocker = {
        "impression": "a" * 32,
        "path": "Selection/Upstream",
        "workflow_id": "upstream-workflow",
        "observed_status": "failed",
    }

    job.set_blocked([blocker], "downstream-workflow")

    status_path = tmp_path / "job" / "status.json"
    saved = json.loads(status_path.read_text(encoding="utf-8"))
    assert saved["status"] == "failed"
    assert saved["failure_kind"] == "blocked"
    assert saved["blocked_by"] == [blocker]
    assert saved["blocked_workflow_id"] == "downstream-workflow"
    assert "aaaaaaa (Selection/Upstream)" in saved["detailed_status"]

    job.set_status(PRELUDE)
    saved = json.loads(status_path.read_text(encoding="utf-8"))
    assert saved["failure_kind"] == ""
    assert saved["blocked_by"] == []
    assert saved["blocked_workflow_id"] == ""


def test_status_response_exposes_saved_blocker_without_querying_upstream(tmp_path):
    """The API serializes the saved snapshot and performs no DAG refresh."""
    job_path = tmp_path / "job"
    job = _bare_job(job_path)
    blocker = {
        "impression": "b" * 32,
        "path": "Fit/Upstream",
        "workflow_id": "upstream-workflow",
        "observed_status": "failed",
    }
    job.set_blocked([blocker], "downstream-workflow")

    payload = _job_status_response(
        str(job_path), "failed", "blocked")

    assert payload["failure_kind"] == "blocked"
    assert payload["blocked_by"] == [blocker]
    assert payload["blocked_workflow_id"] == "downstream-workflow"
