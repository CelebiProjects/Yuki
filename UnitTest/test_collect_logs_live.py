"""Tests for live log collection while a job is running."""
from unittest import mock

from Yuki.kernel.execution.status import CODA, FAILED, IN_MOVEMENT


def _storage(tmp_path):
    from Yuki.kernel.storage import impressions as ims
    s = ims.ImpressionStorage.__new__(ims.ImpressionStorage)
    s.project_uuid = "proj-1"
    s.impression = "imp7"
    s.job_path = str(tmp_path / "job")
    s.runners = ["runner"]
    s.runners_id = {"runner": "runner-1"}
    return s


def _runner(job_status):
    """Build a runner context whose job reports the given musical status."""
    job = mock.Mock()
    job.status.return_value = job_status
    wf = mock.Mock()
    wf.download_logs.return_value = {"collected": [], "skipped": [], "failed": []}
    return job, wf


def test_collect_logs_refreshes_logs_while_in_movement(tmp_path):
    """collect_logs downloads (with refresh) logs for a running job."""
    s = _storage(tmp_path)
    job, wf = _runner(IN_MOVEMENT)
    s._get_runner_contexts = lambda: [("runner", job, wf)]
    report = s.collect_logs()
    wf.download_logs.assert_called_once_with("imp7", refresh=True)
    assert "runner" in report


def test_collect_logs_refreshes_logs_at_terminal_states(tmp_path):
    """collect_logs refreshes logs for finished and failed jobs too."""
    for status in (CODA, FAILED):
        s = _storage(tmp_path)
        job, wf = _runner(status)
        s._get_runner_contexts = lambda: [("runner", job, wf)]
        s.collect_logs()
        wf.download_logs.assert_called_once_with("imp7", refresh=True)


def test_collect_logs_skips_download_before_execution(tmp_path):
    """collect_logs does not download logs for pre-execution statuses."""
    s = _storage(tmp_path)
    job, wf = _runner("prelude")
    s._get_runner_contexts = lambda: [("runner", job, wf)]
    report = s.collect_logs()
    wf.download_logs.assert_not_called()
    assert report["runner"] == {"collected": [], "skipped": [], "failed": []}
