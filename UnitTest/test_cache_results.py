"""Tests for cache_results_job (manual save of results into runner cache)."""
# pylint: disable=protected-access
import json
from unittest import mock

from Yuki.kernel.storage.remote import cache_results_job


class _FakeSsh:
    """Answers exec/walk_files and records commands."""

    def __init__(self, files=None, stageout_exists=True):
        self.files = files or [("a.root", "/cache/a.root", 10),
                               ("b.png", "/cache/b.png", 5)]
        self.exec_calls = []
        self.stageout_exists = stageout_exists

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def exec(self, command, timeout=300):  # pylint: disable=unused-argument
        """Record the command and answer success."""
        self.exec_calls.append(command)
        return "", "", 0

    def walk_files(self, remote_dir):  # pylint: disable=unused-argument
        """Yield canned cache files."""
        for entry in self.files:
            yield entry

    def exists(self, remote_path):
        """Answer existence checks for stageout paths."""
        if "stageout" in remote_path:
            return self.stageout_exists
        return True


def _write_runner_config(tmp_path, runner_id="r1"):
    """Write a Yuki config.json with an ssh runner into tmp_path."""
    with open(tmp_path / "config.json", "w", encoding="utf-8") as f:
        json.dump({
            "runners_id": {"farm": runner_id},
            "backend_types": {runner_id: "ssh"},
            "runner_settings": {
                runner_id: {"ssh_host": "h", "ssh_user": "u",
                            "remote_workdir": "/remote/work"},
            },
        }, f)


def _job_stub(workflow_id="wf-1", short_uuid="abc1234", status="coda"):
    """A VJob stub answering workflow_id/short_uuid/status."""
    job = mock.MagicMock()
    job.workflow_id.return_value = workflow_id
    job.short_uuid.return_value = short_uuid
    job.status.return_value = status
    return job


def test_cache_results_copies_stageout_into_cache(tmp_path):
    """The workflow stageout is fast-copied into the managed cache dir."""
    _write_runner_config(tmp_path)
    fake = _FakeSsh()
    updates = []
    with mock.patch("Yuki.kernel.jobs.base.VJob",
                    return_value=_job_stub()), \
            mock.patch("Yuki.kernel.storage.remote._ssh_connection",
                       return_value=fake):
        result = cache_results_job("r1", "proj", "imp1",
                                   updates.append, yuki_dir=str(tmp_path))
    assert result["cached"] == 2
    copy_calls = [c for c in fake.exec_calls if "cp -a" in c]
    assert len(copy_calls) == 1
    cmd = copy_calls[0]
    assert "mkdir -p /remote/work/impressions/proj/imp1" in cmd
    assert ("cp -a --reflink=auto "
            "/remote/work/workflows/proj/wf-1/impabc1234/stageout/.") in cmd
    assert "chmod -R a-w" in cmd
    assert updates[-1]["status"] == "done"
    assert updates[-1]["result"]["cached"] == 2


def test_cache_results_records_distribution(tmp_path):
    """distribution.json gains a transferred cache entry for the runner."""
    _write_runner_config(tmp_path)
    fake = _FakeSsh(files=[("a.root", "/cache/a.root", 10)])
    with mock.patch("Yuki.kernel.jobs.base.VJob",
                    return_value=_job_stub()), \
            mock.patch("Yuki.kernel.storage.remote._ssh_connection",
                       return_value=fake):
        cache_results_job("r1", "proj", "imp1",
                          lambda _s: None, yuki_dir=str(tmp_path))
    dist_path = tmp_path / "Storage" / "proj" / "imp1" / "distribution.json"
    with open(dist_path, encoding="utf-8") as f:
        dist = json.load(f)
    entry = dist["locations"]["runner:farm"]["cache"]
    assert entry["origin"] == "transferred"
    assert entry["files"] == 1
    assert entry["bytes"] == 10
    assert "updated" in entry


def test_cache_results_no_workflow_noop(tmp_path):
    """A runner with no workflow for the impression caches nothing."""
    _write_runner_config(tmp_path)
    fake = _FakeSsh()
    updates = []
    with mock.patch("Yuki.kernel.jobs.base.VJob",
                    return_value=_job_stub(workflow_id="")), \
            mock.patch("Yuki.kernel.storage.remote._ssh_connection",
                       return_value=fake):
        result = cache_results_job("r1", "proj", "imp1",
                                   updates.append, yuki_dir=str(tmp_path))
    assert result["cached"] == 0
    assert not fake.exec_calls
    assert updates[-1]["status"] == "done"


def test_cache_results_copy_failure_raises(tmp_path):
    """A failed remote copy raises and records no distribution."""
    _write_runner_config(tmp_path)
    fake = _FakeSsh()
    fake.exec = lambda command, timeout=300: ("", "no space left", 1)
    with mock.patch("Yuki.kernel.jobs.base.VJob",
                    return_value=_job_stub()), \
            mock.patch("Yuki.kernel.storage.remote._ssh_connection",
                       return_value=fake):
        try:
            cache_results_job("r1", "proj", "imp1",
                              lambda _s: None, yuki_dir=str(tmp_path))
        except RuntimeError as e:
            assert "no space left" in str(e)
        else:
            raise AssertionError("expected RuntimeError")
    assert not (tmp_path / "Storage" / "proj" / "imp1" /
                "distribution.json").exists()


def test_cache_results_skips_unfinished_jobs(tmp_path):
    """A job that never finished has nothing to cache: skip, don't fail."""
    _write_runner_config(tmp_path)
    fake = _FakeSsh()
    updates = []
    with mock.patch("Yuki.kernel.jobs.base.VJob",
                    return_value=_job_stub(status="failed")), \
            mock.patch("Yuki.kernel.storage.remote._ssh_connection",
                       return_value=fake):
        result = cache_results_job("r1", "proj", "imp1",
                                   updates.append, yuki_dir=str(tmp_path))
    assert result["cached"] == 0
    assert "failed" in result["reason"]
    assert not fake.exec_calls
    assert updates[-1]["status"] == "done"


def test_cache_results_skips_missing_stageout(tmp_path):
    """A finished job without remote stageout is skipped gracefully."""
    _write_runner_config(tmp_path)
    fake = _FakeSsh(stageout_exists=False)
    updates = []
    with mock.patch("Yuki.kernel.jobs.base.VJob",
                    return_value=_job_stub()), \
            mock.patch("Yuki.kernel.storage.remote._ssh_connection",
                       return_value=fake):
        result = cache_results_job("r1", "proj", "imp1",
                                   updates.append, yuki_dir=str(tmp_path))
    assert result["cached"] == 0
    assert "no stageout" in result["reason"]
    assert not any("cp -a" in c for c in fake.exec_calls)
    assert updates[-1]["status"] == "done"
