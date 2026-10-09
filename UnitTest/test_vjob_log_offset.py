"""Tests for VJob.log offset support."""
from Yuki.kernel.jobs.base import VJob


def _make_vjob(path, machine_id="runner-1"):
    """Create a minimal VJob instance for log() tests."""
    c = VJob.__new__(VJob, str(path), machine_id)
    c.path = str(path)
    c.machine_id = machine_id
    return c


def test_log_returns_full_content_by_default(tmp_path):
    """log() returns the full file content when no offset is given."""
    logs_dir = tmp_path / "runner-1" / "logs"
    logs_dir.mkdir(parents=True)
    log_file = logs_dir / "celebi_user_step0.log"
    log_file.write_text("hello world", encoding="utf-8")

    job = _make_vjob(tmp_path)
    assert job.log(0) == "hello world"


def test_log_returns_content_from_offset(tmp_path):
    """log() skips the given byte offset and returns the remainder."""
    logs_dir = tmp_path / "runner-1" / "logs"
    logs_dir.mkdir(parents=True)
    log_file = logs_dir / "celebi_user_step0.log"
    log_file.write_text("hello world", encoding="utf-8")

    job = _make_vjob(tmp_path)
    assert job.log(0, offset=6) == "world"


def test_log_returns_empty_string_when_file_missing(tmp_path):
    """log() returns an empty string when the log file does not exist."""
    job = _make_vjob(tmp_path)
    assert job.log(0) == ""
    assert job.log(0, offset=0) == ""


def test_log_returns_empty_string_when_offset_past_end(tmp_path):
    """log() returns an empty string when the offset is at or past EOF."""
    logs_dir = tmp_path / "runner-1" / "logs"
    logs_dir.mkdir(parents=True)
    log_file = logs_dir / "celebi_user_step0.log"
    log_file.write_text("hi", encoding="utf-8")

    job = _make_vjob(tmp_path)
    assert job.log(0, offset=10) == ""
