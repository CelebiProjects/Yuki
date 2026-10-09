"""Tests for ReanaWorkflow.upload_file."""
# pylint: disable=protected-access
import os
from unittest import mock

from Yuki.kernel.workflows import reana as reana_workflow


class _FakeJob:
    """Minimal job object for upload_file; like a real VJob it has no
    use_eos attribute, only the cache_on_runner replacement."""

    def __init__(self, path):
        """Create the fake input job rooted at path."""
        self.path = path
        self.is_input = True
        self.machine_id = "runner-1"

    def files(self):
        """Input jobs have no contents files to upload."""
        return []

    def environment(self):
        """Environment is neither rawdata nor datalist."""
        return "root"

    def short_uuid(self):
        """Short impression id used in remote file names."""
        return "1234567"

    def workflow_id(self):
        """Source workflow id (unused when stageout exists locally)."""
        return "srcwf"

    def cache_on_runner(self):
        """Whether outputs are cached on the runner (EOS for reana)."""
        return False


def _make_wf():
    wf = reana_workflow.ReanaWorkflow.__new__(reana_workflow.ReanaWorkflow)
    wf.machine_id = "runner-1"
    wf.project_uuid = "proj-1"
    wf.steps = []
    wf.logger = lambda *a, **k: None
    wf.get_name = lambda: "wfname"
    wf.get_access_token = lambda mid: "tok"
    wf.set_environment = lambda mid: None
    return wf


def test_upload_file_input_job_uploads_stageout(tmp_path):
    """Input jobs upload their local stageout files to the workflow workspace.

    Regression: the input-job branch used to call the removed job.use_eos()
    method, raising AttributeError and aborting the whole upload.
    """
    home = tmp_path
    job_path = home / ".Yuki" / "Storage" / "proj-1" / "1234567abc"
    stageout = job_path / "runner-1" / "stageout"
    stageout.mkdir(parents=True)
    (stageout / "mass.png").write_bytes(b"data")

    wf = _make_wf()
    wf.path = str(tmp_path / "wdir")
    os.makedirs(wf.path)
    wf.snakefile_path = str(tmp_path / "Snakefile")
    with open(wf.snakefile_path, "w", encoding="utf-8") as f:
        f.write("rule all:\n")
    wf.jobs = [_FakeJob(str(job_path))]

    with mock.patch.dict(os.environ, {"HOME": str(home)}), \
         mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli:
        wf.upload_file()

    uploaded = {c.args[2] for c in cli.upload_file.call_args_list}
    assert "imp1234567/stageout/mass.png" in uploaded
    assert "Snakefile" in uploaded
    assert "reana.yaml" in uploaded


def test_upload_file_skips_stageout_when_cached_on_same_runner(tmp_path):
    """Inputs already cached on the runner (cache_on_runner=True, same
    machine) are not re-uploaded; the setup step fetches them instead."""
    home = tmp_path
    job_path = home / ".Yuki" / "Storage" / "proj-1" / "1234567abc"
    stageout = job_path / "runner-1" / "stageout"
    stageout.mkdir(parents=True)
    (stageout / "mass.png").write_bytes(b"data")

    job = _FakeJob(str(job_path))
    job.cache_on_runner = lambda: True

    wf = _make_wf()
    wf.path = str(tmp_path / "wdir")
    os.makedirs(wf.path)
    wf.snakefile_path = str(tmp_path / "Snakefile")
    with open(wf.snakefile_path, "w", encoding="utf-8") as f:
        f.write("rule all:\n")
    wf.jobs = [job]

    with mock.patch.dict(os.environ, {"HOME": str(home)}), \
         mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli:
        wf.upload_file()

    uploaded = {c.args[2] for c in cli.upload_file.call_args_list}
    assert "imp1234567/stageout/mass.png" not in uploaded
    assert "Snakefile" in uploaded
    assert "reana.yaml" in uploaded


def test_upload_failure_logs_exception_detail():
    """A failed upload logs the exception type and message instead of a
    bare 'Failed to upload the files', so the workflow log is diagnosable."""
    wf = _make_wf()
    wf.jobs = []
    wf.create_workflow = lambda: None
    wf.set_workflow_status = lambda status: None
    messages = []

    def collect_log(msg):
        messages.append(msg)

    wf.logger = collect_log

    def boom():
        raise RuntimeError("boom")

    wf.upload_file = boom

    import pytest
    with pytest.raises(RuntimeError):
        wf._execute_backend()

    assert any("Failed to upload the files" in m and "RuntimeError" in m
               and "boom" in m for m in messages)
