"""Regressions for REANA listings containing both directories and child files."""
import io
import json
import zipfile
from unittest import mock

import pytest

from Yuki.kernel.storage.reana_download import store_download
from Yuki.kernel.workflows import reana as reana_workflow
from UnitTest.test_reana_workflow_select import _make_wf


def archive_bytes(names):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name in names:
            archive.writestr(name, b"finished\n")
    return stream.getvalue()


@pytest.mark.parametrize("kind", ["logs", "stageout"])
def test_nested_download_skips_directories_and_repairs_old_zip(tmp_path, monkeypatch, kind):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    wf = _make_wf()
    root = tmp_path / "Storage/proj-1/abcdefg000/runner-1" / kind
    root.mkdir(parents=True)
    prefix = f"impabcdefg/{kind}/"
    (root / "0").write_bytes(archive_bytes([prefix + "0/status"]))
    listing = [{"name": prefix + "0"}, {"name": prefix + "0/status"}]
    with mock.patch.object(reana_workflow, "client") as client:
        client.list_files.return_value = listing
        client.download_file.return_value = (b"finished\n", "status", False)
        report = (wf.download_logs("abcdefg000", refresh=True) if kind == "logs"
                  else wf.download_outputs("abcdefg000"))
    assert not report["failed"]
    assert (root / "0/status").read_text() == "finished\n"
    assert (root / "0.reana-directory.zip").is_file()
    assert [call.args[1] for call in client.download_file.call_args_list] == [
        prefix + "0/status"]


def test_directory_zip_is_unpacked_under_requested_root(tmp_path):
    prefix = "impabcdefg/logs/"
    report = store_download((archive_bytes([prefix + "0/status"]), "logs.zip", True),
                            tmp_path / "0", tmp_path, prefix)
    assert report == ["0/status"]
    assert (tmp_path / "0/status").read_bytes() == b"finished\n"


@pytest.mark.parametrize("bad", ["../escape", "/absolute", "other/secret"])
def test_directory_zip_rejects_out_of_scope_members_before_writing(tmp_path, bad):
    payload = archive_bytes(["impabcdefg/logs/0/status", bad])
    with pytest.raises(ValueError):
        store_download((payload, "logs.zip", True), tmp_path / "0", tmp_path,
                       "impabcdefg/logs/")
    assert not (tmp_path / "0/status").exists()


def test_failed_download_does_not_set_complete_marker(tmp_path, monkeypatch):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    wf = _make_wf()
    with mock.patch.object(reana_workflow, "client") as client:
        client.list_files.return_value = [{"name": "impabcdefg/logs/0/status"}]
        client.download_file.side_effect = RuntimeError("temporary failure")
        report = wf.download_logs("abcdefg000", refresh=True)
    assert report["failed"]
    assert not (tmp_path / "Storage/proj-1/abcdefg000/runner-1/logs.downloaded").exists()


def test_subtask_progress_distinguishes_compute_from_aggregate(tmp_path):
    from UnitTest.test_subtasks import make_job
    job = make_job(tmp_path, "a", {"subtasks": 2})
    logs = {
        str(index): {"job_name": "subtaskaaaaaaa", "cmd": f"test {index} -ge 0",
                     "status": "finished", "started_at": "2026-11-06T14:38:05"}
        for index in range(2)
    }
    (tmp_path / "log.json").write_text(json.dumps({"logs": logs}))
    assert job._subtask_progress_detail(str(tmp_path)) == (
        "REANA subtasks: 2/2 finished, 0 running, 0 failed; aggregate/cache step: waiting")
    logs["1"]["status"] = "failed"
    (tmp_path / "log.json").write_text(json.dumps({"logs": logs}))
    assert "1 failed (indices: 1)" in job._subtask_progress_detail(str(tmp_path))
