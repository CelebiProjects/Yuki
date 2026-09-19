"""Tests for impression storage selection helpers."""
# pylint: disable=protected-access
import json
import os
import threading
import time
from unittest import mock

import pytest

from Yuki.kernel.status_constants import CODA


def _storage(tmp_path):
    from Yuki.kernel import impression_storage as ims
    s = ims.ImpressionStorage.__new__(ims.ImpressionStorage)
    s.project_uuid = "proj-1"
    s.impression = "imp7"
    s.job_path = str(tmp_path / "job")
    s.runners = ["runner"]
    s.runners_id = {"runner": "runner-1"}
    return s, ims


def _finished_job(wid="wf-1"):
    job = mock.Mock()
    job.status.return_value = CODA
    job.workflow_id.return_value = wid
    return job


def _write_cache(machine_dir, wid, files, error=None):
    machine_dir.mkdir(parents=True, exist_ok=True)
    payload = {"workflow_id": wid, "files": files,
               "stamp": "2026-08-30 16:00"}
    if error:
        payload["error"] = error
    (machine_dir / "stageout.filelist.json").write_text(json.dumps(payload))


def _read_cache(machine_dir):
    return json.loads(
        (machine_dir / "stageout.filelist.json").read_text())


def test_collect_light_downloads_plots_and_logs(tmp_path):
    """collect() pulls plots and logs for a finished job."""
    s, ims = _storage(tmp_path)
    job = mock.Mock()
    job.status.return_value = CODA
    wf = mock.Mock()
    wf.download_selected.return_value = {"collected": [], "skipped": [], "failed": []}
    wf.download_logs.return_value = {"collected": [], "skipped": [], "failed": []}
    s._get_runner_contexts = lambda: [("runner", job, wf)]
    report = s.collect()
    # download_selected called with the is_plot predicate on stageout
    assert wf.download_selected.call_args.args[1] is ims.file_types.is_plot
    wf.download_logs.assert_called_once()
    wf.download_outputs.assert_not_called()
    assert "runner" in report


def test_collect_files_uses_predicate(tmp_path):
    """collect_files filters downloads by the given predicate."""
    s, _ims = _storage(tmp_path)
    job = mock.Mock()
    job.status.return_value = CODA
    wf = mock.Mock()
    s._get_runner_contexts = lambda: [("runner", job, wf)]
    s.collect_files("stageout", "*.root")
    pred = wf.download_selected.call_args.args[1]
    assert pred("ntuple.root") and not pred("mass.png")


def test_file_status_merges(tmp_path):
    """file_status merges the saved runner listing with local downloads."""
    s, _ims = _storage(tmp_path)
    stageout = tmp_path / "job" / "runner-1" / "stageout"
    stageout.mkdir(parents=True)
    (stageout / "mass.png").write_bytes(b"img")        # downloaded
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [
        {"name": "mass.png", "size": 3},
        {"name": "ntuple.root", "size": 99},
    ])
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"),
                                       mock.Mock())]
    rows = {r["name"]: r for r in s.file_status("stageout")}
    assert rows["mass.png"]["in_yuki"] and rows["mass.png"]["type"] == "plot"
    assert rows["ntuple.root"]["in_runner"] and not rows["ntuple.root"]["in_yuki"]
    assert rows["ntuple.root"]["type"] == "data"


def test_file_status_merges_nested_files(tmp_path):
    """file_status merges nested files from the saved listing and storage."""
    s, _ims = _storage(tmp_path)
    stageout = tmp_path / "job" / "runner-1" / "stageout"
    stageout.mkdir(parents=True)
    (stageout / "mass.png").write_bytes(b"img")
    (stageout / "data").mkdir()
    (stageout / "data" / "ntuple.root").write_bytes(b"data")
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [
        {"name": "mass.png", "size": 3},
        {"name": "data/ntuple.root", "size": 4},
    ])
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"),
                                       mock.Mock())]
    rows = {r["name"]: r for r in s.file_status("stageout")}
    assert rows["mass.png"]["in_yuki"] and rows["mass.png"]["type"] == "plot"
    assert rows["data/ntuple.root"]["in_yuki"]
    assert rows["data/ntuple.root"]["type"] == "data"


def test_file_status_missing_listing(tmp_path):
    """Without a saved listing the status shows no files and an info note."""
    s, _ims = _storage(tmp_path)
    wf = mock.Mock()
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"), wf)]

    detail = s.file_status("stageout", detailed=True)

    wf.list_runner_files.assert_not_called()                 # never live
    assert detail["files"] == []
    note = detail["notes"][0]
    assert note["level"] == "info" and "no stageout listing yet" in note["message"]


def test_file_status_ignores_listing_from_other_workflow(tmp_path):
    """A listing written under another workflow id is ignored after a re-run."""
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [{"name": "old.png", "size": 1}])
    wf = mock.Mock()
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-2"), wf)]

    detail = s.file_status("stageout", detailed=True)

    assert detail["files"] == []
    assert any("no stageout listing yet" in n["message"] for n in detail["notes"])
    assert not wf.list_runner_files.called


def test_file_status_detailed_returns_files_and_notes(tmp_path):
    """file_status(detailed=True) returns {files, notes}; default stays a list."""
    s, _ims = _storage(tmp_path)
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"),
                                       mock.Mock())]

    detail = s.file_status("stageout", detailed=True)

    assert isinstance(detail, dict)
    assert detail["files"] == []
    assert any("no stageout" in n["message"] for n in detail["notes"])
    assert s.file_status("stageout") == []               # default mode unchanged


def test_file_status_reports_persisted_refresh_error(tmp_path):
    """A persisted refresh failure surfaces as a warning naming the error."""
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [{"name": "old.png", "size": 3}],
                 error="ConnectionError: boom")
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"),
                                       mock.Mock())]

    detail = s.file_status("stageout", detailed=True)

    assert {r["name"] for r in detail["files"]} == {"old.png"}
    notes = detail["notes"]
    assert any(n["level"] == "warning" and "refresh failed" in n["message"]
               and "ConnectionError" in n["message"] and n["runner"] == "runner"
               for n in notes)


def test_file_status_annotates_listing_with_stamp(tmp_path):
    """A saved listing carries an info note with its stamp."""
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [{"name": "mass.png", "size": 3}])
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"),
                                       mock.Mock())]

    detail = s.file_status("stageout", detailed=True)

    notes = detail["notes"]
    assert any(n["level"] == "info" and "listing from" in n["message"]
               for n in notes)
    assert {r["name"] for r in detail["files"]} == {"mass.png"}


def test_listing_timestamp_has_explicit_offset(tmp_path):
    """Clients receive an unambiguous instant, independent of server timezone."""
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [])
    path = machine_dir / "stageout.filelist.json"
    payload = json.loads(path.read_text())
    payload["stamp"] = "2026-09-07T22:00:00+08:00"
    path.write_text(json.dumps(payload))
    _files, note = s._runner_files(_finished_job("wf-1"), "stageout", str(machine_dir))
    assert note["listing_time"] == "2026-09-07T14:00+00:00"
    assert note["listing_time"] in note["message"]


def test_new_listing_stamp_is_utc(tmp_path):
    """New caches preserve timezone information instead of naive wall time."""
    from datetime import datetime, timedelta
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    s._write_filelist(str(machine_dir), "stageout", "wf-1", [])
    payload = json.loads((machine_dir / "stageout.filelist.json").read_text())
    assert datetime.fromisoformat(payload["stamp"]).utcoffset() == timedelta(0)


def test_file_status_empty_listing_notes_absence(tmp_path):
    """An empty saved listing says there are no files (with the stamp)."""
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [])
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"),
                                       mock.Mock())]

    detail = s.file_status("stageout", detailed=True)

    assert detail["files"] == []
    assert any("no stageout files on the runner" in n["message"]
               and "listing from" in n["message"] for n in detail["notes"])


def test_refresh_filelists_pre_execution_writes_empty_locally(tmp_path):
    """Pre-execution status writes empty listings without listing the runner."""
    s, _ims = _storage(tmp_path)
    wf = mock.Mock()
    wf.uuid = "wf-1"
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"), wf)]

    s.refresh_filelists(wf, pre_execution=True)

    wf.list_runner_files.assert_not_called()
    payload = _read_cache(tmp_path / "job" / "runner-1")
    assert payload["files"] == [] and payload["workflow_id"] == "wf-1"
    assert "stamp" in payload and "error" not in payload
    assert os.path.isfile(tmp_path / "job" / "runner-1" / "logs.filelist.json")


def test_refresh_filelists_lists_live_when_running(tmp_path):
    """A running refresh lists the runner live and saves both listings."""
    s, _ims = _storage(tmp_path)
    wf = mock.Mock()
    wf.uuid = "wf-1"
    wf.list_runner_files.return_value = [{"name": "partial.root", "size": 1}]
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"), wf)]

    s.refresh_filelists(wf, pre_execution=False)
    s.refresh_filelists(wf, pre_execution=False)

    assert wf.list_runner_files.call_count == 4      # stageout + logs, twice
    payload = _read_cache(tmp_path / "job" / "runner-1")
    assert payload["files"][0]["name"] == "partial.root"
    assert "error" not in payload


def test_refresh_filelists_records_failure(tmp_path):
    """A failed live listing keeps the previous files and records the error."""
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [{"name": "old.png", "size": 3}])
    wf = mock.Mock()
    wf.uuid = "wf-1"
    wf.list_runner_files.side_effect = ConnectionError("boom")
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"), wf)]

    s.refresh_filelists(wf, pre_execution=False)

    payload = _read_cache(machine_dir)
    assert payload["files"][0]["name"] == "old.png"  # previous listing kept
    assert "ConnectionError" in payload["error"]


def test_refresh_filelists_skips_unknown_workflow(tmp_path):
    """A workflow not matching this impression's runner contexts is a no-op."""
    s, _ims = _storage(tmp_path)
    wf = mock.Mock()
    wf.uuid = "wf-other"
    context = mock.Mock()
    context.uuid = "wf-context"
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"),
                                       context)]

    s.refresh_filelists(wf, pre_execution=False)

    wf.list_runner_files.assert_not_called()
    assert not os.path.exists(tmp_path / "job" / "runner-1")


def test_refresh_job_filelists_dispatches_per_job(tmp_path):
    """refresh_job_filelists refreshes each non-algorithm job's storage."""
    s, _ims = _storage(tmp_path)
    s.refresh_filelists = mock.Mock()
    wf = mock.Mock()
    wf.uuid = "wf-1"
    wf.jobs = [mock.Mock(job_type=lambda: "task", uuid="imp-a", is_input=False),
               mock.Mock(job_type=lambda: "algorithm", uuid="imp-b")]

    with mock.patch.object(_ims, "ImpressionStorage", return_value=s):
        _ims.refresh_job_filelists("proj-1", wf, "running")
        s.refresh_filelists.assert_called_once_with(wf, False)

        _ims.refresh_job_filelists("proj-1", wf, "created")
        s.refresh_filelists.assert_called_with(wf, True)   # pre-execution status


def test_refresh_job_filelists_swallows_job_errors(tmp_path):
    """A raising storage refresh never fails the status update."""
    s, _ims = _storage(tmp_path)
    s.refresh_filelists = mock.Mock(side_effect=RuntimeError("boom"))
    wf = mock.Mock()
    wf.uuid = "wf-1"
    wf.jobs = [mock.Mock(job_type=lambda: "task", uuid="imp-a", is_input=False)]

    with mock.patch.object(_ims, "ImpressionStorage", return_value=s):
        _ims.refresh_job_filelists("proj-1", wf, "running")   # must not raise


def test_force_refresh_filelists_lists_live_for_each_kind(tmp_path):
    """force_refresh_filelists re-lists stageout and logs for every context."""
    s, _ims = _storage(tmp_path)
    wf = mock.Mock()
    wf.list_runner_files.return_value = [{"name": "tmva.root", "size": 10}]
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"), wf)]

    report = s.force_refresh_filelists()

    assert wf.list_runner_files.call_args_list[0].args == ("imp7", "stageout")
    assert wf.list_runner_files.call_args_list[1].args == ("imp7", "logs")
    machine_dir = tmp_path / "job" / "runner-1"
    payload = _read_cache(machine_dir)
    assert payload["files"][0]["name"] == "tmva.root"
    assert payload["workflow_id"] == "wf-1"
    assert "stamp" in payload and "error" not in payload
    logs_payload = json.loads(
        (machine_dir / "logs.filelist.json").read_text())
    assert logs_payload["files"][0]["name"] == "tmva.root"
    assert report["runner"]["stageout"] == {"files": 1, "error": None}


def test_force_refresh_filelists_keeps_previous_on_failure(tmp_path):
    """A failed force refresh keeps the previous listing and records the error."""
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [{"name": "old.root", "size": 3}])
    wf = mock.Mock()
    wf.list_runner_files.side_effect = ConnectionError("boom")
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"), wf)]

    report = s.force_refresh_filelists()

    payload = _read_cache(machine_dir)
    assert payload["files"][0]["name"] == "old.root"   # previous kept
    assert "ConnectionError" in payload["error"]
    assert "ConnectionError" in report["runner"]["stageout"]["error"]


def test_force_refresh_filelists_keeps_previous_on_empty_same_workflow(tmp_path):
    """An empty listing keeps a previous listing of the same workflow id."""
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-1", [{"name": "old.root", "size": 3}])
    wf = mock.Mock()
    wf.list_runner_files.return_value = []
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"), wf)]

    s.force_refresh_filelists()

    payload = _read_cache(machine_dir)
    assert payload["files"][0]["name"] == "old.root"   # previous kept
    assert "no files" in payload["error"]


def test_force_refresh_filelists_writes_empty_for_new_workflow(tmp_path):
    """An empty listing with no matching previous listing writes empty."""
    s, _ims = _storage(tmp_path)
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "wf-old", [{"name": "old.root", "size": 3}])
    wf = mock.Mock()
    wf.list_runner_files.return_value = []
    s._get_runner_contexts = lambda: [("runner", _finished_job("wf-1"), wf)]

    s.force_refresh_filelists()

    payload = _read_cache(machine_dir)
    assert payload["files"] == [] and payload["workflow_id"] == "wf-1"
    assert "error" not in payload


def test_file_status_scopes_notes_and_files_to_requested_runner(tmp_path):
    """A runner-specific status must not include another runner's saved listing."""
    s, _ims = _storage(tmp_path)
    s.runners_id = {"cern": "cern-id", "other": "other-id"}
    s._get_runner_contexts = lambda: [
        ("cern", _finished_job("wf-cern"), mock.Mock()),
        ("other", _finished_job("wf-other"), mock.Mock()),
    ]
    _write_cache(tmp_path / "job" / "cern-id", "wf-cern", [])
    _write_cache(tmp_path / "job" / "other-id", "wf-other", [])
    other_download = tmp_path / "job" / "other-id" / "stageout"
    other_download.mkdir()
    (other_download / "other.root").write_bytes(b"other")

    for machine in ("cern", "cern-id"):
        detail = s.file_status(detailed=True, machine=machine)
        assert detail["files"] == []
        assert len(detail["notes"]) == 1
        assert detail["notes"][0]["runner"] == "cern"
    for machine in (None, "none"):
        detail = s.file_status(detailed=True, machine=machine)
        assert len(detail["notes"]) == 2
        assert detail["files"][0]["name"] == "other.root"
    assert s.file_status(detailed=True, machine="unknown") == {"files": [], "notes": []}
    assert s.file_status(machine="cern") == []
    _write_cache(tmp_path / "job" / "other-id", "wf-other",
                 [{"name": "remote-other.root", "size": 15}])
    assert s.file_status(machine="cern") == []
    assert {row["name"] for row in s.file_status(machine="other")} == {
        "other.root", "remote-other.root"}


def test_remote_listing_respects_requested_runner(tmp_path):
    """Remote-hosted files are returned only for their host or an unscoped request."""
    s, _ims = _storage(tmp_path)
    s._get_runner_contexts = lambda: []
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "remote-data", [{"name": "data.root", "size": 10}])
    (tmp_path / "job" / "remote.json").write_text(json.dumps({
        "host_runner_id": "runner-1", "remote_path": "/managed/imp7",
    }))
    for machine in ("runner", "runner-1", None, "none"):
        detail = s.file_status(detailed=True, machine=machine)
        assert detail["files"][0]["name"] == "data.root"
    assert s.file_status(detailed=True, machine="other") == {"files": [], "notes": []}


def _registered_storage(tmp_path, status):
    storage, ims = _storage(tmp_path)
    storage._get_runner_contexts = lambda: []
    job = tmp_path / "job"
    job.mkdir(exist_ok=True)
    (job / "remote.json").write_text(json.dumps({
        "host_runner_id": "runner-1", "remote_path": "/managed/imp7"}))
    (job / "status.json").write_text(json.dumps({"status": status}))
    return storage, ims


def test_copying_status_shows_growing_files_without_freezing_partial_cache(tmp_path):
    """Show live destination files and refresh sizes as copying proceeds."""
    from Yuki.kernel.registration_progress import ProgressCache

    storage, ims = _registered_storage(tmp_path, "running")
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "remote-data",
                 [{"name": "partial.root", "size": 10}])
    snapshots = [
        [{"name": "first.root", "size": 100}],
        [{"name": "first.root", "size": 200}, {"name": "second.root", "size": 10}],
        [{"name": "first.root", "size": 200}, {"name": "second.root", "size": 300}],
    ]
    with mock.patch.object(ims, "remote_listing_cache", ProgressCache(ttl=0, wait=1)), \
            mock.patch.object(ims.remote_data_ops, "list_managed_files",
                              side_effect=snapshots) as scan:
        first = storage.file_status(detailed=True)
        assert first["files"][0]["name"] == "first.root"
        assert first["files"][0]["size"] == 100
        assert "incomplete" in first["notes"][0]["message"]
        second = storage.file_status(detailed=True)
        assert [row["size"] for row in second["files"]] == [200, 10]
        # Intermediate observations never replace the on-disk final listing.
        assert _read_cache(machine_dir)["files"] == [{"name": "partial.root", "size": 10}]
        (tmp_path / "job" / "status.json").write_text(json.dumps({"status": "archived"}))
        final = storage.file_status(detailed=True)
        assert [row["size"] for row in final["files"]] == [200, 300]
        assert _read_cache(machine_dir)["registration_status"] == "archived"
        assert storage.file_status() == final["files"]
    assert scan.call_count == 3


@pytest.mark.parametrize("registration_status", ["running", "archived", "failed"])
def test_file_status_endpoint_returns_while_remote_scan_is_blocked(  # pylint: disable=too-many-locals
        tmp_path, monkeypatch, registration_status):
    """Live and final file lists must not block the HTTP status request."""
    from flask import Flask
    from Yuki.kernel.registration_progress import ProgressCache
    from Yuki.server.routes import execution

    storage, ims = _registered_storage(tmp_path, registration_status)
    cache = ProgressCache(wait=0.01)
    monkeypatch.setattr(ims, "remote_listing_cache", cache)
    entered, release = threading.Event(), threading.Event()
    calls = []

    def scan(*args):
        calls.append(args)
        entered.set()
        release.wait(5)
        return [{"name": "complete.root", "size": 100}]

    monkeypatch.setattr(ims.remote_data_ops, "list_managed_files", scan)
    monkeypatch.setattr(execution, "ImpressionStorage", lambda *_args: storage)
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    client = app.test_client()
    try:
        start = time.monotonic()
        for _ in range(2):
            response = client.get("/file-status/proj/imp7/none?detailed=1")
            assert response.status_code == 200
            assert response.json["files"] == []
            assert "refreshing" in response.json["notes"][0]["message"]
        assert time.monotonic() - start < 1
        assert entered.wait(1)
        assert len(calls) == 1
    finally:
        release.set()
        cache.wait = 1
        cache.get((storage.job_path, "runner-1", "/managed/imp7", registration_status), scan)
    response = client.get("/file-status/proj/imp7/none?detailed=1")
    assert response.json["files"][0]["name"] == "complete.root"
    assert len(calls) == 1


def test_archived_listing_replaces_old_partial_cache_and_caches_empty(tmp_path):
    """Legacy partial listings are refreshed; an empty final listing is cached."""
    storage, ims = _registered_storage(tmp_path, "archived")
    machine_dir = tmp_path / "job" / "runner-1"
    _write_cache(machine_dir, "remote-data", [{"name": "partial.root", "size": 10}])
    from Yuki.kernel.registration_progress import ProgressCache
    with mock.patch.object(ims, "remote_listing_cache", ProgressCache(wait=1)), \
            mock.patch.object(ims.remote_data_ops, "list_managed_files", return_value=[]) as scan:
        assert storage.file_status() == []
        assert storage.file_status() == []
    scan.assert_called_once()
    cached = _read_cache(machine_dir)
    assert cached["registration_status"] == "archived"
    assert cached["files"] == []
