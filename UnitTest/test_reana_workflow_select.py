"""Tests for ReanaWorkflow file selection and download helpers."""
import os
from unittest import mock

import pytest

from Yuki.kernel.workflows import reana as reana_workflow


def _make_wf():
    wf = reana_workflow.ReanaWorkflow.__new__(reana_workflow.ReanaWorkflow)
    wf.machine_id = "runner-1"
    wf.project_uuid = "proj-1"
    wf.logger = lambda *a, **k: None
    wf.get_name = lambda: "wfname"
    wf.get_access_token = lambda mid: "tok"
    wf.set_environment = lambda mid: None
    return wf


def test_list_runner_files_strips_prefix_and_keeps_size():
    """REANA listing prefixes are stripped, sizes preserved."""
    wf = _make_wf()
    fake = [
        {"name": "imp1234567/stageout/mass.png", "size": 10},
        {"name": "imp1234567/stageout/ntuple.root", "size": 999},
        {"name": "imp1234567/stageout/plots/fit.png", "size": 20},
    ]
    with mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli:
        cli.list_files.return_value = fake
        out = wf.list_runner_files("1234567abc", "stageout")
    assert {"name": "mass.png", "size": 10} in out
    assert {"name": "ntuple.root", "size": 999} in out
    assert {"name": "plots/fit.png", "size": 20} in out


def test_download_outputs_creates_subdirectories(tmp_path):
    """download_outputs recreates nested stageout directories locally."""
    wf = _make_wf()
    home = tmp_path
    storage = home / ".Yuki" / "Storage" / "proj-1" / "imp7" / "runner-1"
    fake = [
        {"name": "impimp7/stageout/mass.png", "size": 3},
        {"name": "impimp7/stageout/data/ntuple.root", "size": 5},
    ]
    with mock.patch.dict(os.environ, {"HOME": str(home)}), \
         mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli:
        cli.list_files.return_value = fake
        cli.download_file.return_value = (b"data",)
        report = wf.download_outputs("imp7")
        stageout = storage / "stageout"
        assert (stageout / "mass.png").exists()
        assert (stageout / "data" / "ntuple.root").exists()
        assert "data/ntuple.root" in report["collected"]
        assert (storage / "stageout.downloaded").exists()


def test_download_selected_matches_relative_path(tmp_path):
    """download_selected matches predicates against relative paths."""
    wf = _make_wf()
    home = tmp_path
    storage = home / ".Yuki" / "Storage" / "proj-1" / "imp7" / "runner-1" / "stageout"
    storage.mkdir(parents=True)
    fake = [
        {"name": "impimp7/stageout/mass.png", "size": 3},
        {"name": "impimp7/stageout/plots/fit.png", "size": 4},
    ]
    with mock.patch.dict(os.environ, {"HOME": str(home)}), \
         mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli:
        cli.list_files.return_value = fake
        cli.download_file.return_value = (b"data",)
        wf.download_selected("imp7",
                             reana_workflow.file_types.make_predicate("plots/*.png"),
                             "stageout")
        assert (storage / "plots" / "fit.png").exists()
        assert not (storage / "mass.png").exists()


def test_list_runner_files_normalizes_reana_size_dict():
    """REANA reports size as {"raw": int, "human_readable": str}; it must be
    flattened to int bytes so the file_status JSON contract (size: int) holds
    and the client's _human_size() does not choke on a dict."""
    wf = _make_wf()
    fake = [
        {"name": "imp1234567/stageout/mass.png",
         "size": {"raw": 2048, "human_readable": "2 KiB"}},
        {"name": "imp1234567/stageout/ntuple.root",
         "size": {"raw": 1048576, "human_readable": "1 MiB"}},
    ]
    with mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli:
        cli.list_files.return_value = fake
        out = wf.list_runner_files("1234567abc", "stageout")
    assert {"name": "mass.png", "size": 2048} in out
    assert {"name": "ntuple.root", "size": 1048576} in out
    assert all(isinstance(f["size"], int) for f in out)


def test_list_runner_files_retries_transient_then_succeeds():
    """A transient list_files failure (e.g. SSL EOF) is retried; on recovery
    the files are returned rather than degrading to an empty listing."""
    wf = _make_wf()
    fake = [{"name": "imp1234567/stageout/mass.png",
             "size": {"raw": 5, "human_readable": "5 B"}}]
    calls = {"n": 0}

    def flaky(*_a, **_k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("[SSL: UNEXPECTED_EOF_WHILE_READING]")
        return fake

    with mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli, \
         mock.patch.object(reana_workflow.time, "sleep", lambda *_a: None):
        cli.list_files.side_effect = flaky
        out = wf.list_runner_files("1234567abc", "stageout")
    assert calls["n"] == 2                       # failed once, retried, succeeded
    assert out == [{"name": "mass.png", "size": 5}]


def test_list_runner_files_raises_after_all_retries_fail():
    """If every attempt fails (runner unreachable), the error propagates after
    the bounded retries, so callers can tell failure apart from an empty
    listing ([] means 'listed fine, no files') and report a diagnostic."""
    wf = _make_wf()
    with mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli, \
         mock.patch.object(reana_workflow.time, "sleep", lambda *_a: None):
        cli.list_files.side_effect = RuntimeError("[SSL: UNEXPECTED_EOF_WHILE_READING]")
        with pytest.raises(RuntimeError):
            wf.list_runner_files("1234567abc", "stageout")
    assert cli.list_files.call_count == 3        # bounded retry, then give up


def test_download_selected_only_matching_and_skips_existing(tmp_path):
    """download_selected downloads only matches and skips existing files."""
    wf = _make_wf()
    home = tmp_path
    storage = home / ".Yuki" / "Storage" / "proj-1" / "imp7" / "runner-1" / "stageout"
    storage.mkdir(parents=True)
    (storage / "already.png").write_bytes(b"old")     # pre-existing -> skip
    fake = [
        {"name": "impimp7000/stageout/already.png", "size": 3},
        {"name": "impimp7000/stageout/new.png", "size": 4},
        {"name": "impimp7000/stageout/ntuple.root", "size": 5},
    ]
    with mock.patch.dict(os.environ, {"HOME": str(home)}), \
         mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli:
        cli.list_files.return_value = fake
        cli.download_file.return_value = (b"data",)
        wf.download_selected("imp7", reana_workflow.file_types.is_plot, "stageout")
        downloaded = {c.args[1] for c in cli.download_file.call_args_list}
    assert "impimp7000/stageout/new.png" in downloaded      # matched plot, missing
    assert "impimp7000/stageout/already.png" not in downloaded   # skipped (exists)
    assert "impimp7000/stageout/ntuple.root" not in downloaded   # not a plot
    assert not (storage / "stageout.downloaded").exists()        # no marker on partial


def test_download_logs_refresh_bypasses_collected_marker(tmp_path):
    """refresh=True re-downloads logs even when logs.downloaded exists."""
    wf = _make_wf()
    home = tmp_path
    storage = home / ".Yuki" / "Storage" / "proj-1" / "imp7" / "runner-1"
    storage.mkdir(parents=True)
    (storage / "logs.downloaded").write_text("")
    fake = [{"name": "impimp7/logs/celebi_user_step0.log", "size": 5}]
    with mock.patch.dict(os.environ, {"HOME": str(home)}), \
         mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli:
        cli.list_files.return_value = fake
        cli.download_file.return_value = (b"grown",)
        report = wf.download_logs("imp7", refresh=True)
    assert "celebi_user_step0.log" in report["collected"]


def test_update_workflow_status_terminal_transition_records(tmp_path):
    """The status write that first observes finished records the registry."""
    wf = _make_wf()
    wf.uuid = "wf-1"
    wf.path = str(tmp_path)
    wf.jobs = []
    with mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli, \
         mock.patch("Yuki.kernel.storage.impressions."
                    "refresh_workflow_distributions") as refresh, \
         mock.patch("Yuki.kernel.storage.impressions."
                    "refresh_job_filelists") as refresh_files:
        cli.get_workflow_status.return_value = {"status": "finished",
                                                "logs": "{}"}
        wf.update_workflow_status()
    refresh.assert_called_once_with("proj-1", wf, "finished")
    refresh_files.assert_called_once_with("proj-1", wf, "finished", True)


def test_update_workflow_status_repeated_terminal_poll_is_noop(tmp_path):
    """An already terminal workflow is never queried or refreshed again."""
    wf = _make_wf()
    wf.uuid = "wf-1"
    wf.path = str(tmp_path)
    wf.jobs = []
    (tmp_path / "results.json").write_text(
        '{"results": {"status": "finished"}}')
    with mock.patch.object(reana_workflow, "REANA_AVAILABLE", True), \
         mock.patch.object(reana_workflow, "client") as cli, \
         mock.patch("Yuki.kernel.storage.impressions."
                    "refresh_workflow_distributions") as refresh, \
         mock.patch("Yuki.kernel.storage.impressions."
                    "refresh_job_filelists") as refresh_files:
        cli.get_workflow_status.return_value = {"status": "finished",
                                                "logs": "{}"}
        wf.update_workflow_status()
    refresh.assert_not_called()
    refresh_files.assert_not_called()
    cli.get_workflow_status.assert_not_called()
