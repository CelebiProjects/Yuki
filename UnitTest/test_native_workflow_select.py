"""Tests for NativeWorkflow file selection and download helpers."""
import os
from unittest import mock

from Yuki.kernel.workflows import native as native_workflow


def _make_wf(tmp_path):
    wf = native_workflow.NativeWorkflow.__new__(native_workflow.NativeWorkflow)
    wf.local_exec_path = str(tmp_path / "exec")
    wf.project_uuid = "proj-1"
    wf.machine_id = "runner-1"
    wf.logger = lambda *a, **k: None
    src = tmp_path / "exec" / "imp7654321" / "stageout"
    src.mkdir(parents=True)
    (src / "mass.png").write_bytes(b"img")
    (src / "ntuple.root").write_bytes(b"data")
    # nested directory
    (src / "plots").mkdir()
    (src / "plots" / "fit.png").write_bytes(b"img2")
    (src / "data").mkdir()
    (src / "data" / "ntuple.root").write_bytes(b"data2")
    return wf


def test_list_runner_files_native(tmp_path):
    """list_runner_files walks the native stageout with relative names."""
    wf = _make_wf(tmp_path)
    out = {f["name"]: f["size"] for f in wf.list_runner_files("7654321xyz", "stageout")}
    assert out["mass.png"] == 3
    assert "ntuple.root" in out
    assert out["plots/fit.png"] == 4
    assert out["data/ntuple.root"] == 5


def test_download_selected_native_nested(tmp_path):
    """download_selected copies only files matching the predicate."""
    wf = _make_wf(tmp_path)
    with mock.patch.dict(os.environ, {"HOME": str(tmp_path / "home")}):
        report = wf.download_selected("7654321xyz",
                                       native_workflow.file_types.make_predicate("*.png"),
                                       "stageout")
        dst = (tmp_path / "home" / ".Yuki" / "Storage" / "proj-1"
               / "7654321xyz" / "runner-1" / "stageout")
        assert (dst / "mass.png").exists()
        assert (dst / "plots" / "fit.png").exists()
        assert not (dst / "ntuple.root").exists()
        assert not (dst / "data" / "ntuple.root").exists()
        assert "plots/fit.png" in report["collected"]


def test_download_outputs_native_preserves_subdirs(tmp_path):
    """download_outputs keeps nested subdirectories in the target."""
    wf = _make_wf(tmp_path)
    with mock.patch.dict(os.environ, {"HOME": str(tmp_path / "home")}):
        report = wf.download_outputs("7654321xyz")
        dst = (tmp_path / "home" / ".Yuki" / "Storage" / "proj-1"
               / "7654321xyz" / "runner-1" / "stageout")
        assert (dst / "mass.png").exists()
        assert (dst / "plots" / "fit.png").exists()
        assert (dst / "data" / "ntuple.root").exists()
        assert "data/ntuple.root" in report["collected"]
        assert (dst.parent / "stageout.downloaded").exists()


def test_download_selected_native_only_plots(tmp_path):
    """download_selected with is_plot downloads images only."""
    wf = _make_wf(tmp_path)
    with mock.patch.dict(os.environ, {"HOME": str(tmp_path / "home")}):
        wf.download_selected("7654321xyz", native_workflow.file_types.is_plot, "stageout")
        dst = (tmp_path / "home" / ".Yuki" / "Storage" / "proj-1"
               / "7654321xyz" / "runner-1" / "stageout")
        assert (dst / "mass.png").exists()
        assert not (dst / "ntuple.root").exists()
        assert not (dst.parent / "stageout.downloaded").exists()
