"""Tests for ReanaBooker upload-mode selection."""
# pylint: disable=protected-access
import os
from unittest import mock

from Yuki.server.services import reana_booking as reana_booker


def _booker():
    b = reana_booker.ReanaBooker.__new__(reana_booker.ReanaBooker)
    b.access_token = "tok"
    b._notify = lambda *a, **k: None
    return b


def _layout(tmp_path):
    base = tmp_path / ".Yuki" / "Storage" / "proj-1" / "imp-abc" / "runner-1"
    (base / "stageout").mkdir(parents=True)
    (base / "logs").mkdir(parents=True)
    (base / "stageout" / "mass.png").write_bytes(b"img")
    (base / "stageout" / "ntuple.root").write_bytes(b"data")
    (base / "stageout" / "plots").mkdir()
    (base / "stageout" / "plots" / "fit.png").write_bytes(b"img2")
    (base / "logs" / "celebi.stdout").write_bytes(b"log")
    return base


def _run(booker, tmp_path, mode):
    cfg = tmp_path / "proj" / ".celebi"
    cfg.mkdir(parents=True)
    (cfg / "config.json").write_text('{"project_uuid": "proj-1"}')
    meta = {"objects": [{"impression": "imp-abc"}]}
    uploaded = []
    with mock.patch.dict(os.environ, {"YUKIDIR": str(tmp_path / ".Yuki")}), \
         mock.patch.object(reana_booker, "reana_client") as rc:
        rc.upload_file.side_effect = lambda **kw: uploaded.append(kw["file_name"])
        booker._upload_stageout_files("wf", str(tmp_path / "proj"), meta, upload_mode=mode)
    return uploaded


def test_default_plots_and_logs(tmp_path):
    """plots+logs mode uploads plots and logs but excludes data."""
    _layout(tmp_path)
    names = _run(_booker(), tmp_path, "plots+logs")
    assert any(n.endswith("stageout/mass.png") for n in names)
    assert any(n.endswith("logs/celebi.stdout") for n in names)
    assert not any(n.endswith("ntuple.root") for n in names)   # data excluded


def test_all_includes_data(tmp_path):
    """all mode uploads data files alongside plots and logs."""
    _layout(tmp_path)
    names = _run(_booker(), tmp_path, "all")
    assert any(n.endswith("stageout/ntuple.root") for n in names)


def test_nested_stageout_uploaded_with_relative_path(tmp_path):
    """Nested stageout files keep their relative paths when uploaded."""
    _layout(tmp_path)
    names = _run(_booker(), tmp_path, "plots+logs")
    assert "impression_data/imp-abc/stageout/plots/fit.png" in names
    assert "impression_data/imp-abc/stageout/mass.png" in names
    assert not any(n.endswith("stageout/ntuple.root") for n in names)


def test_filelist_manifest_uploaded(tmp_path):
    """The runner file manifest (stageout.filelist.json from status/collect) is
    uploaded alongside the selected files, so the booked record lists every
    output even when the large data is not uploaded (default plots+logs)."""
    base = _layout(tmp_path)
    (base / "stageout.filelist.json").write_text(
        '{"workflow_id": "wf-1", "files": ['
        '{"name": "mass.png", "size": 3}, {"name": "ntuple.root", "size": 99}]}')
    names = _run(_booker(), tmp_path, "plots+logs")
    assert "impression_data/imp-abc/stageout.filelist.json" in names   # manifest uploaded
    assert not any(n.endswith("ntuple.root") for n in names)           # data itself excluded


def test_filelist_absent_is_skipped(tmp_path):
    """No manifest in storage -> nothing extra uploaded, booking still works."""
    _layout(tmp_path)                                      # no .filelist.json written
    names = _run(_booker(), tmp_path, "plots+logs")
    assert not any(n.endswith("stageout.filelist.json") for n in names)


def test_book_project_default_uploads_plots(tmp_path):
    """Regression: a default book (plots+logs, no legacy --stageout) must run
    the output-upload step and upload plots from Yuki storage. The old
    `if stageout:` gate skipped the upload for every mode except 'all'."""
    b = _booker()
    b.server_url = "http://reana"
    # Stub the REANA-facing / project-walking collaborators so the test isolates
    # the upload gate; only _upload_stageout_files should drive reana upload_file.
    b._setup_env = lambda: None
    b._get_workflow = lambda name: None                       # force create path
    b._create_workflow = lambda name, project_path="": {"workflow_id": "wf-1"}
    b._download_workspace_file = lambda *a, **k: None
    b._clear_old_folders = lambda *a, **k: False
    b._build_repo_metadata = lambda project_path: {
        "objects": [{"impression": "imp-abc", "path": "t"}]}
    b._upload_files = lambda *a, **k: None
    b._upload_repo_yaml = lambda *a, **k: None

    _layout(tmp_path)
    proj = tmp_path / "proj"
    (proj / ".celebi").mkdir(parents=True)
    (proj / ".celebi" / "config.json").write_text('{"project_uuid": "proj-1"}')

    uploaded = []
    with mock.patch.dict(os.environ, {"YUKIDIR": str(tmp_path / ".Yuki")}), \
         mock.patch.object(reana_booker, "reana_client") as rc:
        rc.upload_file.side_effect = lambda **kw: uploaded.append(kw["file_name"])
        b.book_project(str(proj), "test")          # default upload_mode -> plots+logs

    assert any(n.endswith("stageout/mass.png") for n in uploaded)   # plot uploaded
    assert any(n.endswith("logs/celebi.stdout") for n in uploaded)  # logs uploaded
    assert not any(n.endswith("ntuple.root") for n in uploaded)     # data excluded
