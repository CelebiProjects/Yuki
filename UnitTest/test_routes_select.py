"""Tests for select route handlers."""
from unittest import mock

from Yuki.server.routes import workflow as wf_routes
from Yuki.server.routes import execution as ex_routes
from Yuki.server.routes import upload as up_routes


def test_collect_files_route_passes_spec():
    """collect-files forwards kind and pattern to ImpressionStorage."""
    app = _app(wf_routes.bp)
    with mock.patch.object(wf_routes, "ImpressionStorage") as storage_cls:
        inst = storage_cls.return_value
        inst.collect_files.return_value = {
            "runner": {"collected": [], "skipped": [], "failed": []}
        }
        c = app.test_client()
        r = c.get("/collect-files/proj/imp?kind=stageout&pattern=*.root")
        assert r.status_code == 200
        assert r.get_json()["*.root"]["runner"]["collected"] == []
        inst.collect_files.assert_called_once_with("stageout", "*.root")


def test_collect_files_route_type_keyword():
    """collect-files accepts the type keyword shorthand."""
    app = _app(wf_routes.bp)
    with mock.patch.object(wf_routes, "ImpressionStorage") as storage_cls:
        c = app.test_client()
        c.get("/collect-files/proj/imp?type=plots")
        storage_cls.return_value.collect_files.assert_called_once_with("stageout", "plots")


def test_file_status_route_returns_json():
    """file-status returns the ImpressionStorage file_status payload."""
    app = _app(ex_routes.bp)
    with mock.patch.object(ex_routes, "ImpressionStorage") as storage_cls:
        storage_cls.return_value.file_status.return_value = [
            {"name": "mass.png", "size": 3, "type": "plot",
             "in_runner": True, "in_yuki": True}]
        c = app.test_client()
        r = c.get("/file-status/proj/imp/runner?kind=stageout")
        assert r.status_code == 200
        assert r.get_json()[0]["name"] == "mass.png"


def test_file_status_route_detailed_mode():
    """detailed=1 forwards the flag and returns the {files, notes} payload."""
    app = _app(ex_routes.bp)
    with mock.patch.object(ex_routes, "ImpressionStorage") as storage_cls:
        storage_cls.return_value.file_status.return_value = {
            "files": [], "notes": []}
        c = app.test_client()
        r = c.get("/file-status/proj/imp/runner?kind=stageout&detailed=1")
        assert r.status_code == 200
        assert r.get_json() == {"files": [], "notes": []}
        storage_cls.return_value.file_status.assert_called_once_with(
            "stageout", detailed=True)


def _app(bp):
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app


def test_fileview_serves_nested_file(tmp_path):
    """file-view serves files from nested stageout directories."""
    app = _app(up_routes.bp)
    with mock.patch.object(up_routes.config, "get_job_path", return_value=str(tmp_path)):
        stageout = tmp_path / "runner-1" / "stageout"
        (stageout / "plots").mkdir(parents=True)
        (stageout / "plots" / "fit.png").write_bytes(b"img")
        c = app.test_client()
        r = c.get("/file-view/proj/imp/runner-1/plots%2Ffit.png")
        assert r.status_code == 200
        assert r.data == b"img"


def test_export_serves_nested_stageout_file(tmp_path):
    """export serves files from nested stageout directories."""
    app = _app(up_routes.bp)
    with mock.patch.object(up_routes.config, "get_job_path", return_value=str(tmp_path)), \
         mock.patch.object(up_routes.config, "get_config_file") as cfg:
        cfg.return_value.read_variable.side_effect = lambda key, default=None: {
            "runners": ["runner"], "runners_id": {"runner": "runner-1"}
        }.get(key, default)
        stageout = tmp_path / "runner-1" / "stageout"
        (stageout / "data").mkdir(parents=True)
        (stageout / "data" / "ntuple.root").write_bytes(b"data")
        c = app.test_client()
        r = c.get("/export/proj/imp/data%2Fntuple.root")
        assert r.status_code == 200
        assert r.data == b"data"


def test_export_blocks_path_traversal(tmp_path):
    """export refuses paths escaping the job directory."""
    app = _app(up_routes.bp)
    with mock.patch.object(up_routes.config, "get_job_path", return_value=str(tmp_path)):
        (tmp_path / "rawdata").mkdir()
        (tmp_path / "rawdata" / "safe.txt").write_bytes(b"safe")
        c = app.test_client()
        r = c.get("/export/proj/imp/..%2F..%2Frawdata%2Fsafe.txt")
        assert r.status_code == 200
        assert r.get_data(as_text=True) == "NOTFOUND"


def test_refresh_filelists_route_returns_report():
    """refresh-filelists returns the force refresh report."""
    app = _app(ex_routes.bp)
    with mock.patch.object(ex_routes, "ImpressionStorage") as storage_cls:
        storage_cls.return_value.force_refresh_filelists.return_value = {
            "runner": {"stageout": {"files": 23, "error": None},
                       "logs": {"files": 1, "error": None}}}
        c = app.test_client()
        r = c.get("/refresh-filelists/proj/imp")
        assert r.status_code == 200
        assert r.get_json()["runner"]["stageout"]["files"] == 23


def test_purge_impression_data_route_purges():
    """purge-impression-data forwards force and returns the report."""
    app = _app(ex_routes.bp)
    with mock.patch.object(ex_routes, "ImpressionStorage") as storage_cls:
        storage_cls.return_value.purge_collected_data.return_value = {
            "purged": True, "impression": "imp", "machines": {},
            "freed_bytes": 0}
        c = app.test_client()
        r = c.post("/purge-impression-data/proj/imp",
                   json={"force": True})
        assert r.status_code == 200
        assert r.get_json()["purged"] is True
        storage_cls.return_value.purge_collected_data.assert_called_once_with(
            force=True)


def test_purge_impression_data_route_default_force():
    """Without a body the purge is non-forced."""
    app = _app(ex_routes.bp)
    with mock.patch.object(ex_routes, "ImpressionStorage") as storage_cls:
        storage_cls.return_value.purge_collected_data.return_value = {
            "purged": True, "impression": "imp", "machines": {},
            "freed_bytes": 0}
        c = app.test_client()
        r = c.post("/purge-impression-data/proj/imp")
        assert r.status_code == 200
        storage_cls.return_value.purge_collected_data.assert_called_once_with(
            force=False)


def test_purge_impression_data_route_refuses_running():
    """A running refusal maps to 409."""
    app = _app(ex_routes.bp)
    with mock.patch.object(ex_routes, "ImpressionStorage") as storage_cls:
        storage_cls.return_value.purge_collected_data.return_value = {
            "refused": "workflow is running; kill it first",
            "running": True}
        c = app.test_client()
        r = c.post("/purge-impression-data/proj/imp")
        assert r.status_code == 409
        assert "running" in r.get_json()["error"]


def test_purge_impression_data_route_refuses():
    """A re-collectability refusal maps to 400."""
    app = _app(ex_routes.bp)
    with mock.patch.object(ex_routes, "ImpressionStorage") as storage_cls:
        storage_cls.return_value.purge_collected_data.return_value = {
            "refused": "cannot verify a runner copy for backend 'reana'",
            "running": False}
        c = app.test_client()
        r = c.post("/purge-impression-data/proj/imp")
        assert r.status_code == 400
        assert "reana" in r.get_json()["error"]
