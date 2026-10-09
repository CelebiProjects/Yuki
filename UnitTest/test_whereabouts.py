"""Tests for the /whereabouts route (data-location registry)."""
# pylint: disable=protected-access
import json
import os
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.server.routes import status as status_routes


def _app(bp):
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app


def _temp_config(monkeypatch, tmp_path):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    config_obj = mock.MagicMock()
    config_obj.get_job_path.side_effect = lambda p, i: str(
        tmp_path / "Storage" / p / i)
    config_obj.get_config_file.return_value = ConfigFile(
        str(tmp_path / "config.json"))
    monkeypatch.setattr(status_routes, "config", config_obj)
    return config_obj


def _write_runners(config_obj, runners_id):
    with open(config_obj.get_config_file().file_path, "w",
              encoding="utf-8") as f:
        json.dump({"runners_id": runners_id}, f)


def _write_dist(tmp_path, project, impression, locations):
    imp_dir = tmp_path / "Storage" / project / impression
    os.makedirs(imp_dir, exist_ok=True)
    with open(imp_dir / "distribution.json", "w", encoding="utf-8") as f:
        json.dump({"locations": locations}, f)


def _write_remote_marker(tmp_path, project, impression, host_runner):
    imp_dir = tmp_path / "Storage" / project / impression
    os.makedirs(imp_dir, exist_ok=True)
    marker = ConfigFile(str(imp_dir / "remote.json"))
    marker.write_variable("host_runner_id", host_runner)
    marker.write_variable("source_path", "/data/src")
    marker.write_variable("remote_path", "/remote/imp")


def _no_refresh():
    """Context manager replacing the distribution refresh with a no-op."""
    return mock.patch.object(status_routes, "_refresh_distribution",
                             create=True)


def test_whereabouts_returns_distribution(monkeypatch, tmp_path):
    """The route reports yuki, per-runner states, and registration."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _write_runners(config_obj, {"pkufarm212": "r1", "cern": "r2"})
    _write_dist(tmp_path, "proj", "imp1", {
        "yuki": {"origin": "collected", "files": 3, "bytes": 10},
        "runner:r1": {
            "workflow": {"origin": "produced", "files": 2},
            "cache": {"origin": "transferred", "files": 3},
        },
        "runner:r2": {"workflow": {"origin": "produced", "files": 1}},
    })
    _write_remote_marker(tmp_path, "proj", "imp1", "r1")

    with _no_refresh():
        r = _app(status_routes.bp).test_client().get(
            "/whereabouts/proj/imp1")
    body = r.get_json()
    assert body["yuki"]["origin"] == "collected"
    assert body["runners"]["pkufarm212"]["cache"]["files"] == 3
    assert "cache" not in body["runners"]["cern"]
    assert body["registered"]["host_runner"] == "pkufarm212"
    assert body["note"] is None


def test_whereabouts_no_distribution(monkeypatch, tmp_path):
    """Without a distribution registry the route reports the note."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _write_runners(config_obj, {"pkufarm212": "r1"})

    with _no_refresh():
        r = _app(status_routes.bp).test_client().get(
            "/whereabouts/proj/imp1")
    body = r.get_json()
    assert body["yuki"] is None
    assert body["runners"] == {}
    assert body["registered"] is None
    assert "distribution" in body["note"]


def test_whereabouts_corrupt_distribution(monkeypatch, tmp_path):
    """A corrupt distribution file degrades to the note instead of 500."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _write_runners(config_obj, {"pkufarm212": "r1"})
    imp_dir = tmp_path / "Storage" / "proj" / "imp1"
    os.makedirs(imp_dir, exist_ok=True)
    with open(imp_dir / "distribution.json", "w", encoding="utf-8") as f:
        f.write("{not valid json")

    with _no_refresh():
        r = _app(status_routes.bp).test_client().get(
            "/whereabouts/proj/imp1")
    assert r.status_code == 200
    assert r.get_json()["yuki"] is None


def test_whereabouts_refreshes_distribution_before_reading(monkeypatch,
                                                           tmp_path):
    """The route refreshes the registry on every read."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _write_runners(config_obj, {"pkufarm212": "r1"})

    with _no_refresh() as refresh:
        _app(status_routes.bp).test_client().get(
            "/whereabouts/proj/imp1")
    refresh.assert_called_once_with("proj", "imp1")


def test_refresh_distribution_helper_swallows_failures():
    """A broken refresh never raises out of the helper."""
    with mock.patch("Yuki.kernel.storage.impressions.ImpressionStorage",
                    side_effect=OSError("boom")):
        status_routes._refresh_distribution("proj", "imp1")  # no raise
