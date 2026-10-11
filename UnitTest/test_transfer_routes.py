"""Tests for the transfer routes."""
import json
import os
import tempfile
from unittest import mock

from Yuki.server.routes import transfer as transfer_routes


def _app():
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(transfer_routes.bp)
    return app


def _mock_config(runners_id=None, backend_types=None):
    """Return a config mock with runners_id/backend_types lookup tables."""
    cfg = mock.MagicMock()
    cfg.get_config_file.return_value.read_variable.side_effect = (
        lambda key, default: {
            "runners_id": runners_id or {},
            "backend_types": backend_types or {},
        }.get(key, default)
    )
    return cfg


def test_start_transfer_missing_fields():
    client = _app().test_client()
    r = client.post("/transfer", json={})
    assert r.status_code == 400
    assert "error" in r.get_json()


def test_start_transfer_unknown_runner():
    client = _app().test_client()
    with mock.patch("Yuki.server.routes.transfer.config",
                    _mock_config()):
        r = client.post("/transfer", json={
            "project_uuid": "proj",
            "impression": "imp",
            "source": "runner:missing",
            "destination": "yuki",
        })
    assert r.status_code == 404
    assert "not found" in r.get_json()["error"]


def test_start_transfer_non_ssh_runner():
    client = _app().test_client()
    with mock.patch("Yuki.server.routes.transfer.config",
                    _mock_config(
                        runners_id={"reanafarm": "runner-uuid"},
                        backend_types={"runner-uuid": "reana"},
                    )):
        r = client.post("/transfer", json={
            "project_uuid": "proj",
            "impression": "imp",
            "source": "runner:reanafarm",
            "destination": "yuki",
        })
    assert r.status_code == 400
    assert "not an ssh runner" in r.get_json()["error"]


def test_start_transfer_native_explains_shared_storage():
    client = _app().test_client()
    with mock.patch("Yuki.server.routes.transfer.config",
                    _mock_config(
                        runners_id={"local": "runner-uuid"},
                        backend_types={"runner-uuid": "native"},
                    )):
        response = client.post("/transfer", json={
            "project_uuid": "proj",
            "impression": "imp",
            "source": "runner:local",
            "destination": "yuki",
        })
    assert response.status_code == 400
    assert "shares Yuki Storage" in response.get_json()["error"]
    assert "use 'yuki'" in response.get_json()["error"]


def test_start_transfer_reana_to_ssh_allowed():
    """A reana source is allowed when the destination is an ssh runner."""
    client = _app().test_client()
    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch.object(transfer_routes, "task_transfer_results") as task:
            with mock.patch("Yuki.server.routes.transfer.config",
                            _mock_config(
                                runners_id={"reanafarm": "reana-uuid",
                                            "pkufarm": "ssh-uuid"},
                                backend_types={"reana-uuid": "reana",
                                               "ssh-uuid": "ssh"},
                            )):
                with mock.patch.object(
                    transfer_routes.result_transfer,
                    "_resolve_yuki_dir",
                    return_value=tmpdir,
                ):
                    r = client.post("/transfer", json={
                        "project_uuid": "proj",
                        "impression": "imp",
                        "source": "runner:reanafarm",
                        "destination": "runner:pkufarm",
                    })
    assert r.status_code == 200
    assert "job_id" in r.get_json()
    task.apply_async.assert_called_once()


def test_start_transfer_reana_to_reana_rejected():
    """A reana source to a non-ssh destination stays rejected."""
    client = _app().test_client()
    with mock.patch("Yuki.server.routes.transfer.config",
                    _mock_config(
                        runners_id={"reanafarm": "reana-uuid",
                                    "otherreana": "reana-uuid-2"},
                        backend_types={"reana-uuid": "reana",
                                       "reana-uuid-2": "reana"},
                    )):
        r = client.post("/transfer", json={
            "project_uuid": "proj",
            "impression": "imp",
            "source": "runner:reanafarm",
            "destination": "runner:otherreana",
        })
    assert r.status_code == 400
    assert "not an ssh runner" in r.get_json()["error"]


def test_start_transfer_starts_job():
    client = _app().test_client()
    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch.object(transfer_routes, "task_transfer_results") as task:
            with mock.patch("Yuki.server.routes.transfer.config",
                            _mock_config(
                                runners_id={"pkufarm": "runner-uuid"},
                                backend_types={"runner-uuid": "ssh"},
                            )):
                with mock.patch.object(
                    transfer_routes.result_transfer,
                    "_resolve_yuki_dir",
                    return_value=tmpdir,
                ):
                    r = client.post("/transfer", json={
                        "project_uuid": "proj",
                        "impression": "imp",
                        "source": "runner:pkufarm",
                        "destination": "yuki",
                        "pattern": "*.txt",
                        "force": False,
                    })
    assert r.status_code == 200
    body = r.get_json()
    assert "job_id" in body
    task.apply_async.assert_called_once()


def test_transfer_status():
    client = _app().test_client()
    job_id = "a" * 32
    with tempfile.TemporaryDirectory() as tmpdir:
        progress_dir = os.path.join(tmpdir, "transfer-progress")
        os.makedirs(progress_dir)
        progress_path = os.path.join(progress_dir, f"{job_id}.json")
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump({"status": "done", "bytes_done": 42,
                       "bytes_total": 42, "current_file": "a.txt"}, f)
        with mock.patch.object(
            transfer_routes.result_transfer,
            "_resolve_yuki_dir",
            return_value=tmpdir,
        ):
            r = client.get(f"/transfer/{job_id}")
    assert r.status_code == 200
    body = r.get_json()
    assert body["status"] == "done"
    assert body["bytes_done"] == 42


def test_transfer_status_unknown_job():
    client = _app().test_client()
    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch.object(
            transfer_routes.result_transfer,
            "_resolve_yuki_dir",
            return_value=tmpdir,
        ):
            r = client.get("/transfer/" + "b" * 32)
    assert r.status_code == 404
    assert "error" in r.get_json()


def test_transfer_status_rejects_traversal_job_id():
    client = _app().test_client()
    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch.object(
            transfer_routes.result_transfer,
            "_resolve_yuki_dir",
            return_value=tmpdir,
        ):
            # A hostile id with a leading ".." — rejected by UUID_RE before
            # any filesystem access.
            r = client.get("/transfer/.." + "b" * 30)
    assert r.status_code == 404
    assert "error" in r.get_json()


def test_start_transfer_invalid_location():
    client = _app().test_client()
    with mock.patch("Yuki.server.routes.transfer.config",
                    _mock_config()):
        r = client.post("/transfer", json={
            "project_uuid": "proj",
            "impression": "imp",
            "source": "foo",
            "destination": "yuki",
        })
    assert r.status_code == 400
    assert "invalid location" in r.get_json()["error"]


def test_start_transfer_yuki_to_yuki():
    client = _app().test_client()
    with mock.patch("Yuki.server.routes.transfer.config",
                    _mock_config()):
        r = client.post("/transfer", json={
            "project_uuid": "proj",
            "impression": "imp",
            "source": "yuki",
            "destination": "yuki",
        })
    assert r.status_code == 400
    assert "cannot both be yuki" in r.get_json()["error"]


def test_start_transfer_dispatch_failure():
    client = _app().test_client()
    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch.object(
            transfer_routes, "task_transfer_results") as task:
            task.apply_async.side_effect = RuntimeError("broker down")
            with mock.patch("Yuki.server.routes.transfer.config",
                            _mock_config(
                                runners_id={"pkufarm": "runner-uuid"},
                                backend_types={"runner-uuid": "ssh"},
                            )):
                with mock.patch.object(
                    transfer_routes.result_transfer,
                    "_resolve_yuki_dir",
                    return_value=tmpdir,
                ):
                    r = client.post("/transfer", json={
                        "project_uuid": "proj",
                        "impression": "imp",
                        "source": "runner:pkufarm",
                        "destination": "yuki",
                    })
                    assert r.status_code == 500
                    body = r.get_json()
                    assert "error" in body
                    job_id = body["job_id"]
                    progress_path = os.path.join(
                        tmpdir, "transfer-progress", f"{job_id}.json")
                    with open(progress_path, encoding="utf-8") as f:
                        state = json.load(f)
                    assert state["status"] == "failed"
