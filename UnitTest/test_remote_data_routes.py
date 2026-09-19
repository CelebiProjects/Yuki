"""Tests for register-remote-data routes and job state."""
import json
import os
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel import remote_data_ops
from Yuki.server.routes import remote_data as remote_data_routes


def _app(bp):
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app


def _temp_config(monkeypatch, tmp_path):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    config_obj = mock.MagicMock()
    config_obj.config_path = str(tmp_path / "config.json")
    config_obj.get_config_file.return_value = ConfigFile(config_obj.config_path)
    monkeypatch.setattr(remote_data_routes, "config", config_obj)
    return config_obj


def _register_runner(config_obj, name="cluster", backend="ssh"):
    runner_id = "r-uuid"
    data = {"runners": [name], "runners_id": {name: runner_id},
            "backend_types": {runner_id: backend}}
    with open(config_obj.config_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return runner_id


def test_job_state_roundtrip(tmp_path):
    """Job state written by the route is readable back."""
    yuki_dir = tmp_path
    assert remote_data_ops.read_job_state(yuki_dir, "j1") is None
    remote_data_ops.write_job_state(yuki_dir, "j1",
                                    {"status": "hashing", "result": None,
                                     "error": None, "runner_id": "r1",
                                     "remote_path": "/p"})
    state = remote_data_ops.read_job_state(yuki_dir, "j1")
    assert state["status"] == "hashing"
    assert state["runner_id"] == "r1"


def test_read_job_state_corrupt(tmp_path):
    """A corrupt job state file reads back as None."""
    jobs_dir = tmp_path / "register-jobs"
    os.makedirs(str(jobs_dir))
    with open(str(jobs_dir / "j1.json"), "w", encoding="utf-8") as f:
        f.write("{not valid json")
    assert remote_data_ops.read_job_state(str(tmp_path), "j1") is None


def test_read_job_state_valid_non_dict_json_returns_none(tmp_path):
    """Valid JSON that is not an object must not crash callers on .get."""
    jobs_dir = tmp_path / "register-jobs"
    os.makedirs(str(jobs_dir))
    for payload in ("[]", '"just a string"', "42"):
        with open(str(jobs_dir / "j1.json"), "w", encoding="utf-8") as f:
            f.write(payload)
        assert remote_data_ops.read_job_state(str(tmp_path), "j1") is None


def test_find_existing_registration_skips_failed(tmp_path):
    """Failed registrations are not offered as existing matches."""
    yuki_dir = tmp_path
    imp_dir = yuki_dir / "Storage" / "proj" / "imp-failed"
    os.makedirs(imp_dir / "contents")
    with open(imp_dir / "contents" / "celebi.yaml", "w", encoding="utf-8") as f:
        f.write("environment: rawdata\nuuid: abcdef\ndescriptor: d\n")
    marker = ConfigFile(str(imp_dir / "remote.json"))
    marker.write_variable("host_runner_id", "r1")
    marker.write_variable("source_path", "/src/data")
    marker.write_variable("remote_path", "/remote/imp")
    status = ConfigFile(str(imp_dir / "status.json"))
    status.write_variable("status", "failed")
    assert remote_data_ops.find_existing_registration(
        str(yuki_dir), "r1", "/src/data") is None


def test_find_existing_registration(tmp_path):
    """An archived registration is returned as the existing match."""
    yuki_dir = tmp_path
    imp_dir = yuki_dir / "Storage" / "proj" / "imp-123"
    os.makedirs(imp_dir / "contents")
    with open(imp_dir / "contents" / "celebi.yaml", "w", encoding="utf-8") as f:
        f.write("environment: rawdata\nuuid: abcdef\ndescriptor: d\n")
    marker = ConfigFile(str(imp_dir / "remote.json"))
    marker.write_variable("host_runner_id", "r1")
    marker.write_variable("source_path", "/src/data")
    marker.write_variable("remote_path", "/remote/imp")

    hit = remote_data_ops.find_existing_registration(str(yuki_dir), "r1", "/src/data")
    assert hit == {"result": {"uuid": "abcdef", "impression_uuid": "imp-123",
                              "descriptor": "d"}}
    assert remote_data_ops.find_existing_registration(
        str(yuki_dir), "r1", "/other") is None


def test_find_existing_registration_skips_running(tmp_path):
    """A mid-copy (running) impression is not offered as existing.

    The route falls through to the in-flight job lookup instead, so a
    re-run during a background copy returns the live job id.
    """
    yuki_dir = tmp_path
    imp_dir = yuki_dir / "Storage" / "proj" / "imp-run"
    os.makedirs(imp_dir / "contents")
    with open(imp_dir / "contents" / "celebi.yaml", "w", encoding="utf-8") as f:
        f.write("environment: rawdata\nuuid: abcdef\ndescriptor: d\n")
    marker = ConfigFile(str(imp_dir / "remote.json"))
    marker.write_variable("host_runner_id", "r1")
    marker.write_variable("source_path", "/src/data")
    marker.write_variable("remote_path", "/remote/imp")
    status = ConfigFile(str(imp_dir / "status.json"))
    status.write_variable("status", "running")

    assert remote_data_ops.find_existing_registration(
        str(yuki_dir), "r1", "/src/data") is None


def test_find_inflight_job(tmp_path):
    """An in-flight job for the same runner and path is found."""
    remote_data_ops.write_job_state(
        str(tmp_path), "job-9",
        {"status": "hashing", "result": None, "error": None,
         "runner_id": "r1", "remote_path": "/p"})
    assert remote_data_ops.find_inflight_job(str(tmp_path), "r1", "/p") == "job-9"
    assert remote_data_ops.find_inflight_job(str(tmp_path), "r1", "/x") is None


def test_register_remote_data_starts_job(monkeypatch, tmp_path):
    """A register request enqueues the celery task and records state."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    with mock.patch.object(remote_data_routes, "task_register_remote_data") as task:
        r = _app(remote_data_routes.bp).test_client().post(
            "/register-remote-data",
            json={"runner": "cluster", "remote_path": "/src/data",
                  "project_uuid": "proj", "descriptor": "mydata"})
    assert r.status_code == 200
    job_id = r.get_json()["job_id"]
    task.apply_async.assert_called_once()
    state = remote_data_ops.read_job_state(str(tmp_path), job_id)
    assert state["status"] == "hashing"
    assert state["remote_path"] == "/src/data"


def test_register_remote_data_unknown_runner(monkeypatch, tmp_path):
    """An unknown runner name is rejected with 404."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    r = _app(remote_data_routes.bp).test_client().post(
        "/register-remote-data",
        json={"runner": "ghost", "remote_path": "/p", "project_uuid": "proj"})
    assert r.status_code == 404


def test_register_remote_data_non_ssh_runner(monkeypatch, tmp_path):
    """Registering against a non-ssh runner is rejected."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj, name="local", backend="native")
    r = _app(remote_data_routes.bp).test_client().post(
        "/register-remote-data",
        json={"runner": "local", "remote_path": "/p", "project_uuid": "proj"})
    assert r.status_code == 400
    assert "ssh" in r.get_json()["error"]


def test_register_remote_data_existing_still_creates_job(monkeypatch, tmp_path):
    """Even with an archived registration, a new hash job is started.

    The reuse decision happens after the fresh md5 is computed, in the
    hash job — the route always re-hashes.
    """
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    imp_dir = tmp_path / "Storage" / "proj" / "imp-123"
    os.makedirs(imp_dir / "contents")
    with open(imp_dir / "contents" / "celebi.yaml", "w", encoding="utf-8") as f:
        f.write("environment: rawdata\nuuid: abcdef\ndescriptor: d\n")
    marker = ConfigFile(str(imp_dir / "remote.json"))
    marker.write_variable("host_runner_id", "r-uuid")
    marker.write_variable("source_path", "/src/data")
    marker.write_variable("remote_path", "/remote/imp")
    status = ConfigFile(str(imp_dir / "status.json"))
    status.write_variable("status", "archived")
    with mock.patch.object(remote_data_routes, "task_register_remote_data") as task:
        r = _app(remote_data_routes.bp).test_client().post(
            "/register-remote-data",
            json={"runner": "cluster", "remote_path": "/src/data",
                  "project_uuid": "proj"})
    assert r.status_code == 200
    assert r.get_json()["job_id"]
    task.apply_async.assert_called_once()


def test_register_remote_data_missing_field(monkeypatch, tmp_path):
    """A request missing required fields is rejected with 400."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    r = _app(remote_data_routes.bp).test_client().post(
        "/register-remote-data",
        json={"runner": "cluster", "remote_path": "/p"})
    assert r.status_code == 400
    assert "missing required field" in r.get_json()["error"]


def test_register_remote_data_form_body(monkeypatch, tmp_path):
    """The route also accepts a form-encoded body."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    with mock.patch.object(remote_data_routes, "task_register_remote_data") as task:
        r = _app(remote_data_routes.bp).test_client().post(
            "/register-remote-data",
            data={"runner": "cluster", "remote_path": "/src/data",
                  "project_uuid": "proj", "descriptor": "mydata"})
    assert r.status_code == 200
    assert r.get_json()["job_id"]
    task.apply_async.assert_called_once()


def test_register_remote_data_inflight(monkeypatch, tmp_path):
    """An in-flight job is returned instead of enqueueing a duplicate."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    runner_id = _register_runner(config_obj)
    remote_data_ops.write_job_state(
        str(tmp_path), "job-7",
        {"status": "hashing", "result": None, "error": None,
         "runner_id": runner_id, "remote_path": "/src/data",
         "project_uuid": "proj", "descriptor": "data"})
    with mock.patch.object(remote_data_routes, "task_register_remote_data") as task:
        r = _app(remote_data_routes.bp).test_client().post(
            "/register-remote-data",
            json={"runner": "cluster", "remote_path": "/src/data",
                  "project_uuid": "proj"})
    assert r.status_code == 200
    assert r.get_json() == {"job_id": "job-7"}
    task.apply_async.assert_not_called()


def test_register_remote_data_enqueue_failure(monkeypatch, tmp_path):
    """A broker failure marks the job failed and returns 500."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    with mock.patch.object(remote_data_routes, "task_register_remote_data") as task:
        task.apply_async.side_effect = RuntimeError("broker down")
        r = _app(remote_data_routes.bp).test_client().post(
            "/register-remote-data",
            json={"runner": "cluster", "remote_path": "/src/data",
                  "project_uuid": "proj"})
    assert r.status_code == 500
    job_id = r.get_json()["job_id"]
    state = remote_data_ops.read_job_state(str(tmp_path), job_id)
    assert state["status"] == "failed"
    assert "broker down" in state["error"]


def test_register_remote_data_status(monkeypatch, tmp_path):
    """Job status is served by id; unknown ids give 404."""
    _temp_config(monkeypatch, tmp_path)
    remote_data_ops.write_job_state(
        str(tmp_path), "job-9",
        {"status": "copying", "result": None, "error": None,
         "runner_id": "r1", "remote_path": "/p"})
    r = _app(remote_data_routes.bp).test_client().get(
        "/register-remote-data/job-9")
    assert r.get_json()["status"] == "copying"
    r2 = _app(remote_data_routes.bp).test_client().get(
        "/register-remote-data/ghost")
    assert r2.status_code == 404


def test_register_remote_data_status_merges_progress(monkeypatch, tmp_path):
    """A running job's status response includes remote byte progress."""
    _temp_config(monkeypatch, tmp_path)
    remote_data_ops.write_job_state(
        str(tmp_path), "job-9",
        {"status": "copying", "result": None, "error": None,
         "runner_id": "r1", "remote_path": "/p"})
    progress = {"stage": "copying", "bytes_done": 10, "bytes_total": 14}

    with mock.patch.object(remote_data_ops, "read_remote_progress",
                           return_value=progress):
        r = _app(remote_data_routes.bp).test_client().get(
            "/register-remote-data/job-9")
    body = r.get_json()
    assert body["status"] == "copying"
    assert body["progress"] == progress
    # The merged response must not be persisted into the job state file.
    stored = remote_data_ops.read_job_state(str(tmp_path), "job-9")
    assert "progress" not in stored


def test_register_remote_data_status_progress_none_on_failure(monkeypatch, tmp_path):
    """A failed progress read yields progress: null, not a 500."""
    _temp_config(monkeypatch, tmp_path)
    remote_data_ops.write_job_state(
        str(tmp_path), "job-9",
        {"status": "hashing", "result": None, "error": None,
         "runner_id": "r1", "remote_path": "/p"})
    with mock.patch.object(remote_data_ops, "read_remote_progress",
                           return_value=None):
        r = _app(remote_data_routes.bp).test_client().get(
            "/register-remote-data/job-9")
    assert r.status_code == 200
    assert r.get_json()["progress"] is None


def test_register_remote_data_status_terminal_has_no_progress(monkeypatch, tmp_path):
    """Terminal states carry no progress key and skip the ssh read."""
    _temp_config(monkeypatch, tmp_path)
    remote_data_ops.write_job_state(
        str(tmp_path), "job-9",
        {"status": "done", "result": {"uuid": "x"}, "error": None,
         "runner_id": "r1", "remote_path": "/p"})
    with mock.patch.object(remote_data_ops, "read_remote_progress") as read:
        r = _app(remote_data_routes.bp).test_client().get(
            "/register-remote-data/job-9")
    read.assert_not_called()
    assert "progress" not in r.get_json()


def test_register_remote_data_impression_status(monkeypatch, tmp_path):
    """A job is findable by its impression uuid, with merged progress."""
    _temp_config(monkeypatch, tmp_path)
    remote_data_ops.write_job_state(
        str(tmp_path), "job-9",
        {"status": "copying", "error": None,
         "runner_id": "r1", "remote_path": "/p",
         "result": {"uuid": "md5x", "impression_uuid": "imp-1",
                    "descriptor": "d"}})
    progress = {"stage": "copying", "bytes_done": 10, "bytes_total": 14}
    with mock.patch.object(remote_data_ops, "read_remote_progress",
                           return_value=progress):
        r = _app(remote_data_routes.bp).test_client().get(
            "/register-remote-data/impression/imp-1")
    body = r.get_json()
    assert body["status"] == "copying"
    assert body["result"]["impression_uuid"] == "imp-1"
    assert body["progress"] == progress


def test_register_remote_data_impression_status_unknown_404(monkeypatch, tmp_path):
    """An impression with no job record gives 404."""
    _temp_config(monkeypatch, tmp_path)
    remote_data_ops.write_job_state(
        str(tmp_path), "job-9",
        {"status": "done", "result": {"uuid": "x", "impression_uuid": "imp-1",
                                      "descriptor": "d"}, "error": None,
         "runner_id": "r1", "remote_path": "/p"})
    r = _app(remote_data_routes.bp).test_client().get(
        "/register-remote-data/impression/ghost")
    assert r.status_code == 404


def test_impression_status_uses_latest_registration_not_largest_uuid(monkeypatch, tmp_path):
    """An old e827 job must not hide the current af177 copying registration."""
    _temp_config(monkeypatch, tmp_path)
    result = {"uuid": "same-md5", "impression_uuid": "imp-1", "descriptor": "d"}
    old = {"status": "done", "result": result, "runner_id": "r1", "created_at_ns": 100}
    new = dict(old, status="copying", created_at_ns=200)
    remote_data_ops.write_job_state(str(tmp_path), "e827", old)
    remote_data_ops.write_job_state(str(tmp_path), "af177", new)
    # A late write of the old job must not make it the newest registration.
    remote_data_ops.write_job_state(str(tmp_path), "e827", dict(old, created_at_ns=999))
    progress = {"stage": "copying", "bytes_done": 25, "bytes_total": 84}
    with mock.patch.object(remote_data_ops, "read_remote_progress", return_value=progress) as read:
        response = _app(remote_data_routes.bp).test_client().get(
            "/register-remote-data/impression/imp-1")
    assert response.json["status"] == "copying"
    assert response.json["progress"] == progress
    read.assert_called_once_with("r1", "af177")
    assert remote_data_ops.read_job_state(str(tmp_path), "e827")["created_at_ns"] == 100


def test_legacy_registration_order_is_preserved_on_update(tmp_path):
    """Existing records migrate without a later old-job write changing order."""
    jobs = tmp_path / "register-jobs"
    jobs.mkdir()
    state = {"status": "copying", "result": {"impression_uuid": "imp-1"}}
    for job_id, stamp in (("e827", 1000), ("af177", 2000)):
        path = jobs / (job_id + ".json")
        path.write_text(json.dumps(state))
        os.utime(path, ns=(stamp, stamp))
    assert remote_data_ops.find_job_by_impression(str(tmp_path), "imp-1")[0] == "af177"
    remote_data_ops.write_job_state(str(tmp_path), "e827", dict(state, status="done"))
    assert remote_data_ops.read_job_state(str(tmp_path), "e827")["created_at_ns"] == 1000
    assert remote_data_ops.find_job_by_impression(str(tmp_path), "imp-1")[0] == "af177"


def _impression_fixture(tmp_path, project="proj", imp="imp-1", md5="abc123",
                        remote=True):
    imp_dir = tmp_path / "Storage" / project / imp
    (imp_dir / "contents").mkdir(parents=True)
    with open(imp_dir / "contents" / "celebi.yaml", "w", encoding="utf-8") as f:
        f.write(f"environment: rawdata\nuuid: {md5}\ndescriptor: d\n")
    if remote:
        marker = ConfigFile(str(imp_dir / "remote.json"))
        marker.write_variable("host_runner_id", "r1")
        marker.write_variable("source_path", "/src")
        marker.write_variable("remote_path", "/remote/imp")
    return imp_dir


def test_verify_data_remote_match(monkeypatch, tmp_path):
    """verify-data reports a match when the remote md5 agrees."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    config_obj.config_path = str(tmp_path / "config.json")
    data = {"runners": ["cluster"], "runners_id": {"cluster": "r1"}}
    with open(config_obj.config_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    _impression_fixture(tmp_path)

    class FakeSsh:
        """Ssh shim answering md5 queries with the expected digest."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def exec(self, command, timeout=None):  # pylint: disable=unused-argument
            """Answer md5 queries with the expected digest."""
            return "abc123", "", 0

    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=FakeSsh()):
        r = _app(remote_data_routes.bp).test_client().get(
            "/verify-data/proj/imp-1")
    body = r.get_json()
    assert body["match"] is True
    assert body["expected"] == "abc123"
    assert body["location"] == "runner cluster"


def test_verify_data_remote_mismatch(monkeypatch, tmp_path):
    """verify-data reports a mismatch with the actual remote md5."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    config_obj.config_path = str(tmp_path / "config.json")
    with open(config_obj.config_path, "w", encoding="utf-8") as f:
        json.dump({"runners": ["cluster"], "runners_id": {"cluster": "r1"}}, f)
    _impression_fixture(tmp_path)

    class FakeSsh:
        """Ssh shim answering md5 queries with a different digest."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def exec(self, command, timeout=None):  # pylint: disable=unused-argument
            """Answer md5 queries with the mismatch digest."""
            return "different", "", 0

    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=FakeSsh()):
        r = _app(remote_data_routes.bp).test_client().get(
            "/verify-data/proj/imp-1")
    body = r.get_json()
    assert body["match"] is False
    assert body["actual"] == "different"


def test_verify_data_local_match(monkeypatch, tmp_path):
    """verify-data checks local rawdata against the recorded uuid."""
    _temp_config(monkeypatch, tmp_path)
    imp_dir = _impression_fixture(tmp_path, remote=False)
    data_dir = imp_dir / "rawdata"
    (data_dir / "sub").mkdir(parents=True)
    with open(data_dir / "a.txt", "w", encoding="utf-8") as f:
        f.write("alpha")
    with open(data_dir / "sub" / "b.txt", "w", encoding="utf-8") as f:
        f.write("beta")
    from CelebiChrono.utils.file_utils import dir_md5
    expected = dir_md5(str(data_dir))
    with open(imp_dir / "contents" / "celebi.yaml", "w", encoding="utf-8") as f:
        f.write(f"environment: rawdata\nuuid: {expected}\ndescriptor: d\n")

    r = _app(remote_data_routes.bp).test_client().get("/verify-data/proj/imp-1")
    body = r.get_json()
    assert body["match"] is True
    assert body["location"] == "yuki storage"


def test_verify_data_local_missing_dir(monkeypatch, tmp_path):
    """verify-data errors when the local rawdata directory is absent."""
    _temp_config(monkeypatch, tmp_path)
    _impression_fixture(tmp_path, remote=False)
    r = _app(remote_data_routes.bp).test_client().get("/verify-data/proj/imp-1")
    assert "error" in r.get_json()


def test_verify_data_unknown_impression_404(monkeypatch, tmp_path):
    """verify-data for an unknown impression gives 404."""
    _temp_config(monkeypatch, tmp_path)
    r = _app(remote_data_routes.bp).test_client().get("/verify-data/proj/ghost")
    assert r.status_code == 404


def test_purge_runner_cache_route(monkeypatch, tmp_path):
    """POST /purge-runner-cache resolves the runner and purges."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    summary = {"purged": [{"project": "proj", "impression": "imp-a",
                           "kind": "registered",
                           "remote_dir": "/w/impressions/proj/imp-a"}],
               "skipped": [], "dry_run": True}
    with mock.patch.object(remote_data_ops, "purge_runner_cache",
                           return_value=summary) as m_purge:
        r = _app(remote_data_routes.bp).test_client().post(
            "/purge-runner-cache",
            json={"runner": "cluster", "project": "proj", "dry_run": True})
    assert r.status_code == 200
    assert r.get_json() == summary
    m_purge.assert_called_once_with("r-uuid", project="proj",
                                    impression=None, dry_run=True,
                                    superseded=False, yuki_dir=str(tmp_path))


def test_purge_runner_cache_route_unknown_runner(monkeypatch, tmp_path):
    """An unknown runner name is rejected with 404."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    r = _app(remote_data_routes.bp).test_client().post(
        "/purge-runner-cache", json={"runner": "ghost"})
    assert r.status_code == 404
    assert "not found" in r.get_json()["error"].lower()


def test_purge_runner_cache_route_non_ssh_runner(monkeypatch, tmp_path):
    """Purging against a non-ssh runner is rejected."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj, name="local", backend="native")
    r = _app(remote_data_routes.bp).test_client().post(
        "/purge-runner-cache", json={"runner": "local"})
    assert r.status_code == 400
    assert "ssh" in r.get_json()["error"]


def test_purge_runner_cache_route_ssh_failure(monkeypatch, tmp_path):
    """An ssh failure surfaces as a 500 JSON error."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    with mock.patch.object(remote_data_ops, "purge_runner_cache",
                           side_effect=ConnectionError("banner")):
        r = _app(remote_data_routes.bp).test_client().post(
            "/purge-runner-cache", json={"runner": "cluster"})
    assert r.status_code == 500
    assert "banner" in r.get_json()["error"]


def test_purge_runner_cache_route_form_body(monkeypatch, tmp_path):
    """The route accepts a form body with string flags."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    with mock.patch.object(remote_data_ops, "purge_runner_cache",
                           return_value={"purged": [], "skipped": [],
                                         "dry_run": False}) as m_purge:
        r = _app(remote_data_routes.bp).test_client().post(
            "/purge-runner-cache",
            data={"runner": "cluster", "impression": "imp-a",
                  "dry_run": "true"})
    assert r.status_code == 200
    m_purge.assert_called_once_with("r-uuid", project=None,
                                    impression="imp-a", dry_run=True,
                                    superseded=False, yuki_dir=str(tmp_path))


def test_cache_results_starts_job(monkeypatch, tmp_path):
    """A cache-results request enqueues the celery task and records state."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    with mock.patch.object(remote_data_routes, "task_cache_results") as task:
        r = _app(remote_data_routes.bp).test_client().post(
            "/cache-results",
            json={"runner": "cluster", "project_uuid": "proj",
                  "impression": "imp1"})
    assert r.status_code == 200
    job_id = r.get_json()["job_id"]
    task.apply_async.assert_called_once_with(
        args=[job_id, "r-uuid", "proj", "imp1"])
    state = remote_data_ops.read_job_state(str(tmp_path), job_id,
                                           jobs_dir_name="cache-jobs")
    assert state["status"] == "copying"
    assert state["impression"] == "imp1"


def test_cache_results_unknown_runner(monkeypatch, tmp_path):
    """An unknown runner name is rejected with 404."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    r = _app(remote_data_routes.bp).test_client().post(
        "/cache-results",
        json={"runner": "ghost", "project_uuid": "proj", "impression": "i"})
    assert r.status_code == 404
    assert "not found" in r.get_json()["error"].lower()


def test_cache_results_non_ssh_runner(monkeypatch, tmp_path):
    """A non-ssh runner is rejected."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj, name="local", backend="native")
    r = _app(remote_data_routes.bp).test_client().post(
        "/cache-results",
        json={"runner": "local", "project_uuid": "proj", "impression": "i"})
    assert r.status_code == 400
    assert "ssh" in r.get_json()["error"]


def test_cache_results_missing_field(monkeypatch, tmp_path):
    """A request missing required fields is rejected with 400."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    _register_runner(config_obj)
    r = _app(remote_data_routes.bp).test_client().post(
        "/cache-results", json={"runner": "cluster", "project_uuid": "proj"})
    assert r.status_code == 400
    assert "missing required field" in r.get_json()["error"]


def test_cache_results_status(monkeypatch, tmp_path):
    """Job status is served by id; unknown ids give 404."""
    _temp_config(monkeypatch, tmp_path)
    remote_data_ops.write_job_state(
        str(tmp_path), "job-9",
        {"status": "done", "result": {"cached": 3}, "error": None},
        jobs_dir_name="cache-jobs")
    r = _app(remote_data_routes.bp).test_client().get(
        "/cache-results/job-9")
    assert r.get_json()["status"] == "done"
    r2 = _app(remote_data_routes.bp).test_client().get(
        "/cache-results/ghost")
    assert r2.status_code == 404


def test_verify_data_ssh_retry_succeeds(monkeypatch, tmp_path):
    """A transient ssh banner error is retried before succeeding."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    config_obj.config_path = str(tmp_path / "config.json")
    with open(config_obj.config_path, "w", encoding="utf-8") as f:
        json.dump({"runners": ["cluster"], "runners_id": {"cluster": "r1"}}, f)
    _impression_fixture(tmp_path)

    class FakeSsh:
        """Ssh shim counting connections for the retry assertion."""

        def __init__(self):
            self.calls = 0

        def __enter__(self):
            self.calls += 1
            return self

        def __exit__(self, *a):
            return False

        def exec(self, command, timeout=None):  # pylint: disable=unused-argument
            """Answer md5 queries with the expected digest."""
            return "abc123", "", 0

    attempts = {"n": 0}
    fake = FakeSsh()

    def flaky_conn(_runner_id):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConnectionError("Error reading SSH protocol banner")
        return fake

    with mock.patch.object(remote_data_ops, "_ssh_connection",
                           side_effect=flaky_conn):
        r = _app(remote_data_routes.bp).test_client().get(
            "/verify-data/proj/imp-1")
    body = r.get_json()
    assert body["match"] is True
    assert attempts["n"] == 2


def test_verify_data_ssh_failure_returns_error(monkeypatch, tmp_path):
    """A persistent ssh failure yields a match=False body with the error."""
    config_obj = _temp_config(monkeypatch, tmp_path)
    config_obj.config_path = str(tmp_path / "config.json")
    with open(config_obj.config_path, "w", encoding="utf-8") as f:
        json.dump({"runners": ["cluster"], "runners_id": {"cluster": "r1"}}, f)
    _impression_fixture(tmp_path)

    def failing_conn(_runner_id):
        raise ConnectionError("Error reading SSH protocol banner")

    with mock.patch.object(remote_data_ops, "_ssh_connection",
                           side_effect=failing_conn):
        r = _app(remote_data_routes.bp).test_client().get(
            "/verify-data/proj/imp-1")
    body = r.get_json()
    assert body["match"] is False
    assert "Error reading SSH" in body["error"]
    assert body["expected"] == "abc123"


def test_inflight_registration_requires_same_identity(tmp_path):
    remote_data_ops.write_job_state(str(tmp_path), "job-1", {
        "status": "hashing", "runner_id": "r1", "remote_path": "/data",
        "project_uuid": "proj", "descriptor": "original",
    })
    assert remote_data_ops.find_inflight_job(
        str(tmp_path), "r1", "/data", "proj", "original") == "job-1"
    assert remote_data_ops.find_inflight_job(
        str(tmp_path), "r1", "/data", "other", "original") is None
    assert remote_data_ops.find_inflight_job(
        str(tmp_path), "r1", "/data", "proj", "renamed") is None
