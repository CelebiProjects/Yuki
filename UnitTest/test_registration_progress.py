"""Slow runner progress must not prevent polling local registration state."""
import threading
import time
from unittest import mock

import pytest

from Yuki.kernel import remote_data_ops
from Yuki.kernel.registration_progress import ProgressCache
from Yuki.server.routes import remote_data as routes


@pytest.mark.parametrize("path", [
    "/register-remote-data/job-1",
    "/register-remote-data/impression/imp-1",
])
def test_status_returns_while_ssh_is_blocked(monkeypatch, tmp_path, path):
    """Both status routes stay responsive and expose terminal state immediately."""
    from flask import Flask

    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    cache = ProgressCache(wait=0.01)
    monkeypatch.setattr(routes, "progress_cache", cache)
    state = {"status": "copying", "runner_id": "r1", "error": None,
             "result": {"impression_uuid": "imp-1"}}
    remote_data_ops.write_job_state(str(tmp_path), "job-1", state)
    state = remote_data_ops.read_job_state(str(tmp_path), "job-1")
    entered, release = threading.Event(), threading.Event()
    progress = {"stage": "copying", "bytes_done": 12, "bytes_total": 30}

    def slow_read(*_args):
        entered.set()
        release.wait(5)
        return progress

    monkeypatch.setattr(remote_data_ops, "read_remote_progress", slow_read)
    app = Flask(__name__)
    app.register_blueprint(routes.bp)
    client = app.test_client()
    try:
        start = time.monotonic()
        response = client.get(path)
        assert time.monotonic() - start < 1
        assert entered.wait(1)
        assert response.status_code == 200
        assert response.json == dict(state, progress=None)
        # Finishing the registration is visible even with SSH still blocked.
        state["status"] = "done"
        remote_data_ops.write_job_state(str(tmp_path), "job-1", state)
        assert client.get(path).json == state
    finally:
        release.set()


def test_refreshes_are_deduplicated_and_bounded():
    """Slow reads share work and cannot spawn unlimited background threads."""
    cache = ProgressCache(limit=2, wait=0.01)
    release = threading.Event()
    reader = mock.Mock(side_effect=lambda: (release.wait(5), {"bytes_done": 7})[1])
    try:
        assert cache.get("one", reader) is None
        assert cache.get("one", reader) is None
        assert cache.get("two", reader) is None
        assert cache.get("three", reader) is None
        assert reader.call_count == 2
    finally:
        release.set()
    cache.wait = 1
    assert cache.get("one", reader) == {"bytes_done": 7}
    assert cache.get("two", reader) == {"bytes_done": 7}
    assert reader.call_count == 2


def test_failure_is_cached_then_retried_and_entries_are_evicted():
    """Failures expire for recovery, and the snapshot cache has a size limit."""
    cache = ProgressCache(capacity=1, wait=1)
    reader = mock.Mock(side_effect=TimeoutError("SSH unavailable"))
    assert cache.get("one", reader) is None
    assert cache.get("one", reader) is None
    assert reader.call_count == 1
    cache.ttl = 0
    reader.side_effect = None
    reader.return_value = {"bytes_done": 8}
    assert cache.get("one", reader) == {"bytes_done": 8}
    assert reader.call_count == 2
    cache.ttl = 3
    assert cache.get("two", reader) == {"bytes_done": 8}
    assert cache.get("one", reader) == {"bytes_done": 8}
    assert reader.call_count == 4


@pytest.mark.parametrize("path", [
    "/register-remote-data/job-1",
    "/register-remote-data/impression/imp-1",
])
@pytest.mark.parametrize("outcome", ["done", "failed"])
def test_refresh_completion_is_returned_in_same_response(monkeypatch, tmp_path, path, outcome):
    """A normal-latency refresh must not require another status command."""
    from flask import Flask

    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    state = {"status": "copying", "runner_id": "r1", "error": None,
             "result": {"impression_uuid": "imp-1"}}
    remote_data_ops.write_job_state(str(tmp_path), "job-1", state)

    def refresh(*_args):
        time.sleep(0.15)  # Longer than the old 0.1-second request wait.
        remote_data_ops.write_job_state(str(tmp_path), "job-1", dict(
            state, status=outcome, error="disk full" if outcome == "failed" else None))

    monkeypatch.setattr(remote_data_ops, "read_remote_progress", refresh)
    app = Flask(__name__)
    app.register_blueprint(routes.bp)
    response = app.test_client().get(path)
    assert response.json["status"] == outcome
    assert "progress" not in response.json


def test_phase_change_does_not_show_previous_phase_progress(monkeypatch, tmp_path):
    """Hash byte counts cannot become a copying percentage during a refresh."""
    from flask import Flask

    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    state = {"status": "hashing", "runner_id": "r1", "result": None}
    remote_data_ops.write_job_state(str(tmp_path), "job-1", state)

    def refresh(*_args):
        remote_data_ops.write_job_state(str(tmp_path), "job-1", dict(
            state, status="copying", result={"impression_uuid": "imp-1"}))
        return {"stage": "hashing", "bytes_done": 90, "bytes_total": 100}

    monkeypatch.setattr(remote_data_ops, "read_remote_progress", refresh)
    app = Flask(__name__)
    app.register_blueprint(routes.bp)
    response = app.test_client().get("/register-remote-data/job-1")
    assert response.json["status"] == "copying"
    assert response.json["result"]["impression_uuid"] == "imp-1"
    assert response.json["progress"] is None
