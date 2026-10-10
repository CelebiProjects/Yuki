"""Status polling should share one lightweight refresh per workflow."""
import json
import fcntl
import threading
from unittest import mock

from flask import Flask

from Yuki.server.routes import status as status_routes
from Yuki.utils.locked_metadata import read_variable as read_locked_variable
from Yuki.server.workflow_status_refresh import enqueue_once, running_refresh


def test_enqueue_once_and_running_lock(tmp_path):
    """One queued task and one running task are allowed per workflow."""
    workflow_path = str(tmp_path / "workflow")
    task = mock.Mock()

    assert enqueue_once(workflow_path, "project", "workflow", task)
    assert not enqueue_once(workflow_path, "project", "workflow", task)
    task.apply_async.assert_called_once()
    token = task.apply_async.call_args.kwargs["args"][2]

    with running_refresh(workflow_path, token) as acquired:
        assert acquired
        with running_refresh(workflow_path) as second:
            assert not second
        assert not enqueue_once(workflow_path, "project", "workflow", task)

    # Completed refreshes retain the five-second cross-process cooldown.
    assert not enqueue_once(workflow_path, "project", "workflow", task)


def test_losing_queued_refresh_clears_its_marker(tmp_path):
    """A live refresh may win the lock without leaving Celery queued forever."""
    workflow_path = str(tmp_path / "workflow")
    task = mock.Mock()
    assert enqueue_once(workflow_path, "project", "workflow", task)
    token = task.apply_async.call_args.kwargs["args"][2]

    with running_refresh(workflow_path) as live_acquired:
        assert live_acquired
        with running_refresh(workflow_path, token) as worker_acquired:
            assert not worker_acquired

    queue_path = tmp_path / "workflow" / ".status-refresh-queue.lock"
    assert json.loads(queue_path.read_text(encoding="utf-8"))["state"] == "done"


def test_enqueue_failure_allows_retry(tmp_path):
    """A broker error must not leave a stale queued marker."""
    task = mock.Mock()
    task.apply_async.side_effect = RuntimeError("broker offline")
    path = str(tmp_path / "workflow")
    try:
        enqueue_once(path, "project", "workflow", task)
    except RuntimeError:
        pass
    else:
        assert False, "broker failure should propagate"
    task.apply_async.side_effect = None
    assert enqueue_once(path, "project", "workflow", task)


def test_status_reads_snapshot_without_reconstructing_workflow(tmp_path, monkeypatch):
    """Many impression polls can reuse workflow results without loading its jobs."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    workflow_path = tmp_path / "Workflows" / "project" / "workflow"
    workflow_path.mkdir(parents=True)
    (workflow_path / "results.json").write_text(
        json.dumps({"results": {"status": "running"}}), encoding="utf-8")

    app = Flask(__name__)
    app.register_blueprint(status_routes.bp)
    job = mock.Mock()
    job.workflow_id.return_value = "workflow"
    job.status.return_value = "running"
    job.detailed_status.return_value = "Working"
    config_file = mock.Mock()
    config_file.read_variable.side_effect = lambda key, default=None: {
        "runners": ["runner"], "runners_id": {"runner": "runner-id"},
        "object_type": "task",
    }.get(key, default)
    with mock.patch.object(status_routes.config, "get_config_file",
                           return_value=config_file), \
         mock.patch.object(status_routes, "ConfigFile", return_value=config_file), \
         mock.patch.object(status_routes, "VJob", return_value=job), \
         mock.patch.object(status_routes, "VWorkflow") as workflow_cls, \
         mock.patch.object(status_routes.task_update_workflow_status,
                           "apply_async") as dispatch:
        client = app.test_client()
        first = client.get("/status/project/impression")
        second = client.get("/status/project/impression")

    assert first.status_code == second.status_code == 200
    assert first.json["status"] == "running"
    dispatch.assert_called_once()
    workflow_cls.create.assert_not_called()


def test_status_wait_refreshes_workflow_before_responding(tmp_path, monkeypatch):
    """The wait query performs a locked live refresh instead of queueing one."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    workflow_path = tmp_path / "Workflows" / "project" / "workflow"
    workflow_path.mkdir(parents=True)
    (workflow_path / "results.json").write_text(
        json.dumps({"results": {"status": "running"}}), encoding="utf-8")

    app = Flask(__name__)
    app.register_blueprint(status_routes.bp)
    job = mock.Mock()
    job.workflow_id.return_value = "workflow"
    job.status.return_value = "running"
    job.detailed_status.return_value = "Working"
    config_file = mock.Mock()
    config_file.read_variable.side_effect = lambda key, default=None: {
        "runners": ["runner"], "runners_id": {"runner": "runner-id"},
        "object_type": "task",
    }.get(key, default)
    workflow = mock.Mock()
    workflow.status.return_value = "running"
    refresh_lock = mock.MagicMock()
    refresh_lock.__enter__.return_value = True
    with mock.patch.object(status_routes.config, "get_config_file",
                           return_value=config_file), \
         mock.patch.object(status_routes, "ConfigFile", return_value=config_file), \
         mock.patch.object(status_routes, "VJob", return_value=job), \
         mock.patch.object(status_routes.VWorkflow, "create",
                           return_value=workflow), \
         mock.patch.object(status_routes, "running_refresh",
                           return_value=refresh_lock) as locked, \
         mock.patch.object(status_routes.task_update_workflow_status,
                           "apply_async") as dispatch:
        response = app.test_client().get(
            "/status/project/impression?wait=true")

    assert response.status_code == 200
    assert response.json["live_refresh"] is True
    locked.assert_called_once_with(str(workflow_path), blocking=True)
    workflow.update_workflow_status.assert_called_once_with()
    dispatch.assert_not_called()


def test_status_read_waits_for_json_writer(tmp_path):
    """A reader must not parse an in-progress ConfigFile JSON update."""
    path = tmp_path / "status.json"
    path.write_text('{"status": "running"}', encoding="utf-8")
    values = []
    with open(path, "r+", encoding="utf-8") as writer:
        fcntl.flock(writer, fcntl.LOCK_EX)
        writer.seek(0)
        writer.truncate()
        writer.write('{"status":')
        writer.flush()
        reader = threading.Thread(
            target=lambda: values.append(read_locked_variable(str(path), "status")))
        reader.start()
        writer.write('"finished"}')
        writer.flush()
        fcntl.flock(writer, fcntl.LOCK_UN)
    reader.join(timeout=2)
    assert not reader.is_alive()
    assert values == ["finished"]
