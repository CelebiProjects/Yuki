"""Cross-process coordination for workflow status refreshes.

The HTTP server and Celery workers share the workflow directory.  A queued
marker prevents duplicate dispatches; a separate flock prevents overlapping
refreshes even when a queued marker expires or another caller dispatches one.
"""
import fcntl
import json
import os
import time
import uuid
from contextlib import contextmanager

QUEUE_TIMEOUT = 300
REFRESH_INTERVAL = 5


@contextmanager
def _locked(path, blocking=True):
    with open(path, "a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        except BlockingIOError:
            yield None
            return
        try:
            yield handle
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _paths(workflow_path):
    os.makedirs(workflow_path, exist_ok=True)
    return (os.path.join(workflow_path, ".status-refresh-queue.lock"),
            os.path.join(workflow_path, ".status-refresh-running.lock"))


def enqueue_once(workflow_path, project_uuid, workflow_id, task):
    """Dispatch at most one queued refresh per workflow across server processes."""
    queue_path, running_path = _paths(workflow_path)
    with _locked(queue_path) as queue:
        queue.seek(0)
        try:
            pending = json.load(queue)
        except (ValueError, TypeError):
            pending = {}
        interval = (QUEUE_TIMEOUT if pending.get("state") == "queued"
                    else REFRESH_INTERVAL)
        if time.time() - pending.get("at", 0) < interval:
            return False
        with _locked(running_path, blocking=False) as running:
            if running is None:
                return False
        token = uuid.uuid4().hex
        queue.seek(0)
        queue.truncate()
        json.dump({"at": time.time(), "token": token, "state": "queued"}, queue)
        queue.flush()
        try:
            task.apply_async(args=[project_uuid, workflow_id, token])
        except Exception:
            queue.seek(0)
            queue.truncate()
            queue.flush()
            raise
        return True


@contextmanager
def running_refresh(workflow_path, token=None):
    """Allow one worker to refresh; clear only its own queued marker."""
    queue_path, running_path = _paths(workflow_path)
    with _locked(running_path, blocking=False) as running:
        if running is None:
            yield False
            return
        try:
            yield True
        finally:
            if token is not None:
                with _locked(queue_path) as queue:
                    queue.seek(0)
                    try:
                        pending = json.load(queue)
                    except (ValueError, TypeError):
                        pending = {}
                    if pending.get("token") == token:
                        queue.seek(0)
                        queue.truncate()
                        json.dump({"at": time.time(), "state": "done"}, queue)
                        queue.flush()
