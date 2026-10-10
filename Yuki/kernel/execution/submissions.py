"""Persistent submission lifecycle records.

Submission state is deliberately separate from workflow/job execution state:
an accepted request can still be rejected while constructing its workflow.
"""
import fcntl
import json
import os
import tempfile
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone


TERMINAL_SUBMISSION_STATUSES = {
    "scheduled", "deduplicated", "blocked", "failed",
}


def _root(yuki_dir=None):
    return os.path.abspath(os.path.expanduser(
        yuki_dir or os.environ.get("YUKIDIR") or "~/.Yuki"))


def _identifier(value, label):
    if not value or value in (".", "..") or os.path.basename(value) != value:
        raise ValueError(f"Invalid {label}")
    return value


def _now():
    return datetime.now(timezone.utc).isoformat()


class SubmissionStore:
    """Read and atomically update one durable submission record."""

    def __init__(self, project_uuid, submission_id, yuki_dir=None):
        self.project_uuid = _identifier(project_uuid, "project UUID")
        self.submission_id = _identifier(submission_id, "submission ID")
        directory = os.path.join(_root(yuki_dir), "Submissions", self.project_uuid)
        self.path = os.path.join(directory, f"{self.submission_id}.json")
        self.lock_path = os.path.join(directory, f"{self.submission_id}.lock")

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    @classmethod
    def create(cls, project_uuid, impressions, machine_id, yuki_dir=None,
               submission_id=None):
        """Create an accepted submission before it is sent to Celery."""
        submission_id = submission_id or uuid.uuid4().hex
        store = cls(project_uuid, submission_id, yuki_dir)
        now = _now()
        record = {
            "submission_id": submission_id,
            "project_uuid": project_uuid,
            "impressions": list(impressions),
            "machine_id": machine_id,
            "status": "accepted",
            "created_at": now,
            "updated_at": now,
            "celery_task_id": "",
            "workflow_id": "",
            "candidate_workflow_id": "",
            "deduplicated": False,
            "submission_reason": "new",
            "workflow_status": "",
            "status_refreshed": False,
            "refresh_error": "",
            "previous_workflows": {},
            "retryable": False,
            "error": "",
            "conflicts": {},
        }
        with store._locked() as current:
            if current:
                raise FileExistsError(f"Submission {submission_id} already exists")
            store._write(record)
        return store, record

    @contextmanager
    def _locked(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.lock_path, "a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                yield self._read_unlocked()
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)

    def _read_unlocked(self):
        try:
            with open(self.path, encoding="utf-8") as source:
                value = json.load(source)
            return value if isinstance(value, dict) else {}
        except (FileNotFoundError, json.JSONDecodeError, TypeError):
            return {}

    def _write(self, record):
        directory = os.path.dirname(self.path)
        os.makedirs(directory, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".submission-", dir=directory)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as target:
                json.dump(record, target, indent=2, sort_keys=True)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def read(self):
        """Return the current record, or an empty dictionary when absent."""
        if not os.path.exists(self.path):
            return {}
        with self._locked() as record:
            return dict(record)

    def update(self, **changes):
        """Patch a record without allowing a terminal state to be rewritten."""
        with self._locked() as record:
            if not record:
                raise FileNotFoundError(f"Submission {self.submission_id} not found")
            requested_status = changes.get("status")
            current_status = record.get("status")
            if (requested_status and current_status in TERMINAL_SUBMISSION_STATUSES
                    and requested_status != current_status):
                return dict(record)
            record.update(changes)
            record["updated_at"] = _now()
            self._write(record)
            return dict(record)
