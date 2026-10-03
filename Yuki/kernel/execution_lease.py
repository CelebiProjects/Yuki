"""Atomic execution ownership for workflow jobs.

The registry is authoritative.  Per-job ``workflow`` fields are only indexes
for the UI and may lag after a process crash.  One project-wide lock protects
the whole requested job set so claims are all-or-nothing.
"""
# pylint: disable=too-many-arguments,too-many-positional-arguments
import fcntl
import json
import os
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass


TERMINAL_WORKFLOW_STATUSES = {
    "finished", "success", "coda", "failed", "dissonance",
    "stopped", "deleted", "killed",
}


class WorkflowLeaseConflict(RuntimeError):
    """Some requested jobs are owned by another active workflow."""

    def __init__(self, conflicts):
        self.conflicts = conflicts
        detail = ", ".join(
            f"{job} ({entry.get('workflow_id', 'unknown')})"
            for job, entry in sorted(conflicts.items()))
        super().__init__(f"Jobs already belong to active workflows: {detail}")


class WorkflowAlreadyActive(WorkflowLeaseConflict):
    """The same job set is already owned by one active workflow."""

    def __init__(self, workflow_id, conflicts):
        self.workflow_id = workflow_id
        super().__init__(conflicts)


@dataclass(frozen=True)
class LeaseClaim:
    """A successfully acquired execution lease."""

    token: str
    jobs: tuple


def _root(yuki_dir=None):
    return os.path.abspath(os.path.expanduser(
        yuki_dir or os.environ.get("YUKIDIR") or "~/.Yuki"))


def _paths(project_uuid, machine_id, yuki_dir=None):
    directory = os.path.join(_root(yuki_dir), "ExecutionLeases", project_uuid)
    os.makedirs(directory, exist_ok=True)
    if machine_id and os.path.basename(machine_id) != machine_id:
        raise ValueError("Invalid runner ID for execution lease")
    # Deliberately project-wide: one impression may not run concurrently on
    # two different runners unless a future version explicitly namespaces its
    # outputs and cache identity by runner.
    base = os.path.join(directory, "leases")
    return base + ".json", base + ".lock"


@contextmanager
def _registry(project_uuid, machine_id, yuki_dir=None):
    registry_path, lock_path = _paths(project_uuid, machine_id, yuki_dir)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            try:
                with open(registry_path, encoding="utf-8") as source:
                    registry = json.load(source)
            except (FileNotFoundError, json.JSONDecodeError, TypeError):
                registry = {"leases": {}}
            if not isinstance(registry.get("leases"), dict):
                registry["leases"] = {}
            yield registry, registry_path
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _write_registry(registry_path, registry):
    directory = os.path.dirname(registry_path)
    fd, temporary = tempfile.mkstemp(prefix=".leases-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as target:
            json.dump(registry, target, indent=2, sort_keys=True)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, registry_path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _workflow_status(project_uuid, workflow_id, yuki_dir=None):
    path = os.path.join(_root(yuki_dir), "Workflows", project_uuid,
                        workflow_id, "results.json")
    try:
        with open(path, encoding="utf-8") as source:
            return (json.load(source).get("results") or {}).get("status", "")
    except (FileNotFoundError, json.JSONDecodeError, TypeError):
        return ""


def _active(entry, project_uuid, yuki_dir=None):
    workflow_id = entry.get("workflow_id", "")
    if not workflow_id:
        return False
    return _workflow_status(project_uuid, workflow_id, yuki_dir) \
        not in TERMINAL_WORKFLOW_STATUSES


def workflow_is_active(project_uuid, workflow_id, yuki_dir=None):
    """Return whether a recorded workflow has not reached a terminal state."""
    return _workflow_status(project_uuid, workflow_id, yuki_dir) \
        not in TERMINAL_WORKFLOW_STATUSES


def active_owners(project_uuid, machine_id, jobs, yuki_dir=None):
    """Return active lease entries for the requested job IDs."""
    requested = set(jobs)
    with _registry(project_uuid, machine_id, yuki_dir) as (registry, _path):
        return {
            job: dict(entry)
            for job, entry in registry["leases"].items()
            if job in requested and _active(entry, project_uuid, yuki_dir)
        }


def claim_many(project_uuid, machine_id, workflow_id, jobs, yuki_dir=None):  # pylint: disable=too-many-locals
    """Atomically claim all jobs or raise without claiming any of them."""
    requested = tuple(sorted(set(jobs)))
    if not requested:
        return LeaseClaim("", requested)
    with _registry(project_uuid, machine_id, yuki_dir) as (registry, path):
        leases = registry["leases"]
        conflicts = {
            job: dict(leases[job])
            for job in requested
            if job in leases and _active(leases[job], project_uuid, yuki_dir)
            and leases[job].get("workflow_id") != workflow_id
        }
        if conflicts:
            owners = {entry.get("workflow_id") for entry in conflicts.values()}
            owner_runners = {entry.get("machine_id")
                             for entry in conflicts.values()}
            if (len(conflicts) == len(requested) and len(owners) == 1
                    and owner_runners == {machine_id}):
                owner = next(iter(owners))
                owner_sets = {tuple(entry.get("job_set", []))
                              for entry in conflicts.values()}
                if owner_sets == {requested}:
                    raise WorkflowAlreadyActive(owner, conflicts)
            raise WorkflowLeaseConflict(conflicts)

        token = uuid.uuid4().hex
        entry = {
            "workflow_id": workflow_id,
            "token": token,
            "machine_id": machine_id,
            "job_set": list(requested),
        }
        for job in requested:
            leases[job] = dict(entry)
        _write_registry(path, registry)
        return LeaseClaim(token, requested)


def validate_many(project_uuid, machine_id, workflow_id, token, jobs,
                  yuki_dir=None):
    """Return True only when every job is still owned by this exact lease."""
    requested = set(jobs)
    if not requested:
        return True
    with _registry(project_uuid, machine_id, yuki_dir) as (registry, _path):
        return all(
            registry["leases"].get(job, {}).get("workflow_id") == workflow_id
            and registry["leases"].get(job, {}).get("token") == token
            and registry["leases"].get(job, {}).get("machine_id") == machine_id
            for job in requested)


def owns(project_uuid, machine_id, workflow_id, token, job, yuki_dir=None):
    """Return whether one job is still owned by this exact lease."""
    return validate_many(project_uuid, machine_id, workflow_id, token,
                         [job], yuki_dir)
