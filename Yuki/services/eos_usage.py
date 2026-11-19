"""Persistent, resumable EOS usage scans; no EOS access from the Yuki host."""
from contextlib import contextmanager
import copy
import datetime
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.storage import liveness

ACTIVE = {"running", "in movement", "queued", "pending", "waiting", "prelude",
          "submitted", "composing", "built", "orchestrating", "ready", "tuning"}

TERMINAL = {"finished", "success", "coda", "failed", "stopped", "deleted",
            "archived", "final note", "killed", "dissonance"}


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def validate_id(value):
    if not isinstance(value, str) or not liveness.UUID_RE.fullmatch(value):
        raise ValueError("Expected a 32-character lowercase hexadecimal ID")
    return value


def directory(yuki_dir=None):
    return Path(yuki_dir or liveness._yuki_dir()) / "eos-usage"


@contextmanager
def locked(scan_id, yuki_dir=None):
    validate_id(scan_id)
    folder = directory(yuki_dir)
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / f"{scan_id}.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield folder / f"{scan_id}.json"


def save(path, state):
    fd, temp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(state, stream)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def read(path):
    return json.loads(path.read_text()) if path.exists() else None


def reana_call(state, operation, yuki_dir):
    request = {key: state[key] for key in
               ("runner_id", "workflow", "root", "project", "image")}
    known = project_impressions(yuki_dir)
    request.update(operation=operation, yuki_dir=str(yuki_dir), known_projects=list(known),
                   legacy_impressions=sorted(known.get(state["project"], set())))
    # An isolated process owns the SDK's global URL and credential context.
    process = subprocess.run(
        [sys.executable, "-m", "Yuki.services.eos_reana"],
        input=json.dumps(request), text=True, capture_output=True, timeout=90, check=True)
    for line in reversed(process.stdout.splitlines()):
        if line.startswith("EOS_USAGE_RESULT="):
            result = json.loads(line[len("EOS_USAGE_RESULT="):])
            if result.get("error"):
                raise RuntimeError(result["error"])
            return result
    raise RuntimeError("REANA helper returned no result")


def start(runner, scan_id, project=None, yuki_dir=None):
    """An ID is reserved before contacting REANA; retries never submit twice."""
    validate_id(scan_id)
    if project is not None:
        validate_id(project)
    yuki_dir = yuki_dir or liveness._yuki_dir()
    with locked(scan_id, yuki_dir) as path:
        previous = read(path)
        if previous:
            if previous["runner"] != runner or previous["project"] != project:
                raise ValueError("Scan ID already belongs to a different request")
            return previous
        config = ConfigFile(os.path.join(yuki_dir, "config.json"))
        runner_id = config.read_variable("runners_id", {}).get(runner)
        if not runner_id:
            raise ValueError("Runner not found")
        if config.read_variable("backend_types", {}).get(runner_id, "reana") != "reana":
            raise ValueError("EOS usage requires a REANA runner")
        root = config.read_variable("eos_mount_point", {}).get(runner_id, "")
        if not isinstance(root, str) or not root.startswith("/eos/") or ".." in root.split("/"):
            raise ValueError("Runner needs an absolute eos_mount_point under /eos/")
        state = {"id": scan_id, "runner": runner, "runner_id": runner_id,
                 "workflow": f"celebi-eos-usage-{scan_id}", "project": project,
                 "root": root.rstrip("/"), "created_at": now().isoformat(),
                 "image": config.read_variable("eos_usage_image", {}).get(
                     runner_id, "python:3.11-slim"),
                 "status": "submitting", "error": None}
        save(path, state)
        try:
            reana_call(state, "submit", yuki_dir)
            state["status"] = "submitted"
        except Exception as exc:  # Submission may have reached REANA; never retry automatically.
            state["status"] = "submission_unknown"
            state["error"] = ("Submission could not be confirmed "
                              f"({type(exc).__name__}). Query this scan ID; do not resubmit.")
        save(path, state)
        return state


def active_impressions(project, yuki_dir):
    """Conservatively protect inputs and outputs of nonterminal workflow mirrors."""
    protected = set()
    uncertain = False
    folder = Path(yuki_dir) / "Workflows" / project
    if not folder.exists():
        return protected, uncertain
    try:
        for workflow in folder.iterdir():
            if not workflow.is_dir():
                continue
            try:
                config = json.loads((workflow / "config.json").read_text())
                results = json.loads((workflow / "results.json").read_text())
                status = results.get("results", {}).get("status", "unknown")
                if status not in TERMINAL:
                    if status not in ACTIVE:
                        uncertain = True
                        continue
                    jobs = config.get("jobs_info")
                    if not isinstance(jobs, dict) or not jobs:
                        uncertain = True
                    else:
                        protected.update(jobs)
            except (OSError, ValueError, AttributeError, TypeError):
                uncertain = True
    except OSError:
        uncertain = True
    return protected, uncertain



def project_impressions(yuki_dir):
    """Known project ownership from storage and synced sets, without EOS access."""
    projects = {}
    for category in ("Storage", "Live"):
        folder = Path(yuki_dir) / category
        if not folder.exists():
            continue
        for entry in folder.iterdir():
            project = entry.stem if category == "Live" else entry.name
            if not liveness.UUID_RE.fullmatch(project):
                continue
            members = projects.setdefault(project, set())
            if category == "Storage" and entry.is_dir():
                members.update(child.name for child in entry.iterdir()
                               if liveness.UUID_RE.fullmatch(child.name) and child.is_dir())
            elif category == "Live":
                live = liveness.load_live_set(project, yuki_dir) or {}
                for key in ("live", "superseded"):
                    values = live.get(key, [])
                    if isinstance(values, list):
                        members.update(value for value in values if isinstance(value, str)
                                       and liveness.UUID_RE.fullmatch(value))
    return projects


def attribute_legacy(inventory, yuki_dir):
    """Only a unique recorded owner may turn a flat legacy cache into project usage."""
    inventory = copy.deepcopy(inventory)
    owners = {}
    for project, impressions in project_impressions(yuki_dir).items():
        for impression in impressions:
            owners.setdefault(impression, set()).add(project)
    projects = {item["project"]: item for item in inventory["projects"]}
    unassigned = []
    for entry in inventory.get("unassigned", []):
        matches = owners.get(entry.get("impression"), set())
        if len(matches) != 1:
            unassigned.append({**entry, "reason": "no unique recorded project owner"})
            continue
        project = next(iter(matches))
        item = projects.setdefault(project, {"project": project, "impressions": [],
                                            "errors": [], "complete": True})
        item["impressions"].append({**entry, "layout": "legacy_flat"})
        item["complete"] &= entry["complete"]
    inventory["projects"] = sorted(projects.values(), key=lambda item: item["project"])
    inventory["unassigned"] = unassigned
    inventory["unassigned_usage"] = {
        "bytes": sum(entry["bytes"] for entry in unassigned),
        "files": sum(entry["files"] for entry in unassigned), "entries": len(unassigned)}
    return inventory

def classify(inventory, yuki_dir, at=None):
    """Only explicit superseded entries in recent, valid live sets are non-live."""
    if not isinstance(inventory, dict) or inventory.get("schema") != 1:
        raise ValueError("Unsupported EOS inventory schema")
    inventory = attribute_legacy(inventory, yuki_dir)
    at = at or now()
    projects = []
    for item in inventory["projects"]:
        project = validate_id(item["project"])
        live = liveness.load_live_set(project, yuki_dir)
        usable = False
        if live:
            try:
                liveness.validate_sets(live["live"], live["superseded"])
                updated = datetime.datetime.fromisoformat(live["updated"])
                age = (at - updated).total_seconds()
                usable = 0 <= age <= 86400
            except (KeyError, TypeError, ValueError):
                pass
        protected, uncertain = active_impressions(project, yuki_dir)
        buckets = {name: {"bytes": 0, "files": 0, "impressions": 0}
                   for name in ("live", "non_live", "unknown")}
        for entry in item.get("unattributed", []):
            buckets["unknown"]["bytes"] += entry["bytes"]
            buckets["unknown"]["files"] += entry["files"]
        rows = []
        for entry in item["impressions"]:
            impression = validate_id(entry["impression"])
            category, reason = "unknown", "missing, invalid or older-than-24h live set"
            if not entry["complete"]:
                reason = "incomplete scan; byte count is a lower bound"
            elif impression in protected:
                category, reason = "live", "referenced by a nonterminal workflow"
            elif usable:
                if impression in live["live"]:
                    category, reason = "live", "synced live set"
                elif impression in live["superseded"] and not uncertain:
                    category, reason = "non_live", "explicitly superseded in synced live set"
                else:
                    reason = "unknown workflow state or impression absent from synced sets"
            for field in ("bytes", "files"):
                value = entry[field]
                if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                    raise ValueError("Invalid scan size/count")
                buckets[category][field] += value
            buckets[category]["impressions"] += 1
            rows.append({**entry, "category": category, "reason": reason})
        projects.append({**item, "impressions": rows, "usage": buckets,
                         "total": {key: sum(bucket[key] for bucket in buckets.values())
                                   for key in ("bytes", "files", "impressions")},
                         "live_updated_at": live.get("updated") if live else None,
                         "live_set_usable": usable})
    return {**inventory, "projects": projects, "classified_at": at.isoformat(),
            "size_kind": "logical_bytes", "live_max_age_hours": 24,
            "note": "Non-live is not permission to delete. Historical refs are not automatically live."}


def poll(scan_id, yuki_dir=None):
    yuki_dir = yuki_dir or liveness._yuki_dir()
    with locked(scan_id, yuki_dir) as path:
        state = read(path)
        if state is None:
            raise FileNotFoundError("Scan not found")
        # Reclassify cached bytes against today's live sets; never silently rescan EOS.
        if state.get("inventory") is not None:
            state["report"] = classify(state["inventory"], yuki_dir)
        elif state["status"] != "failed":
            try:
                result = reana_call(state, "poll", yuki_dir)
                status = result["remote_status"]
                state["remote_status"] = status
                state["error"] = None
                if "inventory" in result:
                    state["report"] = classify(result["inventory"], yuki_dir)
                    state["inventory"] = result["inventory"]
                    state["status"] = "done"
                elif status in TERMINAL:
                    state["status"] = "failed"
                    state["error"] = f"REANA scan ended with status {status}"
                else:
                    state["status"] = status
            except Exception as exc:
                state["error"] = f"Cannot refresh REANA scan ({type(exc).__name__}); retry this ID."
        state["updated_at"] = now().isoformat()
        save(path, state)
        return state
