"""Preview-bound EOS cleanup, executed and independently verified through REANA."""
from contextlib import contextmanager
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from CelebiChrono.utils.metadata import ConfigFile
from . import eos_usage
from ..kernel.storage import liveness


@contextmanager
def locked(plan_id, yuki_dir):
    eos_usage.validate_id(plan_id)
    folder = Path(yuki_dir) / "eos-cleanup"
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / f"{plan_id}.lock").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield folder / f"{plan_id}.json"


def manifest_text(manifest):
    return json.dumps(manifest, sort_keys=True, separators=(",", ":"))


def digest(manifest):
    return hashlib.sha256(manifest_text(manifest).encode()).hexdigest()


def _recent(timestamp):
    try:
        age = (eos_usage.now() - datetime.datetime.fromisoformat(timestamp)).total_seconds()
        return 0 <= age <= 86400
    except (TypeError, ValueError):
        return False


def _scan(scan_id, yuki_dir):
    eos_usage.validate_id(scan_id)
    state = eos_usage.read(eos_usage.directory(yuki_dir) / f"{scan_id}.json")
    if not state or state.get("status") != "done" or not state.get("inventory"):
        raise ValueError("A completed EOS scan is required")
    inventory = state["inventory"]
    if not inventory.get("complete") or not _recent(inventory.get("finished_at")):
        raise ValueError("Run a complete EOS scan within the last 24 hours")
    return state


def _entry(relative, project, usage, mode, reason):
    fingerprint = usage.get("metadata_sha256", "")
    if (not usage.get("complete") or len(fingerprint) != 64
            or any(c not in "0123456789abcdef" for c in fingerprint)):
        raise ValueError("This scan lacks complete metadata fingerprints; run eos-usage again")
    return {"relative": relative, "project": project, "mode": mode, "reason": reason,
            **{key: usage[key] for key in ("bytes", "files", "metadata_sha256")}}


def _relative(project, entry):
    if entry.get("layout") == "legacy_flat":
        return entry["path"]
    return f"{project}/{entry['impression']}"


def build_manifest(scan, projects, include_unknown, clear_projects, yuki_dir):
    report = eos_usage.classify(scan["inventory"], yuki_dir)
    known = {p["project"] for p in report["projects"]}
    if not projects or not set(projects) <= known:
        raise ValueError("Choose project UUIDs present in the completed scan")
    if not set(clear_projects) <= set(projects):
        raise ValueError("Every clear-project must belong to the selected scope")
    entries, protected = [], []
    skipped = len(report.get("unassigned", []))
    for item in report["projects"]:
        project = item["project"]
        clearing = project in clear_projects
        if clearing and item.get("directory_usage"):
            entries.append(_entry(project, project, item["directory_usage"],
                                  "whole_project", "explicit whole-project cleanup"))
        for row in item["impressions"]:
            relative = _relative(project, row)
            if not clearing and row["category"] == "live":
                protected.append(relative)
            if project not in projects:
                continue
            if clearing:
                if row.get("layout") == "legacy_flat":
                    entries.append(_entry(relative, project, row, "impression",
                                          "explicit whole-project cleanup (legacy cache)"))
            elif row["category"] == "non_live" or (include_unknown and row["category"] == "unknown"):
                entries.append(_entry(relative, project, row, "impression", row["category"]))
        if not clearing:
            skipped += len(item.get("unattributed", []))
    return {"schema": 1, "root": scan["root"], "source_scan": scan["id"],
            "projects": sorted(set(projects)), "clear_projects": sorted(set(clear_projects)),
            "include_unknown": include_unknown, "entries": entries,
            "protected": sorted(set(protected)), "skipped_unattributed": skipped}


def plan(scan_id, projects, include_unknown=False, clear_projects=(), yuki_dir=None):
    yuki_dir = yuki_dir or liveness._yuki_dir()
    if not isinstance(projects, list) or not isinstance(clear_projects, (list, tuple)):
        raise ValueError("projects and clear_projects must be lists")
    if not isinstance(include_unknown, bool):
        raise ValueError("include_unknown must be a boolean")
    for project in [*projects, *clear_projects]:
        eos_usage.validate_id(project)
    scan = _scan(scan_id, yuki_dir)
    manifest = build_manifest(scan, projects, include_unknown, clear_projects, yuki_dir)
    # Validate scope overlap/path constraints before publishing a preview.
    from .eos_delete import validate
    validate(manifest)
    plan_id = uuid.uuid4().hex
    state = {"id": plan_id, "status": "planned", "created_at": eos_usage.now().isoformat(),
             "runner": scan["runner"], "runner_id": scan["runner_id"],
             "root": scan["root"], "image": scan["image"], "manifest": manifest,
             "manifest_sha256": digest(manifest),
             "workflow": f"celebi-eos-cleanup-{plan_id}", "error": None}
    with locked(plan_id, yuki_dir) as path:
        eos_usage.save(path, state)
    return state


def call_reana(state, operation, yuki_dir):
    request = {key: state[key] for key in ("runner_id", "workflow", "root", "image")}
    request.update(operation=operation, yuki_dir=str(yuki_dir),
                   manifest=manifest_text(state["manifest"]))
    if "verify" in operation:
        request["workflow"] = state["workflow"] + "-verify"
    process = subprocess.run([sys.executable, "-m", "Yuki.services.eos_reana"],
                             input=json.dumps(request), text=True, capture_output=True,
                             timeout=90, check=True)
    for line in reversed(process.stdout.splitlines()):
        if line.startswith("EOS_USAGE_RESULT="):
            result = json.loads(line[len("EOS_USAGE_RESULT="):])
            if result.get("error"):
                raise RuntimeError(result["error"])
            return result
    raise RuntimeError("REANA helper returned no result")


def execute(plan_id, confirmed_hash, yuki_dir=None):
    yuki_dir = yuki_dir or liveness._yuki_dir()
    with locked(plan_id, yuki_dir) as path:
        state = eos_usage.read(path)
        if not state:
            raise FileNotFoundError("Cleanup plan not found")
        if confirmed_hash != state["manifest_sha256"]:
            raise ValueError("Confirmation does not match the previewed manifest")
        if state["status"] != "planned":
            return state  # Includes uncertain submissions: never delete twice on retry.
        if not _recent(state["created_at"]):
            raise ValueError("Plan expired; generate a new preview")
        manifest = state["manifest"]
        scan = _scan(manifest["source_scan"], yuki_dir)
        config = ConfigFile(os.path.join(yuki_dir, "config.json"))
        if (config.read_variable("runners_id", {}).get(state["runner"]) != state["runner_id"]
                or config.read_variable("backend_types", {}).get(
                    state["runner_id"], "reana") != "reana"
                or config.read_variable("eos_mount_point", {}).get(
                    state["runner_id"], "").rstrip("/") != state["root"]):
            raise ValueError("Runner or EOS root changed; generate a new preview")
        current = build_manifest(scan, manifest["projects"], manifest["include_unknown"],
                                 manifest["clear_projects"], yuki_dir)
        if digest(current) != state["manifest_sha256"]:
            raise ValueError("Liveness or ownership changed; generate a new preview")
        if not manifest["entries"]:
            state["status"] = "done"
            state["verification"] = {"status": "verified", "note": "Nothing selected"}
        else:
            state["status"] = "submitting"
            eos_usage.save(path, state)
            try:
                result = call_reana(state, "cleanup-submit", yuki_dir)
                state["status"] = "preflight_failed" if result.get("not_submitted") else "submitted"
                if result.get("not_submitted"):
                    state["error"] = result["reason"]
            except Exception as exc:
                state["status"] = "submission_unknown"
                state["error"] = (f"Cannot confirm submission ({type(exc).__name__}); "
                                  "query this plan ID, never resubmit under a new ID")
        eos_usage.save(path, state)
        return state


def status(plan_id, yuki_dir=None):
    yuki_dir = yuki_dir or liveness._yuki_dir()
    with locked(plan_id, yuki_dir) as path:
        state = eos_usage.read(path)
        if not state:
            raise FileNotFoundError("Cleanup plan not found")
        if state["status"] in ("planned", "done", "partial", "failed", "preflight_failed"):
            return state
        verifying = state["status"] in ("verifying", "verification_submitting", "verification_unknown")
        try:
            result = call_reana(state, "cleanup-verify-poll" if verifying else "cleanup-poll",
                                yuki_dir)
            state["remote_status"] = result["remote_status"]
            state["error"] = None
            report = result.get("cleanup_report")
            verification = result.get("verification")
            if verification is not None:
                if verification.get("manifest_sha256") != state["manifest_sha256"]:
                    raise ValueError("Verification manifest mismatch")
                state["verification"] = verification
                state["status"] = "done" if verification["status"] == "verified" else "partial"
            elif report is not None:
                if report.get("manifest_sha256") != state["manifest_sha256"]:
                    raise ValueError("Cleanup report manifest mismatch")
                state["cleanup_report"] = report
                if report["status"] == "deleted_unverified":
                    state["status"] = "verification_submitting"
                    eos_usage.save(path, state)  # Reserve before the read-only verification submission.
                    try:
                        call_reana(state, "cleanup-verify-submit", yuki_dir)
                        state["status"] = "verifying"
                    except Exception:
                        state["status"] = "verification_unknown"
                        state["error"] = "Cannot confirm verification submission; query this plan ID"
                else:
                    state["status"] = report["status"]
            elif result["remote_status"] in eos_usage.TERMINAL:
                state["status"] = "failed"
                state["error"] = "REANA workflow ended without a valid report"
            elif not verifying:
                state["status"] = result["remote_status"]
        except Exception as exc:
            state["error"] = f"Cannot refresh cleanup ({type(exc).__name__}); retry this plan ID"
        state["updated_at"] = eos_usage.now().isoformat()
        eos_usage.save(path, state)
        return state
