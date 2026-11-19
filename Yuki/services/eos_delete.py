"""Exact-manifest EOS cleanup worker (stdlib only, uploaded to REANA).

Verification is also run in a separate REANA job via --verify-only. Neither
mode follows symlinks, broadens the manifest, or retries a partial deletion.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys

try:
    from .eos_scan import measure
except ImportError:  # Standalone upload in REANA.
    from eos_scan import measure

UUID = re.compile(r"^[0-9a-f]{32}$")


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def resolve(root, relative):
    parts = relative.split("/")
    if len(parts) not in (1, 2) or not all(UUID.fullmatch(part) for part in parts):
        raise ValueError("Invalid manifest path")
    target = root.joinpath(*parts)
    for part in [target, *target.parents]:
        if part.is_symlink():
            raise ValueError("Symlink in target path")
        if part == root:
            break
    if root.resolve() not in target.resolve().parents:
        raise ValueError("Target escapes root")
    return target


def validate(manifest):
    root = Path(manifest["root"])
    if (manifest.get("schema") != 1 or not root.is_absolute()
            or not str(root).startswith("/eos/") or ".." in root.parts):
        raise ValueError("Invalid EOS root/schema")
    protected = [resolve(root, p) for p in manifest["protected"]]
    targets = []
    for entry in manifest["entries"]:
        target = resolve(root, entry["relative"])
        if entry["mode"] == "whole_project":
            if (entry["project"] not in manifest["clear_projects"]
                    or entry["relative"] != entry["project"]):
                raise ValueError("Whole-project deletion was not authorized")
        elif entry["mode"] != "impression":
            raise ValueError("Unsupported deletion mode")
        if any(target == p or target in p.parents or p in target.parents for p in protected):
            raise ValueError("Deletion overlaps a protected cache")
        if any(target == p or target in p.parents or p in target.parents for p in targets):
            raise ValueError("Overlapping deletion targets")
        if not re.fullmatch(r"[0-9a-f]{64}", entry.get("metadata_sha256", "")):
            raise ValueError("Missing metadata fingerprint: run a new EOS scan")
        targets.append(target)
    return root, targets, protected


def check_absent(path):
    """A lookup/listing disagreement is a residual, not successful deletion."""
    try:
        os.lstat(path)
        return False
    except FileNotFoundError:
        try:
            return path.name not in os.listdir(path.parent)
        except FileNotFoundError:
            return True
    # Permission errors deliberately propagate to the verification report.


def verify(manifest):
    root, targets, protected = validate(manifest)
    report = {"checked_at": now(), "remaining": [], "protected_missing": [], "errors": []}
    for target in targets:
        try:
            if not check_absent(target):
                report["remaining"].append(str(target.relative_to(root)))
        except OSError as exc:
            report["errors"].append({"path": str(target), "error": str(exc)})
    for target in protected:
        try:
            os.stat(target)
        except OSError as exc:
            report["protected_missing"].append(str(target.relative_to(root)))
            report["errors"].append({"path": str(target), "error": str(exc)})
    report["status"] = ("verified" if not any(report[k] for k in
                        ("remaining", "protected_missing", "errors")) else "partial")
    return report


def execute(manifest):
    root, targets, protected = validate(manifest)
    report = {"started_at": now(), "deleted": [], "already_missing": [],
              "failed": [], "preflight_errors": []}
    if not shutil.rmtree.avoids_symlink_attacks:
        raise RuntimeError("This Python build lacks symlink-safe rmtree")
    # Check the entire manifest before deleting even the first directory.
    for entry, target in zip(manifest["entries"], targets):
        if check_absent(target):
            report["already_missing"].append(entry["relative"])
            continue
        actual = measure(str(target))
        if (not actual["complete"] or any(actual[key] != entry[key]
                for key in ("bytes", "files", "metadata_sha256"))):
            report["preflight_errors"].append({"path": entry["relative"], "actual": actual})
    for target in protected:
        if not target.is_dir():
            report["preflight_errors"].append({"path": str(target),
                                               "error": "Protected cache missing"})
    if report["preflight_errors"]:
        report["status"] = "preflight_failed"
    else:
        for entry, target in zip(manifest["entries"], targets):
            if entry["relative"] in report["already_missing"]:
                continue
            try:
                # Recheck immediately before each deletion as well as the all-target preflight.
                actual = measure(str(target))
                if not actual["complete"] or actual["metadata_sha256"] != entry["metadata_sha256"]:
                    raise RuntimeError("Target changed after preflight")
                shutil.rmtree(target)
                report["deleted"].append(entry)
            except Exception as exc:
                report["failed"].append({"path": entry["relative"], "error": str(exc)})
                break  # No broad retry after partial progress.
        report["status"] = "partial" if report["failed"] else "deleted_unverified"
    report["finished_at"] = now()
    return report


if __name__ == "__main__":
    raw = Path("delete-manifest.json").read_bytes()
    manifest = json.loads(raw)
    verification = "--verify-only" in sys.argv
    try:
        result = verify(manifest) if verification else execute(manifest)
    except Exception as exc:
        result = {"status": "failed", "error": str(exc)}
    result["manifest_sha256"] = hashlib.sha256(raw).hexdigest()
    filename = "verification.json" if verification else "cleanup-report.json"
    Path(filename).write_text(json.dumps(result))
    print(json.dumps({"status": result["status"]}))
