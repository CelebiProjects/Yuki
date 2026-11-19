"""Standalone, read-only EOS inventory program uploaded to a REANA job.

Only stdlib imports: this file also runs without Yuki installed in the image.
Sizes are logical regular-file bytes, not replicated or allocated disk usage.
"""
import datetime
import hashlib
import json
import os
import re
import stat
import sys

UUID = re.compile(r"^[0-9a-f]{32}$")


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def measure(path):
    """Do not follow symlinks; surface errors instead of inventing zero usage."""
    result = {"bytes": 0, "files": 0, "complete": True, "errors": []}

    def error(message):
        result["complete"] = False
        if len(result["errors"]) < 20:
            result["errors"].append(str(message))

    digest = hashlib.sha256()

    def record(filename, info):
        relative = os.path.relpath(filename, path)
        digest.update(json.dumps([relative, info.st_mode, info.st_mtime_ns,
                                  info.st_size if stat.S_ISREG(info.st_mode) else 0],
                                 separators=(",", ":")).encode())
        digest.update(b"\n")

    def walk(directory):
        try:
            record(directory, os.stat(directory, follow_symlinks=False))
            with os.scandir(directory) as entries:
                for entry in sorted(entries, key=lambda item: item.name):
                    try:
                        info = entry.stat(follow_symlinks=False)
                        if stat.S_ISDIR(info.st_mode):
                            walk(entry.path)
                        elif stat.S_ISREG(info.st_mode):
                            record(entry.path, info)
                            result["bytes"] += info.st_size
                            result["files"] += 1
                        else:
                            error(f"Skipped non-regular entry: {entry.path}")
                    except OSError as exc:
                        error(exc)
        except OSError as exc:
            error(exc)
    walk(path)
    result["metadata_sha256"] = digest.hexdigest()
    return result



def measure_entry(path):
    """Measure an unknown-layout entry without following symbolic links."""
    try:
        info = os.lstat(path)
        if stat.S_ISDIR(info.st_mode):
            return measure(path)
        if stat.S_ISREG(info.st_mode):
            return {"bytes": info.st_size, "files": 1, "complete": True, "errors": []}
        return {"bytes": 0, "files": 0, "complete": False,
                "errors": ["Skipped non-regular entry"]}
    except OSError as exc:
        return {"bytes": 0, "files": 0, "complete": False, "errors": [str(exc)]}

def scan(root, project=None, known_projects=(), legacy_impressions=()):
    report = {"schema": 1, "root": root, "started_at": now(),
              "projects": [], "unassigned": [], "errors": [], "complete": True}
    try:
        with os.scandir(root) as entries:
            candidates = sorted(entry.name for entry in entries
                                if project is None or entry.name == project
                                or entry.name in legacy_impressions)
        if project and not candidates:
            raise FileNotFoundError(f"Project directory missing: {project}")
        for name in candidates:
            path = os.path.join(root, name)
            if os.path.islink(path) or not os.path.isdir(path) or not UUID.fullmatch(name):
                usage = measure_entry(path)
                report["unassigned"].append({"path": name, "impression": None, **usage})
                report["complete"] &= usage["complete"]
                continue
            # Old caches have root/<impression>/files, not root/<project>/<impression>.
            # Shape is only a fallback; known project IDs disambiguate empty projects.
            try:
                with os.scandir(path) as children:
                    structured = name in known_projects or name == project or any(
                        UUID.fullmatch(child.name) and child.is_dir(follow_symlinks=False)
                        for child in children)
            except OSError:
                structured = name in known_projects or name == project
            if not structured:
                usage = measure(path)
                report["unassigned"].append({"path": name, "impression": name, **usage})
                report["complete"] &= usage["complete"]
                continue
            item = {"project": name, "impressions": [], "unattributed": [], "errors": [], "complete": True}
            report["projects"].append(item)
            try:
                with os.scandir(path) as entries:
                    for entry in sorted(entries, key=lambda e: e.name):
                        if (not UUID.fullmatch(entry.name) or entry.is_symlink()
                                or not entry.is_dir(follow_symlinks=False)):
                            usage = measure_entry(entry.path)
                            item["unattributed"].append({"path": entry.name, **usage})
                            item["complete"] &= usage["complete"]
                            continue
                        usage = measure(entry.path)
                        item["impressions"].append({"impression": entry.name, **usage})
                        item["complete"] &= usage["complete"]
            except OSError as exc:
                item["errors"].append(str(exc))
                item["complete"] = False
            item["directory_usage"] = measure(path)
            item["complete"] &= item["directory_usage"]["complete"]
            report["complete"] &= item["complete"]
    except OSError as exc:
        report["errors"].append(str(exc))
        report["complete"] = False
    report["finished_at"] = now()
    return report


if __name__ == "__main__":
    with open("scan-request.json", encoding="utf-8") as stream:
        request = json.load(stream)
    result = scan(request["root"], request.get("project"),
                  request.get("known_projects", []), request.get("legacy_impressions", []))
    with open("eos-usage.json", "w", encoding="utf-8") as stream:
        json.dump(result, stream)
    print(json.dumps({"complete": result["complete"], "projects": len(result["projects"])}))
