"""Share complete Yuki impressions through a managed SSH runner store."""

import json
import os
import posixpath
import re
import shlex
import shutil
import tempfile

from CelebiChrono.utils.metadata import ConfigFile

from Yuki.kernel.storage import remote as remote_data_ops
from Yuki.kernel.storage.cache import (
    CACHE_COMPLETE_MARKER,
    CACHE_IN_PROGRESS_MARKER,
    is_cache_marker,
)
from Yuki.kernel.workflows.ssh import _SshConnection


UUID_RE = re.compile(r"^[0-9a-f]{32}$")
JOBS_DIR_NAME = "share-ssh-impression-jobs"
EXCLUDED_CREDENTIAL_FILES = frozenset({"config.local.json", "lbapi_key.json"})


def _connection(runner_id):
    settings = remote_data_ops._ssh_settings(runner_id)  # pylint: disable=protected-access
    return _SshConnection(
        host=settings.get("host", ""), user=settings.get("user", ""),
        key_path=settings.get("key_path"), port=settings.get("port", 22))


def _validate(project_uuid, impressions):
    if (not project_uuid or project_uuid in (".", "..")
            or "/" in project_uuid or "\\" in project_uuid):
        raise ValueError("invalid project UUID")
    if not impressions:
        raise ValueError("no impressions selected")
    invalid = [value for value in impressions
               if not isinstance(value, str) or not UUID_RE.fullmatch(value)]
    if invalid:
        raise ValueError("invalid impression UUID: " + str(invalid[0]))


def _update(yuki_dir, job_id, values):
    current = remote_data_ops.read_job_state(
        yuki_dir, job_id, jobs_dir_name=JOBS_DIR_NAME) or {}
    current.update(values)
    remote_data_ops.write_job_state(
        yuki_dir, job_id, current, jobs_dir_name=JOBS_DIR_NAME)


def _share_root(runner_id, project_uuid, yuki_dir):
    settings = remote_data_ops._ssh_settings(  # pylint: disable=protected-access
        runner_id, yuki_dir)
    base = settings.get("remote_workdir", "/tmp/yuki-workflows")
    return posixpath.join(base, "shared-impressions", project_uuid)


def _local_files(root):
    result = []
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(name for name in dirs if not os.path.islink(
            os.path.join(directory, name)))
        for name in sorted(files):
            if name in EXCLUDED_CREDENTIAL_FILES:
                continue
            path = os.path.join(directory, name)
            if os.path.islink(path):
                continue
            result.append((os.path.relpath(path, root).replace(os.sep, "/"),
                           path, os.path.getsize(path)))
    return result


def _safe_relative_path(path):
    normalized = path.replace("\\", "/")
    parts = normalized.split("/")
    if (not normalized or normalized.startswith("/")
            or ".." in parts or any(not part for part in parts)):
        raise ValueError(f"unsafe shared impression path: {path}")
    return normalized


def _remote_complete(ssh, path):
    return (ssh.exists(posixpath.join(path, CACHE_COMPLETE_MARKER))
            and not ssh.exists(posixpath.join(path, CACHE_IN_PROGRESS_MARKER)))


def _prepare_remote(ssh, path):
    quoted = shlex.quote(path)
    in_progress = shlex.quote(posixpath.join(path, CACHE_IN_PROGRESS_MARKER))
    out, err, code = ssh.exec(
        f"if [ -e {quoted} ]; then chmod -R u+w {quoted} 2>/dev/null || true; "
        f"rm -rf -- {quoted}; fi; mkdir -p {quoted} && touch {in_progress}",
        timeout=3600)
    if code:
        raise RuntimeError(f"could not prepare shared impression: {err or out}")


def _finalize_remote(ssh, path):
    complete = shlex.quote(posixpath.join(path, CACHE_COMPLETE_MARKER))
    in_progress = shlex.quote(posixpath.join(path, CACHE_IN_PROGRESS_MARKER))
    quoted = shlex.quote(path)
    out, err, code = ssh.exec(
        f"rm -f {in_progress} && touch {complete} && "
        f"find {quoted} -mindepth 1 -maxdepth 1 -exec chmod -R a-w -- {{}} +",
        timeout=3600)
    if code:
        raise RuntimeError(f"could not finalize shared impression: {err or out}")


def _export(job_id, runner_id, project_uuid, impressions, force, yuki_dir):
    shared_root = _share_root(runner_id, project_uuid, yuki_dir)
    sources = {}
    missing = []
    for impression in impressions:
        source = os.path.join(yuki_dir, "Storage", project_uuid, impression)
        if os.path.isdir(source):
            sources[impression] = _local_files(source)
        else:
            missing.append(impression)
    total = sum(size for files in sources.values() for _rel, _path, size in files)
    done = 0
    exported, skipped, failed = [], [], []
    _update(yuki_dir, job_id, {
        "status": "exporting", "bytes_done": 0, "bytes_total": total})
    with _connection(runner_id) as ssh:
        for impression, files in sources.items():
            destination = posixpath.join(shared_root, impression)
            if _remote_complete(ssh, destination) and not force:
                skipped.append(impression)
                done += sum(size for _rel, _path, size in files)
                continue
            try:
                _prepare_remote(ssh, destination)
                for rel, source, size in files:
                    ssh.put(source, posixpath.join(destination, rel))
                    done += size
                    _update(yuki_dir, job_id, {
                        "status": "exporting", "bytes_done": done,
                        "bytes_total": total, "current_impression": impression,
                        "current_file": rel})
                _finalize_remote(ssh, destination)
                exported.append(impression)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                failed.append({"impression": impression,
                               "reason": str(exc) or type(exc).__name__})
    return {"operation": "export", "count": len(exported),
            "impressions": exported, "skipped": skipped,
            "missing": missing, "failed": failed}


def _merge_tree(source, destination, force):
    copied, skipped = 0, 0
    for rel, path, _size in _local_files(source):
        target = os.path.join(destination, *rel.split("/"))
        if os.path.exists(target) and not force:
            skipped += 1
            continue
        os.makedirs(os.path.dirname(target), exist_ok=True)
        os.replace(path, target)
        copied += 1
    return copied, skipped


def _import(job_id, runner_id, project_uuid, impressions, force, yuki_dir):
    shared_root = _share_root(runner_id, project_uuid, yuki_dir)
    imported, missing, skipped, failed = [], [], [], []
    done = 0
    _update(yuki_dir, job_id, {
        "status": "importing", "bytes_done": 0, "bytes_total": 0})
    work_dir = tempfile.mkdtemp(prefix="share-impressions-", dir=yuki_dir)
    try:
        with _connection(runner_id) as ssh:
            selected = []
            total = 0
            for impression in impressions:
                source = posixpath.join(shared_root, impression)
                if not _remote_complete(ssh, source):
                    missing.append(impression)
                    continue
                files = [(rel, remote, size)
                         for rel, remote, size in ssh.walk_files(source)
                         if not is_cache_marker(rel)]
                files = [(_safe_relative_path(rel), remote, size)
                         for rel, remote, size in files]
                selected.append((impression, files))
                total += sum(size for _rel, _remote, size in files)
            _update(yuki_dir, job_id, {"bytes_total": total})
            for impression, files in selected:
                staged = os.path.join(work_dir, impression)
                try:
                    os.makedirs(staged, exist_ok=True)
                    for rel, remote, size in files:
                        local = os.path.join(staged, *rel.split("/"))
                        ssh.get(remote, local)
                        done += size
                        _update(yuki_dir, job_id, {
                            "status": "importing", "bytes_done": done,
                            "bytes_total": total,
                            "current_impression": impression,
                            "current_file": rel})
                    config_path = os.path.join(staged, "config.json")
                    if not os.path.isfile(config_path):
                        raise ValueError("shared impression is missing config.json")
                    with open(config_path, encoding="utf-8") as stream:
                        if not isinstance(json.load(stream), dict):
                            raise ValueError("shared config.json is not an object")
                    destination = os.path.join(
                        yuki_dir, "Storage", project_uuid, impression)
                    copied, existing = _merge_tree(staged, destination, force)
                    if copied:
                        config_path = os.path.join(destination, "config.json")
                        if os.path.isfile(config_path):
                            ConfigFile(config_path).write_variable(
                                "origin", "imported")
                        imported.append(impression)
                    else:
                        skipped.append(impression)
                    if existing:
                        skipped.append({"impression": impression,
                                        "existing_files": existing})
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    failed.append({"impression": impression,
                                   "reason": str(exc) or type(exc).__name__})
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
    return {"operation": "import", "count": len(imported),
            "impressions": imported, "skipped": skipped,
            "missing": missing, "failed": failed}


def run_share_job(job_id, runner_id, project_uuid, operation, impressions,
                  force=False, yuki_dir=None):
    """Run one project-scoped SSH impression sharing job."""
    yuki_dir = yuki_dir or remote_data_ops._yuki_dir()  # pylint: disable=protected-access
    _validate(project_uuid, impressions)
    if operation == "export":
        return _export(job_id, runner_id, project_uuid, impressions,
                       force, yuki_dir)
    if operation == "import":
        return _import(job_id, runner_id, project_uuid, impressions,
                       force, yuki_dir)
    raise ValueError("operation must be import or export")
