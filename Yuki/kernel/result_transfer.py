"""Result transfer logic for celebi-cli transfer."""
import datetime
import fnmatch
import json
import os
from typing import List, Optional, Tuple

from Yuki.kernel.runners import config as runner_config
from Yuki.kernel.workflows.ssh import _SshConnection


def _resolve_yuki_dir():
    """Return the Yuki data root ($YUKIDIR or ~/.Yuki)."""
    return os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))


def _sanitize_rel_path(rel: str) -> str:
    """Validate a relative path and reject traversal/absolute paths."""
    if not rel:
        raise ValueError("empty relative path")
    if os.path.isabs(rel) or rel.startswith("/"):
        raise ValueError(f"absolute path not allowed: {rel}")
    parts = rel.replace(os.sep, "/").split("/")
    if ".." in parts:
        raise ValueError(f"path traversal not allowed: {rel}")
    return rel


def _parse_location(location: str) -> Tuple[str, Optional[str]]:
    """Parse 'yuki' or 'runner:<runner-id>' into (kind, runner_id)."""
    if location == "yuki":
        return "yuki", None
    if location.startswith("runner:"):
        runner_id = location[len("runner:"):]
        if not runner_id:
            raise ValueError("runner id is empty")
        return "runner", runner_id
    raise ValueError(f"invalid location: {location}")


def _list_local_files(root: str, pattern: Optional[str] = None) -> List[dict]:
    """List files under root as [{'name': rel_path, 'size': bytes}]."""
    result = []
    if not os.path.isdir(root):
        return result
    for dirpath, _dirs, filenames in os.walk(root):
        for fname in filenames:
            full = os.path.join(dirpath, fname)
            rel = os.path.relpath(full, root)
            rel = _sanitize_rel_path(rel)
            if pattern and not fnmatch.fnmatch(rel, pattern):
                continue
            result.append({"name": rel, "size": os.path.getsize(full)})
    return result


def _list_yuki_stageout(job_path: str,
                        pattern: Optional[str] = None) -> List[dict]:
    """List the impression's stageout files across all machine dirs.

    Yuki stores stageout files per runner machine:
    <job_path>/<machine_id>/stageout/. Entries carry 'full' source paths so
    copies can span the per-machine roots. Duplicate names keep the first
    machine's file (machine dirs are visited in sorted order).
    """
    result = []
    seen = set()
    if not os.path.isdir(job_path):
        return result
    for machine in sorted(os.listdir(job_path)):
        root = os.path.join(job_path, machine, "stageout")
        if not os.path.isdir(root):
            continue
        for entry in _list_local_files(root, pattern):
            if entry["name"] in seen:
                continue
            seen.add(entry["name"])
            entry["full"] = os.path.join(root, entry["name"])
            result.append(entry)
    return result


def _make_progress_dir(yuki_dir: str) -> str:
    """Create and return the transfer progress directory."""
    path = os.path.join(yuki_dir, "transfer-progress")
    os.makedirs(path, exist_ok=True)
    return path


def _aggregate(origin: str, files: List[dict]) -> dict:
    """Summarize a location's file listing for the distribution registry."""
    return {
        "origin": origin,
        "files": len(files),
        "bytes": sum(f.get("size", 0) for f in files),
        "updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def _runner_name(location: str) -> str:
    """Extract the runner name from a 'runner:<name>' location."""
    return location[len("runner:"):]


def _backend_type(runner_name: str) -> str:
    """Return the backend type registered for a runner."""
    config_file = runner_config.open_config()
    runners_id = config_file.read_variable("runners_id", {})
    backend_types = config_file.read_variable("backend_types", {})
    return backend_types.get(runners_id.get(runner_name, ""), "reana")


def _reana_context(project_uuid: str, impression: str, runner_name: str,
                   yuki_dir: str) -> Tuple[str, str, str, str]:
    """Resolve (url, token, workflow_id, prefix) for a reana runner."""
    config_file = runner_config.open_config()
    runners_id = config_file.read_variable("runners_id", {})
    runner_id = runners_id[runner_name]
    urls = config_file.read_variable("urls", {})
    tokens = config_file.read_variable("tokens", {})
    url = urls.get(runner_id, "")
    token = tokens.get(runner_id, "")
    job_path = os.path.join(yuki_dir, "Storage", project_uuid, impression)
    from Yuki.kernel.jobs.base import VJob
    workflow_id = VJob(job_path, runner_id).workflow_id()
    if not workflow_id:
        raise ValueError(
            f"no workflow recorded for reana runner '{runner_name}'")
    prefix = f"imp{impression[:7]}/stageout/"
    return url, token, workflow_id, prefix


def _reana_env(url: str, token: str) -> str:
    """Environment assignment for reana-cli commands."""
    return f"REANA_SERVER_URL='{url}' REANA_ACCESS_TOKEN='{token}'"


def _reana_cli_available(ssh: _SshConnection) -> bool:
    """Return whether the ssh host has reana-cli (reana-client)."""
    _out, _err, code = ssh.exec("which reana-client")
    return code == 0


def _reana_list_files(ssh: _SshConnection, workflow_id: str, url: str,
                      token: str, prefix: str) -> List[dict]:
    """List stageout files via reana-cli running on the ssh host.

    Returns entries as [{'name': rel, 'size': bytes}] with names relative
    to the stageout prefix.
    """
    out, err, code = ssh.exec(
        f"{_reana_env(url, token)} reana-client ls -w '{workflow_id}' "
        f"--format json")
    if code != 0:
        raise RuntimeError(f"reana-cli ls failed: {err or out}")
    try:
        files = json.loads(out)
    except ValueError as exc:
        raise RuntimeError(
            f"unparseable reana-cli ls output: {out!r}") from exc
    result = []
    for entry in files:
        name = entry["name"]
        if name.startswith(prefix):
            rel = name[len(prefix):]
            if rel:
                result.append(
                    {"name": rel, "size": int(entry.get("size", 0))})
    return result


# pylint: disable=too-many-arguments,too-many-positional-arguments
def _reana_pull_file(ssh: _SshConnection, workflow_id: str, name: str,
                     rel: str, dest_dir: str, url: str, token: str) -> None:
    """Download one workspace file on the ssh host into dest_dir/<rel>.

    reana-client download preserves the workspace path under the current
    directory, so the file is moved from dest_dir/<name> to dest_dir/<rel>.
    """
    out, err, code = ssh.exec(
        f"cd '{dest_dir}' && {_reana_env(url, token)} "
        f"reana-client download -w '{workflow_id}' '{name}' "
        f"&& mv -f '{name}' '{rel}'")
    if code != 0:
        raise RuntimeError(
            f"reana-cli download failed for {rel}: {err or out}")


def _resolve_path(location: str, project_uuid: str, impression: str,
                  yuki_dir: str = None) -> Tuple[str, Optional[str]]:
    """Return (path, runner_id) for a location.

    For yuki: ~/.Yuki/Storage/<project>/<impression> (the job path; stageout
    files live in per-machine <machine_id>/stageout/ subdirectories).
    For runner: <remote_workdir>/impressions/<project>/<impression>
    """
    kind, runner_name = _parse_location(location)
    if kind == "yuki":
        yuki_dir = yuki_dir or _resolve_yuki_dir()
        return os.path.join(yuki_dir, "Storage", project_uuid,
                            impression), None

    # runner
    yuki_dir = yuki_dir or _resolve_yuki_dir()
    config_file = runner_config.open_config()
    runners_id = config_file.read_variable("runners_id", {})
    if runner_name not in runners_id:
        raise ValueError(f"runner '{runner_name}' not found")
    runner_id = runners_id[runner_name]
    settings = runner_config.get_ssh_settings(config_file, runner_id)
    remote_workdir = settings.get("remote_workdir", "/tmp/yuki-workflows")
    remote_path = os.path.join(
        remote_workdir, "impressions", project_uuid, impression
    ).replace(os.sep, "/")
    return remote_path, runner_id


def _ssh_connection(runner_id: str) -> _SshConnection:
    """Build an SSH connection for runner_id from config."""
    config_file = runner_config.open_config()
    settings = runner_config.get_ssh_settings(config_file, runner_id)
    return _SshConnection(
        host=settings.get("host", ""),
        user=settings.get("user", ""),
        key_path=settings.get("key_path"),
        port=settings.get("port", 22),
    )


def _list_remote_files(ssh: _SshConnection, remote_root: str,
                       pattern: Optional[str] = None) -> List[dict]:
    """List files on the remote host under remote_root."""
    result = []
    if not ssh.exists(remote_root):
        return result
    for rel, full_path, size in ssh.walk_files(remote_root):
        rel = _sanitize_rel_path(rel)
        if pattern and not fnmatch.fnmatch(rel, pattern):
            continue
        result.append({"name": rel, "size": size, "remote_path": full_path})
    return result


# pylint: disable=too-many-arguments,too-many-positional-arguments
def _copy_local_to_local(src_root: str, dst_root: str, force: bool,
                         progress: dict, report: dict,
                         files: Optional[List[dict]] = None) -> None:
    """Copy files from src_root to dst_root.

    files optionally provides a precomputed listing; entries may carry a
    'full' source path (used when the listing spans multiple roots).
    """
    files = files if files is not None else _list_local_files(src_root)
    for entry in files:
        rel = entry["name"]
        progress["current_file"] = rel
        src_file = entry.get("full", os.path.join(src_root, rel))
        dst_file = os.path.join(dst_root, rel)
        os.makedirs(os.path.dirname(dst_file), exist_ok=True)
        if os.path.exists(dst_file) and not force:
            report["skipped"].append(rel)
            continue
        try:
            with open(src_file, "rb") as sf, open(dst_file, "wb") as df:
                while True:
                    chunk = sf.read(65536)
                    if not chunk:
                        break
                    df.write(chunk)
            progress["bytes_done"] += entry["size"]
            report["transferred"].append(rel)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            report["failed"].append({"file": rel, "reason": str(exc)})


# pylint: disable=too-many-arguments,too-many-positional-arguments
def _copy_local_to_remote(src_root: str, dst_root: str, ssh: _SshConnection,
                          force: bool, progress: dict, report: dict,
                          files: Optional[List[dict]] = None) -> None:
    """Upload files from src_root to dst_root on the remote host.

    files optionally provides a precomputed listing; entries may carry a
    'full' source path (used when the listing spans multiple roots).
    """
    files = files if files is not None else _list_local_files(src_root)
    for entry in files:
        rel = entry["name"]
        progress["current_file"] = rel
        src_file = entry.get("full", os.path.join(src_root, rel))
        dst_file = f"{dst_root}/{rel}"
        if ssh.exists(dst_file) and not force:
            report["skipped"].append(rel)
            continue
        try:
            ssh.put(src_file, dst_file)
            progress["bytes_done"] += entry["size"]
            report["transferred"].append(rel)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            report["failed"].append({"file": rel, "reason": str(exc)})


# pylint: disable=too-many-arguments,too-many-positional-arguments
def _copy_remote_to_local(src_root: str, dst_root: str, ssh: _SshConnection,
                          force: bool, progress: dict, report: dict) -> None:
    """Download files from src_root on the remote host to dst_root."""
    files = _list_remote_files(ssh, src_root)
    for entry in files:
        rel = entry["name"]
        progress["current_file"] = rel
        src_file = entry["remote_path"]
        dst_file = os.path.join(dst_root, rel)
        os.makedirs(os.path.dirname(dst_file), exist_ok=True)
        if os.path.exists(dst_file) and not force:
            report["skipped"].append(rel)
            continue
        try:
            ssh.get(src_file, dst_file)
            progress["bytes_done"] += entry["size"]
            report["transferred"].append(rel)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            report["failed"].append({"file": rel, "reason": str(exc)})


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
def _copy_remote_to_remote(src_root: str, dst_root: str,
                           src_ssh: _SshConnection, dst_ssh: _SshConnection,
                           force: bool, progress: dict, report: dict) -> None:
    """Stream files from src runner to dst runner through Yuki host."""
    import tempfile
    files = _list_remote_files(src_ssh, src_root)
    for entry in files:
        rel = entry["name"]
        src_file = entry["remote_path"]
        dst_file = f"{dst_root}/{rel}"
        progress["current_file"] = rel
        if dst_ssh.exists(dst_file) and not force:
            report["skipped"].append(rel)
            continue
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
                for chunk in src_ssh.stream(src_file):
                    tmp.write(chunk)
            dst_ssh.put(tmp_path, dst_file)
            progress["bytes_done"] += entry["size"]
            report["transferred"].append(rel)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            report["failed"].append({"file": rel, "reason": str(exc)})
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)


def _report_counts(report: dict) -> dict:
    """Return top-level count keys plus the full report."""
    return {
        "transferred": len(report["transferred"]),
        "skipped": len(report["skipped"]),
        "failed": len(report["failed"]),
        "report": report,
    }


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-statements
def run_transfer(job_id: str, project_uuid: str, impression: str,
                 source: str, destination: str,
                 pattern: Optional[str] = None, force: bool = False,
                 yuki_dir: str = None) -> dict:
    """Run a transfer job and return the final report."""
    yuki_dir = yuki_dir or _resolve_yuki_dir()
    progress_dir = _make_progress_dir(yuki_dir)
    progress_path = os.path.join(progress_dir, f"{job_id}.json")

    progress = {"bytes_done": 0, "bytes_total": 0, "current_file": ""}

    def write_status(status, extra=None):
        state = {
            "status": status,
            "bytes_done": progress["bytes_done"],
            "bytes_total": progress["bytes_total"],
            "current_file": progress.get("current_file", ""),
        }
        if extra:
            state.update(extra)
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump(state, f)

    report = {"transferred": [], "skipped": [], "failed": []}

    try:
        src_path, src_runner = _resolve_path(
            source, project_uuid, impression, yuki_dir)
        dst_path, dst_runner = _resolve_path(
            destination, project_uuid, impression, yuki_dir)

        if source == "yuki" and destination == "yuki":
            raise ValueError("source and destination cannot both be yuki")

        dist_override = None
        if source == "yuki" and destination.startswith("runner:"):
            with _ssh_connection(dst_runner) as dst_ssh:
                src_files = _list_yuki_stageout(src_path, pattern)
                progress["bytes_total"] = sum(f["size"] for f in src_files)
                write_status("running")
                _copy_local_to_remote(src_path, dst_path, dst_ssh, force,
                                      progress, report, files=src_files)
                dist_override = (destination, _aggregate(
                    "transferred", _list_remote_files(dst_ssh, dst_path)))
        elif source.startswith("runner:") and destination == "yuki":
            # Land under the source runner's machine dir so status reports
            # the files as IN YUKI for that runner.
            dst_root = os.path.join(dst_path, src_runner, "stageout")
            with _ssh_connection(src_runner) as src_ssh:
                src_files = _list_remote_files(src_ssh, src_path, pattern)
                progress["bytes_total"] = sum(f["size"] for f in src_files)
                write_status("running")
                _copy_remote_to_local(src_path, dst_root, src_ssh, force,
                                      progress, report)
                dist_override = (destination, _aggregate(
                    "transferred", _list_local_files(dst_root)))
        elif (source.startswith("runner:")
              and destination.startswith("runner:")
              and _backend_type(_runner_name(source)) == "reana"):
            # The ssh destination pulls the reana workspace files itself
            # with its own reana-cli; nothing flows through Yuki.
            url, token, workflow_id, prefix = _reana_context(
                project_uuid, impression, _runner_name(source), yuki_dir)
            with _ssh_connection(dst_runner) as dst_ssh:
                if not _reana_cli_available(dst_ssh):
                    raise RuntimeError(
                        f"ssh runner '{_runner_name(destination)}' does not "
                        "have reana-cli installed")
                src_files = _reana_list_files(
                    dst_ssh, workflow_id, url, token, prefix)
                if pattern:
                    src_files = [f for f in src_files
                                 if fnmatch.fnmatch(f["name"], pattern)]
                progress["bytes_total"] = sum(
                    f["size"] for f in src_files)
                write_status("running")
                for entry in src_files:
                    rel = entry["name"]
                    progress["current_file"] = rel
                    dst_file = f"{dst_path}/{rel}"
                    if dst_ssh.exists(dst_file) and not force:
                        report["skipped"].append(rel)
                        continue
                    try:
                        _reana_pull_file(dst_ssh, workflow_id, prefix + rel,
                                         rel, dst_path, url, token)
                        progress["bytes_done"] += entry["size"]
                        report["transferred"].append(rel)
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        report["failed"].append(
                            {"file": rel, "reason": str(exc)})
                dist_override = (destination, _aggregate(
                    "transferred", _list_remote_files(dst_ssh, dst_path)))
        elif source.startswith("runner:") and destination.startswith("runner:"):
            with _ssh_connection(src_runner) as src_ssh, \
                 _ssh_connection(dst_runner) as dst_ssh:
                src_files = _list_remote_files(src_ssh, src_path, pattern)
                progress["bytes_total"] = sum(f["size"] for f in src_files)
                write_status("running")
                _copy_remote_to_remote(src_path, dst_path, src_ssh, dst_ssh,
                                       force, progress, report)
                dist_override = (destination, _aggregate(
                    "transferred", _list_remote_files(dst_ssh, dst_path)))
        write_status("done", _report_counts(report))
        if dist_override:
            # Record the destination entry in the impression's distribution
            # registry, so Yuki knows where the data lives and how it got
            # there.
            from Yuki.kernel.impression_storage import ImpressionStorage
            ImpressionStorage(project_uuid, impression).update_distribution(
                overrides={dist_override[0]: dist_override[1]})
    except Exception as exc:  # pylint: disable=broad-exception-caught
        write_status("failed", {**_report_counts(report), "error": str(exc)})
        raise

    return report
