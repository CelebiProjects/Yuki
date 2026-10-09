"""Runner data inventory: what lives on a runner's disk and whether
Yuki knows about it.

ssh runners are walked over SFTP (the managed impressions cache and the
workflow workspaces); native runners are walked on the local filesystem
(LocalWorkflows). Each entry is classified against Yuki's local records:
registered (remote.json), recorded (a distribution.json cache entry),
or orphan (the runner holds data Yuki has no record of). Workflow
entries are matched against the local ~/.Yuki/Workflows mirror for
their project and status.
"""
import json
import os

from Yuki.kernel.runners import config as runner_config
from Yuki.kernel.file_staging import walk_files
from Yuki.kernel.workflows.ssh import _SshConnection


def _yuki_dir():
    """Yuki data root ($YUKIDIR or ~/.Yuki)."""
    return os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))


def _summarize(entries):
    """Build the {total_files, total_bytes, entries} section shape."""
    return {
        "total_files": sum(e["files"] for e in entries),
        "total_bytes": sum(e["bytes"] for e in entries),
        "entries": entries,
    }


def _runner_name(runner_id):
    """Runner name for a runner id (fallback: the id itself)."""
    config_file = runner_config.open_config()
    runners_id = config_file.read_variable("runners_id", {})
    for name, rid in runners_id.items():
        if rid == runner_id:
            return name
    return runner_id


def _cache_known(yuki_dir, project, impression, runner_id):
    """Classify a cache entry: registered / recorded / orphan."""
    imp_dir = os.path.join(yuki_dir, "Storage", project, impression)
    if os.path.isfile(os.path.join(imp_dir, "remote.json")):
        return "registered"
    dist_path = os.path.join(imp_dir, "distribution.json")
    if os.path.isfile(dist_path):
        try:
            with open(dist_path, encoding="utf-8") as fh:
                locations = json.load(fh).get("locations", {}) or {}
        except (OSError, ValueError):
            locations = {}
        key = f"runner:{_runner_name(runner_id)}"
        if isinstance(locations, dict) and locations.get(key, {}).get("cache"):
            return "recorded"
    return "orphan"


def _workflow_record(yuki_dir, project, workflow):
    """(known, status) from the local Workflows mirror, else (False, None)."""
    wf_dir = os.path.join(yuki_dir, "Workflows", project, workflow)
    if not os.path.isdir(wf_dir):
        return False, None
    status = None
    results_path = os.path.join(wf_dir, "results.json")
    if os.path.isfile(results_path):
        try:
            with open(results_path, encoding="utf-8") as fh:
                status = json.load(fh).get("results", {}).get("status")
        except (OSError, ValueError):
            status = None
    return True, status


def _inventory_ssh(runner_id, yuki_dir):  # pylint: disable=too-many-locals
    """Walk an ssh runner's impressions cache and workflow workspaces."""
    settings = runner_config.get_ssh_settings(
        runner_config.open_config(), runner_id)
    base = settings.get("remote_workdir", "/tmp/yuki-workflows")
    cache_root = f"{base}/impressions"
    workflows_root = f"{base}/workflows"

    cache_entries = []
    workflow_entries = []
    with _SshConnection(
            host=settings.get("host", ""),
            user=settings.get("user", ""),
            key_path=settings.get("key_path"),
            port=settings.get("port", 22)) as ssh:
        for project in ssh.listdir(cache_root):
            for impression in ssh.listdir(f"{cache_root}/{project}"):
                files = list(ssh.walk_files(
                    f"{cache_root}/{project}/{impression}"))
                cache_entries.append({
                    "project": project,
                    "impression": impression,
                    "files": len(files),
                    "bytes": sum(size for _, _, size in files),
                    "known": _cache_known(
                        yuki_dir, project, impression, runner_id),
                })
        for project in ssh.listdir(workflows_root):
            for workflow in ssh.listdir(f"{workflows_root}/{project}"):
                files = list(ssh.walk_files(
                    f"{workflows_root}/{project}/{workflow}"))
                known, status = _workflow_record(
                    yuki_dir, project, workflow)
                workflow_entries.append({
                    "project": project,
                    "workflow": workflow,
                    "files": len(files),
                    "bytes": sum(size for _, _, size in files),
                    "status": status,
                    "known": known,
                })
    return {"cache": _summarize(cache_entries),
            "workflows": _summarize(workflow_entries)}


def _find_project(yuki_dir, workflow):
    """Project of a local workflow uuid, or None when Yuki has no record."""
    workflows_root = os.path.join(yuki_dir, "Workflows")
    if os.path.isdir(workflows_root):
        for project in os.listdir(workflows_root):
            if os.path.isdir(os.path.join(
                    workflows_root, project, workflow)):
                return project
    return None


def _inventory_native(runner_id, yuki_dir):
    """Walk a native runner's LocalWorkflows directory."""
    settings = runner_config.get_runner_settings(
        runner_config.open_config(), runner_id)
    base = settings.get("workdir") or os.path.join(
        yuki_dir, "LocalWorkflows")

    entries = []
    if os.path.isdir(base):
        for workflow in sorted(os.listdir(base)):
            wf_dir = os.path.join(base, workflow)
            if not os.path.isdir(wf_dir):
                continue
            files = list(walk_files(wf_dir))
            project = _find_project(yuki_dir, workflow)
            known, status = False, None
            if project:
                known, status = _workflow_record(
                    yuki_dir, project, workflow)
            entries.append({
                "project": project,
                "workflow": workflow,
                "files": len(files),
                "bytes": sum(os.path.getsize(path) for _, path in files),
                "status": status,
                "known": known,
            })
    # Native runners have no managed cache.
    return {"cache": _summarize([]),
            "workflows": _summarize(entries)}


def inventory_runner(runner_id, backend_type):
    """Return the {cache, workflows} data inventory of a runner."""
    yuki_dir = _yuki_dir()
    if runner_config.is_ssh_backend(backend_type):
        return _inventory_ssh(runner_id, yuki_dir)
    if backend_type == "native":
        return _inventory_native(runner_id, yuki_dir)
    raise ValueError(f"backend '{backend_type}' has no listable data")
