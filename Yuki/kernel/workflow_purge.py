"""Purge non-live workflow workspaces from a runner."""
import os
import shlex

from CelebiChrono.utils.metadata import ConfigFile

from . import liveness
from . import runner_config
from .ssh_workflow import _SshConnection
from .status_constants import IN_MOVEMENT, translate_to_musical
from .vworkflow import VWorkflow


def _project_live_workflows(project, yuki_dir):
    """Fresh per-project live workflow uuids, or None without a synced set.

    Liveness is derived at purge time rather than read from the stored
    snapshot's live_workflows: a re-run of a live impression after the
    last sync points at a new workflow uuid the snapshot does not list.
    """
    live_set = liveness.load_live_set(project, yuki_dir)
    if live_set is None:
        return None
    return liveness.derive_live_workflows(
        project, live_set.get("live", []), yuki_dir)


def _record_purged(workflow_dir):
    """Record workspace_purged_at in the workflow mirror's config.json."""
    import datetime
    ConfigFile(os.path.join(workflow_dir, "config.json")).write_variable(
        "workspace_purged_at",
        datetime.datetime.now(datetime.timezone.utc).isoformat())


def record_workspace_purged(workflow):
    """Record the purge in the workflow mirror's config.json.

    Called by every deletion path so the mirror — Yuki's kept record —
    knows the runner-side workspace is gone and future purge scans skip
    it instead of re-listing it.
    """
    _record_purged(workflow.path)


def _runner_backend_type(runner_id):
    return runner_config.open_config().read_variable(
        "backend_types", {}).get(runner_id, "reana")


def _workspace_paths(backend, runner_id, candidates, yuki_dir):
    """Map each candidate (project, workflow) to its workspace path."""
    config_file = runner_config.open_config()
    paths = {}
    if runner_config.is_ssh_backend(backend):
        settings = runner_config.get_ssh_settings(config_file, runner_id)
        base = settings.get("remote_workdir", "/tmp/yuki-workflows")
        for project, workflow_uuid in candidates:
            paths[(project, workflow_uuid)] = \
                f"{base}/workflows/{project}/{workflow_uuid}"
    elif backend == "native":
        settings = runner_config.get_runner_settings(config_file, runner_id)
        base = settings.get("workdir") or os.path.join(
            yuki_dir, "LocalWorkflows")
        for project, workflow_uuid in candidates:
            paths[(project, workflow_uuid)] = os.path.join(
                base, workflow_uuid)
    return paths


def _existing_ssh_workspaces(runner_id, paths, timeout=600):
    """The subset of paths that exist on the ssh runner (one round trip).

    A failed check returns all paths: the sweep then attempts the
    deletions and reports per-entry failures, as before.
    """
    settings = runner_config.get_ssh_settings(
        runner_config.open_config(), runner_id)
    with _SshConnection(host=settings.get("host", ""),
                        user=settings.get("user", ""),
                        key_path=settings.get("key_path"),
                        port=settings.get("port", 22)) as ssh:
        command = "; ".join(
            f"test -d {shlex.quote(p)} && echo {shlex.quote(p)}"
            for p in paths)
        out, _err, code = ssh.exec(command, timeout=timeout)
        if code != 0:
            return set(paths)
        return {line.strip() for line in out.splitlines() if line.strip()}


def purge_stale_workflows(runner_id, dry_run=False, yuki_dir=None,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-branches,too-many-statements
                           project_uuid=None):
    """Delete the runner-side workspaces of workflows whose projects'
    synced live sets exclude them.

    Workflows are found in the local mirror
    ~/.Yuki/Workflows/<project>/<workflow> where config.json machine_id
    equals runner_id (covers ssh, native, and reana uniformly). With
    project_uuid given, only that project's workflows are scanned. Live
    workflows are derived fresh at purge time from the projects' run
    configs (a re-run after the last sync points at a new workflow the
    stored snapshot does not list); running workflows and workflows
    without an explicitly synced set are skipped with a reason. A
    corrupt mirror entry is a per-workflow skip, never a sweep abort.
    The mirror is always kept.

    Yuki records every purge: a successful deletion writes
    workspace_purged_at into the mirror, so later scans skip it; mirrors
    whose runner-side workspace is already gone (e.g. purged before
    records existed) are healed with the same record and counted in
    already_gone instead of being re-listed.

    Returns {"purged": [...], "skipped": [...], "already_gone": n,
    "dry_run": bool}.
    """
    yuki_dir = yuki_dir or liveness._yuki_dir()  # pylint: disable=protected-access
    workflows_root = os.path.join(yuki_dir, "Workflows")
    purged, skipped, already_gone = [], [], 0
    if not os.path.isdir(workflows_root):
        return {"purged": purged, "skipped": skipped,
                "already_gone": already_gone, "dry_run": bool(dry_run)}

    backend = _runner_backend_type(runner_id)
    projects = [project_uuid] if project_uuid else sorted(
        os.listdir(workflows_root))
    for project in projects:
        project_dir = os.path.join(workflows_root, project)
        if not os.path.isdir(project_dir):
            continue
        # Cached per project for the whole sweep (fresh per invocation).
        live_workflows = _project_live_workflows(project, yuki_dir)
        candidates = []
        for workflow_uuid in sorted(os.listdir(project_dir)):
            workflow_dir = os.path.join(project_dir, workflow_uuid)
            if not os.path.isdir(workflow_dir):
                continue
            entry = {"project": project, "workflow": workflow_uuid}
            try:
                workflow_config = ConfigFile(
                    os.path.join(workflow_dir, "config.json"))
                if workflow_config.read_variable("machine_id", "") != runner_id:
                    continue
                if workflow_config.read_variable(
                        "workspace_purged_at", ""):
                    already_gone += 1
                    continue
                if live_workflows is None:
                    skipped.append({**entry,
                                    "reason": "no live set synced"})
                    continue
                if workflow_uuid in live_workflows:
                    skipped.append({**entry, "reason": "live"})
                    continue
                workflow = VWorkflow.create(project, [], workflow_uuid)
                if translate_to_musical(workflow.status()) == IN_MOVEMENT:
                    skipped.append({**entry,
                                    "reason": "running"})
                    continue
                candidates.append((entry, workflow_dir, workflow))
            except Exception as exc:  # pylint: disable=broad-exception-caught
                skipped.append({**entry, "reason": f"purge failed: {exc}"})
                continue

        if not candidates:
            continue

        # Reconcile the candidates against the runner: workspaces that
        # are already gone get the purge record retroactively instead of
        # being listed for deletion.
        existing = set()
        if runner_config.is_ssh_backend(backend) or backend == "native":
            paths = _workspace_paths(
                backend, runner_id,
                [(e["project"], e["workflow"]) for e, _, _ in candidates],
                yuki_dir)
            if runner_config.is_ssh_backend(backend):
                existing_paths = _existing_ssh_workspaces(
                    runner_id, list(paths.values()))
                existing = {key for key, path in paths.items()
                            if path in existing_paths}
            else:
                existing = {key for key, path in paths.items()
                            if os.path.isdir(path)}

        for entry, workflow_dir, workflow in candidates:
            key = (entry["project"], entry["workflow"])
            try:
                if (runner_config.is_ssh_backend(backend)
                        or backend == "native") and key not in existing:
                    _record_purged(workflow_dir)
                    already_gone += 1
                    continue
                if dry_run:
                    purged.append(entry)
                    continue
                workflow.delete_workspace()
                _record_purged(workflow_dir)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                skipped.append({**entry, "reason": f"purge failed: {exc}"})
                continue
            purged.append(entry)

    return {"purged": purged, "skipped": skipped,
            "already_gone": already_gone, "dry_run": bool(dry_run)}
