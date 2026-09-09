"""Preview and force-stop recorded-running workflows on one runner."""
import os

from CelebiChrono.utils.metadata import ConfigFile

from . import liveness, workflow_purge
from .status_constants import IN_MOVEMENT, translate_to_musical
from .vworkflow import VWorkflow


def kill_running_workflows(runner_id, project_uuid, dry_run=True,
                           workflows=None, yuki_dir=None):
    """Kill only explicitly previewed IDs; recheck runner, state and liveness.

    Selection uses recorded status deliberately: this operation also recovers
    stale running records. Workspaces are retained for a separate purge.
    """
    if not dry_run and workflows is None:
        raise ValueError("Execution requires workflow IDs from a preview")
    yuki_dir = yuki_dir or liveness._yuki_dir()  # pylint: disable=protected-access
    root = os.path.join(yuki_dir, "Workflows", project_uuid)
    report = {"selected": [], "killed": [], "skipped": [], "failed": [],
              "dry_run": dry_run}
    if not os.path.isdir(root):
        return report
    names = sorted(os.listdir(root)) if dry_run else sorted(set(workflows))
    for name in names:
        entry = {"project": project_uuid, "workflow": name}
        try:
            path = os.path.join(root, name)
            if not os.path.isdir(path):
                report["skipped"].append({**entry, "reason": "not found"})
                continue
            config = ConfigFile(os.path.join(path, "config.json"))
            if config.read_variable("machine_id", "") != runner_id:
                continue
            # Re-derive on execution so a newly referenced workflow is protected.
            live = workflow_purge._project_live_workflows(  # pylint: disable=protected-access
                project_uuid, yuki_dir)
            if live is None or name in live:
                reason = "no live set synced" if live is None else "live"
                report["skipped"].append({**entry, "reason": reason})
                continue
            workflow = VWorkflow.create(project_uuid, [], name)
            if translate_to_musical(workflow.status()) != IN_MOVEMENT:
                report["skipped"].append({**entry, "reason": "not recorded running"})
                continue
            report["selected"].append(entry)
            if dry_run:
                continue
            # An old workflow must not change jobs now assigned to a rerun.
            workflow.jobs = [job for job in workflow.jobs
                             if job.is_input or job.job_type() == "algorithm"
                             or job.workflow_id() == name]
            if workflow.backend_type() == "ssh":
                workflow.force_kill(strict=True)
            else:
                workflow.force_kill()
            report["killed"].append(entry)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            report["failed"].append({**entry, "reason": str(exc)})
    return report
