"""Host-side execution of a prepared native workflow."""
# pylint: disable=protected-access
import os
import time

from CelebiChrono.utils.metadata import ConfigFile

from ..runners import config as runner_config
from ..storage import staging as file_staging
from . import monitor as snakemake_monitor
from . import lease as execution_lease


def workflow_location(yuki_dir, workflow_uuid):
    """Find a workflow by UUID in the shared storage directory."""
    workflows_dir = os.path.join(yuki_dir, "Workflows")
    if os.path.isdir(workflows_dir):
        for project_uuid in sorted(os.listdir(workflows_dir)):
            workflow_path = os.path.join(workflows_dir, project_uuid, workflow_uuid)
            if os.path.isfile(os.path.join(workflow_path, "config.json")):
                return project_uuid, workflow_path
    raise FileNotFoundError(f"Workflow {workflow_uuid} not found in {workflows_dir}")


def execute_workflow(workflow_uuid, cores=None, logger=None):  # pylint: disable=too-many-locals
    """Stage, execute and collect a native workflow; return a process exit code."""
    logger = logger or (lambda message: None)
    yuki_dir = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
    project_uuid, workflow_path = workflow_location(yuki_dir, workflow_uuid)
    workflow_cfg = ConfigFile(os.path.join(workflow_path, "config.json"))
    if workflow_cfg.read_variable("backend_type", "native") not in ("native", "dry"):
        raise ValueError(f"Workflow {workflow_uuid} is not native")
    machine_id = workflow_cfg.read_variable("machine_id", "")
    lease = workflow_cfg.read_variable("execution_lease", {})
    lease_token = lease.get("token", "")
    leased_jobs = lease.get("jobs", [])
    if lease_token and not execution_lease.validate_many(
            project_uuid, machine_id, workflow_uuid,
            lease_token, leased_jobs, yuki_dir=yuki_dir):
        raise RuntimeError(
            f"Workflow {workflow_uuid} no longer owns its execution jobs")
    settings = runner_config.get_runner_settings(runner_config.open_config(), machine_id)
    cores = cores or settings.get("cores", "all")
    base_dir = settings.get("workdir") or os.path.join(yuki_dir, "LocalWorkflows")
    local_exec_dir = os.path.join(base_dir, workflow_uuid)
    if not os.path.isfile(os.path.join(local_exec_dir, "Snakefile")):
        raise FileNotFoundError(f"No Snakefile in {local_exec_dir}")

    logger(f"[LOCAL] Running {workflow_uuid} in {local_exec_dir}")
    stager = file_staging.FileStager(workflow_path, local_exec_dir, project_uuid, logger)
    monitor = snakemake_monitor.SnakemakeMonitor(
        workflow_path, local_exec_dir,
        project_uuid=project_uuid, workflow_uuid=workflow_uuid)
    if not stager.stage_in():
        monitor._update_results("failed", 0, 0, {"error": "File staging failed"})
        return 1
    if os.path.exists(os.path.join(local_exec_dir, "native-runner.cancel")):
        monitor._update_results("stopped", 0, 0,
                                {"error": "Cancelled by user"})
        return 1

    exit_code = monitor.execute_snakemake(
        cores, logger, mem_mb=settings.get("mem_mb"),
        snakemake_path=settings.get("snakemake_path") or None,
        conda_path=settings.get("conda_path") or None,
        defer_success=True)
    if exit_code == 0:
        if not stager.stage_out():
            monitor._update_results("failed", 0, 0,
                                    {"error": "Result collection failed"})
            return 1
        monitor._finalize_results(logger)
    return exit_code


def timestamp_logger(message):
    """Print a timestamped workflow message."""
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), message, flush=True)
