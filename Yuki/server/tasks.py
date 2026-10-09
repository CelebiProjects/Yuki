"""
Celery tasks for Yuki server.
"""
import logging
import os
from celery import Celery
from CelebiChrono.utils import metadata
from ..kernel.storage import remote as remote_data_ops
from ..services import result_transfer
from ..kernel.jobs.base import VJob
from ..kernel.workflows.base import VWorkflow, _yuki_dir
from ..kernel.execution.lease import (
    WorkflowAlreadyActive, WorkflowLeaseConflict)
from ..kernel.execution.submissions import SubmissionStore
from .workflow_status_refresh import running_refresh
from ..utils.logging_config import apply_channel_levels

_debug = logging.getLogger("Yuki.kernel")


def create_celery_app():
    """Create and configure Celery application."""
    app = Celery('yuki-server', broker='amqp://localhost')
    app.conf.update(
        result_backend='rpc://',
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
    )
    apply_channel_levels()
    return app


# Create celery app instance
celeryapp = create_celery_app()


@celeryapp.task
# pylint: disable=too-many-arguments,too-many-positional-arguments
def task_exec_impression(project_uuid, impressions, machine_uuid, timeout=None,
                         cache_on_runner=None, submission_id=None):
    """Execute impressions as a background task."""
    submission = (SubmissionStore(project_uuid, submission_id)
                  if submission_id else None)
    if submission:
        submission.update(status="building")
    try:
        return _exec_impression(
            project_uuid, impressions, machine_uuid, timeout,
            cache_on_runner, submission)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if submission:
            submission.update(
                status="failed",
                error=f"{type(exc).__name__}: {exc}")
        raise


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
def _exec_impression(project_uuid, impressions, machine_uuid, timeout,
                     cache_on_runner, submission):
    """Build and schedule a workflow, recording every submission outcome."""
    config = metadata.ConfigFile(os.path.join(os.environ["HOME"], ".Yuki/config.json"))
    backend_types = config.read_variable("backend_types", {})
    backend_type = backend_types.get(machine_uuid, "reana")
    _debug.debug(f"[task_exec_impression] machine_uuid={machine_uuid} "
                 f"backend_type={backend_type} impressions={impressions}")
    jobs = [
        VJob(os.path.join(os.environ["HOME"], ".Yuki/Storage", project_uuid, imp),
             machine_uuid)
        for imp in impressions.split(" ")
    ]
    _debug.debug("jobs %s", jobs)
    workflow = VWorkflow.create(project_uuid, jobs, None, mode=backend_type)
    workflow.cache_on_runner_requests = dict(cache_on_runner or {})
    if timeout is not None:
        workflow.config_file.write_variable("submission_timeout", timeout)
    _debug.debug("workflow %s", workflow)

    marks = _validate_remote_data_binding(workflow, project_uuid, machine_uuid)
    if marks:
        from ..kernel.execution.status import DISSONANCE
        workflow.set_workflow_status("failed")
        for job, message in marks:
            job.set_status(DISSONANCE, message)
        result = {"workflow_id": "", "candidate_workflow_id": workflow.uuid,
                  "deduplicated": False,
                  "error": "remote data runner mismatch"}
        if submission:
            submission.update(status="failed", **result)
        return result

    try:
        workflow.run()
    except WorkflowAlreadyActive as exc:
        workflow.set_workflow_status("stopped")
        _debug.info(
            "[task_exec_impression] duplicate submission workflow=%s "
            "existing_workflow=%s", workflow.uuid, exc.workflow_id)
        result = {"workflow_id": exc.workflow_id, "deduplicated": True}
        if submission:
            submission.update(status="deduplicated", **result)
        return result
    except WorkflowLeaseConflict as exc:
        workflow.set_workflow_status("failed")
        _debug.warning(
            "[task_exec_impression] overlapping submission rejected "
            "workflow=%s conflicts=%s", workflow.uuid, exc.conflicts)
        result = {"workflow_id": "", "candidate_workflow_id": workflow.uuid,
                  "deduplicated": False, "retryable": True,
                  "error": str(exc),
                  "conflicts": {job: entry.get("workflow_id", "")
                                for job, entry in exc.conflicts.items()}}
        if submission:
            submission.update(status="blocked", **result)
        return result
    _debug.debug("[task_exec_impression] submit finished workflow=%s",
                 workflow.uuid)
    result = {"workflow_id": workflow.uuid, "deduplicated": False}
    if submission:
        submission.update(status="scheduled", **result)
    return result


def _validate_remote_data_binding(workflow, project_uuid, machine_uuid):
    """Validate the runner binding of remote-hosted data impressions.

    Builds the workflow's real job set via construct_workflow_jobs (the same
    walk run() performs) and checks each input job's remote.json marker.

    Returns a list of (job, message) pairs for the workflow's own execution
    jobs to mark dissonant when an input impression is hosted on a different
    runner, or an empty list when the bindings are fine.
    """
    workflow.construct_workflow_jobs(workflow.start_job or [])
    runners_id = metadata.ConfigFile(
        os.path.join(os.environ["HOME"], ".Yuki", "config.json")
    ).read_variable("runners_id", {})
    runner_names = {v: k for k, v in runners_id.items()}

    violations = []
    for job in workflow.jobs:
        if not job.is_input:
            continue
        impression = job.path.split("/")[-1] if job.path else ""
        marker = os.path.join(os.environ["HOME"], ".Yuki", "Storage",
                              project_uuid, impression, "remote.json")
        if not os.path.exists(marker):
            continue
        host = metadata.ConfigFile(marker).read_variable("host_runner_id", "")
        if host and host != machine_uuid:
            violations.append((impression, host))

    if not violations:
        return []
    impression, host = violations[0]
    host_name = runner_names.get(host, host)
    message = (f"Data impression {impression} is hosted on runner "
               f"{host_name}. Submit this workflow to {host_name}, "
               "or move the data via collect (coming later).")
    return [(job, message) for job in workflow.jobs
            if not job.is_input and job.job_type() != "algorithm"]


@celeryapp.task
def task_update_workflow_status(project_uuid, workflow_id, token=None):
    """Update workflow status as a background task.

    The distribution refresh on the terminal transition happens inside
    update_workflow_status itself.
    """
    _debug.debug("# >>> task_update_workflow_status")
    _debug.debug(f"[task_update_workflow_status] project_uuid={project_uuid} "
                 f"workflow_id={workflow_id}")
    workflow_path = os.path.join(_yuki_dir(), "Workflows", project_uuid, workflow_id)
    with running_refresh(workflow_path, token) as acquired:
        if not acquired:
            _debug.debug("[task_update_workflow_status] already running: %s", workflow_id)
            return
        workflow = VWorkflow.create(project_uuid, [], workflow_id)
        _debug.debug(f"[task_update_workflow_status] backend={workflow.backend_type()} "
                     f"uuid={workflow.uuid} path={workflow.path}")
        from ..kernel.execution.status import is_terminal_status
        current_status = workflow.status()
        if is_terminal_status(current_status):
            _debug.debug(f"[task_update_workflow_status] workflow already terminal "
                         f"status={current_status}; skipping update_workflow_status")
            return
        workflow.update_workflow_status()
    _debug.debug("# <<< task_update_workflow_status")


@celeryapp.task
def task_register_remote_data(job_id, runner_id, remote_path, project_uuid,
                              descriptor):
    """Register remote data on an ssh runner: hash, then dispatch the copy."""
    yuki_dir = remote_data_ops._yuki_dir()  # pylint: disable=protected-access

    def update(state):
        current = remote_data_ops.read_job_state(yuki_dir, job_id) or {}
        current.update(state)
        remote_data_ops.write_job_state(yuki_dir, job_id, current)

    try:
        remote_data_ops.register_remote_data_job(
            job_id, runner_id, remote_path, project_uuid, descriptor, update)
    except Exception as e:  # pylint: disable=broad-exception-caught
        update({"status": "failed", "result": None,
                "error": str(e) or type(e).__name__})
        return
    state = remote_data_ops.read_job_state(yuki_dir, job_id) or {}
    if state.get("status") != "copying":
        # Unchanged data: the job reused the archived registration and
        # recorded done itself; there is nothing to copy.
        return
    impression_uuid = (state.get("result") or {}).get("impression_uuid", "")
    try:
        task_copy_remote_data.apply_async(
            args=[job_id, impression_uuid, project_uuid, runner_id,
                  remote_path])
    except Exception as e:  # pylint: disable=broad-exception-caught
        # Nobody will clean the progress file if the copy is never
        # dispatched; remove it so a later re-run starts fresh.
        update({"status": "failed", "result": None,
                "error": str(e) or type(e).__name__})
        remote_data_ops.remove_remote_progress_file(runner_id, job_id)


@celeryapp.task(bind=True, max_retries=None, acks_late=True,
                reject_on_worker_lost=True)
def task_copy_remote_data(self, job_id, impression_uuid, project_uuid, runner_id,  # pylint: disable=too-many-arguments,too-many-positional-arguments
                          remote_path):
    """Launch/observe a detached copy, releasing the worker between checks."""
    try:
        state = remote_data_ops.copy_remote_data_job(
            job_id, impression_uuid, project_uuid, runner_id, remote_path)
    except Exception as e:  # pylint: disable=broad-exception-caught
        # Loss of contact says nothing about the independent remote process.
        raise self.retry(exc=e, countdown=10)
    if state is None:
        raise self.retry(countdown=2)
    remote_data_ops.remove_remote_progress_file(runner_id, job_id)


@celeryapp.task
def task_transfer_results(job_id, project_uuid, impression,
                          source, destination, pattern, force):
    """Transfer impression results between yuki and runner cache."""
    return result_transfer.run_transfer(
        job_id, project_uuid, impression,
        source, destination,
        pattern=pattern, force=force)


@celeryapp.task
def task_cache_results(job_id, runner_id, project_uuid, impression):
    """Cache the impression's workflow stageout on its runner (background)."""
    yuki_dir = remote_data_ops._yuki_dir()  # pylint: disable=protected-access

    def update(state):
        current = remote_data_ops.read_job_state(
            yuki_dir, job_id, jobs_dir_name="cache-jobs") or {}
        current.update(state)
        remote_data_ops.write_job_state(yuki_dir, job_id, current,
                                        jobs_dir_name="cache-jobs")

    try:
        remote_data_ops.cache_results_job(
            runner_id, project_uuid, impression, update)
    except Exception as e:  # pylint: disable=broad-exception-caught
        update({"status": "failed", "result": None,
                "error": str(e) or type(e).__name__})
