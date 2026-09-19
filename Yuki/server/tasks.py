"""
Celery tasks for Yuki server.
"""
import logging
import os
from celery import Celery
from CelebiChrono.utils import metadata
from ..kernel import remote_data_ops, result_transfer
from ..kernel.vjob import VJob
from ..kernel.vworkflow import VWorkflow
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
def task_exec_impression(project_uuid, impressions, machine_uuid, timeout=None):
    """Execute impressions as a background task."""
    config = metadata.ConfigFile(os.path.join(os.environ["HOME"], ".Yuki/config.json"))
    backend_types = config.read_variable("backend_types", {})
    backend_type = backend_types.get(machine_uuid, "reana")
    runners_id = config.read_variable("runners_id", {})
    runner_name = {v: k for k, v in runners_id.items()}.get(machine_uuid, machine_uuid)
    _debug.debug(f"[task_exec_impression] runner={runner_name} machine_uuid={machine_uuid} "
                 f"backend_type={backend_type} impressions={impressions}")
    jobs = [
        VJob(os.path.join(os.environ["HOME"], ".Yuki/Storage", project_uuid, imp),
             machine_uuid)
        for imp in impressions.split(" ")
    ]
    _debug.debug("jobs %s", jobs)
    workflow = VWorkflow.create(project_uuid, jobs, None, mode=backend_type)
    if timeout is not None:
        workflow.config_file.write_variable("submission_timeout", timeout)
    _debug.debug("workflow %s", workflow)

    marks = _validate_remote_data_binding(workflow, project_uuid, machine_uuid)
    if marks:
        from ..kernel.status_constants import DISSONANCE
        workflow.set_workflow_status("failed")
        for job, message in marks:
            job.set_status(DISSONANCE, message)
        return

    workflow.run()
    _debug.debug("[task_exec_impression] submit finished workflow=%s",
                 workflow.uuid)


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
def task_update_workflow_status(project_uuid, workflow_id):
    """Update workflow status as a background task.

    The distribution refresh on the terminal transition happens inside
    update_workflow_status itself.
    """
    _debug.debug("# >>> task_update_workflow_status")
    _debug.debug(f"[task_update_workflow_status] project_uuid={project_uuid} "
                 f"workflow_id={workflow_id}")
    workflow = VWorkflow.create(project_uuid, [], workflow_id)
    _debug.debug(f"[task_update_workflow_status] backend={workflow.backend_type()} "
                 f"uuid={workflow.uuid} path={workflow.path}")
    from ..kernel.status_constants import CODA, FAILED, translate_to_musical
    current_status = workflow.status()
    if translate_to_musical(current_status) in (CODA, FAILED):
        _debug.debug(f"[task_update_workflow_status] workflow already terminal "
                     f"status={current_status}; skipping update_workflow_status")
        _debug.debug("# <<< task_update_workflow_status")
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
