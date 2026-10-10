"""
Job execution routes.
"""
from logging import getLogger

from flask import Blueprint, jsonify, request

from ...kernel.jobs.base import VJob
from ...kernel.jobs.container import ContainerJob
from ...kernel.workflows.base import VWorkflow
from ...kernel.storage.impressions import ImpressionStorage
from ...kernel.execution.submissions import SubmissionStore
from ...kernel.execution.lease import workflow_is_active, workflow_status
from ...kernel.execution.status import (
    SILENCE, TUNING, FAILED, DISSONANCE, CODA, FINAL_NOTE, ARCHIVED,
)
from ..config import config
from ..tasks import task_exec_impression
from ..workflow_status_refresh import running_refresh
import shutil  # pylint: disable=wrong-import-order
import json  # pylint: disable=wrong-import-order

bp = Blueprint('execution', __name__)
logger = getLogger("YukiLogger")
_debug = getLogger("Yuki.execution")


def _refresh_unknown_workflow(project_uuid, workflow_id):
    """Synchronously refresh one unknown workflow without overlapping polls."""
    workflow = VWorkflow.create(project_uuid, [], workflow_id)
    try:
        with running_refresh(workflow.path) as acquired:
            if not acquired:
                return (workflow_status(project_uuid, workflow_id) or "unknown",
                        "workflow status refresh is already in progress")
            workflow.update_workflow_status()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return (workflow_status(project_uuid, workflow_id) or "unknown",
                f"{type(exc).__name__}: {exc}")
    return workflow_status(project_uuid, workflow_id) or "unknown", ""

@bp.route('/execute', methods=['GET', 'POST'])
def execute():  # pylint: disable=too-many-locals,too-many-branches,too-many-statements,too-many-return-statements
    """Execute impressions."""
    _debug.debug("# >>> execute")
    if request.method == 'POST':
        _debug.debug("%s", request)
        timeout = request.form.get("timeout")
        if timeout is not None:
            try:
                timeout = int(timeout)
                if timeout <= 0:
                    raise ValueError
            except ValueError:
                return jsonify({"error": "timeout must be a positive integer in seconds"}), 400
        machine = request.form["machine"]
        project_uuid = request.form['project_uuid']
        with_unknown = str(request.form.get("with_unknown", "")).lower() \
            in ("1", "true", "yes")
        cache_dict = request.form["cache_on_runner"]
        cache_dict = json.loads(cache_dict)
        contents = request.files["impressions"].read().decode()
        start_jobs = []
        requested_jobs = []
        previous_workflows = {}
        status_refreshed = False
        refresh_error = ""
        _debug.debug("cache_on_runner: %s", cache_dict)
        _debug.debug("machine: %s", machine)
        _debug.debug("contents: %s", contents.split(" "))

        for impression in contents.split(" "):
            _debug.debug("--------------")
            _debug.debug("impression: %s", impression)
            job_path = config.get_job_path(project_uuid, impression)
            job = VJob(job_path, None)
            job_status = job.status()
            _debug.debug("job %s %s %s", job, job.job_type(), job_status)

            if job.job_type() == "task":
                requested_jobs.append(job)
                if job_status not in (SILENCE, FAILED, DISSONANCE):
                    _debug.debug("job status is not raw or failed")
                    continue
                previous_workflow_id = job.workflow_id()
                if (job_status in (FAILED, DISSONANCE)
                        and previous_workflow_id):
                    previous_workflows[job.uuid] = {
                        "workflow_id": previous_workflow_id,
                        "job_status": job_status,
                    }
                cache_dict.setdefault(impression, False)
                start_jobs.append(job)
            elif job.job_type() == "algorithm":
                job.set_status(TUNING, "Algorithm job ready for configuration")
                # if job.environment() == "script":
                #     continue
                # start_jobs.append(job)

        workflow_ids = {job.workflow_id() for job in requested_jobs
                        if job.workflow_id()}
        if (with_unknown and requested_jobs
                and len(workflow_ids) == 1
                and workflow_status(project_uuid, next(iter(workflow_ids)))
                in ("", "unknown")):
            workflow_id = next(iter(workflow_ids))
            # An unknown workflow is authoritative over a stale per-job
            # failure. Do not reuse the initially selected retry candidates
            # unless this explicit refresh confirms workflow failure.
            start_jobs = []
            previous_workflows = {}
            refreshed_status, refresh_error = _refresh_unknown_workflow(
                project_uuid, workflow_id)
            status_refreshed = True
            _debug.debug(
                "unknown workflow refresh workflow=%s status=%s error=%s",
                workflow_id, refreshed_status, refresh_error)
            if not refresh_error and refreshed_status in (FAILED, DISSONANCE):
                for job in requested_jobs:
                    job_status = job.status()
                    if job_status in (CODA, FINAL_NOTE, ARCHIVED):
                        continue
                    previous_workflows[job.uuid] = {
                        "workflow_id": workflow_id,
                        "job_status": job_status,
                        "workflow_status": refreshed_status,
                    }
                    cache_dict.setdefault(job.uuid, False)
                    start_jobs.append(job)

        if len(start_jobs) == 0:
            if (requested_jobs and len(workflow_ids) == 1
                    and all(job.workflow_id() in workflow_ids
                            for job in requested_jobs)):
                workflow_id = next(iter(workflow_ids))
                current_workflow_status = (
                    workflow_status(project_uuid, workflow_id) or "unknown")
                if status_refreshed and refresh_error:
                    submission, _ = SubmissionStore.create(
                        project_uuid, [job.uuid for job in requested_jobs], machine)
                    record = submission.update(
                        status="deduplicated", workflow_id=workflow_id,
                        deduplicated=True, submission_reason="refresh_failed",
                        workflow_status=current_workflow_status,
                        status_refreshed=True, refresh_error=refresh_error)
                    for job in requested_jobs:
                        VJob(job.path, machine).set_submission_id(
                            submission.submission_id)
                    return jsonify(record)
                if workflow_is_active(project_uuid, workflow_id):
                    submission, _ = SubmissionStore.create(
                        project_uuid, [job.uuid for job in requested_jobs], machine)
                    reason = ("unknown" if with_unknown and status_refreshed
                              and current_workflow_status == "unknown"
                              else "active")
                    record = submission.update(
                        status="deduplicated", workflow_id=workflow_id,
                        deduplicated=True, submission_reason=reason,
                        workflow_status=current_workflow_status,
                        status_refreshed=status_refreshed,
                        refresh_error=refresh_error)
                    for job in requested_jobs:
                        VJob(job.path, machine).set_submission_id(
                            submission.submission_id)
                    return jsonify(record)
                if all(job.status() in (CODA, FINAL_NOTE, ARCHIVED)
                       for job in requested_jobs):
                    submission, _ = SubmissionStore.create(
                        project_uuid, [job.uuid for job in requested_jobs], machine)
                    record = submission.update(
                        status="deduplicated", workflow_id=workflow_id,
                        deduplicated=True, submission_reason="completed",
                        workflow_status=current_workflow_status,
                        status_refreshed=status_refreshed,
                        refresh_error=refresh_error)
                    for job in requested_jobs:
                        VJob(job.path, machine).set_submission_id(
                            submission.submission_id)
                    return jsonify(record)
                if status_refreshed:
                    submission, _ = SubmissionStore.create(
                        project_uuid, [job.uuid for job in requested_jobs], machine)
                    record = submission.update(
                        status="deduplicated", workflow_id=workflow_id,
                        deduplicated=True,
                        submission_reason="not_retryable",
                        workflow_status=current_workflow_status,
                        status_refreshed=True)
                    for job in requested_jobs:
                        VJob(job.path, machine).set_submission_id(
                            submission.submission_id)
                    return jsonify(record)
            _debug.debug("no job to run")
            _debug.debug("# <<< execute")
            return "no job to run"

        contents = " ".join([job.uuid for job in start_jobs])

        submission, accepted = SubmissionStore.create(
            project_uuid, contents.split(" "), machine)
        if previous_workflows or status_refreshed:
            accepted = submission.update(
                submission_reason=("replacement" if previous_workflows
                                   else "new"),
                previous_workflows=previous_workflows,
                status_refreshed=status_refreshed,
                refresh_error=refresh_error)
        try:
            for impression in contents.split(" "):
                job_path = config.get_job_path(project_uuid, impression)
                VJob(job_path, machine).set_submission_id(
                    submission.submission_id)

            _debug.debug("Asynchronous execution")
            _debug.debug("contents %s", contents)
            task_kwargs = {"cache_on_runner": cache_dict}
            if timeout is not None:
                task_kwargs["timeout"] = timeout
            task_kwargs["submission_id"] = submission.submission_id
            task = task_exec_impression.apply_async(
                args=[project_uuid, contents, machine], kwargs=task_kwargs)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            failed = submission.update(
                status="failed",
                error=f"Failed to dispatch submission: {type(exc).__name__}: {exc}")
            return jsonify(failed), 503
        accepted = submission.update(celery_task_id=task.id)

        _debug.debug("Contents is: %s", contents)
        for impression in contents.split(" "):
            job_path = config.get_job_path(project_uuid, impression)
            _debug.debug("Project_uuid is: %s", project_uuid)
            _debug.debug("Job path is: %s", job_path)
            job = VJob(job_path, machine)
            job.set_runid(task.id)
        _debug.debug("### <<< execute")
        response = jsonify(accepted)
        response.status_code = 202
        response.headers["Location"] = (
            f"/submissions/{project_uuid}/{submission.submission_id}")
        return response

    return ""  # For GET requests


@bp.route('/submissions/<project_uuid>/<submission_id>', methods=['GET'])
def submission_status(project_uuid, submission_id):
    """Return the durable outcome of an asynchronous submission."""
    try:
        record = SubmissionStore(project_uuid, submission_id).read()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not record:
        return jsonify({"error": "submission not found"}), 404
    return jsonify(record)

@bp.route('/purge', methods=['GET', 'POST'])
def purge():
    """Purge impressions."""
    _debug.debug("# >>> purge")
    if request.method == 'POST':
        contents = request.files["impressions"].read().decode()
        project_uuid = request.form['project_uuid']
        _debug.debug("contents: %s", contents.split(" "))

        for impression in contents.split(" "):
            _debug.debug("impression: %s", impression)
            job_path = config.get_job_path(project_uuid, impression)
            # try to remove the job
            shutil.rmtree(job_path, ignore_errors=True)

        _debug.debug("contents %s", contents)
        _debug.debug("### <<< purge")
    return ""  # For GET requests




@bp.route("/run/<project_uuid>/<impression>/<machine>", methods=['GET'])
def run(project_uuid, impression, machine):
    """Run a specific impression on a machine."""
    logger.info("Trying to run it")
    task = task_exec_impression.apply_async(args=[project_uuid, impression, machine])
    job_path = config.get_job_path(project_uuid, impression)
    VJob(job_path, machine).set_runid(task.id)
    logger.info("Run id = %s", task.id)
    return task.id


@bp.route("/outputs/<project_uuid>/<impression>/<machine>", methods=['GET'])
def outputs(project_uuid, impression, machine):
    """Get outputs for an impression on a specific machine."""
    if machine == "none":
        path = config.get_job_path(project_uuid, impression)
        job = VJob(path, None)
        if job.job_type() == "task":
            return " ".join(ContainerJob(path, None).outputs())

    path = config.get_job_path(project_uuid, impression)
    job = VJob(path, machine)
    if job.job_type() == "task":
        return " ".join(ContainerJob(path, machine).outputs())
    return ""


@bp.route("/file-status/<project_uuid>/<impression>/<machine>", methods=['GET'])
def file_status(project_uuid, impression, machine):
    """Return merged runner + Storage file listing for an impression.

    With ?detailed=1 the payload is {"files": [...], "notes": [...]} where
    notes explain e.g. an unreachable runner or a cached listing.
    """
    kind = request.args.get("kind", "stageout")
    detailed = request.args.get("detailed") == "1"
    storage = ImpressionStorage(project_uuid, impression)
    return jsonify(storage.file_status(kind, detailed=detailed, machine=machine))


@bp.route("/refresh-filelists/<project_uuid>/<impression>", methods=['GET'])
def refresh_filelists(project_uuid, impression):
    """Force a live re-listing of the runner's stageout and logs.

    Terminal workflows are no longer polled, so their saved listing
    freezes at the terminal stamp; this endpoint re-lists the runner on
    demand and rewrites the saved listings.
    """
    storage = ImpressionStorage(project_uuid, impression)
    return jsonify(storage.force_refresh_filelists())


@bp.route("/purge-impression-data/<project_uuid>/<impression>",
          methods=['POST'])
def purge_impression_data(project_uuid, impression):
    """Purge locally collected data (stageout/logs/watermarks) for an impression.

    Body: {"force": bool}. Without force the purge refuses when the data
    cannot be re-collected from a runner. Running workflows always refuse.
    """
    data = request.get_json(silent=True) or request.form
    force = str(data.get("force", "")).lower() in ("1", "true", "yes")
    storage = ImpressionStorage(project_uuid, impression)
    report = storage.purge_collected_data(force=force)
    if "refused" in report:
        status_code = 409 if report.get("running") else 400
        return jsonify({"error": report["refused"]}), status_code
    return jsonify(report)
