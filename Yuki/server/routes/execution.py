"""
Job execution routes.
"""
from logging import getLogger

from flask import Blueprint, jsonify, request

from ...kernel.vjob import VJob
from ...kernel.container_job import ContainerJob
from ...kernel.impression_storage import ImpressionStorage
from ...kernel.status_constants import (
    SILENCE, PRELUDE, TUNING, FAILED, DISSONANCE,
)
from ..config import config
from ..tasks import task_exec_impression
import shutil  # pylint: disable=wrong-import-order
import json  # pylint: disable=wrong-import-order

bp = Blueprint('execution', __name__)
logger = getLogger("YukiLogger")
_debug = getLogger("Yuki.execution")

@bp.route('/execute', methods=['GET', 'POST'])
def execute():
    """Execute impressions."""
    _debug.debug("# >>> execute")
    if request.method == 'POST':
        _debug.debug("%s", request)
        machine = request.form["machine"]
        project_uuid = request.form['project_uuid']
        cache_dict = request.form["cache_on_runner"]
        cache_dict = json.loads(cache_dict)
        contents = request.files["impressions"].read().decode()
        start_jobs = []
        _debug.debug("cache_on_runner: %s", cache_dict)
        _debug.debug("machine: %s", machine)
        _debug.debug("contents: %s", contents.split(" "))

        for impression in contents.split(" "):
            _debug.debug("--------------")
            _debug.debug("impression: %s", impression)
            job_path = config.get_job_path(project_uuid, impression)
            job = VJob(job_path, None)
            _debug.debug("job %s %s %s", job, job.job_type(), job.status())

            if job.job_type() == "task":
                if job.status() not in (SILENCE, FAILED, DISSONANCE):
                    _debug.debug("job status is not raw or failed")
                    continue
                job.set_status(PRELUDE, "Job queued for execution")
                # Redefine, only aim for write use_eos variable
                start_job = VJob(job_path, machine)
                use_eos = cache_dict.get(impression, False)
                start_job.set_cache_on_runner(use_eos)
                start_jobs.append(job)
            elif job.job_type() == "algorithm":
                job.set_status(TUNING, "Algorithm job ready for configuration")
                # if job.environment() == "script":
                #     continue
                # start_jobs.append(job)

        if len(start_jobs) == 0:
            _debug.debug("no job to run")
            _debug.debug("# <<< execute")
            return "no job to run"

        contents = " ".join([job.uuid for job in start_jobs])

        _debug.debug("Asynchronous execution")
        _debug.debug("contents %s", contents)
        task = task_exec_impression.apply_async(args=[project_uuid, contents, machine])

        _debug.debug("Contents is: %s", contents)
        for impression in contents.split(" "):
            job_path = config.get_job_path(project_uuid, impression)
            _debug.debug("Project_uuid is: %s", project_uuid)
            _debug.debug("Job path is: %s", job_path)
            job = VJob(job_path, machine)
            job.set_runid(task.id)
        _debug.debug("### <<< execute")
        return task.id

    return ""  # For GET requests

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
def file_status(project_uuid, impression, machine):  # pylint: disable=unused-argument
    """Return merged runner + Storage file listing for an impression.

    With ?detailed=1 the payload is {"files": [...], "notes": [...]} where
    notes explain e.g. an unreachable runner or a cached listing.
    """
    kind = request.args.get("kind", "stageout")
    detailed = request.args.get("detailed") == "1"
    storage = ImpressionStorage(project_uuid, impression)
    return jsonify(storage.file_status(kind, detailed=detailed))


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
