"""
Job execution routes.
"""
from logging import getLogger

from flask import Blueprint, request

from Yuki.kernel.VJob import VJob
from Yuki.kernel.VContainer import VContainer
from ..config import config
from ..tasks import task_exec_impression

bp = Blueprint('execution', __name__)
logger = getLogger("YukiLogger")


@bp.route('/execute', methods=['GET', 'POST'])
def execute():
    """Execute impressions."""
    print("# >>> execute")
    if request.method == 'POST':
        machine = request.form["machine"]
        contents = request.files["impressions"].read().decode()
        start_jobs = []
        print("machine:", machine)
        print("contents:", contents.split(" "))

        for impression in contents.split(" "):
            print("impression:", impression)
            job_path = config.get_job_path(impression)
            job = VJob(job_path, None)
            print("job", job, job.job_type(), job.status())

            if job.job_type() == "task":
                if job.status() not in ("raw", "failed"):
                    print("job status is not raw or failed")
                    continue
                job.set_status("waiting")
                start_jobs.append(job)
            elif job.job_type() == "algorithm":
                if job.environment() == "script":
                    continue
                job.set_status("waiting")
                start_jobs.append(job)

        if len(start_jobs) == 0:
            print("no job to run")
            print("# <<< execute")
            return "no job to run"

        contents = " ".join([job.uuid for job in start_jobs])

        print("Asynchronous execution")
        print("contents", contents)
        task = task_exec_impression.apply_async(args=[contents, machine])

        for impression in contents.split(" "):
            job_path = config.get_job_path(impression)
            VJob(job_path, machine).set_runid(task.id)
        print("### <<< execute")
        return task.id

    return ""  # For GET requests


@bp.route("/run/<impression>/<machine>", methods=['GET'])
def run(impression, machine):
    """Run a specific impression on a machine."""
    logger.info("Trying to run it")
    task = task_exec_impression.apply_async(args=[impression, machine])
    job_path = config.get_job_path(impression)
    VJob(job_path, machine).set_runid(task.id)
    logger.info("Run id = %s", task.id)
    return task.id


@bp.route("/outputs/<impression>/<machine>", methods=['GET'])
def outputs(impression, machine):
    """Get outputs for an impression on a specific machine."""
    if machine == "none":
        path = config.get_job_path(impression)
        job = VJob(path, None)
        if job.job_type() == "task":
            return " ".join(VContainer(path, None).outputs())

    path = config.get_job_path(impression)
    job = VJob(path, machine)
    if job.job_type() == "task":
        return " ".join(VContainer(path, machine).outputs())
    return ""
