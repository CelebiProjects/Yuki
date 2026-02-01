"""
Job execution routes.
"""
from logging import getLogger

from flask import Blueprint, request

from ..kernel.vjob import vjob
from ..kernel.container_job import container_job
from ..config import config
from ..tasks import task_exec_impression
import shutil
import json

bp = Blueprint('execution', __name__)
logger = getLogger("YukiLogger")

@bp.route('/execute', methods=['GET', 'POST'])
def execute():
    """Execute impressions."""
    print("# >>> execute")
    if request.method == 'POST':
        print(request)
        machine = request.form["machine"]
        project_uuid = request.form['project_uuid']
        use_eos_dict = request.form["use_eos"]
        use_eos_dict = json.loads(use_eos_dict)
        contents = request.files["impressions"].read().decode()
        start_jobs = []
        print("use_eos:", use_eos_dict)
        print("machine:", machine)
        print("contents:", contents.split(" "))

        for impression in contents.split(" "):
            print("--------------")
            print("impression:", impression)
            job_path = config.get_job_path(project_uuid, impression)
            job = vjob(job_path, None)
            print("job", job, job.job_type(), job.status())

            if job.job_type() == "task":
                if job.status() not in ("raw", "failed"):
                    print("job status is not raw or failed")
                    continue
                job.set_status("waiting")
                # Redefine, only aim for write use_eos variable
                start_job = vjob(job_path, machine)
                use_eos = use_eos_dict.get(impression, False)
                start_job.set_use_eos(use_eos)
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
        task = task_exec_impression.apply_async(args=[project_uuid, contents, machine])

        print("Contents is:", contents)
        for impression in contents.split(" "):
            job_path = config.get_job_path(project_uuid, impression)
            print("Project_uuid is:", project_uuid)
            print("Job path is:", job_path)
            job = vjob(job_path, machine)
            job.set_runid(task.id)
        print("### <<< execute")
        return task.id

    return ""  # For GET requests

@bp.route('/purge', methods=['GET', 'POST'])
def purge():
    """Purge impressions."""
    print("# >>> purge")
    if request.method == 'POST':
        contents = request.files["impressions"].read().decode()
        project_uuid = request.form['project_uuid']
        start_jobs = []
        print("contents:", contents.split(" "))

        for impression in contents.split(" "):
            print("impression:", impression)
            job_path = config.get_job_path(project_uuid, impression)
            # try to remove the job
            shutil.rmtree(job_path, ignore_errors=True)

        print("contents", contents)
        print("### <<< purge")
    return ""  # For GET requests




@bp.route("/run/<project_uuid>/<impression>/<machine>", methods=['GET'])
def run(project_uuid, impression, machine):
    """Run a specific impression on a machine."""
    logger.info("Trying to run it")
    task = task_exec_impression.apply_async(args=[project_uuid, impression, machine])
    job_path = config.get_job_path(project_uuid, impression)
    vjob(job_path, machine).set_runid(task.id)
    logger.info("Run id = %s", task.id)
    return task.id


@bp.route("/outputs/<project_uuid>/<impression>/<machine>", methods=['GET'])
def outputs(project_uuid, impression, machine):
    """Get outputs for an impression on a specific machine."""
    if machine == "none":
        path = config.get_job_path(project_uuid, impression)
        job = vjob(path, None)
        if job.job_type() == "task":
            return " ".join(container_job(path, None).outputs())

    path = config.get_job_path(project_uuid, impression)
    job = vjob(path, machine)
    if job.job_type() == "task":
        return " ".join(container_job(path, machine).outputs())
    return ""
