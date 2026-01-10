"""
Job execution routes.
"""
from logging import getLogger

from flask import Blueprint, request

from Yuki.kernel.VJob import VJob
from Yuki.kernel.VContainer import VContainer
from ..config import config
from ..tasks import task_exec_impression
import shutil
import json

bp = Blueprint('execution', __name__)
logger = getLogger("YukiLogger")


# @bp.route('/execute', methods=['GET', 'POST'])
# def execute():
#     """Execute impressions with memory-safe processing."""
#     print("# >>> execute")
#
#     if request.method != 'POST':
#         return ""
#
#     # 1. Extraction and Validation
#     try:
#         machine = request.form.get("machine")
#         project_uuid = request.form.get("project_uuid")
#
#         # Safely parse JSON with a default empty dict
#         use_eos_dict = json.loads(request.form.get("use_eos", "{}"))
#
#         # Access the file handle without reading it into memory yet
#         impressions_file = request.files.get("impressions")
#         if not impressions_file:
#             return "No impressions file provided", 400
#
#     except (json.JSONDecodeError, KeyError) as e:
#         logging.error(f"Input validation failed: {e}")
#         return "Invalid request data", 400
#
#     # 2. Memory-Safe Impression Generator
#     # This prevents loading a massive file into RAM all at once.
#     def get_impression_tokens(file_storage):
#         for line in file_storage:
#             # Decode line by line and split on any whitespace
#             for token in line.decode().split():
#                 yield token
#
#     start_jobs = []
#
#     # 3. Single-Pass Job Preparation
#     for impression in get_impression_tokens(impressions_file):
#         job_path = config.get_job_path(project_uuid, impression)
#
#         # Instantiate once with the target machine
#         job = VJob(job_path, machine)
#
#         job_type = job.job_type()
#         job_status = job.status()
#
#         # Logic for Task types
#         if job_type == "task":
#             if job_status not in ("raw", "failed"):
#                 continue
#
#             # Apply EOS setting directly
#             use_eos = use_eos_dict.get(impression, False)
#             job.set_use_eos(use_eos)
#
#         # Logic for Algorithm types
#         elif job_type == "algorithm":
#             if job.environment() == "script":
#                 continue
#
#         # Skip unknown job types
#         else:
#             continue
#
#         # Prepare for execution
#         job.set_status("waiting")
#         start_jobs.append(job)
#
#     # 4. Final Execution Check
#     if not start_jobs:
#         print("no job to run")
#         print("# <<< execute")
#         return "no job to run"
#
#     # Construct the UUID string for the async task
#     job_uuids_content = " ".join([j.uuid for j in start_jobs])
#
#     print("Asynchronous execution")
#     try:
#         # Trigger the Celery task
#         task = task_exec_impression.apply_async(
#             args=[project_uuid, job_uuids_content, machine]
#         )
#
#         # 5. Link Run ID to all prepared job objects
#         for job in start_jobs:
#             job.set_runid(task.id)
#
#     except Exception as e:
#         logging.error(f"Failed to dispatch async task: {e}")
#         return "Internal execution error", 500
#
#     print(f"### <<< execute | Task ID: {task.id}")
#     return task.id

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
            job = VJob(job_path, None)
            print("job", job, job.job_type(), job.status())

            if job.job_type() == "task":
                if job.status() not in ("raw", "failed"):
                    print("job status is not raw or failed")
                    continue
                job.set_status("waiting")
                # Redefine, only aim for write use_eos variable
                start_job = VJob(job_path, machine)
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
            job = VJob(job_path, machine)
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
            return " ".join(VContainer(path, None).outputs())

    path = config.get_job_path(project_uuid, impression)
    job = VJob(path, machine)
    if job.job_type() == "task":
        return " ".join(VContainer(path, machine).outputs())
    return ""
