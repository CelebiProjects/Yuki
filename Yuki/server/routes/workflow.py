"""
Workflow management routes.
"""
from flask import Blueprint
from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.VJob import VJob
from Yuki.kernel.VWorkflow import VWorkflow
from ..config import config

bp = Blueprint('workflow', __name__)


@bp.route("/kill/<impression>", methods=['GET'])
def kill(impression):
    """Kill workflows for an impression."""
    job_path = config.get_job_path(impression)
    config_file = config.get_config_file()
    runners = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    job_config_file = ConfigFile(config.get_job_config_path(impression))
    job_config_file.read_variable("object_type", "")  # Read but don't store unused value

    for machine in runners:
        machine_id = runners_id[machine]
        job = VJob(job_path, machine_id)
        if job.workflow_id() == "":
            continue
        job_workflow = VWorkflow([], job.workflow_id())
        job_workflow.kill()

    job = VJob(job_path, None)
    job.set_status("failed")
    return "ok"

# @bp.route("/reset/<impression>", methods=['GET'])
#     """ Reset the job status to unknown """
#     job_path = config.get_job_path(impression)
#     config_file = config.get_config_file()
#     runners = config_file.read_variable("runners", [])
#     runners_id = config_file.read_variable("runners_id", {})
#     job_config_file = ConfigFile(config.get_job_config_path(impression))
#     job_config_file.read_variable("object_type", "")  # Read but don't store unused value


@bp.route("/collect/<impression>", methods=['GET'])
def collect(impression):
    """Collect results from workflows."""
    job_path = config.get_job_path(impression)
    config_file = config.get_config_file()
    runners = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    job_config_file = ConfigFile(config.get_job_config_path(impression))
    job_config_file.read_variable("object_type", "")  # Read but don't store unused value

    for machine in runners:
        machine_id = runners_id[machine]
        job = VJob(job_path, machine_id)
        if job.workflow_id() == "":
            continue
        job_workflow = VWorkflow([], job.workflow_id())
        # if job_workflow.status() == "finished":
        if job.status() == "finished":
            print("Download starting")
            job_workflow.download(impression)
        elif job.status() == "failed":
            print("Download starting: [failed]")
            job_workflow.download_logs(impression)
    return "ok"


@bp.route('/workflow/<impression>', methods=['GET'])
def workflow(impression):
    """Get workflow information for an impression."""
    job_path = config.get_job_path(impression)
    config_file = config.get_config_file()
    runners = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    job_config_file = ConfigFile(config.get_job_config_path(impression))
    job_config_file.read_variable("object_type", "")  # Read but don't store unused value

    for machine in runners:
        machine_id = runners_id[machine]
        job = VJob(job_path, machine_id)
        if job.workflow_id() == "":
            continue
        job_workflow = VWorkflow([], job.workflow_id())
        return f"{machine} {job_workflow.uuid}"
    return "UNDEFINED"
