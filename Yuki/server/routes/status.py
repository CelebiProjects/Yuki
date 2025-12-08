"""
Status and monitoring routes.
"""
import os
import time
from flask import Blueprint, render_template
from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.VJob import VJob
from Yuki.kernel.VWorkflow import VWorkflow
from ..config import config
from ..tasks import task_update_workflow_status
from CelebiChrono.kernel.chern_cache import ChernCache

bp = Blueprint('status', __name__)

CHERN_CACHE = ChernCache.instance()


@bp.route('/set-job-status/<impression_name>/<job_status>', methods=['GET'])
def setjobstatus(impression_name, job_status):
    """Set job status for an impression."""
    job_path = config.get_job_path(impression_name)
    job = VJob(job_path, None)
    job.set_status(job_status)
    return "ok"


@bp.route("/status/<impression_name>", methods=['GET'])
def status(impression_name):
    """Get status for an impression."""
    job_path = config.get_job_path(impression_name)
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    job_config_file = ConfigFile(config.get_job_config_path(impression_name))
    object_type = job_config_file.read_variable("object_type", "")

    if object_type == "":
        return "empty"

    for machine in runners_list:
        machine_id = runners_id[machine]

        job = VJob(job_path, machine_id)
        if job.workflow_id() == "":
            continue
        print("Checking status for job", job)
        workflow = VWorkflow([], job.workflow_id())
        workflow_status = workflow.status()
        # print("Status from workflow", workflow_status)
        print("Path:", os.path.join(os.path.join(os.environ["HOME"], ".Yuki", "Workflows", job.workflow_id())))
        job.update_status_from_workflow( # workflow path
                os.path.join(os.path.join(os.environ["HOME"], ".Yuki", "Workflows", job.workflow_id()))
                )
        if workflow_status not in ('finished', 'failed'):
            last_update_time = CHERN_CACHE.update_table.get(workflow.uuid, -1)
            print(f"Time difference: {time.time() - last_update_time}")
            if (time.time() - last_update_time) > 5:
                CHERN_CACHE.update_table[workflow.uuid] = time.time()
            else:
                print("Skipping workflow status update to avoid frequent updates.")
                task_update_workflow_status.apply_async(args=[workflow.uuid])

        job_status = job.status()

        if job_status != "unknown":
            return job_status

        if os.path.exists(job_path):
            return "deposited"

    job = VJob(job_path, None)
    return job.status()


@bp.route("/run-status/<impression_name>/<machine>", methods=['GET'])
def runstatus(impression_name, machine):
    """Get run status for an impression on a specific machine."""
    job_path = config.get_job_path(impression_name)
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})

    job_config_file = ConfigFile(config.get_job_config_path(impression_name))
    object_type = job_config_file.read_variable("object_type", "")
    if object_type == "":
        return "empty"

    if machine == "none":
        for runner in runners_id:
            machine_id = runners_id[runner]
            job = VJob(job_path, None)
            workflow = VWorkflow([], job.workflow_id())
            return workflow.status()

    machine_id = runners_id[machine]
    job = VJob(job_path, machine_id)
    workflow = VWorkflow([], job.workflow_id())
    return workflow.status()


@bp.route("/deposited/<impression_name>", methods=['GET'])
def deposited(impression_name):
    """Check if an impression is deposited."""
    job_path = config.get_job_path(impression_name)
    if os.path.exists(job_path):
        return "TRUE"
    return "FALSE"


@bp.route("/dite-status", methods=['GET'])
def ditestatus():
    """Get DITE status."""
    return "ok"


@bp.route("/sample-status/<impression_name>", methods=['GET'])
def samplestatus(impression_name):
    """Get sample status for an impression."""
    job_config_file = ConfigFile(config.get_job_config_path(impression_name))
    return job_config_file.read_variable("sample_uuid", "")


@bp.route("/impression/<impression_name>", methods=['GET'])
def impression(impression_name):
    """Get impression path."""
    return config.get_job_path(impression_name)


@bp.route("/imp-view/<impression_name>", methods=['GET'])
def impview(impression_name):
    """View impression files."""
    job_path = config.get_job_path(impression_name)
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    job = VJob(job_path, None)
    runner_id = job.machine_id

    file_infos = []

    if os.path.exists(os.path.join(job_path, runner_id, "outputs")):
        files = os.listdir(os.path.join(job_path, runner_id, "outputs"))
        # sort with ext first and then name
        files.sort(
            key=lambda x: ((0 if x == "chern.stdout" else 1),
                           os.path.splitext(x)[1].lower(),
                           x.lower())
                )

        for filename in files:
            ext = os.path.splitext(filename)[1].lower()
            is_image = ext in ('.png', '.jpg', '.jpeg', '.gif')
            is_text = ext in ('.txt', '.log', '.stdout')
            file_info = {
                'name': filename,
                'is_image': is_image,
                'is_text': is_text,
            }

            MAX_PREVIEW_CHARS = 10000
            if is_text:
                file_path = os.path.join(job_path, runner_id, "outputs", filename)  # adjust path
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        if len(content) > MAX_PREVIEW_CHARS * 2:
                            head = content[:MAX_PREVIEW_CHARS]
                            tail = content[-MAX_PREVIEW_CHARS:]
                            # Add styled markers
                            content_preview = (
                                f'<span class="txt-message">[First {MAX_PREVIEW_CHARS} characters from head: **begin**]</span>\n'
                                f'{head}\n'
                                f'<span class="txt-message">[First {MAX_PREVIEW_CHARS} characters from head: **end**]</span>\n'
                                f'<span class="txt-separator"></span>\n'
                                f'<span class="txt-message">[Last {MAX_PREVIEW_CHARS} characters from tail: **begin**]</span>\n'
                                f'{tail}\n'
                                f'<span class="txt-message">[Last {MAX_PREVIEW_CHARS} characters from tail: **end**]</span>'
                            )
                        else:
                            content_preview = f'<span class="txt-message">[Full content]</span>\n{content}'
                        file_info['content'] = content_preview
                except Exception as e:
                    file_info['content'] = f"[Error reading file: {e}]"

            file_infos.append(file_info)

    if os.path.exists(os.path.join(job_path, runner_id, "logs")):
        files = os.listdir(os.path.join(job_path, runner_id, "logs"))
        files.sort(
            key=lambda x: ((0),
                           os.path.splitext(x)[1].lower(),
                           x.lower())
                )

        for filename in files:
            ext = os.path.splitext(filename)[1].lower()
            is_image = ext in ('.png', '.jpg', '.jpeg', '.gif')
            is_text = ext in ('.txt', '.log', '.stdout')
            file_info = {
                'name': filename,
                'is_image': is_image,
                'is_text': is_text,
            }

            MAX_PREVIEW_CHARS = 10000
            if is_text:
                file_path = os.path.join(job_path, runner_id, "logs", filename)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        if len(content) > MAX_PREVIEW_CHARS * 2:
                            head = content[:MAX_PREVIEW_CHARS]
                            tail = content[-MAX_PREVIEW_CHARS:]
                            # Add styled markers
                            content_preview = (
                                f'<span class="txt-message">[First {MAX_PREVIEW_CHARS} characters from head: **begin**]</span>\n'
                                f'{head}\n'
                                f'<span class="txt-message">[First {MAX_PREVIEW_CHARS} characters from head: **end**]</span>\n'
                                f'<span class="txt-separator"></span>\n'
                                f'<span class="txt-message">[Last {MAX_PREVIEW_CHARS} characters from tail: **begin**]</span>\n'
                                f'{tail}\n'
                                f'<span class="txt-message">[Last {MAX_PREVIEW_CHARS} characters from tail: **end**]</span>'
                            )
                        else:
                            content_preview = f'<span class="txt-message">[Full content]</span>\n{content}'
                        file_info['content'] = content_preview
                except Exception as e:
                    file_info['content'] = f"[Error reading file: {e}]"

            file_infos.append(file_info)


    return render_template('impview.html',
                          impression=impression_name,
                          runner_id=runner_id,
                          files=file_infos)
