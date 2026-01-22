"""
Status and monitoring routes.
"""
import os
import time
from flask import Blueprint, render_template
from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.VJob import VJob
from Yuki.kernel.vworkflow import VWorkflow
from ..config import config
from ..tasks import task_update_workflow_status
from CelebiChrono.kernel.chern_cache import ChernCache
import json
from flask import request, jsonify
from werkzeug.utils import secure_filename

bp = Blueprint('status', __name__)

CHERN_CACHE = ChernCache.instance()


@bp.route('/set-job-status/<project_uuid>/<impression_name>/<job_status>', methods=['GET'])
def setjobstatus(project_uuid, impression_name, job_status):
    """Set job status for an impression."""
    job_path = config.get_job_path(project_uuid, impression_name)
    job = VJob(job_path, None)
    job.set_status(job_status)
    return "ok"


@bp.route("/status/<project_uuid>/<impression_name>", methods=['GET'])
def status(project_uuid, impression_name):
    """Get status for an impression."""
    job_path = config.get_job_path(project_uuid, impression_name)
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    job_config_file = ConfigFile(config.get_job_config_path(project_uuid, impression_name))
    object_type = job_config_file.read_variable("object_type", "")

    if object_type == "":
        return "empty"

    for machine in runners_list:
        machine_id = runners_id[machine]

        job = VJob(job_path, machine_id)
        if job.workflow_id() == "":
            continue
        print("Checking status for job", job)
        workflow = VWorkflow.create(project_uuid, [], job.workflow_id())
        workflow_status = workflow.status()
        # print("Status from workflow", workflow_status)
        workflow_path = os.path.join(
            os.environ["HOME"],
            ".Yuki",
            "Workflows",
            project_uuid,
            job.workflow_id()
        )

        print("Path:", workflow_path)
        job.update_status_from_workflow( # workflow path
                    workflow_path
                )
        if workflow_status not in ('finished', 'failed'):
            last_update_time = CHERN_CACHE.update_table.get(workflow.uuid, -1)
            print(f"Time difference: {time.time() - last_update_time}")
            if (time.time() - last_update_time) > 5:
                CHERN_CACHE.update_table[workflow.uuid] = time.time()
            else:
                print("Skipping workflow status update to avoid frequent updates.")
                task_update_workflow_status.apply_async(args=[project_uuid, workflow.uuid])

        job_status = job.status()

        if job_status != "unknown":
            return job_status

        if os.path.exists(job_path):
            return "deposited"

    job = VJob(job_path, None)
    return job.status()


@bp.route("/run-status/<project_uuid>/<impression_name>/<machine>", methods=['GET'])
def runstatus(project_uuid, impression_name, machine):
    """Get run status for an impression on a specific machine."""
    job_path = config.get_job_path(project_uuid, impression_name)
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})

    job_config_file = ConfigFile(config.get_job_config_path(project_uuid, impression_name))
    object_type = job_config_file.read_variable("object_type", "")
    if object_type == "":
        return "empty"

    if machine == "none":
        for runner in runners_id:
            machine_id = runners_id[runner]
            job = VJob(job_path, None)
            workflow = VWorkflow.create(project_uuid, [], job.workflow_id())
            return workflow.status()

    machine_id = runners_id[machine]
    job = VJob(job_path, machine_id)
    workflow = VWorkflow.create(project_uuid, [], job.workflow_id())
    return workflow.status()


@bp.route("/deposited/<project_uuid>/<impression_name>", methods=['GET'])
def deposited(project_uuid, impression_name):
    """Check if an impression is deposited."""
    job_path = config.get_job_path(project_uuid, impression_name)
    if os.path.exists(job_path):
        return "TRUE"
    return "FALSE"


@bp.route("/dite-status", methods=['GET'])
def ditestatus():
    """Get DITE status."""
    return "ok"


@bp.route("/sample-status/<project_uuid>/<impression_name>", methods=['GET'])
def samplestatus(project_uuid, impression_name):
    """Get sample status for an impression."""
    job_config_file = ConfigFile(config.get_job_config_path(project_uuid, impression_name))
    return job_config_file.read_variable("sample_uuid", "")


@bp.route("/impression/<project_uuid>/<impression_name>", methods=['GET'])
def impression(project_uuid, impression_name):
    """Get impression path."""
    return config.get_job_path(project_uuid, impression_name)

def process_directory(job_path, runner_id, base_dir, file_infos_dict, max_preview_chars):
    """Lists files in a directory and adds their metadata/preview content to file_infos_dict."""
    full_path = os.path.join(job_path, runner_id, base_dir)

    if not os.path.exists(full_path):
        return

    files = os.listdir(full_path)

    # Define directory-specific sort priority
    # outputs: chern.stdout first (0), logs: standard sort (1)
    dir_priority = 0 if base_dir == 'stageout' else 1

    # Sort files according to the original logic
    files.sort(
        key=lambda x: (
            (0 if x == "chern.stdout" and base_dir == 'stageout' else 1),
            os.path.splitext(x)[1].lower(),
            x.lower()
        )
    )

    for filename in files:
        # Prevent 'logs' from overwriting files already found in 'outputs'
        if filename in file_infos_dict and file_infos_dict[filename].get('source_dir') == 'stageout':
            continue

        ext = os.path.splitext(filename)[1].lower()
        is_image = ext in ('.png', '.jpg', '.jpeg', '.gif')
        is_text = ext in ('.txt', '.log', '.stdout')
        watermarked = (base_dir == 'watermarks')
        is_log = (base_dir == 'logs')

        file_info = {
            'name': filename,
            'is_image': is_image,
            'is_text': is_text,
            'is_log': is_log,
            'watermarked': watermarked,
            'source_dir': base_dir, # Store the source directory
            'content': None,
        }

        if is_text:
            file_info['content'] = generate_text_preview(job_path, runner_id, base_dir, filename, max_preview_chars)

        file_infos_dict[filename] = file_info

def generate_text_preview(job_path, runner_id, base_dir, filename, max_chars):
    """Reads a text file and returns the HTML-formatted preview content."""
    file_path = os.path.join(job_path, runner_id, base_dir, filename)

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

            if len(content) > max_chars * 2:
                # Truncated view for large files
                head = content[:max_chars]
                tail = content[-max_chars:]
                content_preview = (
                    f'<span class="txt-message">[First {max_chars} characters from head: **begin**]</span>\n'
                    f'{head}\n'
                    f'<span class="txt-message">[First {max_chars} characters from head: **end**]</span>\n'
                    f'<span class="txt-separator">--- Content Omitted (Full file available for download) ---</span>\n' # Added descriptive text
                    f'<span class="txt-message">[Last {max_chars} characters from tail: **begin**]</span>\n'
                    f'{tail}\n'
                    f'<span class="txt-message">[Last {max_chars} characters from tail: **end**]</span>'
                )
            else:
                # Full view for smaller files
                content_preview = f'<span class="txt-message">[Full content]</span>\n{content}'

            return content_preview

    except Exception as e:
        return f"[Error reading file: {e}]"


@bp.route("/imp-view/<project_uuid>/<impression_name>", methods=['GET'])
def impview(project_uuid, impression_name):
    """View impression files by gathering metadata from 'stageout' and 'logs'."""
    job_path = config.get_job_path(project_uuid, impression_name)

    # Get runner_id (assuming the VJob logic is necessary for this)
    try:
        job = VJob(job_path, None)
        runner_id = job.machine_id
    except Exception:
        # Fallback if VJob/job is not fully configured
        runner_id = "default_runner"

    # Use a dictionary to store file info keyed by filename to avoid duplicates when processing 'logs'
    file_infos_dict = {}

    MAX_PREVIEW_CHARS = 1000  # Maximum characters to read for text file previews
    # Process 'outputs' and 'logs' directories
    process_directory(job_path, runner_id, "stageout", file_infos_dict, MAX_PREVIEW_CHARS)
    process_directory(job_path, runner_id, "logs", file_infos_dict, MAX_PREVIEW_CHARS)
    process_directory(job_path, runner_id, "watermarks", file_infos_dict, MAX_PREVIEW_CHARS)

    print(file_infos_dict)
    # Convert dictionary values to a list for the template
    final_file_infos = list(file_infos_dict.values())

    # NOTE: The provided original code had sorting logic after processing 'outputs' and a different one for 'logs'.
    # We will need a final, consistent sort on the combined list before rendering,
    # and the helper function ensures the original logic's file properties are carried over.

    # Final sort (using a simplified sort for the combined list)
    def final_sort_key(file_info):
        filename = file_info['name']
        source_dir = file_info.get('source_dir', 'stageout') # Default to 'outputs'
        source_dir_order = 0 if source_dir == 'stageout' else 1
        chern_stdout_order = 0 if filename == "chern.stdout" and source_dir == 'stageout' else 1
        ext_lower = os.path.splitext(filename)[1].lower()
        return (source_dir_order, chern_stdout_order, ext_lower, filename.lower())

    final_file_infos.sort(key=final_sort_key)


    return render_template('impview.html',
                           project_uuid=project_uuid,
                           impression=impression_name,
                           runner_id=runner_id,
                           files=final_file_infos)

import os
import json
from flask import render_template, url_for

def process_directory2(job_path, runner_id, sub_dir, file_infos_dict, max_chars, project_uuid, imp_id):
    """
    Scans sub-directories and generates URLs for images (plots) and text files.
    """
    target_dir = os.path.join(job_path, runner_id, sub_dir)
    if not os.path.exists(target_dir):
        return

    route_map = {
        "logs": "upload.logview",
        "stageout": "upload.fileview",
        "watermarks": "upload.watermarkview"
    }
    target_route = route_map.get(sub_dir, "upload.fileview")

    for fname in os.listdir(target_dir):
        fpath = os.path.join(target_dir, fname)
        if os.path.isfile(fpath):
            ext = os.path.splitext(fname)[1].lower()
            # Plot/Image extensions
            is_image = ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg']
            # Data/Log extensions
            is_text = ext in ['.txt', '.md', '.json', '.yaml', '.py', '.log', '.stdout', '.stderr', '.csv']

            file_url = url_for(target_route,
                               project_uuid=project_uuid,
                               impression=imp_id,
                               runner_id=runner_id,
                               filename=fname)

            content = ""
            if is_text:
                try:
                    with open(fpath, 'r', errors='replace') as f:
                        content = f.read(max_chars)
                except:
                    content = "[Error reading text content]"

            file_infos_dict[fname] = {
                "name": fname,
                "source_dir": sub_dir,
                "is_image": is_image,
                "is_text": is_text,
                "content": content,
                "url": file_url
            }

@bp.route("/test/<project_uuid>", methods=['GET'])
def test(project_uuid):
    base_path = os.path.join(os.path.expanduser("~"), ".Yuki", "Bookkeep", project_uuid)

    if not os.path.exists(base_path):
        return "Project metadata not found", 404

    def build_tree_data(path, is_root=False):
        folder_name = os.path.basename(path)
        display_name = folder_name[:8] if is_root else folder_name

        data = {
            "name": display_name,
            "object_type": "directory",
            "impression_id": None,
            "readme_content": "",
            "impression_data": [],
            "children": [],
            "imp_view_url": None
        }

        config_path = os.path.join(path, "config.json")
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    conf = json.load(f)
                    data["object_type"] = conf.get("object_type", "directory")
                    imp_id = conf.get("impression", "")

                    if imp_id:
                        data["impression_id"] = imp_id
                        data["imp_view_url"] = url_for('status.impview',
                                                       project_uuid=project_uuid,
                                                       impression_name=imp_id)

                        if data["object_type"] == "task":
                            # Use your specific config helper
                            job_path = config.get_job_path(project_uuid, imp_id)

                            try:
                                job = VJob(job_path, None)
                                runner_id = job.machine_id
                            except:
                                runner_id = "default_runner"

                            file_infos_dict = {}
                            MAX_PREVIEW_CHARS = 1000

                            # Gather Plots and Files from all three locations
                            for d in ["stageout", "logs", "watermarks"]:
                                process_directory2(job_path, runner_id, d, file_infos_dict,
                                                   MAX_PREVIEW_CHARS, project_uuid, imp_id)

                            # Sort: Plots (images) usually in stageout, so we prioritize that
                            data["impression_data"] = sorted(
                                file_infos_dict.values(),
                                key=lambda x: (0 if x['is_image'] else 1, x['name'].lower())
                            )
            except:
                pass

        readme_path = os.path.join(path, "README.md")
        if os.path.exists(readme_path):
            with open(readme_path, 'r', errors='replace') as f:
                data["readme_content"] = f.read()

        for item in sorted(os.listdir(path)):
            item_path = os.path.join(path, item)
            if os.path.isdir(item_path):
                # Only recurse if it's not a raw data folder
                if not (len(item) == 32 and all(c in '0123456789abcdef' for c in item.lower())):
                    data["children"].append(build_tree_data(item_path))

        return data

    tree_data = build_tree_data(base_path, is_root=True)
    return render_template('test.html', project_data=tree_data)

@bp.route("/bookkeeping", methods=['POST'])
def bookkeeping():
    """
    Receives project manifest and associated files,
    saving them to .Yuki/Bookkeep/[project_uuid]
    """
    # 1. Extract metadata
    manifest_str = request.form.get("manifest")
    project_uuid = request.form.get("project_uuid")

    if not manifest_str or not project_uuid:
        return jsonify({"error": "Missing manifest or project_uuid"}), 400

    manifest = json.loads(manifest_str)

    # 2. Setup the storage path
    # Path: .Yuki/Bookkeep/[project_uuid]
    base_save_path = os.path.join(
            os.environ["HOME"],
            ".Yuki", "Bookkeep", secure_filename(project_uuid))

    if not os.path.exists(base_save_path):
        os.makedirs(base_save_path)

    # 3. Save the manifest itself for reference
    with open(os.path.join(base_save_path, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=4)

    # 4. Save the transmitted files
    # The 'files' dictionary in the request contains the binary data
    for storage_key, file_obj in request.files.items():
        # storage_key is the relative path (e.g., "subfolder/README.md")
        # We join this with our base path
        target_file_path = os.path.join(base_save_path, storage_key)

        # Ensure subdirectories exist (e.g., .Yuki/Bookkeep/uuid/subfolder/)
        os.makedirs(os.path.dirname(target_file_path), exist_ok=True)

        # Save the file
        file_obj.save(target_file_path)

    print(f"Project {project_uuid} bookkept successfully at {base_save_path}")

    return jsonify({
        "status": "success",
        "message": "Project structure and files saved",
        "path": base_save_path
    }), 200
