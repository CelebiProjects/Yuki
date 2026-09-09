"""
Status and monitoring routes.
"""
import json
import os
import time
from logging import getLogger
from flask import Blueprint, render_template, request, jsonify, url_for, send_from_directory
from werkzeug.utils import secure_filename
from CelebiChrono.utils.metadata import ConfigFile, YamlFile
from CelebiChrono.kernel.chern_cache import ChernCache
from Yuki.kernel import file_types
from ...kernel.vjob import VJob
from ...kernel.vworkflow import VWorkflow
from ...kernel.status_constants import (
    translate_to_musical, translate_to_legacy, is_valid_status,
    CODA, FAILED
)
from ..config import config
from ..tasks import task_update_workflow_status

bp = Blueprint('status', __name__)
_debug = getLogger("Yuki.execution")

CHERN_CACHE = ChernCache.instance()


@bp.route('/set-job-status/<project_uuid>/<impression_name>/<job_status>', methods=['GET'])
def setjobstatus(project_uuid, impression_name, job_status):
    """Set job status for an impression.

    Accepts both legacy and musical status names.
    """
    # Validate the status
    if not is_valid_status(job_status):
        return jsonify({"error": f"Invalid status: {job_status}"}), 400

    job_path = config.get_job_path(project_uuid, impression_name)
    job = VJob(job_path, None)
    job.set_status(job_status)
    return jsonify({
        "status": "ok",
        "message": f"Status set to {translate_to_musical(job_status)}",
        "status_musical": translate_to_musical(job_status),
        "status_legacy": translate_to_legacy(job_status)
    })


@bp.route('/set-detailed-status/<project_uuid>/<impression_name>/<message>', methods=['GET'])
def set_detailed_status(project_uuid, impression_name, message):
    """Set detailed status message for an impression."""
    job_path = config.get_job_path(project_uuid, impression_name)
    job = VJob(job_path, None)
    job.set_detailed_status(message)
    return jsonify({
        "status": "ok",
        "message": "Detailed status set",
        "detailed_status": message
    })


@bp.route("/status/<project_uuid>/<impression_name>", methods=['GET'])
def status(project_uuid, impression_name):  # pylint: disable=too-many-locals
    """Get status for an impression.

    Returns JSON with musical status name and detailed status message.
    Optional query parameter: legacy=true to return legacy status name.
    """
    _debug.debug(
        f"[status] request path={request.path} "
        f"project={project_uuid} impression={impression_name} "
        f"remote={request.remote_addr} "
        f"ua={request.headers.get('User-Agent', '')!r} "
        f"referer={request.headers.get('Referer', '')!r}"
    )
    job_path = config.get_job_path(project_uuid, impression_name)
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    job_config_file = ConfigFile(config.get_job_config_path(project_uuid, impression_name))
    object_type = job_config_file.read_variable("object_type", "")
    current_path = job_config_file.read_variable("current_path", "")
    dependencies = job_config_file.read_variable("dependencies", [])
    aliases = job_config_file.read_variable("alias_to_impression", {})
    _debug.debug(
        f"[status] job_config object_type={object_type!r} "
        f"current_path={current_path!r} "
        f"dependencies={dependencies} "
        f"alias_to_impression={aliases}"
    )

    if object_type == "":
        return jsonify({
            "status": "empty",
            "detailed_status": "Job object type is empty",
            "status_legacy": "empty"
        })

    for machine in runners_list:
        machine_id = runners_id[machine]

        job = VJob(job_path, machine_id)
        if job.workflow_id() == "":
            _debug.debug(
                f"[status] skipping runner={machine} machine_id={machine_id} "
                f"because workflow_id is empty"
            )
            continue
        _debug.debug(f"[status] impression={impression_name} runner={machine} "
                     f"machine_id={machine_id} workflow_id={job.workflow_id()}")
        workflow = VWorkflow.create(project_uuid, [], job.workflow_id())
        _debug.debug(f"[status] workflow backend_type={workflow.backend_type()}")
        workflow_status = workflow.status()
        # print("Status from workflow", workflow_status)
        workflow_path = os.path.join(
            os.environ["HOME"],
            ".Yuki",
            "Workflows",
            project_uuid,
            job.workflow_id()
        )

        _debug.debug("Path: %s", workflow_path)
        job.update_status_from_workflow( # workflow path
                    workflow_path
                )
        # Update workflow status check to use musical names
        workflow_musical = translate_to_musical(workflow_status)
        _debug.debug("The status is: %s", workflow_musical)
        if workflow_musical not in (CODA, FAILED):
            last_update_time = CHERN_CACHE.update_table.get(workflow.uuid, -1)
            _debug.debug(f"Time difference: {time.time() - last_update_time}")
            if (time.time() - last_update_time) > 5:
                CHERN_CACHE.update_table[workflow.uuid] = time.time()
                _debug.debug(f"[status] scheduling task_update_workflow_status for "
                             f"workflow={workflow.uuid} backend={workflow.backend_type()}")
                task_update_workflow_status.apply_async(args=[project_uuid, workflow.uuid])
            else:
                _debug.debug("Skipping workflow status update to avoid frequent updates.")
        else:
            _debug.debug(f"[status] not scheduling task_update_workflow_status for "
                         f"workflow={workflow.uuid}: already terminal ({workflow_musical})")

        job_status = job.status()
        detailed_status = job.detailed_status()

        if job_status != "unknown":
            return jsonify({
                "status": job_status,
                "detailed_status": detailed_status,
                "status_legacy": translate_to_legacy(job_status),
                "status_musical": translate_to_musical(job_status)
            })

        if os.path.exists(job_path):
            return jsonify({
                "status": "deposited",
                "detailed_status": "Data has been deposited",
                "status_legacy": "deposited",
                "status_musical": "deposited"
            })

    job = VJob(job_path, None)
    job_status = job.status()
    detailed_status = job.detailed_status()

    return jsonify({
        "status": job_status,
        "detailed_status": detailed_status,
        "status_legacy": translate_to_legacy(job_status),
        "status_musical": translate_to_musical(job_status)
    })


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
    """Return the data UUID for a ready rawdata impression.

    Older uploads store sample_uuid in config.json. Canonical registrations
    keep it in contents/celebi.yaml and record copy completion in status.json.
    """
    job_config_file = ConfigFile(config.get_job_config_path(project_uuid, impression_name))
    sample_uuid = job_config_file.read_variable("sample_uuid", "")
    if sample_uuid:
        return sample_uuid

    job_path = config.get_job_path(project_uuid, impression_name)
    job_status = ConfigFile(os.path.join(job_path, "status.json")).read_variable("status", "")
    if translate_to_legacy(job_status) not in ("finished", "success", "archived"):
        return ""
    task_config = YamlFile(os.path.join(job_path, "contents", "celebi.yaml"))
    if task_config.read_variable("environment", "") != "rawdata":
        return ""
    return task_config.read_variable("uuid", "")


@bp.route("/impression/<project_uuid>/<impression_name>", methods=['GET'])
def impression(project_uuid, impression_name):
    """Get impression path."""
    return config.get_job_path(project_uuid, impression_name)


def _refresh_distribution(project_uuid, impression_name):
    """Best-effort refresh of the impression's data-status registry.

    The registry is maintained opportunistically (workflow completion,
    transfers), so a read refreshes the cheap parts — produced workflow
    entries and the yuki entry — without the live cache checks. Failures
    are swallowed: whereabouts must never 500 over a stale registry.
    """
    try:
        from Yuki.kernel.impression_storage import ImpressionStorage
        ImpressionStorage(project_uuid, impression_name).update_distribution()
    except Exception:  # pylint: disable=broad-exception-caught
        pass


@bp.route("/whereabouts/<project_uuid>/<impression_name>", methods=['GET'])
def whereabouts(project_uuid, impression_name):
    """Report where an impression's data lives (runner cache / yuki)."""
    _refresh_distribution(project_uuid, impression_name)
    job_path = config.get_job_path(project_uuid, impression_name)
    dist_path = os.path.join(job_path, "distribution.json")
    locations = {}
    if os.path.isfile(dist_path):
        try:
            with open(dist_path, encoding="utf-8") as fh:
                locations = json.load(fh).get("locations", {}) or {}
        except (OSError, ValueError):
            locations = {}

    runners_id = config.get_config_file().read_variable("runners_id", {})
    id_to_name = {rid: name for name, rid in runners_id.items()}
    runners = {}
    for loc, value in locations.items():
        if not loc.startswith("runner:"):
            continue
        runner_id = loc[len("runner:"):]
        block = value if isinstance(value, dict) else {}
        entry = {}
        if block.get("workflow"):
            entry["workflow"] = block["workflow"]
        if block.get("cache"):
            entry["cache"] = block["cache"]
        runners[id_to_name.get(runner_id, runner_id)] = entry

    registered = None
    remote_marker = os.path.join(job_path, "remote.json")
    if os.path.isfile(remote_marker):
        marker = ConfigFile(remote_marker)
        host_id = marker.read_variable("host_runner_id", "")
        registered = {
            "host_runner": id_to_name.get(host_id, host_id),
            "source_path": marker.read_variable("source_path", ""),
            "remote_path": marker.read_variable("remote_path", ""),
        }

    note = None
    if not os.path.isfile(dist_path):
        note = "distribution.json not recorded"
    elif not locations:
        note = "no distribution entries recorded"
    return jsonify({"impression": impression_name,
                    "yuki": locations.get("yuki"),
                    "runners": runners, "registered": registered,
                    "note": note})


@bp.route("/refresh-distribution/<project_uuid>/<impression_name>", methods=['POST'])
def refresh_distribution(project_uuid, impression_name):
    """Refresh the impression's distribution.json registry."""
    try:
        from ...kernel.impression_storage import ImpressionStorage
        summary = ImpressionStorage(project_uuid, impression_name).update_distribution()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(exc)}), 500
    return jsonify(summary)

def _walk_files(full_path, base_dir):
    """Relative paths of every file under full_path, sorted for display.

    Subdirectories are walked so nested files (e.g. plots/*.png) render
    as entries themselves rather than as a bare directory name.
    """
    files = []
    for root, _dirs, names in os.walk(full_path):
        for name in names:
            files.append(os.path.relpath(os.path.join(root, name),
                                         full_path))

    # Define directory-specific sort priority
    # outputs: chern.stdout first (0), logs: standard sort (1)
    files.sort(
        key=lambda x: (
            (0 if x == "chern.stdout" and base_dir == 'stageout' else 1),
            os.path.splitext(x)[1].lower(),
            x.lower()
        )
    )
    return files


def process_directory(job_path, runner_id, base_dir, file_infos_dict, max_preview_chars):
    """Lists files in a directory tree and adds their metadata/preview
    content to file_infos_dict, keyed by the path relative to base_dir.
    """
    full_path = os.path.join(job_path, runner_id, base_dir)

    if not os.path.exists(full_path):
        return

    files = _walk_files(full_path, base_dir)

    for filename in files:
        # Prevent 'logs' from overwriting files already found in 'outputs'
        if (filename in file_infos_dict and
            file_infos_dict[filename].get('source_dir') == 'stageout'):
            continue

        ext = os.path.splitext(filename)[1].lower()
        is_image = file_types.is_inline_image(filename)
        is_text = ext in ('.txt', '.log', '.stdout')
        watermarked = base_dir == 'watermarks'
        is_log = base_dir == 'logs'

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
            file_info['content'] = generate_text_preview(
                job_path, runner_id, base_dir, filename, max_preview_chars)

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
                    f'<span class="txt-message">[First {max_chars} characters '
                    f'from head: **begin**]</span>\n'
                    f'{head}\n'
                    f'<span class="txt-message">[First {max_chars} characters '
                    f'from head: **end**]</span>\n'
                    f'<span class="txt-separator">--- Content Omitted '
                    f'(Full file available for download) ---</span>\n'  # Added descriptive text
                    f'<span class="txt-message">[Last {max_chars} characters '
                    f'from tail: **begin**]</span>\n'
                    f'{tail}\n'
                    f'<span class="txt-message">[Last {max_chars} characters '
                    f'from tail: **end**]</span>'
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

    # Use a dictionary to store file info keyed by filename
    # to avoid duplicates when processing 'logs'
    file_infos_dict = {}

    max_preview_chars = 1000  # Maximum characters to read for text file previews
    # Process 'outputs' and 'logs' directories
    process_directory(job_path, runner_id, "stageout", file_infos_dict, max_preview_chars)
    process_directory(job_path, runner_id, "logs", file_infos_dict, max_preview_chars)
    process_directory(job_path, runner_id, "watermarks", file_infos_dict, max_preview_chars)

    _debug.debug(file_infos_dict)
    # Convert dictionary values to a list for the template
    final_file_infos = list(file_infos_dict.values())

    # NOTE: The provided original code had sorting logic after processing
    # 'outputs' and a different one for 'logs'.
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


def process_directory2(job_path, runner_id, sub_dir, file_infos_dict,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
                       max_chars, project_uuid, imp_id):
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
            is_image = file_types.is_inline_image(fname)
            # Data/Log extensions
            is_text = ext in ['.txt', '.md', '.json', '.yaml', '.py',
                              '.log', '.stdout', '.stderr', '.csv']

            file_url = url_for(target_route,
                               project_uuid=project_uuid,
                               impression=imp_id,
                               runner_id=runner_id,
                               filename=fname)

            content = ""
            if is_text:
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                        content = f.read(max_chars)
                except (IOError, OSError):
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
    """Test endpoint for project tree visualization."""
    base_path = os.path.join(os.path.expanduser("~"), ".Yuki", "Bookkeep", project_uuid)

    if not os.path.exists(base_path):
        return "Project metadata not found", 404

    def build_tree_data(path, is_root=False):  # pylint: disable=too-many-locals
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
                with open(config_path, 'r', encoding='utf-8') as f:
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
                            except Exception:
                                runner_id = "default_runner"

                            file_infos_dict = {}
                            max_preview_chars = 1000

                            # Gather Plots and Files from all three locations
                            for d in ["stageout", "logs", "watermarks"]:
                                process_directory2(job_path, runner_id, d, file_infos_dict,
                                                   max_preview_chars, project_uuid, imp_id)

                            # Sort: Plots (images) usually in stageout, so we prioritize that
                            data["impression_data"] = sorted(
                                file_infos_dict.values(),
                                key=lambda x: (0 if x['is_image'] else 1, x['name'].lower())
                            )
            except (json.JSONDecodeError, IOError, OSError):
                pass

        readme_path = os.path.join(path, "README.md")
        if os.path.exists(readme_path):
            with open(readme_path, 'r', encoding='utf-8', errors='replace') as f:
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

    # Clean the folder first
    if os.path.exists(base_save_path):
        import shutil
        shutil.rmtree(base_save_path)

    os.makedirs(base_save_path)

    # 3. Save the manifest itself for reference
    with open(os.path.join(base_save_path, "manifest.json"), "w", encoding='utf-8') as f:
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

    _debug.debug("Project %s bookkept successfully at %s",
                 project_uuid, base_save_path)

    return jsonify({
        "status": "success",
        "message": "Project structure and files saved",
        "path": base_save_path
    }), 200

@bp.route("/workflows/<project_uuid>", methods=['GET'])
def workflows(project_uuid):
    """Get list of workflows for a project."""
    workflows_path = os.path.join(
        os.environ["HOME"],
        ".Yuki",
        "Workflows",
        project_uuid
    )
    if not os.path.exists(workflows_path):
        return ""

    workflow_ids = os.listdir(workflows_path)
    return " ".join(workflow_ids)

@bp.route("/homekeep/<project_uuid>", methods=['GET'])
def homekeep(project_uuid):
    """Deprecated: collect results, then free the workspace explicitly."""
    del project_uuid  # kept for route compatibility
    return jsonify({
        "error": ("homekeep is outdated; collect results then free the "
                  "workspace with /delete-workflow/<project>/<workflow>")
    }), 410

# The error log folder is like:
# You can use the VJob.log(index)
@bp.route("/error-log/<project_uuid>/<impression_name>/<index>", methods=['GET'])
def error_log(project_uuid, impression_name, index):
    """Get error log content for a specific impression and log index.

    Query parameters:
        offset (int): Byte offset to start reading from. Defaults to 0.
    """
    job_path = config.get_job_path(project_uuid, impression_name)
    job = VJob(job_path, None)
    offset = request.args.get("offset", 0, type=int)
    log_content = job.log(index, offset=offset)
    if not log_content:
        return "", 204
    return log_content


@bp.route("/engine-log/<project_uuid>/<impression_name>", methods=['GET'])
def engine_log(project_uuid, impression_name):
    """Get the engine log for a specific impression.

    Engine logs are stored in the workflow directory as engine_logs.json.
    The impression_name can be either a job UUID or a workflow UUID.
    """
    # First, try to interpret impression_name as a workflow ID directly
    workflow_id = impression_name

    # Check if the log exists at the workflow path
    log_path = os.path.join(
        os.environ["HOME"],
        ".Yuki",
        "Workflows",
        project_uuid,
        workflow_id,
        "engine_logs.json"
    )

    # If not found and impression exists in storage, try to get workflow from job
    if not os.path.exists(log_path):
        job_path = config.get_job_path(project_uuid, impression_name)
        if os.path.exists(job_path):
            try:
                job = VJob(job_path, None)
                # Check if job has machine_id (required for run_config_file)
                if job.machine_id is not None:
                    workflow_id = job.workflow_id()
                    if workflow_id:
                        log_path = os.path.join(
                            os.environ["HOME"],
                            ".Yuki",
                            "Workflows",
                            project_uuid,
                            workflow_id,
                            "engine_logs.json"
                        )
            except (AttributeError, OSError):
                pass  # Fall through to check if log_path exists

    # On demand: generate the log file by asking the workflow backend to
    # fetch its engine logs (ssh pulls snakemake.log from the runner, etc).
    if not os.path.exists(log_path) and request.args.get("fetch") == "true" \
            and workflow_id:
        try:
            workflow = VWorkflow.create(project_uuid, [], workflow_id)
            workflow.get_workflow_logs()
        except Exception:  # pylint: disable=broad-exception-caught
            pass  # fall through to the yuki-log fallback below

    if not os.path.exists(log_path):
        # Even without fetch, serve the Yuki-side workflow log (written
        # while the workflow ran); only the remote engine log needs fetch.
        workflow_log_path = os.path.join(
            os.path.dirname(log_path), "workflow.log")
        if os.path.exists(workflow_log_path):
            with open(workflow_log_path, "r", encoding="utf-8") as fh:
                content = fh.read()
            return jsonify({"logs": {
                "backend": "yuki",
                "workflow_uuid": workflow_id,
                "workflow_log": content,
            }})
        return jsonify({"error": "Engine log not found"}), 404

    return send_from_directory(
        os.path.dirname(log_path),
        "engine_logs.json"
    )
