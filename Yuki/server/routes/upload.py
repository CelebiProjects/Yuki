"""
File upload and download routes.
"""
import os
import uuid
import tarfile
from contextlib import ExitStack
from logging import getLogger

from flask import Blueprint, request, send_from_directory, jsonify, Response
from CelebiChrono.utils import metadata

from ..config import config
from ..resumable_upload_handler import get_upload_manager
from ...kernel.impression_storage import ImpressionStorage
from ...kernel.vworkflow import VWorkflow

bp = Blueprint('upload', __name__)
logger = getLogger("YukiLogger")
_debug = getLogger("Yuki.execution")


def _safe_send_from_directory(directory, filename):
    """Validate filename stays inside directory before serving it.

    Returns a Flask Response on success, or a "NOTFOUND" string if the
    requested path would escape the intended directory.
    """
    real_dir = os.path.realpath(directory)
    real_path = os.path.realpath(os.path.join(directory, filename))
    if real_path != real_dir and not real_path.startswith(real_dir + os.sep):
        logger.warning("Path traversal attempt blocked: %s under %s", filename, directory)
        return "NOTFOUND"
    return send_from_directory(directory, filename)


@bp.route('/upload', methods=['POST'])
def upload_file():
    """Upload a tar file and extract it to the target directory."""
    project_uuid = request.form["project_uuid"]
    tarname = request.form["tarname"]

    target_dir = os.path.join(
        config.storage_path, project_uuid, tarname[:-7]
    )
    os.makedirs(target_dir, exist_ok=True)

    tar_file = request.files[tarname]

    with tarfile.open(fileobj=tar_file.stream, mode="r:*") as tar:
        tar.extractall(target_dir)

    config_file = request.form['config']
    request.files[config_file].save(
        os.path.join(target_dir, config_file)
    )

    # Save config.local.json if provided; Yuki uses it for per-task local
    # settings such as the APD token.
    local_config_file = request.files.get("config.local.json")
    if local_config_file:
        local_config_file.save(
            os.path.join(target_dir, "config.local.json")
        )

    return "Successful"

# @bp.route('/upload', methods=['GET', 'POST'])
# def upload_file():
#     """Handle file uploads."""
#     if request.method == 'POST':
#         print("Trying to upload files:", request.form)
#         project_uuid = request.form["project_uuid"]
#         tarname = request.form["tarname"]
#         request.files[tarname].save(os.path.join("/tmp", tarname))
#
#         with tarfile.open(os.path.join("/tmp", tarname), "r") as tar:
#             for ti in tar:
#                 tar.extract(ti, os.path.join(config.storage_path, project_uuid, tarname[:-7]))
#
#         config_file = request.form['config']
#         logger.info(config_file)
#         request.files[config_file].save(
#             os.path.join(config.storage_path, project_uuid, tarname[:-7], config_file)
#         )
#     return "Successful"


@bp.route("/download/<filename>", methods=['GET'])
def download_file(filename):
    """Download a file."""
    directory = os.path.join(os.getcwd(), "data")
    return send_from_directory(directory, filename, as_attachment=True)


@bp.route("/export/<project_uuid>/<impression>/<path:filename>", methods=['GET'])
def export(project_uuid, impression, filename):
    """Export a file from an impression."""
    job_path = config.get_job_path(project_uuid, impression)
    config_file = config.get_config_file()

    _debug.debug("EXPORTING %s %s", job_path, filename)
    rawdata_dir = os.path.join(job_path, "rawdata")
    full_path = os.path.join(rawdata_dir, filename)
    if os.path.exists(full_path):
        return _safe_send_from_directory(rawdata_dir, filename)

    runners = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    # Search for the first machine that has the file
    for runner in runners:
        runner_id = runners_id[runner]
        path = os.path.join(job_path, runner_id, "stageout")
        full_path = os.path.join(path, filename)
        _debug.debug("path %s", full_path)
        if os.path.exists(full_path):
            return _safe_send_from_directory(path, filename)
    return "NOTFOUND"


@bp.route("/remote-export/<project_uuid>/<impression>/<path:filename>", methods=['GET'])
def remote_export(project_uuid, impression, filename):
    """Stream a file directly from an SSH runner without copying it to Yuki storage."""
    storage = ImpressionStorage(project_uuid, impression)
    for name, job, workflow in storage._get_runner_contexts():  # pylint: disable=protected-access
        if workflow.__class__.__name__ != "SshWorkflow":
            continue
        src_path = f"{workflow.remote_exec_path}/imp{impression[0:7]}/stageout/{filename}"
        try:
            with ExitStack() as resources:
                ssh = resources.enter_context(workflow._ssh())  # pylint: disable=protected-access
                if not ssh.isfile(src_path):
                    continue
                size = ssh._sftp.stat(src_path).st_size  # pylint: disable=protected-access
                range_header = request.headers.get("Range", "")
                start, end = 0, size - 1
                status = 200
                if range_header.startswith("bytes="):
                    requested = range_header[6:].split(",", 1)[0].strip()
                    first, _, last = requested.partition("-")
                    if first:
                        start = int(first)
                        end = int(last) if last else end
                    elif last:
                        start = max(0, size - int(last))
                    if start >= size or start > end:
                        return Response(status=416, headers={"Content-Range": f"bytes */{size}"})
                    end = min(end, size - 1)
                    status = 206
                # Transfer ownership of the existing connection to the response.
                # Flask consumes the body after the route has returned.
                remote_file = resources.enter_context(ssh._sftp.file(src_path, "rb"))  # pylint: disable=protected-access
                remote_file.seek(start)
                owned = None

                def stream_remote():
                    try:
                        remaining = end - start + 1
                        while remaining:
                            chunk = remote_file.read(min(65536, remaining))
                            if not chunk:
                                raise OSError("Remote file ended before the requested range was read")
                            remaining -= len(chunk)
                            yield chunk
                    finally:
                        owned.close()

                headers = {
                    "Content-Disposition": f"inline; filename={filename}",
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(end - start + 1),
                }
                if status == 206:
                    headers["Content-Range"] = f"bytes {start}-{end}/{size}"
                response = Response(stream_remote(), headers=headers, status=status)
                # Also release resources if the response closes before iteration
                # starts (HEAD requests and disconnected clients).
                owned = resources.pop_all()
                response.call_on_close(owned.close)
                return response
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("remote-export failed for %s on %s: %s", filename, name, exc)
            continue
    return "NOTFOUND"


@bp.route("/get-file/<project_uuid>/<impression>/<path:filename>", methods=['GET'])
def get_file(project_uuid, impression, filename):
    """Get the path to a specific file in an impression."""
    job_path = config.get_job_path(project_uuid, impression)
    config_file = config.get_config_file()
    runners = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    for machine in runners:
        machine_id = runners_id[machine]
        path = os.path.join(job_path, machine_id, "stageout")
        full_path = os.path.join(path, filename)
        real_path = os.path.realpath(full_path)
        real_dir = os.path.realpath(path)
        if real_path != real_dir and not real_path.startswith(real_dir + os.sep):
            continue
        if os.path.exists(full_path):
            return full_path
    return "NOTFOUND"

@bp.route("/log-view/<project_uuid>/<impression>/<runner_id>/<path:filename>", methods=['GET'])
def logview(project_uuid, impression, runner_id, filename):
    """View a specific file."""
    job_path = config.get_job_path(project_uuid, impression)
    path = os.path.join(job_path, runner_id, "logs")
    return _safe_send_from_directory(path, filename)


@bp.route("/file-view/<project_uuid>/<impression>/<runner_id>/<path:filename>", methods=['GET'])
def fileview(project_uuid, impression, runner_id, filename):
    """View a specific file."""
    job_path = config.get_job_path(project_uuid, impression)
    path = os.path.join(job_path, runner_id, "stageout")
    return _safe_send_from_directory(path, filename)

@bp.route(
    "/watermark-view/<project_uuid>/<impression>/<runner_id>/<path:filename>",
    methods=['GET']
)
def watermarkview(project_uuid, impression, runner_id, filename):
    """View a specific file."""
    job_path = config.get_job_path(project_uuid, impression)
    path = os.path.join(job_path, runner_id, "watermarks")
    return _safe_send_from_directory(path, filename)


# =============================================================================
# Resumable Upload Endpoints
# =============================================================================

@bp.route('/upload/create', methods=['POST'])
def create_resumable_upload():
    """Create a new resumable upload session.

    Request Body:
        - file_size: Total size of the file in bytes
        - file_md5: MD5 hash of the complete file
        - chunk_size: Size of each chunk in bytes
        - total_chunks: Total number of chunks
        - project_uuid: Project identifier
        - impression_uuid: Impression identifier

    Returns:
        JSON with upload_id for the new session
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Missing request body"}), 400

        required_fields = ['file_size', 'file_md5', 'chunk_size',
                          'total_chunks', 'project_uuid', 'impression_uuid']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Generate upload ID
        upload_id = str(uuid.uuid4())

        # Create upload session
        manager = get_upload_manager(config.storage_path)
        state = manager.create_upload(
            upload_id=upload_id,
            file_size=data['file_size'],
            file_md5=data['file_md5'],
            chunk_size=data['chunk_size'],
            total_chunks=data['total_chunks'],
            project_uuid=data['project_uuid'],
            impression_uuid=data['impression_uuid']
        )

        return jsonify({
            "upload_id": state.upload_id,
            "status": "created"
        })

    except Exception as e:
        logger.error("Failed to create upload: %s", e)
        return jsonify({"error": str(e)}), 500


@bp.route('/upload/chunk/<upload_id>/<int:chunk_index>', methods=['PATCH'])
def upload_chunk(upload_id, chunk_index):
    """Upload a single chunk.

    Args:
        upload_id: Upload session identifier
        chunk_index: Index of the chunk (0-based)

    Headers:
        - Content-MD5: MD5 hash of the chunk data

    Returns:
        200 if chunk stored successfully
        400 if MD5 verification fails
        404 if upload session not found
    """
    try:
        manager = get_upload_manager(config.storage_path)

        # Check upload exists
        state = manager.get_upload(upload_id)
        if state is None:
            return jsonify({"error": "Upload not found"}), 404

        # Get chunk MD5 from header
        chunk_md5 = request.headers.get('Content-MD5')
        if not chunk_md5:
            return jsonify({"error": "Missing Content-MD5 header"}), 400

        # Get chunk data
        chunk_data = request.get_data()

        # Store chunk
        success = manager.store_chunk(upload_id, chunk_index, chunk_data, chunk_md5)

        if success:
            return jsonify({
                "status": "ok",
                "chunk_index": chunk_index,
                "received_bytes": len(chunk_data)
            })
        return jsonify({"error": "Chunk verification failed"}), 400

    except Exception as e:
        logger.error("Failed to store chunk %s for upload %s: %s", chunk_index, upload_id, e)
        return jsonify({"error": str(e)}), 500


@bp.route('/upload/status/<upload_id>', methods=['GET'])
def get_upload_status(upload_id):
    """Get the status of an upload.

    Args:
        upload_id: Upload session identifier

    Returns:
        JSON with completed_chunks list and upload completion status
    """
    try:
        manager = get_upload_manager(config.storage_path)
        state = manager.get_upload(upload_id)

        if state is None:
            return jsonify({"error": "Upload not found"}), 404

        return jsonify({
            "upload_id": upload_id,
            "completed_chunks": list(state.completed_chunks),
            "total_chunks": state.total_chunks,
            "is_complete": manager.is_upload_complete(upload_id),
            "finalized": state.finalized
        })

    except Exception as e:
        logger.error("Failed to get upload status for %s: %s", upload_id, e)
        return jsonify({"error": str(e)}), 500


@bp.route('/upload/complete/<upload_id>', methods=['POST'])
def complete_upload(upload_id):
    """Finalize an upload and extract the archive.

    Args:
        upload_id: Upload session identifier

    Request Body:
        - project_uuid: Project identifier
        - impression_uuid: Impression identifier

    Returns:
        200 if upload finalized successfully
        400 if upload incomplete
        404 if upload not found
    """
    try:
        data = request.get_json() or {}

        manager = get_upload_manager(config.storage_path)

        # Check upload is complete
        if not manager.is_upload_complete(upload_id):
            return jsonify({"error": "Upload incomplete"}), 400

        # Finalize upload
        project_uuid = data.get('project_uuid')
        impression_uuid = data.get('impression_uuid')

        extract_path = manager.finalize_upload(upload_id, project_uuid, impression_uuid)

        if extract_path:
            return jsonify({
                "status": "completed",
                "extract_path": extract_path
            })
        return jsonify({"error": "Failed to finalize upload"}), 500

    except Exception as e:
        logger.error("Failed to complete upload %s: %s", upload_id, e)
        return jsonify({"error": str(e)}), 500


@bp.route('/upload/<upload_id>', methods=['DELETE'])
def cancel_upload(upload_id):
    """Cancel an upload and clean up resources.

    Args:
        upload_id: Upload session identifier

    Returns:
        200 if cancelled successfully
    """
    try:
        manager = get_upload_manager(config.storage_path)
        manager.cancel_upload(upload_id)
        return jsonify({"status": "cancelled"})

    except Exception as e:
        logger.error("Failed to cancel upload %s: %s", upload_id, e)
        return jsonify({"error": str(e)}), 500


@bp.route('/upload-config/<project_uuid>/<impression_uuid>', methods=['POST'])
def upload_config(project_uuid, impression_uuid):
    """Upload config.json for an impression after tar upload.

    This endpoint handles the config.json file upload separately from the
    main tar.gz archive in resumable uploads.

    Args:
        project_uuid: Project identifier
        impression_uuid: Impression identifier

    Returns:
        200 if config saved successfully
    """
    try:
        target_dir = os.path.join(
            config.storage_path, project_uuid, impression_uuid
        )
        os.makedirs(target_dir, exist_ok=True)

        if 'config' not in request.files:
            return jsonify({"error": "No config file provided"}), 400

        config_file = request.files['config']
        config_path = os.path.join(target_dir, "config.json")
        config_file.save(config_path)

        logger.info("Saved config.json for %s/%s", project_uuid, impression_uuid)
        return jsonify({"status": "ok"})

    except Exception as e:
        logger.error("Failed to upload config: %s", e)
        return jsonify({"error": str(e)}), 500


@bp.route('/set-impression-status', methods=['POST'])
def set_impression_status():
    """Set the status of an impression in Yuki storage."""
    project_uuid = request.form["project_uuid"]
    impression = request.form["impression"]
    status = request.form["status"]
    status_path = os.path.join(
        config.storage_path, project_uuid, impression, "status.json"
    )
    os.makedirs(os.path.dirname(status_path), exist_ok=True)
    status_file = metadata.ConfigFile(status_path)
    status_file.write_variable("status", status)
    return "OK"


@bp.route('/get-impression-info/<project_uuid>/<impression>')
def get_impression_info(project_uuid, impression):
    """Get descriptor, md5 and environment from an impression's celebi.yaml."""
    job_path = config.get_job_path(project_uuid, impression)
    yaml_path = os.path.join(job_path, "contents", "celebi.yaml")
    if not os.path.exists(yaml_path):
        return jsonify({
            "descriptor": "",
            "md5": "",
            "environment": "",
        })
    yaml_file = metadata.YamlFile(yaml_path)
    return jsonify({
        "descriptor": yaml_file.read_variable("descriptor", ""),
        "md5": yaml_file.read_variable("uuid", ""),
        "environment": yaml_file.read_variable("environment", ""),
    })
