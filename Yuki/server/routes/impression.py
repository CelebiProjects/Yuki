"""Impression import/export API routes."""

from logging import getLogger

from flask import Blueprint, request, send_file, jsonify
from CelebiChrono.utils import csys

from Yuki.kernel.runners import config as runner_config
from Yuki.kernel.storage import remote as remote_data_ops
from Yuki.services.impression_transfer import (
    export_impression_to_buffer,
    import_impression_from_stream,
)
from Yuki.services.ssh_impression_transfer import JOBS_DIR_NAME
from Yuki.server.config import config
from Yuki.server.tasks import task_share_ssh_impressions

bp = Blueprint('impression', __name__)
logger = getLogger("YukiLogger")


@bp.route('/impression-export', methods=['POST'])
def impression_export():
    """Export full impressions as a tar.gz archive.

    JSON body: {"project_uuid": "...", "impressions": ["...", ...]}
    Returns: tar.gz download.
    """
    data = request.get_json()
    project_uuid = data.get("project_uuid")
    impressions = data.get("impressions", [])

    if not project_uuid or not impressions:
        return jsonify({"error": "project_uuid and impressions are required"}), 400

    buf = export_impression_to_buffer(project_uuid, impressions)
    return send_file(
        buf,
        mimetype="application/gzip",
        as_attachment=True,
        download_name="impressions_export.tar.gz",
    )


@bp.route('/impression-import', methods=['POST'])
def impression_import():
    """Import impressions from a tar.gz archive.

    Multipart form: project_uuid + tarfile.
    Returns: JSON with imported/skipped/count.
    """
    project_uuid = request.form.get("project_uuid")
    if not project_uuid:
        return jsonify({"error": "project_uuid is required"}), 400

    tar_file = request.files.get("tarfile")
    if not tar_file:
        return jsonify({"error": "tarfile is required"}), 400

    result = import_impression_from_stream(project_uuid, tar_file.stream)
    return jsonify(result)


@bp.route('/share-ssh-impressions', methods=['POST'])
def share_ssh_impressions():  # pylint: disable=too-many-return-statements
    """Start a scoped import/export job through a registered SSH runner."""
    data = request.get_json(silent=True) or request.form
    runner = data.get("runner", "")
    project_uuid = data.get("project_uuid", "")
    operation = data.get("operation", "")
    impressions = data.get("impressions", [])
    force = bool(data.get("force", False))
    if not (runner and project_uuid and operation):
        return jsonify({"error": "missing required field: runner/project_uuid/"
                        "operation"}), 400
    if operation not in ("import", "export"):
        return jsonify({"error": "operation must be import or export"}), 400
    if not isinstance(impressions, list) or not impressions:
        return jsonify({"error": "impressions must be a non-empty list"}), 400

    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        return jsonify({"error": f"Runner '{runner}' not found"}), 404
    runner_id = runners_id[runner]
    backend_types = config_file.read_variable("backend_types", {})
    if not runner_config.is_ssh_backend(
            backend_types.get(runner_id, "reana")):
        return jsonify({"error": "share-ssh-impressions requires an ssh runner"}), 400

    job_id = csys.generate_uuid()
    yuki_dir = remote_data_ops._yuki_dir()  # pylint: disable=protected-access
    state = {
        "status": "queued", "result": None, "error": None,
        "runner_id": runner_id, "runner": runner,
        "project_uuid": project_uuid,
        "operation": operation, "impressions": impressions, "force": force,
    }
    remote_data_ops.write_job_state(
        yuki_dir, job_id, state, jobs_dir_name=JOBS_DIR_NAME)
    try:
        task_share_ssh_impressions.apply_async(args=[
            job_id, runner_id, project_uuid, operation, impressions, force,
        ])
    except Exception as exc:  # pylint: disable=broad-exception-caught
        state.update({"status": "failed", "error": str(exc)})
        remote_data_ops.write_job_state(
            yuki_dir, job_id, state, jobs_dir_name=JOBS_DIR_NAME)
        return jsonify({"job_id": job_id, "error": str(exc)}), 500
    return jsonify({"job_id": job_id}), 202


@bp.route('/share-ssh-impressions/<job_id>', methods=['GET'])
def share_ssh_impressions_status(job_id):
    """Return persisted progress for an SSH impression sharing job."""
    state = remote_data_ops.read_job_state(
        remote_data_ops._yuki_dir(), job_id,  # pylint: disable=protected-access
        jobs_dir_name=JOBS_DIR_NAME)
    if state is None:
        return jsonify({"error": "job not found"}), 404
    return jsonify(state)
