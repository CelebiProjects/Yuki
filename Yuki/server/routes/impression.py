"""Impression import/export API routes."""

from logging import getLogger

from flask import Blueprint, request, send_file, jsonify

from Yuki.services.impression_transfer import (
    export_impression_to_buffer,
    import_impression_from_stream,
)

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
