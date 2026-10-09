"""
Bulk impression stageout export/import routes.
"""
import io
import json
import os
import re
import tarfile
from logging import getLogger

from CelebiChrono.utils import csys
from flask import Blueprint, request, send_file, jsonify

from ...kernel import result_transfer
from ...kernel.runners import config as runner_config
from ..config import config
from ..tasks import task_transfer_results

bp = Blueprint('transfer', __name__)
logger = getLogger("YukiLogger")

UUID_RE = re.compile(r'^[0-9a-f]{32}$')


@bp.route('/export-imp-stageout', methods=['POST'])
def export_impressions():  # pylint: disable=too-many-locals
    """Export stageout results from multiple impressions as a tar.gz."""
    data = request.get_json()
    project_uuid = data.get("project_uuid")
    impressions = data.get("impressions", [])

    if not project_uuid or not impressions:
        return jsonify({"error": "project_uuid and impressions are required"}), 400

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for impression in impressions:
            job_path = config.get_job_path(project_uuid, impression)
            if not os.path.isdir(job_path):
                continue
            for entry in os.listdir(job_path):
                stageout_dir = os.path.join(job_path, entry, "stageout")
                if not os.path.isdir(stageout_dir):
                    continue
                runner_id = entry
                for root, _, files in os.walk(stageout_dir):
                    for fname in files:
                        full_path = os.path.join(root, fname)
                        rel = os.path.relpath(full_path, stageout_dir)
                        arcname = os.path.join(impression, runner_id, "stageout", rel)
                        tar.add(full_path, arcname=arcname)

    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/gzip",
        as_attachment=True,
        download_name="impressions_export.tar.gz",
    )


@bp.route('/import-imp-stageout', methods=['POST'])
def import_impressions():
    """Import stageout results from a tar into the corresponding job folders."""
    project_uuid = request.form.get("project_uuid")
    if not project_uuid:
        return jsonify({"error": "project_uuid is required"}), 400

    tar_file = request.files.get("tarfile")
    if not tar_file:
        return jsonify({"error": "tarfile is required"}), 400

    imported = set()
    with tarfile.open(fileobj=tar_file.stream, mode="r:*") as tar:
        for member in tar.getmembers():
            if member.isdir():
                continue
            parts = member.name.split("/")
            # Expected: {impression}/{runner_id}/stageout/{...files}
            if len(parts) < 4 or parts[2] != "stageout":
                continue
            if ".." in parts or any(p.startswith("/") for p in parts):
                continue
            impression = parts[0]
            if not UUID_RE.match(impression):
                continue

            target_dir = os.path.join(
                config.storage_path, project_uuid, *parts[:-1]
            )
            os.makedirs(target_dir, exist_ok=True)

            source = tar.extractfile(member)
            if source is None:
                continue
            target_path = os.path.join(config.storage_path, project_uuid, member.name)
            with open(target_path, "wb") as f:
                f.write(source.read())

            imported.add(impression)

    return jsonify({"imported": sorted(imported), "count": len(imported)})


@bp.route("/transfer", methods=['POST'])
def start_transfer():  # pylint: disable=too-many-locals,too-many-return-statements,too-many-branches
    """Start a result transfer job."""
    data = request.get_json(silent=True) or request.form
    project_uuid = data.get("project_uuid", "")
    impression = data.get("impression", "")
    source = data.get("source", "")
    destination = data.get("destination", "")
    pattern = data.get("pattern") or None
    force = bool(data.get("force", False))

    if not (project_uuid and impression and source and destination):
        return jsonify({"error": "missing required field"}), 400

    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    backend_types = config_file.read_variable("backend_types", {})

    names = {}
    for role, loc in (("source", source), ("destination", destination)):
        if loc == "yuki":
            names[role] = None
            continue
        if loc.startswith("runner:"):
            name = loc[len("runner:"):]
            if not name:
                return jsonify({"error": f"invalid location: {loc}"}), 400
            if name not in runners_id:
                return jsonify({"error": f"runner '{name}' not found"}), 404
            names[role] = name
        else:
            return jsonify({"error": f"invalid location: {loc}"}), 400

    for role, name in names.items():
        if name is None:
            continue
        backend = backend_types.get(runners_id[name], "reana")
        if runner_config.is_ssh_backend(backend):
            continue
        # Only exception: a reana source may transfer to an ssh runner,
        # which pulls the files with its own reana-cli.
        if (role == "source" and backend == "reana"
                and names["destination"] is not None
                and runner_config.is_ssh_backend(
                    backend_types.get(runners_id[names["destination"]],
                                      "reana"))):
            continue
        return jsonify({
            "error": f"runner '{name}' is not an ssh runner"
        }), 400

    if source == "yuki" and destination == "yuki":
        return jsonify({
            "error": "source and destination cannot both be yuki"
        }), 400

    job_id = csys.generate_uuid()
    yuki_dir = result_transfer._resolve_yuki_dir()  # pylint: disable=protected-access
    progress_dir = os.path.join(yuki_dir, "transfer-progress")
    os.makedirs(progress_dir, exist_ok=True)
    progress_path = os.path.join(progress_dir, f"{job_id}.json")
    with open(progress_path, "w", encoding="utf-8") as f:
        json.dump({"status": "pending", "bytes_done": 0,
                   "bytes_total": 0, "current_file": ""}, f)

    try:
        task_transfer_results.apply_async(
            args=[job_id, project_uuid, impression,
                  source, destination, pattern, force])
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # Record the failure so the progress file never stays pending.
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump({"status": "failed", "bytes_done": 0,
                       "bytes_total": 0, "current_file": "",
                       "error": str(exc)}, f)
        return jsonify({"job_id": job_id, "error": str(exc)}), 500
    return jsonify({"job_id": job_id})


@bp.route("/transfer/<job_id>", methods=['GET'])
def transfer_status(job_id):
    """Poll a transfer job's state."""
    if not UUID_RE.match(job_id):
        return jsonify({"error": "job not found"}), 404
    yuki_dir = result_transfer._resolve_yuki_dir()  # pylint: disable=protected-access
    progress_path = os.path.join(yuki_dir, "transfer-progress", f"{job_id}.json")
    if not os.path.exists(progress_path):
        return jsonify({"error": "job not found"}), 404
    try:
        with open(progress_path, encoding="utf-8") as f:
            state = json.load(f)
    except (OSError, ValueError):
        return jsonify({"error": "corrupt job state"}), 500
    return jsonify(state)
