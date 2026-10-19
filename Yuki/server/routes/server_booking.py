"""Small-result bookkeeping endpoints; archive paths are resolved by Yuki."""
import json
import re
import tempfile
from pathlib import Path

from flask import Blueprint, request, jsonify
from Yuki.server.config import config
from Yuki.services.celebi_server_booker import (
    pack_booking_archives, upload_booking_archives, validate_scope,
    collect_finished_booking_outputs, still_finished,
)

bp = Blueprint("server_booking", __name__)

def _staging_root():
    root = Path(config.storage_path).parent / "bookkeeping-staging"
    root.mkdir(parents=True, exist_ok=True)
    return root

@bp.route("/book-celebi-server/pack", methods=["POST"])
def pack():
    body = request.get_json(silent=True) or {}
    try:
        validate_scope(body.get("project_uuid"), body.get("impressions"))
        limit = body.get("max_file_size", 5 * 1024 * 1024)
        if not isinstance(limit, int) or not 0 < limit <= 5 * 1024 * 1024:
            raise ValueError("Invalid size limit")
        prepared = collect_finished_booking_outputs(body["project_uuid"], body["impressions"], limit)
        directory = Path(tempfile.mkdtemp(prefix="pack-", dir=_staging_root()))
        result = pack_booking_archives(config.storage_path, body["project_uuid"],
            body["impressions"], directory, limit,
            eligible=prepared["eligible"], filelists=prepared["filelists"])
        # Not-finished is an intentional exclusion, not a failed collect.
        skipped = [uid for uid in sorted(set(body["impressions"]))
                   if uid not in prepared["eligible"] and not prepared["reports"].get(uid)]
        result["missing"] = [uid for uid in result["missing"] if uid not in skipped]
        result["skipped"] = [{"uuid": uid, "reason": "job-not-finished"} for uid in skipped]
        metadata = {"project_uuid": body["project_uuid"], "packed": result["packed"],
                    "eligible": prepared["eligible"]}
        (directory / "manifest.json").write_text(json.dumps(metadata))
        result["pack_id"] = directory.name
        result["collection"] = prepared["reports"]
        return jsonify(result)
    except (ValueError, TypeError, KeyError):
        return jsonify({"error": "Invalid project, impressions, or size limit"}), 400

@bp.route("/book-celebi-server/upload", methods=["POST"])
def upload():
    body = request.get_json(silent=True) or {}
    try:
        pack_id = body.get("pack_id", "")
        if not re.fullmatch(r"pack-[a-zA-Z0-9_-]+", pack_id):
            raise ValueError("Invalid pack ID")
        directory = _staging_root() / pack_id
        metadata = json.loads((directory / "manifest.json").read_text())
        if metadata["project_uuid"] != body.get("project_uuid"):
            raise ValueError("Project mismatch")
        archives = body.get("archives")
        validate_scope(body["project_uuid"], [entry["uuid"] for entry in archives])
        declared = {entry["uuid"]: entry for entry in metadata["packed"]}
        if any(entry != declared.get(entry["uuid"]) for entry in archives):
            raise ValueError("Archive was not packed in this batch")
        if any(not still_finished(body["project_uuid"], entry["uuid"],
                                  metadata.get("eligible", {}).get(entry["uuid"], {}))
               for entry in archives):
            raise ValueError("Only finished workflow attempts may be uploaded")
        grant = body.get("grant")
        if not isinstance(grant, str) or not grant:
            raise ValueError("Missing upload grant")
        result = upload_booking_archives(body["celebi_server_url"], body["project_uuid"],
                                         archives, grant, directory)
        return jsonify(result)
    except (OSError, ValueError, TypeError, KeyError):
        return jsonify({"error": "Invalid upload request; pack results first"}), 400
