"""
Live-set sync routes: Celebi pushes which impressions are the current
versions of tasks/algorithms; Yuki stores and serves the set.
"""
from flask import Blueprint, jsonify, request

from ...kernel.storage import liveness

bp = Blueprint('liveness', __name__)


@bp.route("/live-set/<project_uuid>", methods=['PUT'])
def put_live_set(project_uuid):
    """Replace the project's live set (idempotent full-state sync)."""
    data = request.get_json(silent=True) or {}
    live = data.get("live") or []
    superseded = data.get("superseded") or []
    try:
        summary = liveness.save_live_set(project_uuid, live, superseded)
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 500
    return jsonify(summary)


@bp.route("/live/<project_uuid>", methods=['GET'])
def get_live(project_uuid):
    """The stored live set, or 404 when none has been synced."""
    data = liveness.load_live_set(project_uuid)
    if data is None:
        return jsonify({"error": f"no live set for project "
                                 f"'{project_uuid}'"}), 404
    return jsonify({
        "live_impressions": data.get("live", []),
        "live_workflows": data.get("live_workflows", []),
        "superseded": data.get("superseded", []),
        "updated": data.get("updated", ""),
    })
