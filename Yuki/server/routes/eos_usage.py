"""Submit or inspect a read-only REANA EOS inventory job."""
from flask import Blueprint, jsonify, request
from ...services import eos_usage

bp = Blueprint("eos_usage", __name__)


def public(state):
    # The detailed report includes inventory; avoid sending it twice.
    return {key: value for key, value in state.items() if key != "inventory"}


@bp.route('/eos-usage', methods=['POST'])
def start():
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or not isinstance(data.get("runner"), str):
        return jsonify(error="JSON runner and scan_id are required"), 400
    try:
        state = eos_usage.start(data["runner"], data.get("scan_id"), data.get("project"))
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    return jsonify(public(state)), 202


@bp.route('/eos-usage/<scan_id>', methods=['GET'])
def status(scan_id):
    try:
        state = eos_usage.poll(scan_id)
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    except FileNotFoundError:
        return jsonify(error="Scan not found"), 404
    return jsonify(public(state))


@bp.route('/eos-cleanup/plan', methods=['POST'])
def cleanup_plan():
    from ...services import eos_cleanup
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify(error="JSON object required"), 400
    try:
        state = eos_cleanup.plan(data.get("scan_id"), data.get("projects"),
                                 data.get("include_unknown", False),
                                 data.get("clear_projects", []))
    except (ValueError, TypeError) as exc:
        return jsonify(error=str(exc)), 400
    return jsonify(state), 201


@bp.route('/eos-cleanup/<plan_id>/execute', methods=['POST'])
def cleanup_execute(plan_id):
    from ...services import eos_cleanup
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or not isinstance(data.get("manifest_sha256"), str):
        return jsonify(error="Preview manifest_sha256 confirmation is required"), 400
    try:
        state = eos_cleanup.execute(plan_id, data["manifest_sha256"])
    except ValueError as exc:
        return jsonify(error=str(exc)), 409
    except FileNotFoundError:
        return jsonify(error="Cleanup plan not found"), 404
    return jsonify(state), 202


@bp.route('/eos-cleanup/<plan_id>', methods=['GET'])
def cleanup_status(plan_id):
    from ...services import eos_cleanup
    try:
        state = eos_cleanup.status(plan_id)
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    except FileNotFoundError:
        return jsonify(error="Cleanup plan not found"), 404
    return jsonify(state)
