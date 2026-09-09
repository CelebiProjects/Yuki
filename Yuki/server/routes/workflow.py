"""
Workflow management routes for starting, stopping, and monitoring workflows.
"""
import os
from flask import Blueprint, request, jsonify
from Yuki.kernel.impression_storage import ImpressionStorage
from Yuki.kernel.vworkflow import VWorkflow
from Yuki.kernel.status_constants import IN_MOVEMENT, translate_to_musical
from ...kernel import workflow_purge, workflow_kill
from ..config import config

bp = Blueprint('workflow', __name__)

@bp.route("/kill/<project_uuid>/<impression>", methods=['GET'])
def kill(project_uuid, impression):
    """Kill a workflow for a specific project and impression."""
    storage = ImpressionStorage(project_uuid, impression)
    storage.kill()
    return "ok"

@bp.route("/collect/<project_uuid>/<impression>", methods=['GET'])
def collect(project_uuid, impression):
    """Collect workflow results for a specific project and impression."""
    storage = ImpressionStorage(project_uuid, impression)
    return jsonify(storage.collect())

@bp.route("/collect-outputs/<project_uuid>/<impression>", methods=['GET'])
def collect_outputs(project_uuid, impression):
    """Collect workflow outputs for a specific project and impression."""
    storage = ImpressionStorage(project_uuid, impression)
    return jsonify(storage.collect_outputs())

@bp.route("/collect-logs/<project_uuid>/<impression>", methods=['GET'])
def collect_logs(project_uuid, impression):
    """Collect workflow logs for a specific project and impression."""
    storage = ImpressionStorage(project_uuid, impression)
    return jsonify(storage.collect_logs())

@bp.route("/watermark/<project_uuid>/<impression>", methods=['GET'])
def watermark(project_uuid, impression):
    """Apply watermark to workflow results for a specific project and impression."""
    storage = ImpressionStorage(project_uuid, impression)
    storage.watermark()
    return "ok"

@bp.route('/workflow/<project_uuid>/<impression>', methods=['GET'])
def workflow(project_uuid, impression):
    """Get workflow information for a specific project and impression."""
    storage = ImpressionStorage(project_uuid, impression)
    return storage.get_info()


@bp.route("/collect-files/<project_uuid>/<impression>", methods=['GET'])
def collect_files(project_uuid, impression):
    """Collect a subset of files matching a selection spec.

    Query: kind (default stageout); one of type / pattern / names.
    """
    kind = request.args.get("kind", "stageout")
    if request.args.get("type"):
        spec = request.args.get("type")
    elif request.args.get("pattern"):
        spec = request.args.get("pattern")
    elif request.args.get("names"):
        spec = request.args.get("names")   # comma list handled below
    else:
        spec = "all"
    storage = ImpressionStorage(project_uuid, impression)
    report = {}
    if request.args.get("names"):
        for one in request.args.get("names").split(","):
            if one:
                report[one] = storage.collect_files(kind, one)
    else:
        report[spec] = storage.collect_files(kind, spec)
    return jsonify(report)


@bp.route("/delete-workflow/<project_uuid>/<workflow_uuid>", methods=['GET'])
def delete_workflow(project_uuid, workflow_uuid):
    """Free the runner-side workspace of a workflow (all backends).

    The local Workflows mirror is always kept; only the runner-side
    workspace is removed.
    """
    workflow_dir = os.path.join(os.environ["HOME"], ".Yuki", "Workflows",
                                project_uuid, workflow_uuid)
    if not os.path.isdir(workflow_dir):
        return jsonify({"error": f"workflow '{workflow_uuid}' not found"}), 404

    try:
        wf = VWorkflow.create(project_uuid, [], workflow_uuid)
        if translate_to_musical(wf.status()) == IN_MOVEMENT:
            return jsonify({"error": "workflow is running; kill it first"}), 409
        wf.delete_workspace()
        workflow_purge.record_workspace_purged(wf)
        backend_type = wf.backend_type()
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 500
    return jsonify({"status": "deleted",
                    "project_uuid": project_uuid,
                    "workflow": workflow_uuid,
                    "backend_type": backend_type})


@bp.route("/kill-workflow/<project_uuid>/<workflow_uuid>",
           methods=['GET'])
def kill_workflow(project_uuid, workflow_uuid):
    """Force-stop a workflow (works even for zombie runs).

    Escalates TERM -> KILL on ssh runners, force-stops reana workflows,
    and marks the status killed so a stale 'running' clears.
    """
    workflow_dir = os.path.join(os.environ["HOME"], ".Yuki", "Workflows",
                                project_uuid, workflow_uuid)
    if not os.path.isdir(workflow_dir):
        return jsonify({"error": f"workflow '{workflow_uuid}' "
                                 "not found"}), 404
    try:
        wf = VWorkflow.create(project_uuid, [], workflow_uuid)
        wf.force_kill()
        backend_type = wf.backend_type()
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 500
    return jsonify({"status": "killed",
                    "project_uuid": project_uuid,
                    "workflow": workflow_uuid,
                    "backend_type": backend_type})


@bp.route("/kill-running-workflows", methods=['POST'])
def kill_running_workflows():
    """Preview or stop a bounded list of recorded-running workflows."""
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "JSON object required"}), 400
    runner = data.get("runner")
    project = data.get("project")
    dry_run = data.get("dry_run", True)
    workflows = data.get("workflows")

    def valid_name(value):
        return (isinstance(value, str) and value not in ("", ".", "..")
                and "/" not in value and "\\" not in value)

    if not valid_name(project) or not isinstance(runner, str):
        return jsonify({"error": "runner and project are required"}), 400
    if not isinstance(dry_run, bool):
        return jsonify({"error": "dry_run must be a boolean"}), 400
    if (workflows is not None and
            (not isinstance(workflows, list) or not all(map(valid_name, workflows)))):
        return jsonify({"error": "workflows must be a list of workflow IDs"}), 400
    if not dry_run and workflows is None:
        return jsonify({"error": "Execution requires workflow IDs from a preview"}), 400
    runners = config.get_config_file().read_variable("runners_id", {})
    if runner not in runners:
        return jsonify({"error": f"runner '{runner}' not found"}), 400
    try:
        result = workflow_kill.kill_running_workflows(
            runners[runner], project, dry_run=dry_run, workflows=workflows)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(exc)}), 500
    return jsonify(result)


@bp.route("/purge-runner-workflows", methods=['POST'])
def purge_runner_workflows():
    """Delete the non-live workflow workspaces on a runner.

    Workflows whose project's synced live set excludes them (and which
    are not running) have their runner-side workspace deleted via the
    per-backend delete_workspace. Live, running, and unknown workflows
    are skipped with reasons. The local Workflows mirror is kept.
    """
    data = request.get_json(silent=True) or request.form
    runner = data.get("runner", "")
    if not runner:
        return jsonify({"error": "missing required field: runner"}), 400
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        return jsonify({"error": f"Runner '{runner}' not found"}), 404
    runner_id = runners_id[runner]
    backend_types = config_file.read_variable("backend_types", {})
    backend_type = backend_types.get(runner_id, "reana")
    if backend_type not in ("ssh", "native", "reana"):
        return jsonify({"error": f"runner '{runner}' has backend "
                                 f"'{backend_type}'"}), 400
    dry_run = str(data.get("dry_run", "")).lower() in ("1", "true", "yes")
    project_uuid = data.get("project_uuid") or None
    try:
        summary = workflow_purge.purge_stale_workflows(
            runner_id, dry_run, project_uuid=project_uuid)
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 500
    return jsonify(summary)
