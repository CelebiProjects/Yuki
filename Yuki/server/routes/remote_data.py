"""
Remote data registration routes.

POST /register-remote-data            start a registration job (returns job_id)
GET  /register-remote-data/<job_id>   poll job state
"""
import os
from flask import Blueprint, request, jsonify
from CelebiChrono.utils import csys
from ...kernel import remote_data_ops
from ...kernel.registration_progress import progress_cache
from ..config import config
from ..tasks import task_cache_results, task_register_remote_data

bp = Blueprint('remote_data', __name__)


def _progress(state, job_id):
    runner_id = state.get("runner_id", "")
    # Include the data root and phase so snapshots cannot cross jobs or stages.
    yuki_dir = remote_data_ops._yuki_dir()  # pylint: disable=protected-access
    key = (yuki_dir, runner_id, job_id, state.get("status"))
    return progress_cache.get(
        key, lambda: remote_data_ops.read_remote_progress(runner_id, job_id))


def _state_with_progress(state, job_id):
    """Return completion discovered by this refresh in the same response."""
    if state.get("status") not in ("hashing", "copying"):
        return state
    progress = _progress(state, job_id)
    latest = remote_data_ops.read_job_state(
        remote_data_ops._yuki_dir(), job_id) or state  # pylint: disable=protected-access
    if latest.get("status") not in ("hashing", "copying"):
        return latest
    if isinstance(progress, dict) and progress.get("stage") != latest.get("status"):
        progress = None
    return dict(latest, progress=progress)


@bp.route("/register-remote-data", methods=['POST'])
def register_remote_data():  # pylint: disable=too-many-return-statements
    """Start a remote data registration job."""
    data = request.get_json(silent=True) or request.form
    runner = data.get("runner", "")
    remote_path = data.get("remote_path", "")
    project_uuid = data.get("project_uuid", "")
    if not (runner and remote_path and project_uuid):
        return jsonify({"error": "missing required field: runner/remote_path/project_uuid"}), 400

    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        return jsonify({"error": f"Runner '{runner}' not found"}), 404
    runner_id = runners_id[runner]
    backend_types = config_file.read_variable("backend_types", {})
    if backend_types.get(runner_id, "reana") != "ssh":
        return jsonify({"error": "register-ssh-data requires an ssh runner; "
                                 "native data should use upload-data"}), 400

    descriptor = data.get("descriptor") or os.path.basename(
        os.path.normpath(remote_path))

    yuki_dir = remote_data_ops._yuki_dir()  # pylint: disable=protected-access
    # No fast path for existing registrations: the data may have changed,
    # so every run re-hashes. The hash job reuses an archived record
    # when the fresh md5 matches (see register_remote_data_job).
    inflight = remote_data_ops.find_inflight_job(
        yuki_dir, runner_id, remote_path, project_uuid, descriptor)
    if inflight:
        return jsonify({"job_id": inflight})

    job_id = csys.generate_uuid()
    remote_data_ops.write_job_state(yuki_dir, job_id, {
        "status": "hashing", "result": None, "error": None,
        "runner_id": runner_id, "remote_path": remote_path,
        "project_uuid": project_uuid, "descriptor": descriptor,
    })
    try:
        task_register_remote_data.apply_async(
            args=[job_id, runner_id, remote_path, project_uuid, descriptor])
    except Exception as e:  # pylint: disable=broad-exception-caught
        # Record the failure so find_inflight_job never wedges on this job.
        remote_data_ops.write_job_state(yuki_dir, job_id, {
            "status": "failed", "result": None, "error": str(e),
            "runner_id": runner_id, "remote_path": remote_path,
        })
        return jsonify({"job_id": job_id, "error": str(e)}), 500
    return jsonify({"job_id": job_id})


@bp.route("/register-remote-data/<job_id>", methods=['GET'])
def register_remote_data_status(job_id):
    """Poll a registration job's state."""
    state = remote_data_ops.read_job_state(
        remote_data_ops._yuki_dir(), job_id)  # pylint: disable=protected-access
    if state is None:
        return jsonify({"error": "job not found"}), 404
    return jsonify(_state_with_progress(state, job_id))


@bp.route("/register-remote-data/impression/<impression_uuid>", methods=['GET'])
def register_remote_data_impression_status(impression_uuid):
    """Poll a registration job's state by impression uuid."""
    yuki_dir = remote_data_ops._yuki_dir()  # pylint: disable=protected-access
    found = remote_data_ops.find_job_by_impression(yuki_dir, impression_uuid)
    if found is None:
        return jsonify({"error": "no registration job for impression"}), 404
    job_id, state = found
    return jsonify(_state_with_progress(state, job_id))


@bp.route("/purge-runner-cache", methods=['POST'])
def purge_runner_cache_route():
    """Purge cached impressions from an ssh runner (synchronous)."""
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
    if backend_types.get(runner_id, "reana") != "ssh":
        return jsonify({"error": "purge-ssh-runner-cache requires an ssh "
                                 "runner"}), 400

    superseded = str(data.get("superseded", "")).lower() in (
        "1", "true", "yes")
    if superseded and data.get("impression"):
        return jsonify({"error": "superseded scope cannot be combined "
                                 "with an impression filter"}), 400

    dry_run = str(data.get("dry_run", "")).lower() in ("1", "true", "yes")
    try:
        summary = remote_data_ops.purge_runner_cache(
            runner_id,
            project=data.get("project") or None,
            impression=data.get("impression") or None,
            dry_run=dry_run,
            superseded=superseded,
            yuki_dir=remote_data_ops._yuki_dir())  # pylint: disable=protected-access
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 500
    return jsonify(summary)


@bp.route("/cache-results", methods=['POST'])
def cache_results_route():
    """Start a job caching the impression's workflow stageout on its runner."""
    data = request.get_json(silent=True) or request.form
    runner = data.get("runner", "")
    project_uuid = data.get("project_uuid", "")
    impression = data.get("impression", "")
    if not (runner and project_uuid and impression):
        return jsonify({"error": "missing required field: "
                                 "runner/project_uuid/impression"}), 400

    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        return jsonify({"error": f"Runner '{runner}' not found"}), 404
    runner_id = runners_id[runner]
    backend_types = config_file.read_variable("backend_types", {})
    if backend_types.get(runner_id, "reana") != "ssh":
        return jsonify({"error": "cache-results requires an ssh runner"}), 400

    job_id = csys.generate_uuid()
    yuki_dir = remote_data_ops._yuki_dir()  # pylint: disable=protected-access
    remote_data_ops.write_job_state(yuki_dir, job_id, {
        "status": "copying", "result": None, "error": None,
        "runner_id": runner_id, "project_uuid": project_uuid,
        "impression": impression,
    }, jobs_dir_name="cache-jobs")
    try:
        task_cache_results.apply_async(
            args=[job_id, runner_id, project_uuid, impression])
    except Exception as e:  # pylint: disable=broad-exception-caught
        remote_data_ops.write_job_state(yuki_dir, job_id, {
            "status": "failed", "result": None, "error": str(e),
        }, jobs_dir_name="cache-jobs")
        return jsonify({"job_id": job_id, "error": str(e)}), 500
    return jsonify({"job_id": job_id})


@bp.route("/cache-results/<job_id>", methods=['GET'])
def cache_results_status(job_id):
    """Poll a cache-results job's state."""
    state = remote_data_ops.read_job_state(
        remote_data_ops._yuki_dir(), job_id,  # pylint: disable=protected-access
        jobs_dir_name="cache-jobs")
    if state is None:
        return jsonify({"error": "job not found"}), 404
    return jsonify(state)


@bp.route("/verify-data/<project_uuid>/<impression>", methods=['GET'])
def verify_data(project_uuid, impression):
    """Recompute the data md5 and compare with the registered uuid."""
    yuki_dir = remote_data_ops._yuki_dir()  # pylint: disable=protected-access
    imp_dir = os.path.join(yuki_dir, "Storage", project_uuid, impression)
    if not os.path.isdir(imp_dir):
        return jsonify({"error": f"Impression '{impression}' not found"}), 404
    return jsonify(remote_data_ops.verify_registered_data(
        project_uuid, impression))
