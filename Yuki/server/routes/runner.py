"""
Runner management routes.
"""
import os
import json
from logging import getLogger
from flask import Blueprint, request, jsonify
from CelebiChrono.utils import csys
from ...kernel import liveness
from ...kernel import runner_config
from ...kernel import runner_inventory
from ...kernel.ssh_workflow import (
    environment_needs_conda, resolve_conda_environment)
from .. import runner_probe
from ..config import config
from ..utils import ping

bp = Blueprint('runner', __name__)
_debug = getLogger("Yuki.execution")


def _ssh_ping(host, user, key_path, port=22):
    """Test SSH connectivity to a runner.

    Returns a dict with status and an optional message.
    """
    try:
        import paramiko
    except ImportError:
        return {"status": "Failed", "message": "paramiko is not installed"}

    if not host or not user:
        return {"status": "Failed", "message": "Missing ssh_host or ssh_user"}

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        connect_kwargs = {
            "hostname": host,
            "port": port,
            "username": user,
            "timeout": 10,
            "banner_timeout": 10,
        }
        key_path = os.path.expanduser(key_path) if key_path else None
        if key_path and os.path.exists(key_path):
            connect_kwargs["key_filename"] = key_path
        client.connect(**connect_kwargs)
        return {"status": "Connected"}
    except Exception as e:
        return {"status": "Failed", "message": str(e)}
    finally:
        client.close()


def _walk_local_files(root):
    """Return (files, bytes) for a local directory tree."""
    total_files = 0
    total_bytes = 0
    if not os.path.isdir(root):
        return total_files, total_bytes
    for dirpath, _dirs, filenames in os.walk(root):
        for name in filenames:
            path = os.path.join(dirpath, name)
            try:
                total_bytes += os.path.getsize(path)
            except OSError:
                continue
            total_files += 1
    return total_files, total_bytes


def _project_distribution_summary(yuki_dir, project_uuid):
    """Summarize current-project knowledge from distribution.json files."""
    storage_root = os.path.join(yuki_dir, "Storage", project_uuid)
    summary = {
        "path": storage_root,
        "files": 0,
        "bytes": 0,
        "impressions": 0,
        "live_files": 0,
        "live_bytes": 0,
        "stale_files": 0,
        "stale_bytes": 0,
        "unknown_files": 0,
        "unknown_bytes": 0,
        "runners": {},
    }
    live_set = liveness.load_live_set(project_uuid, yuki_dir)
    if not os.path.isdir(storage_root):
        return summary
    for impression in os.listdir(storage_root):
        dist_path = os.path.join(storage_root, impression, "distribution.json")
        if not os.path.isfile(dist_path):
            continue
        impression_state = liveness.impression_live(project_uuid, impression,
                                                    yuki_dir)
        try:
            with open(dist_path, encoding="utf-8") as fh:
                dist = json.load(fh)
        except (OSError, ValueError):
            continue
        locations = dist.get("locations", {}) or {}
        imp_seen = False
        yuki_entry = locations.get("yuki")
        if isinstance(yuki_entry, dict):
            imp_seen = True
            files = int(yuki_entry.get("files", 0) or 0)
            bytes_ = int(yuki_entry.get("bytes", 0) or 0)
            summary["files"] += files
            summary["bytes"] += bytes_
            if impression_state is False:
                summary["stale_files"] += files
                summary["stale_bytes"] += bytes_
            elif impression_state is True:
                summary["live_files"] += files
                summary["live_bytes"] += bytes_
            else:
                summary["unknown_files"] += files
                summary["unknown_bytes"] += bytes_
        for loc, block in locations.items():
            if not loc.startswith("runner:"):
                continue
            runner_name = loc[len("runner:"):]
            block = block if isinstance(block, dict) else {}
            for kind in ("cache", "workflow"):
                entry = block.get(kind)
                if not isinstance(entry, dict):
                    continue
                imp_seen = True
                summary["files"] += int(entry.get("files", 0) or 0)
                summary["bytes"] += int(entry.get("bytes", 0) or 0)
                if impression_state is False:
                    summary["stale_files"] += int(entry.get("files", 0) or 0)
                    summary["stale_bytes"] += int(entry.get("bytes", 0) or 0)
                elif impression_state is True:
                    summary["live_files"] += int(entry.get("files", 0) or 0)
                    summary["live_bytes"] += int(entry.get("bytes", 0) or 0)
                else:
                    summary["unknown_files"] += int(entry.get("files", 0) or 0)
                    summary["unknown_bytes"] += int(entry.get("bytes", 0) or 0)
                runner_entry = summary["runners"].setdefault(
                    runner_name, {"files": 0, "bytes": 0, "kinds": {}})
                runner_entry["files"] += int(entry.get("files", 0) or 0)
                runner_entry["bytes"] += int(entry.get("bytes", 0) or 0)
                kind_entry = runner_entry["kinds"].setdefault(
                    kind, {"files": 0, "bytes": 0})
                kind_entry["files"] += int(entry.get("files", 0) or 0)
                kind_entry["bytes"] += int(entry.get("bytes", 0) or 0)
        if imp_seen:
            summary["impressions"] += 1
    summary["has_live_set"] = live_set is not None
    return summary


@bp.route("/runners", methods=['GET'])
def runners():
    """Get list of available runners."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    return " ".join(runners_list)


@bp.route("/runners-url", methods=['GET'])
def runnersurl():
    """Get URLs of all runners."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})
    runners_url = config_file.read_variable("urls", {})
    urls = []
    for runner in runners_list:
        url = runners_url.get(runners_id.get(runner, ""), "")
        if url:
            urls.append(url)
    return " ".join(urls)


@bp.route("/runner-connection/<runner>", methods=['GET'])
def runnerconnection(runner):
    """Test connection to a specific runner."""
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    runner_id = runners_id.get(runner, "")
    tokens = config_file.read_variable("tokens", {})
    token = tokens.get(runner_id, "")
    urls = config_file.read_variable("urls", {})
    url = urls.get(runner_id, "")
    backend_types = config_file.read_variable("backend_types", {})
    backend_type = backend_types.get(runner_id, "reana")

    if backend_type == "ssh":
        ssh_hosts = config_file.read_variable("ssh_hosts", {})
        ssh_users = config_file.read_variable("ssh_users", {})
        ssh_key_paths = config_file.read_variable("ssh_key_paths", {})
        ssh_ports = config_file.read_variable("ssh_ports", {})
        return _ssh_ping(
            host=ssh_hosts.get(runner_id, ""),
            user=ssh_users.get(runner_id, ""),
            key_path=ssh_key_paths.get(runner_id, ""),
            port=ssh_ports.get(runner_id, 22),
        )
    if backend_type != "reana":
        return {'status': 'Connected'}
    return ping(url, token)


@bp.route("/register-runner", methods=['POST'])
def registerrunner():
    """Register a new runner."""
    for field in ("runner", "url", "token"):
        if field not in request.form:
            return f"missing required field: {field}", 400
    runner = request.form["runner"]
    backend_type = request.form.get("backend_type", "native")

    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    if runner in runners_list:
        return f"runner '{runner}' already exists", 409

    runner_id = csys.generate_uuid()
    runners_id = config_file.read_variable("runners_id", {})
    runners_url = config_file.read_variable("urls", {})
    tokens = config_file.read_variable("tokens", {})
    backend_types = config_file.read_variable("backend_types", {})

    runners_list.append(runner)
    runners_id[runner] = runner_id
    runners_url[runner_id] = request.form["url"]
    tokens[runner_id] = request.form["token"]
    backend_types[runner_id] = backend_type

    config_file.write_variable("runners", runners_list)
    config_file.write_variable("runners_id", runners_id)
    config_file.write_variable("urls", runners_url)
    config_file.write_variable("tokens", tokens)
    config_file.write_variable("backend_types", backend_types)

    if backend_type == "ssh":
        _write_ssh_config(config_file, runner_id, request.form)

    settings = _collect_settings(request.form)
    if settings:
        runner_config.merge_runner_settings(config_file, runner_id, settings)

    if request.form.get("ssh_key_data"):
        key_path = _store_ssh_key(runner_id, request.form["ssh_key_data"])
        _write_ssh_config(config_file, runner_id, {"ssh_key_path": key_path})
        runner_config.merge_runner_settings(
            config_file, runner_id, {"ssh_key_path": key_path})
    return "successful"


_SETTING_FIELDS = ("workdir", "conda_path", "snakemake_path",
                   "ssh_host", "ssh_user", "ssh_key_path", "remote_workdir")
_SETTING_INT_FIELDS = ("cores", "mem_mb", "ssh_port")


def _collect_settings(data):
    """Collect runner_settings fields from form data or a JSON dict."""
    settings = {}
    for field in _SETTING_FIELDS:
        if data.get(field):
            settings[field] = data.get(field)
    for field in _SETTING_INT_FIELDS:
        if data.get(field) is not None:
            try:
                settings[field] = int(data.get(field))
            except (ValueError, TypeError):
                pass
    return settings


def _store_ssh_key(runner_id, key_data):
    """Persist an uploaded ssh private key under $YUKIDIR/keys/ (mode 600).

    Returns the server-side path of the stored key.
    """
    keys_dir = os.path.join(os.path.dirname(config.config_path), "keys")
    os.makedirs(keys_dir, exist_ok=True)
    key_path = os.path.join(keys_dir, runner_id)
    with open(key_path, "w", encoding="utf-8") as f:
        f.write(key_data if key_data.endswith("\n") else key_data + "\n")
    os.chmod(key_path, 0o600)
    return key_path


def _write_ssh_config(config_file, runner_id, data):
    """Store SSH-specific runner settings in config.

    ``data`` may be a werkzeug MultiDict (request.form) or a plain dict.
    Merge semantics: only keys present in ``data`` are written, so a
    partial update (e.g. only ssh_key_path) never wipes sibling fields.
    """
    for field, key in (("ssh_host", "ssh_hosts"),
                       ("ssh_user", "ssh_users"),
                       ("ssh_key_path", "ssh_key_paths"),
                       ("remote_workdir", "remote_workdirs")):
        if data.get(field):
            mapping = config_file.read_variable(key, {})
            mapping[runner_id] = data.get(field)
            config_file.write_variable(key, mapping)

    if data.get("ssh_port") is not None:
        try:
            port = int(data.get("ssh_port"))
        except (ValueError, TypeError):
            port = 22
        ssh_ports = config_file.read_variable("ssh_ports", {})
        ssh_ports[runner_id] = port
        config_file.write_variable("ssh_ports", ssh_ports)


def _remove_ssh_config(config_file, runner_id):
    """Remove SSH-specific settings for a runner."""
    for key in ("ssh_hosts", "ssh_users", "ssh_key_paths", "ssh_ports", "remote_workdirs"):
        data = config_file.read_variable(key, {})
        if runner_id in data:
            del data[runner_id]
            config_file.write_variable(key, data)


@bp.route("/remove-runner/<runner>", methods=['GET'])
def removerunner(runner):
    """Remove a runner."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})
    urls = config_file.read_variable("urls", {})
    tokens = config_file.read_variable("tokens", {})
    backend_types = config_file.read_variable("backend_types", {})

    if runner not in runners_list:
        return "runner not found"

    runner_id = runners_id[runner]
    _debug.debug("runner_id %s", runner_id)
    runners_list.remove(runner)
    del runners_id[runner]

    # Safe deletion of URL
    if runner_id in urls:
        del urls[runner_id]

    # Safe deletion of token
    if runner_id in tokens:
        del tokens[runner_id]

    # Safe deletion of backend type
    if runner_id in backend_types:
        del backend_types[runner_id]

    config_file.write_variable("runners", runners_list)
    config_file.write_variable("runners_id", runners_id)
    config_file.write_variable("urls", urls)
    config_file.write_variable("tokens", tokens)
    config_file.write_variable("backend_types", backend_types)

    _remove_ssh_config(config_file, runner_id)
    stored_key = os.path.join(os.path.dirname(config.config_path),
                              "keys", runner_id)
    if os.path.exists(stored_key):
        os.remove(stored_key)
    for key in ("runner_settings", "runner_health"):
        data = config_file.read_variable(key, {})
        if runner_id in data:
            del data[runner_id]
            config_file.write_variable(key, data)

    return "successful"


@bp.route("/register-machine/<machine>/<machine_uuid>", methods=['GET'])
def register_machine(machine, machine_uuid):
    """Register a machine."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})
    runners_list.append(machine)
    runners_id[machine] = machine_uuid
    config_file.write_variable("runners", runners_list)
    config_file.write_variable("runners_id", runners_id)
    return "successful"


@bp.route("/update-runner/<runner>", methods=['PATCH'])
def update_runner(runner):  # pylint: disable=too-many-locals
    """Update settings for an existing runner."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    if runner not in runners_list:
        return jsonify({"error": f"Runner '{runner}' not found"}), 404

    runner_id = runners_id[runner]
    data = request.get_json(silent=True) or {}

    urls = config_file.read_variable("urls", {})
    tokens = config_file.read_variable("tokens", {})
    backend_types = config_file.read_variable("backend_types", {})
    use_kerberos = config_file.read_variable("use_kerberos", {})
    eos_mount_points = config_file.read_variable("eos_mount_point", {})
    cvmfs_repos = config_file.read_variable("cvmfs", {})

    old_backend_type = backend_types.get(runner_id, "reana")

    if "url" in data:
        urls[runner_id] = data["url"]
    if "token" in data:
        tokens[runner_id] = data["token"]
    if "backend_type" in data:
        backend_types[runner_id] = data["backend_type"]
    if "use_kerberos" in data:
        use_kerberos[runner_id] = data["use_kerberos"]
    if "eos_mount_point" in data:
        eos_mount_points[runner_id] = data["eos_mount_point"]
    if "cvmfs" in data:
        cvmfs_repos[runner_id] = data["cvmfs"]

    settings = _collect_settings(data)
    if settings:
        runner_config.merge_runner_settings(config_file, runner_id, settings)

    if data.get("ssh_key_data"):
        key_path = _store_ssh_key(runner_id, data["ssh_key_data"])
        _write_ssh_config(config_file, runner_id, {"ssh_key_path": key_path})
        runner_config.merge_runner_settings(
            config_file, runner_id, {"ssh_key_path": key_path})

    new_backend_type = backend_types.get(runner_id, "reana")

    config_file.write_variable("urls", urls)
    config_file.write_variable("tokens", tokens)
    config_file.write_variable("backend_types", backend_types)
    config_file.write_variable("use_kerberos", use_kerberos)
    config_file.write_variable("eos_mount_point", eos_mount_points)
    config_file.write_variable("cvmfs", cvmfs_repos)

    if new_backend_type == "ssh":
        _write_ssh_config(config_file, runner_id, data)
    elif old_backend_type == "ssh":
        _remove_ssh_config(config_file, runner_id)

    return jsonify({"message": f"Runner '{runner}' updated successfully"})


@bp.route("/runners-config", methods=['GET'])
def runners_config():  # pylint: disable=too-many-locals
    """Get full configuration for all registered runners."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})
    urls = config_file.read_variable("urls", {})
    tokens = config_file.read_variable("tokens", {})
    backend_types = config_file.read_variable("backend_types", {})
    use_kerberos = config_file.read_variable("use_kerberos", {})
    eos_mount_points = config_file.read_variable("eos_mount_point", {})
    cvmfs_repos = config_file.read_variable("cvmfs", {})
    ssh_hosts = config_file.read_variable("ssh_hosts", {})
    ssh_users = config_file.read_variable("ssh_users", {})
    ssh_key_paths = config_file.read_variable("ssh_key_paths", {})
    ssh_ports = config_file.read_variable("ssh_ports", {})
    remote_workdirs = config_file.read_variable("remote_workdirs", {})

    result = []
    for runner in runners_list:
        runner_id = runners_id.get(runner, "")
        backend_type = backend_types.get(runner_id, "reana")
        runner_cfg = {
            "name": runner,
            "id": runner_id,
            "url": urls.get(runner_id, ""),
            "token": tokens.get(runner_id, ""),
            "backend_type": backend_type,
            "use_kerberos": use_kerberos.get(runner_id, False),
            "eos_mount_point": eos_mount_points.get(runner_id, ""),
            "cvmfs": cvmfs_repos.get(runner_id, []),
        }
        runner_cfg["settings"] = runner_config.get_runner_settings(
            config_file, runner_id)
        runner_cfg["health"] = runner_config.get_runner_health(
            config_file, runner_id)
        if backend_type == "ssh":
            runner_cfg.update({
                "ssh_host": ssh_hosts.get(runner_id, ""),
                "ssh_user": ssh_users.get(runner_id, ""),
                "ssh_key_path": ssh_key_paths.get(runner_id, ""),
                "ssh_port": ssh_ports.get(runner_id, 22),
                "remote_workdir": remote_workdirs.get(runner_id, "/tmp/yuki-workflows"),
            })
        result.append(runner_cfg)
    return jsonify(result)


@bp.route("/runner-ssh-config/<runner>", methods=['GET'])
def runner_ssh_config(runner):
    """Return ssh connection settings plus the key content for a runner."""
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        return jsonify({"error": f"runner '{runner}' not found"}), 404
    runner_id = runners_id[runner]
    backend_types = config_file.read_variable("backend_types", {})
    if backend_types.get(runner_id) != "ssh":
        return jsonify({"error": f"runner '{runner}' is not an ssh runner"}), 400
    settings = runner_config.get_ssh_settings(config_file, runner_id)
    key_path = settings.get("key_path", "")
    key = ""
    expanded = os.path.expanduser(key_path) if key_path else ""
    if expanded and os.path.exists(expanded):
        try:
            with open(expanded, encoding="utf-8") as f:
                key = f.read()
        except OSError:
            key = ""
    payload = {
        "host": settings.get("host", ""),
        "user": settings.get("user", ""),
        "port": settings.get("port", 22),
        "key": key,
        "key_path": key_path,
        "remote_workdir": settings.get("remote_workdir", "/tmp/yuki-workflows"),
    }
    environment = request.args.get("environment", "")
    if environment_needs_conda(environment):
        payload["conda_env"] = resolve_conda_environment(
            environment, config.config_path)
    return jsonify(payload)


@bp.route("/machine-id/<machine>", methods=["GET"])
def machine_id(machine):
    """Get machine ID for a specific machine."""
    config_file = config.get_config_file()
    runner_ids = config_file.read_variable("runners_id", {})
    if machine not in runner_ids:
        return "machine not found", 404
    return runner_ids[machine]


@bp.route("/test-runner/<runner>", methods=['GET'])
def test_runner(runner):
    """Probe a runner's capabilities and persist the result."""
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        return jsonify({"error": f"Runner '{runner}' not found"}), 404
    runner_id = runners_id[runner]
    backend_types = config_file.read_variable("backend_types", {})
    backend_type = backend_types.get(runner_id, "reana")
    settings = runner_config.get_runner_settings(config_file, runner_id)

    timeout = request.args.get("timeout", type=int) or runner_probe.PROBE_TIMEOUT
    if backend_type == "ssh":
        checks = runner_probe.probe_ssh(
            runner_config.get_ssh_settings(config_file, runner_id),
            timeout=timeout)
    elif backend_type == "reana":
        urls = config_file.read_variable("urls", {})
        tokens = config_file.read_variable("tokens", {})
        checks = runner_probe.probe_reana(
            urls.get(runner_id, ""), tokens.get(runner_id, ""), ping)
    elif backend_type == "native":
        checks = runner_probe.probe_native(settings, timeout=timeout)
    else:  # dry / unknown backends: connectivity-only, always ok
        checks = {"connectivity": {"ok": True}}

    health = runner_probe.summarize(checks)
    runner_config.set_runner_health(config_file, runner_id, health)
    return jsonify(health)


@bp.route("/runner-health/<runner>", methods=['GET'])
def runner_health(runner):
    """Return the persisted health of a runner (never re-probes)."""
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        return jsonify({"error": f"Runner '{runner}' not found"}), 404
    return jsonify(runner_config.get_runner_health(config_file,
                                                   runners_id[runner]))


@bp.route("/runner-envs/<runner>", methods=['GET'])
def runner_envs(runner):
    """List conda environments available on a runner (ssh/native)."""
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        return jsonify({"error": f"Runner '{runner}' not found"}), 404
    runner_id = runners_id[runner]
    backend_types = config_file.read_variable("backend_types", {})
    backend_type = backend_types.get(runner_id, "reana")

    if backend_type == "ssh":
        result = runner_probe.list_envs_ssh(
            runner_config.get_ssh_settings(config_file, runner_id))
    elif backend_type == "native":
        result = runner_probe.list_envs_native(
            runner_config.get_runner_settings(config_file, runner_id))
    else:
        result = {"envs": [],
                  "error": f"backend '{backend_type}' has no conda environments"}
    return jsonify(result)


@bp.route("/runner-data/<runner>", methods=['GET'])
def runner_data(runner):
    """Return the full data inventory of a runner (cache + workflows)."""
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    if runner not in runners_id:
        return jsonify({"error": f"runner '{runner}' not found"}), 404
    runner_id = runners_id[runner]
    backend_types = config_file.read_variable("backend_types", {})
    backend_type = backend_types.get(runner_id, "reana")
    if backend_type not in ("ssh", "native"):
        return jsonify({"error": f"runner '{runner}' is a {backend_type} "
                                 "runner — no listable data"}), 400
    try:
        inventory = runner_inventory.inventory_runner(
            runner_id, backend_type)
    except Exception as e:  # pylint: disable=broad-exception-caught
        return jsonify({"error": str(e)}), 500
    return jsonify({"runner": runner,
                    "backend_type": backend_type, **inventory})


@bp.route("/yuki-overview", methods=['GET'])
def yuki_overview():
    """Return an aggregate overview from distribution.json and local mirrors."""
    config_file = config.get_config_file()
    project_uuid = request.args.get("project_uuid", "").strip()
    yuki_dir = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
    storage_root = os.path.join(yuki_dir, "Storage", project_uuid) \
        if project_uuid else os.path.join(yuki_dir, "Storage")
    workflows_root = os.path.join(yuki_dir, "Workflows", project_uuid) \
        if project_uuid else os.path.join(yuki_dir, "Workflows")
    dist_summary = (_project_distribution_summary(yuki_dir, project_uuid)
                    if project_uuid else {
                        "path": storage_root, "files": 0, "bytes": 0,
                        "impressions": 0, "runners": {},
                    })
    storage_files, storage_bytes = _walk_local_files(storage_root)
    workflow_files, workflow_bytes = _walk_local_files(workflows_root)
    return jsonify({
        "project_uuid": project_uuid,
        "distribution": dist_summary,
        "storage": {
            "path": storage_root,
            "files": storage_files,
            "bytes": storage_bytes,
        },
        "workflows": {
            "path": workflows_root,
            "files": workflow_files,
            "bytes": workflow_bytes,
        },
    })
