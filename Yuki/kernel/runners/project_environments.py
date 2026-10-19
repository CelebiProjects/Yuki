"""Project-scoped Conda environments for SSH and native runners."""

import os
import json
import posixpath
import re
import shlex
import subprocess
import time
import uuid

from . import config as runner_config
from ..workflows.ssh import _SshConnection


ENVIRONMENT_JOBS_DIR = "environment-jobs"
NATIVE_REQUESTS_DIR = os.path.join("NativeRunner", "environment-requests")
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_RESERVED_ENVIRONMENTS = frozenset(
    ("script", "rawdata", "datalist", "lhcb_ap_datalist"))


def validate_segment(value, label):
    """Validate a project UUID or environment name used in remote paths."""
    if not isinstance(value, str) or not _SAFE_SEGMENT.fullmatch(value):
        raise ValueError(
            f"invalid {label}: use letters, numbers, '.', '_' or '-' only")
    return value


def validate_environment_name(name):
    """Validate a user-managed environment name."""
    validate_segment(name, "environment name")
    if name in _RESERVED_ENVIRONMENTS:
        raise ValueError(f"environment name '{name}' is reserved by Celebi")
    return name


def _backend(settings):
    return settings.get("backend_type", "ssh")


def environment_root(settings, project_uuid):
    """Return the project-specific environment root on a runner."""
    validate_segment(project_uuid, "project UUID")
    if _backend(settings) == "native":
        return runner_config.native_environment_root(settings, project_uuid)
    base = settings.get("remote_workdir", "/tmp/yuki-workflows")
    return posixpath.join(base, "environments", project_uuid)


def environment_path(settings, project_uuid, name):
    """Return the deterministic prefix for a project environment."""
    validate_environment_name(name)
    return posixpath.join(environment_root(settings, project_uuid), name)


def _connection(settings):
    return _SshConnection(
        host=settings.get("host", ""),
        user=settings.get("user", ""),
        key_path=settings.get("key_path"),
        port=settings.get("port", 22),
    )


def list_project_environments(settings, project_uuid):
    """List valid Conda prefixes below one project's environment root."""
    root = environment_root(settings, project_uuid)
    envs = []
    if _backend(settings) == "native":
        try:
            names = sorted(os.listdir(root)) if os.path.isdir(root) else []
            for name in names:
                try:
                    validate_environment_name(name)
                except ValueError:
                    continue
                path = os.path.join(root, name)
                if os.path.isdir(os.path.join(path, "conda-meta")):
                    envs.append({"name": name, "path": path, "active": False})
        except OSError as exc:
            return {"envs": [], "error": str(exc) or type(exc).__name__}
        return {"envs": envs, "error": None, "project_uuid": project_uuid,
                "scope": "project"}
    try:
        with _connection(settings) as ssh:
            for name in sorted(ssh.listdir(root)):
                try:
                    validate_environment_name(name)
                except ValueError:
                    continue
                path = posixpath.join(root, name)
                if ssh.exists(posixpath.join(path, "conda-meta")):
                    envs.append({"name": name, "path": path, "active": False})
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"envs": [], "error": str(exc) or type(exc).__name__}
    return {"envs": envs, "error": None, "project_uuid": project_uuid,
            "scope": "project"}


def _write_json_atomic(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = f"{path}.{os.getpid()}.tmp"
    with open(temporary, "w", encoding="utf-8") as output:
        json.dump(payload, output)
    os.replace(temporary, path)


def install_native_environment_local(settings, project_uuid, name, yaml_text):
    """Install one native prefix on the host that owns the native agent."""
    validate_segment(project_uuid, "project UUID")
    validate_environment_name(name)
    if not isinstance(yaml_text, str) or not yaml_text.strip():
        raise ValueError("environment YAML is empty")
    prefix = environment_path(settings, project_uuid, name)
    spec_path = os.path.join(os.path.dirname(prefix), ".specs", f"{name}.yaml")
    lock_path = f"{prefix}.install.lock"
    conda = settings.get("conda_path") or "conda"
    os.makedirs(os.path.dirname(spec_path), exist_ok=True)
    with open(spec_path, "w", encoding="utf-8") as spec_file:
        spec_file.write(yaml_text)
    try:
        os.mkdir(lock_path)
    except FileExistsError as exc:
        raise RuntimeError(
            "environment installation is already running") from exc
    try:
        result = subprocess.run(
            [conda, "env", "update", "--prefix", prefix,
             "--file", spec_path, "--prune"],
            capture_output=True, text=True, timeout=7200, check=False)
    finally:
        os.rmdir(lock_path)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip()
                           or f"conda exited with status {result.returncode}")
    return {"name": name, "path": prefix, "output": result.stdout.strip()}


def _request_native_install(settings, project_uuid, name, yaml_text):
    """Ask the host native agent to perform the Conda operation."""
    running, _lock_path = runner_config.native_agent_running(yuki_dir())
    if not running:
        raise RuntimeError(
            "native runner agent is not running; run: yuki native-runner start")
    request_id = str(uuid.uuid4())
    request_path = os.path.join(yuki_dir(), NATIVE_REQUESTS_DIR,
                                f"{request_id}.json")
    _write_json_atomic(request_path, {
        "status": "queued", "project_uuid": project_uuid, "name": name,
        "yaml": yaml_text,
        "settings": {key: settings.get(key, "")
                     for key in ("workdir", "conda_path")},
    })
    deadline = time.monotonic() + 7210
    while time.monotonic() < deadline:
        try:
            with open(request_path, encoding="utf-8") as request_file:
                state = json.load(request_file)
        except (OSError, json.JSONDecodeError):
            time.sleep(0.2)
            continue
        if state.get("status") == "finished":
            try:
                os.unlink(request_path)
            except OSError:
                pass
            return state["result"]
        if state.get("status") == "failed":
            try:
                os.unlink(request_path)
            except OSError:
                pass
            raise RuntimeError(state.get("error") or "native installation failed")
        time.sleep(1)
    raise TimeoutError("timed out waiting for the native runner agent")


def _ihep_conda_preamble(settings):
    """Return shell setup that keeps Conda state off an AFS home directory."""
    workdir = settings.get("remote_workdir", "/tmp/yuki-workflows")
    state_root = posixpath.join(workdir, ".celebi-conda")
    paths = {
        "HOME": posixpath.join(state_root, "home"),
        "CONDA_PKGS_DIRS": posixpath.join(state_root, "pkgs"),
        "CONDA_ENVS_PATH": posixpath.join(state_root, "envs"),
        "XDG_CACHE_HOME": posixpath.join(state_root, "cache"),
        "XDG_CONFIG_HOME": posixpath.join(state_root, "config"),
    }
    directories = " ".join(shlex.quote(path) for path in paths.values())
    exports = " ".join(
        f"export {key}={shlex.quote(value)};" for key, value in paths.items())
    return (
        f"mkdir -p {directories}; cd {shlex.quote(workdir)}; {exports} "
        "export CONDA_REGISTER_ENVS=false; "
    )


def install_project_environment(settings, project_uuid, name, yaml_text):
    """Create or update a project environment from a Conda YAML document."""
    prefix = environment_path(settings, project_uuid, name)
    root = environment_root(settings, project_uuid)
    spec_path = posixpath.join(root, ".specs", f"{name}.yaml")
    lock_path = f"{prefix}.install.lock"
    conda = settings.get("conda_path") or "conda"
    if _backend(settings) == "native":
        return _request_native_install(settings, project_uuid, name, yaml_text)
    conda_preamble = ""
    if _backend(settings) == "ihep":
        # IHEP batch and non-interactive SSH sessions cannot rely on an AFS
        # token.  Keep every Conda-owned path on the shared runner filesystem,
        # even when conda_path accidentally names the real binary instead of
        # the wrapper installed by setup-ihep-runner.sh.
        conda_preamble = _ihep_conda_preamble(settings)
    with _connection(settings) as ssh:
        ssh.put_text(yaml_text, spec_path)
        command = (
            "set -eu; "
            f"{conda_preamble}"
            f"_celebi_lock={shlex.quote(lock_path)}; "
            "if ! mkdir \"$_celebi_lock\" 2>/dev/null; then "
            f"echo {shlex.quote('environment installation is already running')} >&2; "
            "exit 75; fi; "
            "trap 'rmdir \"$_celebi_lock\"' EXIT; "
            f"{shlex.quote(conda)} env update --prefix {shlex.quote(prefix)} "
            f"--file {shlex.quote(spec_path)} --prune"
        )
        output, error, code = ssh.exec(command, timeout=7200)
    if code != 0:
        raise RuntimeError(error.strip() or output.strip()
                           or f"conda exited with status {code}")
    return {"name": name, "path": prefix, "output": output.strip()}


def settings_for_runner(config_file, runner_id):
    """Load settings for an SSH/IHEP or native runner."""
    backend = config_file.read_variable("backend_types", {}).get(
        runner_id, "reana")
    if runner_config.is_ssh_backend(backend):
        settings = runner_config.get_ssh_settings(config_file, runner_id)
    elif backend == "native":
        settings = runner_config.get_runner_settings(config_file, runner_id)
    else:
        raise ValueError(
            f"backend '{backend}' does not support project environments")
    settings = dict(settings)
    settings["backend_type"] = backend
    return settings


def yuki_dir():
    """Return Yuki's state directory."""
    return os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
