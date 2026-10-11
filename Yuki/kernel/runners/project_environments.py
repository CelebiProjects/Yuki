"""Project-scoped Conda environments for SSH-backed runners."""

import os
import posixpath
import re
import shlex

from . import config as runner_config
from ..workflows.ssh import _SshConnection


ENVIRONMENT_JOBS_DIR = "environment-jobs"
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


def environment_root(settings, project_uuid):
    """Return the project-specific environment root on a runner."""
    validate_segment(project_uuid, "project UUID")
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


def install_project_environment(settings, project_uuid, name, yaml_text):
    """Create or update a project environment from a Conda YAML document."""
    prefix = environment_path(settings, project_uuid, name)
    root = environment_root(settings, project_uuid)
    spec_path = posixpath.join(root, ".specs", f"{name}.yaml")
    lock_path = f"{prefix}.install.lock"
    conda = settings.get("conda_path") or "conda"
    with _connection(settings) as ssh:
        ssh.put_text(yaml_text, spec_path)
        command = (
            "set -eu; "
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
    """Load SSH settings and reject non-SSH runners."""
    backend = config_file.read_variable("backend_types", {}).get(
        runner_id, "reana")
    if not runner_config.is_ssh_backend(backend):
        raise ValueError(
            f"backend '{backend}' does not support project environments")
    return runner_config.get_ssh_settings(config_file, runner_id)


def yuki_dir():
    """Return Yuki's state directory."""
    return os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
