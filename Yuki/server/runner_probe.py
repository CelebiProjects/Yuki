"""Capability probing for runners (snakemake / conda / workdir)."""
import datetime
import os
import shlex
import shutil
import subprocess

PROBE_TIMEOUT = 10
ENV_LIST_TIMEOUT = 30
EXEC_TIMEOUT = 30


def _ok(**extra):
    return {"ok": True, **extra}


def _err(error):
    # Some exceptions (e.g. paramiko.SSHException) have an empty str();
    # fall back to the type name so the error stays readable.
    text = str(error) or type(error).__name__
    return {"ok": False, "error": text}


def _probe_tool(path_setting, binary, timeout=EXEC_TIMEOUT):
    """Probe one executable: configured path, else PATH lookup."""
    path = path_setting or shutil.which(binary)
    if not path:
        return _err(f"{binary} not found in PATH")
    try:
        result = subprocess.run([path, "--version"], capture_output=True,
                                text=True, timeout=timeout, check=False)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return _err(f"{binary} at {path} failed: {exc}")
    if result.returncode != 0:
        return _err(f"{binary} --version exited {result.returncode}: "
                    f"{result.stderr.strip()}")
    return _ok(version=result.stdout.strip(), path=path)


def _default_workdir():
    yuki_dir = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
    return os.path.join(yuki_dir, "LocalWorkflows")


def probe_native(settings, timeout=EXEC_TIMEOUT):
    """Probe snakemake/conda/workdir on the Yuki host."""
    checks = {
        "snakemake": _probe_tool(settings.get("snakemake_path", ""),
                                  "snakemake", timeout),
        "conda": _probe_tool(settings.get("conda_path", ""),
                             "conda", timeout),
    }
    workdir = settings.get("workdir") or _default_workdir()
    try:
        os.makedirs(workdir, exist_ok=True)
        checks["workdir_writable"] = (
            _ok(path=workdir) if os.access(workdir, os.W_OK)
            else _err(f"{workdir} is not writable"))
    except OSError as exc:
        checks["workdir_writable"] = _err(str(exc))
    return checks


# pylint: disable-next=too-many-locals,too-many-branches,too-many-statements
def probe_ssh(ssh_settings, timeout=PROBE_TIMEOUT, extra_tools=None,
              require_cluster_executor=False):
    """Probe connectivity plus snakemake/conda/workdir on the remote host."""
    try:
        import paramiko
    except ImportError:
        return {"connectivity": _err("paramiko is not installed")}

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        key_path = ssh_settings.get("key_path") or None
        if key_path:
            key_path = os.path.expanduser(key_path)
            if not os.path.exists(key_path):
                key_path = None
        client.connect(hostname=ssh_settings.get("host", ""),
                       port=ssh_settings.get("port", 22),
                       username=ssh_settings.get("user", ""),
                       key_filename=key_path,
                       timeout=PROBE_TIMEOUT, banner_timeout=PROBE_TIMEOUT)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"connectivity": _err(exc)}
    finally:
        if client.get_transport() is None:
            client.close()

    checks = {"connectivity": _ok()}

    def remote(cmd):
        _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
        return stdout.read().decode().strip(), stderr.read().decode().strip()

    check_names = ("snakemake", "conda", "workdir_writable") + tuple(
        name for name, _setting in (extra_tools or ()))
    if require_cluster_executor:
        check_names += ("snakemake_cluster",)
    current = check_names[0]
    try:
        for name, setting, binary in (
                ("snakemake", "snakemake_path", "snakemake"),
                ("conda", "conda_path", "conda")):
            current = name
            tool = ssh_settings.get(setting) or binary
            out, err = remote(f"{tool} --version")
            checks[name] = _err(err or f"{binary} not usable") if err else _ok(version=out)
        current = "workdir_writable"
        workdir = ssh_settings.get("remote_workdir", "/tmp/yuki-workflows")
        _, err = remote(f"mkdir -p {workdir} && test -w {workdir}")
        checks["workdir_writable"] = _err(err) if err else _ok(path=workdir)
        for name, setting in (extra_tools or ()):
            current = name
            tool = str(ssh_settings.get(setting) or name)
            if "/" in tool:
                out, err = remote(f"test -x {shlex.quote(tool)} && "
                                  f"printf '%s' {shlex.quote(tool)}")
            else:
                out, err = remote(f"command -v {shlex.quote(tool)}")
            checks[name] = _err(err or f"{tool} not found") \
                if not out else _ok(path=out)
        if require_cluster_executor:
            current = "snakemake_cluster"
            snakemake = str(
                ssh_settings.get("snakemake_path") or "snakemake")
            out, err = remote(f"{shlex.quote(snakemake)} --help")
            if "--cluster-generic-submit-cmd" in out:
                checks[current] = _ok(mode="cluster-generic")
            elif "--cluster CMD" in out:
                checks[current] = _ok(mode="legacy-cluster")
            else:
                checks[current] = _err(
                    err or "Snakemake lacks cluster-generic/--cluster support")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A mid-probe transport failure must fail the record, not drop keys.
        detail = str(exc) or type(exc).__name__
        checks[current] = _err(f"probe aborted: {detail}")
        for name in check_names:
            checks.setdefault(name, _err(f"probe aborted: {detail}"))
    finally:
        client.close()
    return checks


def parse_conda_env_list(output):
    """Parse `conda env list` text into [{'name', 'path', 'active'}]."""
    envs = []
    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        active = "*" in parts
        parts = [p for p in parts if p != "*"]
        if len(parts) == 2:
            name, path = parts
        elif len(parts) == 1:
            name, path = "", parts[0]
        else:
            continue
        envs.append({"name": name, "path": path, "active": active})
    return envs


def list_envs_native(settings):
    """List conda environments on the Yuki host."""
    conda = settings.get("conda_path") or shutil.which("conda")
    if not conda:
        return {"envs": [], "error": "conda not found in PATH"}
    try:
        result = subprocess.run([conda, "env", "list"], capture_output=True,
                                text=True, timeout=ENV_LIST_TIMEOUT, check=False)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"envs": [], "error": str(exc) or type(exc).__name__}
    if result.returncode != 0:
        error = result.stderr.strip() or f"conda env list exited {result.returncode}"
        return {"envs": [], "error": error}
    return {"envs": parse_conda_env_list(result.stdout), "error": None}


def list_envs_ssh(ssh_settings):
    """List conda environments on the remote host via SSH."""
    try:
        import paramiko
    except ImportError:
        return {"envs": [], "error": "paramiko is not installed"}

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        key_path = ssh_settings.get("key_path") or None
        if key_path:
            key_path = os.path.expanduser(key_path)
            if not os.path.exists(key_path):
                key_path = None
        client.connect(hostname=ssh_settings.get("host", ""),
                       port=ssh_settings.get("port", 22),
                       username=ssh_settings.get("user", ""),
                       key_filename=key_path,
                       timeout=PROBE_TIMEOUT, banner_timeout=PROBE_TIMEOUT)
        conda = ssh_settings.get("conda_path") or "conda"
        _, stdout, stderr = client.exec_command(f"{conda} env list",
                                                timeout=ENV_LIST_TIMEOUT)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        if err and not out:
            return {"envs": [], "error": err}
        return {"envs": parse_conda_env_list(out), "error": None}
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"envs": [], "error": str(exc) or type(exc).__name__}
    finally:
        client.close()


def probe_reana(url, token, ping_func):
    """Probe a REANA runner via the existing ping helper."""
    try:
        result = ping_func(url, token)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"connectivity": _err(exc)}
    if isinstance(result, dict) and result.get("status") not in (None, "Connected"):
        return {"connectivity": _err(result.get("message", "ping failed"))}
    return {"connectivity": _ok()}


def summarize(checks):
    """Build the persisted health record from check results."""
    return {
        "status": "failed" if any(not c.get("ok") for c in checks.values()) else "ok",
        "checked_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "checks": checks,
    }
