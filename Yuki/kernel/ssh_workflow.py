"""SSH/Remote workflow implementation.

This module provides the SshWorkflow class which implements workflow execution
by copying files to a remote host over SFTP and running Snakemake there over SSH.
"""
# pylint: disable=cyclic-import,too-many-lines
import json
import os
import shlex
import socket
import stat
import time
from logging import getLogger

from CelebiChrono.utils import metadata
from Yuki.kernel import runner_config
from Yuki.utils.env_interpreter import EnvInterpreter
from .vworkflow import VWorkflow
from .status_constants import (FAILED, DISSONANCE, STOPPED,
                               translate_to_musical, is_terminal_status)
from .file_staging import walk_files

logger = getLogger("YukiLogger")

DEFAULT_ENVIRONMENT = "docker.io/reanahub/reana-env-root6:6.18.04"
DEFAULT_SSH_PORT = 22
SSH_START_CONFIRM_TIMEOUT = 5
SSH_START_CONFIRM_INTERVAL = 0.1

# Environments that need no conda activation on ssh runners
PURE_COPY_ENVIRONMENTS = ("rawdata", "datalist", "lhcb_ap_datalist", "script")


class SSHStartNotConfirmed(RuntimeError):
    """The remote never confirmed a detached start within the grace period."""


def resolve_conda_environment(environment, config_path):
    """Map an environment string to a conda environment name.

    Resolution order: the server's ``conda_env_map``, then the same lookup
    after stripping docker:// prefixes, then a name mangled from the raw
    string (docker image names become valid-ish env names).
    """
    if not environment:
        environment = DEFAULT_ENVIRONMENT

    resolved = EnvInterpreter.resolve(environment, config_path)
    if resolved is not None:
        return resolved

    stripped = environment
    for prefix in ("docker://", "docker.io/", "docker:"):
        if stripped.startswith(prefix):
            stripped = stripped[len(prefix):]
            break

    if stripped != environment:
        resolved = EnvInterpreter.resolve(stripped, config_path)
        if resolved is not None:
            return resolved

    return stripped.replace("/", "_").replace(":", "_")


def environment_needs_conda(environment):
    """True when the environment requires conda activation on an ssh runner."""
    if not environment or environment == DEFAULT_ENVIRONMENT:
        return False
    return environment not in PURE_COPY_ENVIRONMENTS


class _SshConnection:
    """Thin wrapper around Paramiko for remote SSH/SFTP operations."""

    def __init__(self, host, user, key_path=None, port=DEFAULT_SSH_PORT):
        self.host = host
        self.user = user
        self.key_path = os.path.expanduser(key_path) if key_path else None
        self.port = port
        self._client = None
        self._sftp = None

    def connect(self):
        """Open SSH connection."""
        import paramiko

        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = {
            "hostname": self.host,
            "port": self.port,
            "username": self.user,
            "timeout": 30,
            "banner_timeout": 30,
        }
        if self.key_path and os.path.exists(self.key_path):
            connect_kwargs["key_filename"] = self.key_path
        self._client.connect(**connect_kwargs)
        self._sftp = self._client.open_sftp()

    def close(self):
        """Close SFTP and SSH connection."""
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def mkdir_p(self, remote_path, mode=0o755):
        """Recursively create remote directories."""
        if not remote_path or remote_path == "/":
            return
        try:
            self._sftp.stat(remote_path)
            return
        except FileNotFoundError:
            pass
        parent = os.path.dirname(remote_path)
        if parent and parent != remote_path:
            self.mkdir_p(parent, mode)
        self._sftp.mkdir(remote_path)
        self._sftp.chmod(remote_path, mode)

    def put(self, local_path, remote_path):
        """Upload a local file to the remote host."""
        self.mkdir_p(os.path.dirname(remote_path))
        self._sftp.put(local_path, remote_path)

    def put_text(self, text, remote_path, encoding="utf-8"):
        """Upload text content to a remote file."""
        self.mkdir_p(os.path.dirname(remote_path))
        with self._sftp.file(remote_path, "w") as remote_file:
            remote_file.write(text.encode(encoding) if isinstance(text, str) else text)

    def get(self, remote_path, local_path):
        """Download a remote file to the local host."""
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        self._sftp.get(remote_path, local_path)

    def stream(self, remote_path, chunk_size=65536):
        """Yield chunks of a remote file."""
        with self._sftp.file(remote_path, "rb") as remote_file:
            remote_file.prefetch()
            while True:
                chunk = remote_file.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def listdir(self, remote_path):
        """List entries in a remote directory.

        Returns a list of filenames, or an empty list if the directory
        does not exist.
        """
        try:
            return self._sftp.listdir(remote_path)
        except FileNotFoundError:
            return []

    def exists(self, remote_path):
        """Check whether a remote path exists."""
        try:
            self._sftp.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def isfile(self, remote_path):
        """Check whether a remote path is a regular file."""
        try:
            return stat.S_ISREG(self._sftp.stat(remote_path).st_mode)
        except FileNotFoundError:
            return False

    def remove(self, remote_path):
        """Remove a remote file."""
        try:
            self._sftp.remove(remote_path)
        except FileNotFoundError:
            pass

    def walk_files(self, remote_dir):
        """Recursively yield (rel_path, remote_path, size) for remote files.

        Skips entries that cannot be stat'd. Directories are traversed; other
        non-regular, non-directory entries are ignored.
        """
        try:
            entries = self.listdir(remote_dir)
        except FileNotFoundError:
            return
        for entry in entries:
            remote_path = f"{remote_dir}/{entry}"
            try:
                st = self._sftp.stat(remote_path)
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(st.st_mode):
                for rel, rpath, size in self.walk_files(remote_path):
                    yield f"{entry}/{rel}", rpath, size
            elif stat.S_ISREG(st.st_mode):
                yield entry, remote_path, st.st_size

    def exec(self, command, timeout=300):
        """Execute a command on the remote host.

        Returns (stdout_str, stderr_str, exit_code). The timeout bounds
        the whole wait by polling for exit status readiness. This avoids
        hanging on sshd setups that keep exec sessions alive while a
        background job still exists.
        """
        logger.debug("[SSH] Executing command: %s with timeout %s",
                     command, timeout)
        stdin, stdout, stderr = self._client.exec_command(command, timeout=timeout)
        logger.debug("[SSH] Command executed, waiting for exit status...")
        channel = stdout.channel
        channel.settimeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        while not channel.exit_status_ready():
            if deadline is not None and time.monotonic() >= deadline:
                stdin.close()
                raise socket.timeout(
                    f"SSH command timed out after {timeout} seconds")
            time.sleep(0.1)
        exit_code = channel.recv_exit_status()
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        stdin.close()
        return out, err, exit_code

    def exec_start_detached(self, command, timeout=30, grace=2):
        """Start a detached remote command without waiting forever.

        Waits up to grace seconds for the exec session to report its exit
        status; on hosts where sshd keeps exec sessions alive while a
        background job exists (observed on a WSL runner), the status never
        arrives. In that case SSHStartNotConfirmed is raised — the
        detached remote process (setsid'd, fds on /dev/null) survives;
        the channel is left to the connection teardown (closing it
        directly blocks on such hosts). Returns (out, err, code) when
        the remote reports within the grace period.
        """
        logger.debug("[SSH] Starting detached command: %s with timeout %s",
                     command, timeout)
        stdin, stdout, _stderr = self._client.exec_command(command, timeout=timeout)
        logger.debug("[SSH] Detached command started, waiting for confirmation...")
        channel = stdout.channel
        channel.settimeout(timeout)
        deadline = time.monotonic() + grace
        while not channel.exit_status_ready():
            logger.debug("[SSH] Waiting for remote confirmation, "
                         "time left: %.2fs",
                         deadline - time.monotonic())
            if time.monotonic() >= deadline:
                # Do NOT channel.close() here: on a hung session the
                # remote never confirms the close either, so close()
                # blocks until the channel timeout (30s) and raises a
                # bare socket.timeout that escapes. The connection
                # teardown reaps the channel.
                stdin.close()
                raise SSHStartNotConfirmed(
                    f"remote did not confirm the start within {grace}s: "
                    f"{command[:80]}")
            time.sleep(0.05)
        exit_code = channel.recv_exit_status()
        logger.debug("[SSH] Remote command finished, exit code: %s", exit_code)
        # The channel may never reach EOF on hosts that keep the session
        # open for background jobs (exit-status arrives, EOF does not);
        # read only what is already buffered instead of waiting for EOF.
        out = (channel.recv(65536).decode("utf-8", errors="replace")
               if channel.recv_ready() else "")
        logger.debug("[SSH] Remote command output: %s", out)
        err = (channel.recv_stderr(65536).decode("utf-8", errors="replace")
               if channel.recv_stderr_ready() else "")
        logger.debug("[SSH] Remote command error: %s", err)
        stdin.close()
        logger.debug("[SSH] Remote command output: %s, error: %s", out, err)
        return out, err, exit_code


class SshWorkflow(VWorkflow):
    """Remote/SSH implementation of VWorkflow.

    Behaves like NativeWorkflow but stages files and executes Snakemake on a
    remote host accessed over SSH/SFTP.
    """

    def __init__(self, project_uuid, jobs, uuid=None):
        """Initialize remote SSH workflow."""
        super().__init__(project_uuid, jobs, uuid)
        self.ssh_config = self._load_ssh_config()
        base = self.ssh_config.get("remote_workdir", "/tmp/yuki-workflows")
        self.remote_exec_path = os.path.join(
            base, "workflows", self.project_uuid, self.uuid,
        ).replace(os.sep, "/")
        self.remote_impressions_path = os.path.join(
            base, "impressions", self.project_uuid,
        ).replace(os.sep, "/")

    def _load_ssh_config(self):
        """Read SSH connection settings (new map preferred, legacy fallback)."""
        runner_id = self.machine_id or ""
        if not runner_id:
            return {}
        return runner_config.get_ssh_settings(
            runner_config.open_config(), runner_id)

    def _ssh(self):
        """Return a connected _SshConnection context manager."""
        cfg = self.ssh_config
        return _SshConnection(
            host=cfg.get("host", ""),
            user=cfg.get("user", ""),
            key_path=cfg.get("key_path"),
            port=cfg.get("port", DEFAULT_SSH_PORT),
        )

    def _rawdata_cache_dir(self, impression):
        """The runner-side rawdata cache dir for an impression."""
        base = self.ssh_config.get("remote_workdir", "/tmp/yuki-workflows")
        return f"{base}/impressions/{self.project_uuid}/{impression}"

    def _cache_hit(self, ssh, cache_dir):
        """True when the runner-side rawdata cache holds files."""
        _, _, code = ssh.exec(
            f"test -d {shlex.quote(cache_dir)} && "
            f"test -n \"$(ls -A {shlex.quote(cache_dir)})\"")
        return code == 0

    def _chmod_cache_ro(self, ssh, cache_dir):
        """Make runner-side cached data read-only after write-through."""
        out, err, code = ssh.exec(
            f"chmod -R a-w {shlex.quote(cache_dir)}/*", timeout=3600)
        if code != 0:
            self.logger(f"[SSH] chmod cache read-only failed for "
                        f"{cache_dir}: {err or out}")

    def _execute_backend(self):
        """Execute workflow using a remote SSH backend."""
        try:
            self.logger("[SSH] Creating remote workflow structure")
            self._create_remote_structure()
        except Exception as e:
            self.logger(f"[SSH] Failed to create remote workflow structure: {e}")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status(DISSONANCE, "SSH workflow construction failed")
            raise

        try:
            self.logger("[SSH] Uploading files")
            self._upload_files_remote()
        except Exception as e:
            self.logger(f"[SSH] Failed to upload files: {e}")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status(DISSONANCE, "SSH workflow file upload failed")
            raise

        try:
            self.logger("[SSH] Starting remote Snakemake")
            self._start_remote_snakemake()
            self.set_workflow_status("running")
            self.logger(f"[SSH] Workflow running remotely in: {self.remote_exec_path}")
        except Exception as e:
            self.logger(f"[SSH] Failed to start remote Snakemake: {e}")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status(DISSONANCE, "SSH workflow remote start failed")
            raise

    def _create_remote_structure(self):
        """Create remote workflow structure and write workflow_info.json."""
        workflow_info = {
            "workflow": {
                "uuid": self.uuid,
                "name": self.get_name(),
                "specification": {
                    "job_dependencies": self.dependencies,
                    "steps": self.steps,
                },
                "type": "snakemake",
                "file": "Snakefile"
            }
        }
        with self._ssh() as ssh:
            ssh.mkdir_p(self.remote_exec_path)
            # Reserved for future output caching (à la EOS in reana_workflow)
            ssh.mkdir_p(self.remote_impressions_path)
            remote_info_path = f"{self.remote_exec_path}/workflow_info.json"
            ssh.put_text(json.dumps(workflow_info, indent=2), remote_info_path)

    def _upload_files_remote(self):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """Upload all files to the remote execution directory.

        Unlike the native backend, the remote host has no FileStager to resolve
        a stage manifest, so input and rawdata files are copied directly into
        each job's ``imp<short>/stageout`` directory. Downstream jobs reach them
        through their ``gen -> ../imp<short>`` symlink (``gen/stageout/...``).
        """
        with self._ssh() as ssh:
            total_jobs = len(self.jobs)
            for j_idx, job in enumerate(self.jobs):
                files = job.files()
                total_files = len(files)
                for f_idx, name in enumerate(files):
                    src_path = os.path.join(job.path, "contents", name[8:])
                    dst_path = f"{self.remote_exec_path}/imp{name}"
                    if os.path.exists(src_path):
                        ssh.put(src_path, dst_path)
                        self.logger(
                            f"[SSH] [Job {j_idx+1}/{total_jobs}] Uploaded file "
                            f"{f_idx+1}/{total_files}: {name}"
                        )

                # Remote-hosted impressions must be staged with a remote cp
                # regardless of environment classification: rawdata jobs also
                # have is_input=True, so this check has to run before the
                # rawdata branch below.
                if job.is_input:
                    impression = job.path.split("/")[-1]
                    remote_marker = os.path.join(
                        os.environ["HOME"], ".Yuki", "Storage",
                        self.project_uuid, impression, "remote.json")
                    if os.path.exists(remote_marker):
                        marker_cfg = metadata.ConfigFile(remote_marker)
                        host_runner = marker_cfg.read_variable(
                            "host_runner_id", "")
                        if host_runner != (self.machine_id or ""):
                            raise RuntimeError(
                                f"Data impression {impression} is hosted on "
                                f"another runner ({host_runner}); cannot stage "
                                "remotely")
                        # The data already sits in this runner's managed
                        # impressions cache; the Snakefile setup rule copies
                        # it into imp<short>/stageout as the first step.
                        self.logger(f"[SSH] Cache hit: {impression} is "
                                    "registered on this runner")
                        continue

                if job.is_input:
                    cache_dir = self._rawdata_cache_dir(impression)
                    if self._cache_hit(ssh, cache_dir):
                        # The setup rule copies from the cache; nothing to
                        # upload here.
                        self.logger(f"[SSH] Cache hit: {impression} is in "
                                    "the runner impressions cache")
                        continue

                if job.environment() == "rawdata":
                    rawdata_path = os.path.join(job.path, "rawdata")
                    if os.path.exists(rawdata_path):
                        filelist = list(walk_files(rawdata_path))
                        total_raw = len(filelist)
                        for f_idx, (rel_path, src_path) in enumerate(filelist):
                            # Upload into the runner cache; the Snakefile
                            # setup rule links it into imp<short>/stageout.
                            dst_path = f"{cache_dir}/{rel_path}"
                            ssh.put(src_path, dst_path)
                            self.logger(
                                f"[SSH] [Job {j_idx+1}/{total_jobs}] Cached rawdata "
                                f"{f_idx+1}/{total_raw}: {rel_path}"
                            )
                        if total_raw:
                            self._chmod_cache_ro(ssh, cache_dir)

                elif job.is_input:
                    src_stageout = os.path.join(
                        os.environ["HOME"],
                        ".Yuki",
                        "Storage",
                        self.project_uuid,
                        impression,
                        job.machine_id,
                        "stageout"
                    )
                    if os.path.exists(src_stageout):
                        filelist = list(walk_files(src_stageout))
                        total_input = len(filelist)
                        for f_idx, (rel_path, src_path) in enumerate(filelist):
                            # Upload into the runner cache; the Snakefile
                            # setup rule links it into imp<short>/stageout.
                            dst_path = f"{cache_dir}/{rel_path}"
                            ssh.put(src_path, dst_path)
                            self.logger(
                                f"[SSH] [Job {j_idx+1}/{total_jobs}] Cached input "
                                f"{f_idx+1}/{total_input}: {rel_path}"
                            )
                        if total_input:
                            self._chmod_cache_ro(ssh, cache_dir)

            ssh.put(self.snakefile_path, f"{self.remote_exec_path}/Snakefile")
            self.logger("[SSH] Uploaded: Snakefile")

    def _build_remote_wrapper(self):
        """Build the supervised Snakemake wrapper uploaded to the runner."""
        snakemake_bin = self.ssh_config.get("snakemake_path") or "snakemake"
        cores = self.ssh_config.get("cores", "all")
        conda_path = self.ssh_config.get("conda_path") or ""
        if conda_path:
            # conda_path is the conda binary (as the runner probe expects);
            # its directory is what must land on PATH.
            conda_bin_dir = os.path.dirname(conda_path)
            conda_setup = f"CONDA_BIN={shlex.quote(conda_bin_dir)}\n"
        else:
            conda_setup = (
                'CONDA_BIN="$(conda info --base 2>/dev/null || true)"\n'
                'CONDA_BIN="${CONDA_BIN:+$CONDA_BIN/bin}"\n')
        snakemake_command = (
            f"{shlex.quote(str(snakemake_bin))} --use-conda --cores "
            f"{shlex.quote(str(cores))} --snakefile Snakefile")
        return f'''#!/bin/bash
set -Eeuo pipefail
# Deterministic run environment: drop user-shell leakage (.bashrc etc.) so
# workflow behaviour never depends on the submitter's shell configuration.
{conda_setup}unset PYTHONPATH LD_LIBRARY_PATH
export PATH="${{CONDA_BIN:+$CONDA_BIN:}}$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"
cd "$(dirname "$0")"

atomic_write() {{
    local value="$1"
    local destination="$2"
    local temporary="${{destination}}.tmp.$$"
    printf '%s\n' "$value" > "$temporary"
    mv -f "$temporary" "$destination"
}}

record_exit() {{
    local rc=$?
    trap - EXIT
    atomic_write "$rc" yuki.exit
    exit "$rc"
}}
trap record_exit EXIT

nohup {snakemake_command} > snakemake.log 2>&1 &
snakemake_pid=$!
atomic_write "$snakemake_pid" yuki.pid
atomic_write "$$ $snakemake_pid" yuki.started
wait "$snakemake_pid"
'''

    def _start_remote_snakemake(self):
        """Upload a wrapper script and start Snakemake remotely in the background."""
        wrapper = self._build_remote_wrapper()
        self.logger(f"[SSH] Uploading remote wrapper script to {self.remote_exec_path}/yuki_run.sh")
        remote_wrapper = f"{self.remote_exec_path}/yuki_run.sh"
        with self._ssh() as ssh:
            self.logger(f"[SSH] Uploading wrapper script to {remote_wrapper}")
            ssh.put_text(wrapper, remote_wrapper)
            self.logger(f"[SSH] Making wrapper script executable: {remote_wrapper}")
            out, err, code = ssh.exec(
                f"chmod +x {shlex.quote(remote_wrapper)}")
            self.logger(f"[SSH] chmod output: {out}, error: {err}, exit code: {code}")
            if code != 0:
                raise RuntimeError(
                    f"Failed to make yuki_run.sh executable: {err or out} (exit {code})"
                )
            # Detach the wrapper: it waits for snakemake itself, so a
            # foreground exec would block the submit until the whole
            # workflow finishes. All three fds must leave the channel
            # or sshd keeps the channel open and recv_exit_status blocks.
            # The polling path reads yuki.pid/yuki.exit.
            self.logger("[SSH] Starting remote Snakemake in background")
            for name in ("yuki.started", "yuki.pid", "yuki.exit"):
                ssh.remove(f"{self.remote_exec_path}/{name}")
            try:
                result = ssh.exec_start_detached(
                    self._build_remote_launch_command(),
                    timeout=30, grace=2)
            except SSHStartNotConfirmed:
                # Some sshd implementations retain the exec channel for
                # detached descendants. Marker verification below is the
                # authoritative handshake in that case.
                getLogger("Yuki.workflow").warning(
                    "[SSH] remote did not confirm the start within 2s; "
                    "verifying remote markers")
            else:
                out, err, code = result
                self.logger(
                    f"[SSH] start output: {out}, error: {err}, exit code: {code}")
                if code != 0:
                    detail = err.strip() if err.strip() else out.strip()
                    wrapper_tail = self._read_remote_wrapper_tail(ssh)
                    if wrapper_tail:
                        detail = (detail +
                                  f" (yuki-wrapper.log: {wrapper_tail})").strip()
                    raise RuntimeError(
                        f"Remote Snakemake failed to launch: {detail} (exit {code})"
                    )

            self._confirm_remote_start(ssh)
            self.logger("[SSH] Remote Snakemake started")

    def _build_remote_launch_command(self):
        """Build a detached launch with synchronous preflight checks."""
        return (
            f"cd {shlex.quote(self.remote_exec_path)} || exit 1; "
            "test -r yuki_run.sh || exit 2; "
            "command -v setsid > /dev/null 2>&1 || exit 3; "
            "setsid bash ./yuki_run.sh > yuki-wrapper.log 2>&1 "
            "< /dev/null &")

    def _read_remote_int(self, ssh, name):
        """Read an integer runtime marker, returning None if unavailable."""
        path = f"{self.remote_exec_path}/{name}"
        if not ssh.exists(path):
            return None
        out, _err, code = ssh.exec(f"cat {shlex.quote(path)}")
        if code != 0:
            return None
        try:
            return int(out.strip())
        except (TypeError, ValueError):
            return None

    def _read_remote_started(self, ssh):
        """Return (wrapper_pid, snakemake_pid) from the startup marker."""
        path = f"{self.remote_exec_path}/yuki.started"
        if not ssh.exists(path):
            return None
        out, _err, code = ssh.exec(f"cat {shlex.quote(path)}")
        if code != 0:
            return None
        try:
            wrapper_pid, snakemake_pid = (int(value) for value in out.split())
        except (TypeError, ValueError):
            return None
        if wrapper_pid <= 0 or snakemake_pid <= 0:
            return None
        return wrapper_pid, snakemake_pid

    @staticmethod
    def _remote_pid_alive(ssh, pid):
        """Return whether a numeric PID currently exists on the runner."""
        if not isinstance(pid, int) or pid <= 0:
            return False
        _out, _err, code = ssh.exec(f"kill -0 {pid} 2>/dev/null")
        return code == 0

    def _confirm_remote_start(self, ssh, timeout=SSH_START_CONFIRM_TIMEOUT):
        """Require wrapper markers instead of trusting SSH channel completion."""
        deadline = time.monotonic() + timeout
        while True:
            exit_code = self._read_remote_exit(ssh)
            if exit_code is not None:
                if exit_code == 0:
                    self.logger("[SSH] Remote workflow completed during startup")
                    return
                detail = self._read_remote_snakemake_tail(ssh)
                if not detail:
                    detail = self._read_remote_wrapper_tail(ssh)
                suffix = f": {detail}" if detail else ""
                raise RuntimeError(
                    f"Remote Snakemake exited during startup with code "
                    f"{exit_code}{suffix}")

            started = self._read_remote_started(ssh)
            if started and self._remote_pid_alive(ssh, started[0]):
                return

            if time.monotonic() >= deadline:
                detail = self._read_remote_wrapper_tail(ssh)
                suffix = f": {detail}" if detail else ""
                raise RuntimeError(
                    "Remote Snakemake start was not confirmed by yuki.started"
                    f"{suffix}")
            time.sleep(SSH_START_CONFIRM_INTERVAL)

    def _sync_external_job_status(self, job):
        """Poll remote status for an external dependency."""
        job.update_status_from_workflow(self.path, self.logger)

    def _read_remote_log_tail(self, ssh, short_uuid, max_chars=500):  # pylint: disable=too-many-locals
        """Return tail of the highest-indexed celebi_user_step*.log on the remote host."""
        import re

        logs_dir = f"{self.remote_exec_path}/imp{short_uuid}/logs"
        try:
            filelist = list(ssh.walk_files(logs_dir))
        except FileNotFoundError:
            return ""

        pattern = re.compile(r"^celebi_user_step(\d+)\.log$")
        candidates = []
        for rel_path, remote_path, _size in filelist:
            fname = os.path.basename(rel_path)
            m = pattern.match(fname)
            if m:
                candidates.append((int(m.group(1)), remote_path))
        if not candidates:
            return ""

        candidates.sort(reverse=True)
        latest = candidates[0][1]
        out, _err, _code = ssh.exec(f"tail -c {max_chars} {latest}")
        return out

    def _record_job_distribution(self, job):
        """Best-effort: record a finished/failed job's produced data registry.

        Runs on the per-job terminal transition inside
        propagate_job_statuses, after the job's live listing has been
        refreshed, so /whereabouts sees the final file set before the
        whole workflow ends. Failures are swallowed: a status update must
        never fail over a stale registry.
        """
        try:
            from Yuki.kernel.impression_storage import ImpressionStorage
            ImpressionStorage(self.project_uuid, job.uuid).update_distribution()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.logger(f"[SSH] Failed to record distribution for "
                        f"{job.short_uuid()}: {exc}")

    def propagate_job_statuses(self, workflow_terminal=False):
        """Reconcile each VJob's status.json with remote markers."""
        with self._ssh() as ssh:
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                if is_terminal_status(translate_to_musical(job.status())):
                    continue

                short = job.short_uuid()
                done_path = f"{self.remote_exec_path}/{short}.done"
                if ssh.exists(done_path):
                    job.set_status("finished", "Remote execution completed")
                    self._record_job_distribution(job)
                    continue

                if not workflow_terminal:
                    continue

                logs_dir = f"{self.remote_exec_path}/imp{short}/logs"
                has_logs = bool(list(ssh.walk_files(logs_dir)))
                if has_logs:
                    tail = self._read_remote_log_tail(ssh, short)
                    detail = (f"Remote execution failed: {tail}"
                              if tail else "Remote execution failed")
                    job.set_status(FAILED, detail)
                    self._record_job_distribution(job)
                else:
                    job.set_status(
                        FAILED,
                        "Skipped: upstream dependency failed before this job ran",
                    )

    def _read_remote_exit(self, ssh):
        """Return the remote wrapper's exit code, or None while still running."""
        return self._read_remote_int(ssh, "yuki.exit")

    def _read_remote_snakemake_tail(self, ssh, max_chars=2000):
        """Return the tail of the remote snakemake log for failure detail."""
        log_file = f"{self.remote_exec_path}/snakemake.log"
        if not ssh.exists(log_file):
            return ""
        out, _err, _code = ssh.exec(
            f"tail -c {int(max_chars)} {shlex.quote(log_file)}")
        return out

    def _read_remote_wrapper_tail(self, ssh, max_chars=2000):
        """Return launch diagnostics emitted before Snakemake owns its log."""
        log_file = f"{self.remote_exec_path}/yuki-wrapper.log"
        if not ssh.exists(log_file):
            return ""
        out, _err, _code = ssh.exec(
            f"tail -c {int(max_chars)} {shlex.quote(log_file)}")
        return out

    def _remote_execution_state(self, ssh, jobs):
        """Return status, detail, exit code, and completed remote jobs."""
        completed = [
            job_uuid for job_uuid in jobs
            if ssh.exists(f"{self.remote_exec_path}/{job_uuid}.done")
        ]
        missing = [job_uuid for job_uuid in jobs if job_uuid not in completed]
        exit_code = self._read_remote_exit(ssh)
        status = "running"
        detail = ""

        if exit_code not in (None, 0):
            status = "failed"
            detail = self._read_remote_snakemake_tail(ssh)
            if not detail:
                detail = self._read_remote_wrapper_tail(ssh)
        elif exit_code == 0:
            if not missing:
                status = "finished"
            else:
                status = "failed"
                detail = (
                    "Remote Snakemake exited successfully but did not create "
                    f"completion markers for: {', '.join(missing)}")
        elif not missing:
            status = "finished"
        else:
            started = self._read_remote_started(ssh)
            if started and not self._remote_pid_alive(ssh, started[0]):
                # record_exit writes atomically before the wrapper exits. Check
                # once more for the process-exit/marker-observation race.
                exit_code = self._read_remote_exit(ssh)
                if exit_code is not None:
                    return self._remote_execution_state(ssh, jobs)
                status = "failed"
                detail = (
                    "Remote workflow supervisor exited without writing "
                    "yuki.exit")
            # No startup marker identifies workflows launched by older Yuki
            # versions. They retain legacy marker-only monitoring as running.

        return status, detail, exit_code, completed

    def update_workflow_status(self):
        """Update workflow status from remote execution."""
        try:
            self.logger(
                f"[SSH] update_workflow_status workflow={self.uuid} "
                f"path={self.path} machine_id={self.machine_id} "
                f"remote_exec_path={self.remote_exec_path} "
                f"jobs={[(j.short_uuid(), j.is_input, j.job_type()) for j in self.jobs]}"
            )
            self.logger("[SSH] Checking jobs status...")

            # Derive the tracked jobs from self.jobs (loaded from local
            # config.json jobs_info when the workflow is reloaded). Do not rely
            # on workflow_info.json, which is only uploaded to the remote host.
            jobs = [
                job.short_uuid()
                for job in self.jobs
                if not job.is_input and job.job_type() != "algorithm"
            ]

            with self._ssh() as ssh:
                status, failure_detail, exit_code, completed_jobs = \
                    self._remote_execution_state(ssh, jobs)

            results = {
                "status": status,
                "progress": {
                    "total": len(jobs),
                    "completed": len(completed_jobs),
                }
            }

            if status == "failed":
                results["failure_detail"] = (
                    failure_detail or
                    f"Remote Snakemake exited with code {exit_code}")

            self.logger(
                f"[SSH] Workflow status: {status}, "
                f"Progress: {results['progress']['completed']}/{results['progress']['total']}"
            )

            # Checked before the write: after it, the recorded status is
            # already terminal and the transition would be invisible.
            entered_terminal = self._entered_terminal_state(status)

            path = os.path.join(self.path, "results.json")
            results_file = metadata.ConfigFile(path)
            results_file.write_variable("results", results)

            workflow_terminal = status in ("finished", "failed")

            # Refresh listings before propagating per-job statuses: a job
            # that just finished must be listed one final time while it is
            # still non-terminal (the refresh skips terminal jobs while the
            # workflow is running), and the per-job distribution recording
            # inside propagate_job_statuses reads that fresh listing.
            self._refresh_job_filelists(status, entered_terminal)
            self.propagate_job_statuses(workflow_terminal=workflow_terminal)
            self.logger(
                f"[SSH] propagate_job_statuses finished "
                f"workflow_terminal={workflow_terminal}"
            )

            # The terminal distribution recording below also reads the
            # refreshed listings and must see the final file set.
            if entered_terminal:
                self.logger(
                    f"[SSH] workflow={self.uuid} entered terminal status={status} "
                    "recording distributions"
                )
                self._record_terminal_distributions(status)

        except Exception as e:
            self.logger(f"[SSH] Failed to update workflow status: {e}")
            raise

    def check_status(self):
        """Check the status of remote workflow execution."""
        self.logger("[SSH] Checking status...")
        self.update_workflow_status()
        return self.status()

    def force_kill(self):
        """Force-stop the remote workflow: TERM, then KILL, then pkill.

        Works even when the pid file is missing or the process ignores
        SIGTERM (zombie runs): the workspace status is marked killed
        either way, so a stale 'running' clears and the workflow
        becomes purgeable.
        """
        self.logger(f"[SSH] Force-killing remote workflow: "
                    f"{self.remote_exec_path}")
        try:
            with self._ssh() as ssh:
                started = self._read_remote_started(ssh)
                wrapper_pid = started[0] if started else None
                snakemake_pid = (started[1] if started else
                                 self._read_remote_int(ssh, "yuki.pid"))
                pid = wrapper_pid or snakemake_pid
                if pid:
                    target = f"-{pid}" if wrapper_pid else str(pid)
                    ssh.exec(f"kill -TERM -- {target}")
                    time.sleep(3)
                    _out, _err, alive = ssh.exec(
                        f"kill -0 {pid} 2>/dev/null")
                    if alive == 0:
                        ssh.exec(f"kill -KILL -- {target}")
                # Catch orphaned snakemake children regardless of the pid.
                ssh.exec(f"pkill -f {shlex.quote(self.remote_exec_path)} "
                         "|| true")
                exit_path = f"{self.remote_exec_path}/yuki.exit"
                exit_tmp = f"{exit_path}.tmp.kill"
                ssh.exec(
                    f"printf '137\\n' > {shlex.quote(exit_tmp)} && "
                    f"mv -f {shlex.quote(exit_tmp)} {shlex.quote(exit_path)}")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.logger(f"[SSH] Remote force-kill failed "
                        f"(marking killed anyway): {exc}")

        self.set_workflow_status("killed")
        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            job.set_status(STOPPED, "Workflow force-killed by user")

    def kill(self):
        """Kill remote workflow execution."""
        try:
            with self._ssh() as ssh:
                started = self._read_remote_started(ssh)
                if started:
                    wrapper_pid = started[0]
                    ssh.exec(f"kill -TERM -- -{wrapper_pid}")
                    self.logger(
                        f"[SSH] Sent SIGTERM to remote process group {wrapper_pid}")
                else:
                    pid = self._read_remote_int(ssh, "yuki.pid")
                    if pid:
                        ssh.exec(f"kill -TERM -- {pid}")
                        self.logger(f"[SSH] Sent SIGTERM to remote PID {pid}")
                    else:
                        self.logger(
                            "[SSH] No PID marker found; cannot kill remote process")
        except Exception as e:
            self.logger(f"[SSH] Error killing remote workflow: {e}")

        self.set_workflow_status("killed")
        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            job.set_status(FAILED, "SSH workflow killed by user")

    def delete_workspace(self):
        """Delete the remote workflow workspace on the runner."""
        self.logger(f"[SSH] Deleting remote workspace: {self.remote_exec_path}")
        with self._ssh() as ssh:
            out, err, code = ssh.exec(
                f"rm -rf {shlex.quote(self.remote_exec_path)}",
                timeout=3600)
            if code != 0:
                raise RuntimeError(
                    f"Failed to delete remote workspace: "
                    f"{err or out} (exit {code})")

    # pylint: disable=too-many-locals,too-many-arguments,too-many-positional-arguments
    def _collect_remote_artifacts(self, impression, artifact_dir, marker_name,
                                  label, *, refresh=False):
        """Collect a job artifact directory from remote execution into Storage.

        With refresh=True, files that already exist locally are downloaded
        again instead of skipped, so growing logs can be re-synced.
        """
        src_path = f"{self.remote_exec_path}/imp{impression[0:7]}/{artifact_dir}"
        dst_path = os.path.join(
            os.environ["HOME"],
            ".Yuki",
            "Storage",
            self.project_uuid,
            impression,
            self.machine_id,
            artifact_dir
        )
        report = {"collected": [], "skipped": [], "failed": []}

        with self._ssh() as ssh:
            try:
                filelist = list(ssh.walk_files(src_path))
            except FileNotFoundError:
                self.logger(f"[SSH] No {label} found at: {src_path}")
                report["skipped"].append(
                    {"file": f"<{label}>", "reason": "source missing"})
                return report

            if not filelist:
                report["skipped"].append(
                    {"file": f"<{label}>", "reason": "source empty"})
                return report

            os.makedirs(dst_path, exist_ok=True)
            total_files = len(filelist)
            for i, (rel_path, remote_file, _size) in enumerate(filelist):
                local_file = os.path.join(dst_path, rel_path)
                if os.path.exists(local_file) and not refresh:
                    report["skipped"].append(
                        {"file": rel_path, "reason": "already in Yuki"})
                    continue
                try:
                    ssh.get(remote_file, local_file)
                    report["collected"].append(rel_path)
                    self.logger(f"[SSH] [{i+1}/{total_files}] Collected {label}: {rel_path}")
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    report["failed"].append(
                        {"file": rel_path, "reason": str(exc)})

        marker_path = os.path.join(os.path.dirname(dst_path), marker_name)
        with open(marker_path, "w", encoding='utf-8') as _:
            pass
        return report

    def download(self, impression=None):
        """Download/collect results from remote execution."""
        self.logger("[SSH] Collecting results from remote execution")
        report = {"collected": [], "skipped": [], "failed": []}
        if impression:
            stageout_report = self._collect_remote_artifacts(
                impression, "stageout", "stageout.downloaded", "output"
            )
            logs_report = self._collect_remote_artifacts(
                impression, "logs", "logs.downloaded", "log"
            )
            for key, value in report.items():
                value.extend(stageout_report.get(key, []))
                value.extend(logs_report.get(key, []))
        return report

    def download_outputs(self, impression=None):
        """Download outputs from remote execution."""
        if impression:
            self.logger("[SSH] Collecting outputs from remote execution")
            return self._collect_remote_artifacts(
                impression, "stageout", "stageout.downloaded", "output"
            )
        return {"collected": [], "skipped": [], "failed": []}

    def download_logs(self, impression=None, refresh=False):
        """Download logs from remote execution."""
        if impression:
            self.logger("[SSH] Collecting logs from remote execution")
            return self._collect_remote_artifacts(
                impression, "logs", "logs.downloaded", "log", refresh=refresh
            )
        return {"collected": [], "skipped": [], "failed": []}

    def list_runner_files(self, impression, kind="stageout"):
        """List files in the remote execution dir under imp<short>/<kind>."""
        src_path = f"{self.remote_exec_path}/imp{impression[0:7]}/{kind}"
        result = []
        with self._ssh() as ssh:
            try:
                filelist = list(ssh.walk_files(src_path))
            except FileNotFoundError:
                return []
            for rel_path, _remote_file, size in filelist:
                result.append({"name": rel_path, "size": size})
        return result

    @staticmethod
    def _sftp_file_size(ssh, remote_path):
        """Return the size of a remote file via SFTP stat."""
        try:
            return ssh._sftp.stat(remote_path).st_size  # pylint: disable=protected-access
        except Exception:
            return 0

    def download_selected(self, impression, predicate, kind="stageout"):
        """Copy only matching, not-yet-present files into Storage. No marker."""
        src_path = f"{self.remote_exec_path}/imp{impression[0:7]}/{kind}"
        dst_path = os.path.join(
            os.environ["HOME"], ".Yuki", "Storage",
            self.project_uuid, impression, self.machine_id, kind)
        report = {"collected": [], "skipped": [], "failed": []}
        os.makedirs(dst_path, exist_ok=True)

        with self._ssh() as ssh:
            try:
                filelist = list(ssh.walk_files(src_path))
            except FileNotFoundError:
                self.logger(f"[SSH] No {kind} found at: {src_path}")
                report["skipped"].append(
                    {"file": f"<{kind}>", "reason": "source missing"})
                return report
            for rel_path, remote_file, _size in filelist:
                if not predicate(rel_path):
                    report["skipped"].append(
                        {"file": rel_path, "reason": "does not match selector"})
                    continue
                dst_file = os.path.join(dst_path, rel_path)
                if os.path.exists(dst_file):
                    report["skipped"].append(
                        {"file": rel_path, "reason": "already in Yuki"})
                    continue
                try:
                    ssh.get(remote_file, dst_file)
                    report["collected"].append(rel_path)
                    self.logger(f"[SSH] Collected selected {kind}: {rel_path}")
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    report["failed"].append(
                        {"file": rel_path, "reason": str(exc)})
        return report

    def get_workflow_logs(self):
        """Persist remote engine logs in the same workflow-level location as REANA."""
        logpath = os.path.join(self.path, "engine_logs.json")
        if os.path.exists(logpath):
            return

        engine_logs = {
            "backend": "ssh",
            "workflow_uuid": self.uuid,
            "remote_exec_path": self.remote_exec_path,
        }

        try:
            with self._ssh() as ssh:
                snakemake_log = f"{self.remote_exec_path}/snakemake.log"
                if ssh.exists(snakemake_log):
                    out, _err, _code = ssh.exec(f"cat {snakemake_log}")
                    engine_logs["snakemake_log"] = out
        except Exception as e:
            engine_logs["snakemake_log_error"] = str(e)

        if os.path.exists(self.log_path):
            with open(self.log_path, "r", encoding="utf-8") as f:
                engine_logs["workflow_log"] = f.read()

        results_path = os.path.join(self.path, "results.json")
        if os.path.exists(results_path):
            with open(results_path, "r", encoding="utf-8") as f:
                engine_logs["results"] = json.load(f)

        log_file = metadata.ConfigFile(logpath)
        log_file.write_variable("logs", engine_logs)

    def ping(self):
        """Ping remote SSH host."""
        try:
            with self._ssh() as ssh:
                _out, err, code = ssh.exec("echo ok")
                if code == 0:
                    self.logger("[SSH] Remote host is reachable")
                    return True
                self.logger(f"[SSH] Remote host ping failed: {err}")
                return False
        except Exception as e:
            self.logger(f"[SSH] Remote host ping failed: {e}")
            return False

    def _write_environment_directive(self, snake_file, environment, indent=1):
        """Write a conda environment directive for remote SSH execution.

        Skips writing the directive for pure-copy procedures that do not need
        a conda environment.
        """
        if not environment_needs_conda(environment):
            return
        conda_env = self._resolve_conda_environment(environment)
        snake_file.addline("conda:", indent)
        snake_file.addline(f'"{conda_env}"', indent + 1)

    def _resolve_conda_environment(self, environment):
        """Map a job environment string to a conda environment name."""
        config_path = os.path.join(
            os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki")),
            "config.json"
        )
        return resolve_conda_environment(environment, config_path)
