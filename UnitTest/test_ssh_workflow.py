"""Unit tests for SshWorkflow remote backend."""
# pylint: disable=protected-access,too-many-lines
import json
import os
import shutil
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch


class _MockSftp:
    """Minimal in-memory SFTP double for SshWorkflow tests."""

    def __init__(self):
        self.files = {}
        self.dirs = set()

    def mkdir(self, path):
        """Record the created remote directory."""
        self.dirs.add(path)

    def chmod(self, path, _mode):
        """No-op chmod."""

    def close(self):
        """No-op close."""

    def put(self, local_path, remote_path):
        """Read the local file into the in-memory store."""
        with open(local_path, "rb") as f:
            self.files[remote_path] = f.read()

    def get(self, remote_path, local_path):
        """Write the stored remote file to local_path."""
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(self.files[remote_path])

    def file(self, remote_path, mode="r"):
        """Open a writable handle into the in-memory store."""
        class _File:
            def __init__(self, store, path):
                self._store = store
                self._path = path
                self._data = b""
            def write(self, data):
                """Accumulate the uploaded bytes."""
                self._data += data if isinstance(data, bytes) else data.encode("utf-8")
            def close(self):
                """Flush the accumulated bytes into the store."""
                self._store[self._path] = self._data
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.close()
                return False
        if mode == "w":
            return _File(self.files, remote_path)
        raise NotImplementedError

    def listdir(self, path):
        """List direct children of the remote directory."""
        if path not in self.dirs:
            raise FileNotFoundError(path)
        seen = set()
        for store in (self.files, self.dirs):
            for name in store:
                if name.startswith(path + "/"):
                    rel = name[len(path) + 1:]
                    if "/" not in rel:
                        seen.add(rel)
                    else:
                        seen.add(rel.split("/", 1)[0])
        return list(seen)

    def stat(self, path):
        """Return a minimal stat result for the remote path."""
        if path in self.dirs:
            from stat import S_IFDIR
            return self._stat(S_IFDIR, 0)
        if path in self.files:
            from stat import S_IFREG
            return self._stat(S_IFREG, len(self.files[path]))
        raise FileNotFoundError(path)

    def _stat(self, mode, size):
        """Build a minimal stat result with the given mode and size."""
        class _Stat:  # pylint: disable=too-few-public-methods
            """Stat double carrying mode and size."""
            st_mode = mode
            st_size = size
        return _Stat()

    def remove(self, path):
        """Remove the remote file from the store."""
        self.files.pop(path, None)


class _MockChannel:  # pylint: disable=too-few-public-methods
    """A channel double reporting a fixed exit code."""

    def __init__(self, exit_code=0, ready=True, data=b"", stderr_data=b""):
        self._exit_code = exit_code
        self._ready = ready
        self.timeout = None
        self._closed = False
        self._data = data
        self._stderr_data = stderr_data

    def settimeout(self, timeout):
        """Record the channel timeout."""
        self.timeout = timeout

    def exit_status_ready(self):
        """Report whether the remote exit status is available."""
        return self._ready

    def recv_exit_status(self):
        """Return the configured exit code."""
        return self._exit_code

    def recv_ready(self):
        """Report whether buffered stdout data is available."""
        return bool(self._data)

    def recv(self, _size):
        """Return the buffered stdout data."""
        return self._data

    def recv_stderr_ready(self):
        """Report whether buffered stderr data is available."""
        return bool(self._stderr_data)

    def recv_stderr(self, _size):
        """Return the buffered stderr data."""
        return self._stderr_data

    def close(self):
        """Record the client-side channel close."""
        self._closed = True


class _MockStdout:  # pylint: disable=too-few-public-methods
    """An exec stdout double returning the fixture text."""

    def __init__(self, text, exit_code=0):
        self._text = text
        self.channel = _MockChannel(exit_code)

    def read(self):
        """Return the fixture text as bytes."""
        return self._text.encode("utf-8")


class _MockStderr:  # pylint: disable=too-few-public-methods
    """An exec stderr double returning the fixture text."""

    def __init__(self, text):
        self._text = text

    def read(self):
        """Return the fixture text as bytes."""
        return self._text.encode("utf-8")


def _fake_server_config(home):
    """A server-config double rooted at the test HOME.

    ImpressionStorage resolves paths through the Yuki.server.config
    singleton, whose home_dir is baked at import time; this double keeps
    real-storage tests inside the tmpdir.
    """
    from CelebiChrono.utils.metadata import ConfigFile
    config = MagicMock()
    config.storage_path = os.path.join(home, ".Yuki", "Storage")
    config.get_job_path.side_effect = lambda project_uuid, impression: os.path.join(
        config.storage_path, project_uuid, impression)
    config.get_job_config_path.side_effect = lambda project_uuid, impression: os.path.join(
        config.storage_path, project_uuid, impression, "config.json")
    config.get_config_file.side_effect = lambda: ConfigFile(
        os.path.join(home, ".Yuki", "config.json"))
    return config


class TestSshWorkflow(unittest.TestCase):
    """Test SshWorkflow with an in-memory Paramiko mock."""
    # pylint: disable=too-many-instance-attributes,too-many-public-methods

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._home_patcher = patch.dict(os.environ, {"HOME": self.tmpdir})
        self._home_patcher.start()

        self.project_uuid = "p" * 32
        self.workflow_uuid = "w" * 32

        # Write SSH config for the fake runner.
        self._write_ssh_config()

        from Yuki.kernel.ssh_workflow import SshWorkflow
        self.workflow = SshWorkflow(self.project_uuid, [], None)
        self.workflow.uuid = self.workflow_uuid
        self.workflow.machine_id = "runner-uuid"
        self.workflow.ssh_config = {
            "host": "remote.host",
            "user": "alice",
            "key_path": "~/.ssh/id_rsa",
            "port": 22,
            "remote_workdir": "/tmp/yuki-workflows",
        }
        self.workflow.remote_exec_path = f"/tmp/yuki-workflows/{self.workflow_uuid}"
        self.workflow.jobs = []

        self.mock_client = MagicMock()
        self.mock_sftp = _MockSftp()
        self.mock_client.open_sftp.return_value = self.mock_sftp

        # Status updates that observe the terminal transition refresh the
        # distribution registry; isolate tests from that heavy side effect.
        self._refresh_patcher = patch(
            "Yuki.kernel.impression_storage.refresh_workflow_distributions",
            create=True)
        self.mock_refresh = self._refresh_patcher.start()

    def tearDown(self):
        self._refresh_patcher.stop()
        self._home_patcher.stop()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_ssh_config(self):
        config_path = os.path.join(self.tmpdir, ".Yuki", "config.json")
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump({
                "runners_id": {"myrunner": "runner-uuid"},
                "backend_types": {"runner-uuid": "ssh"},
                "ssh_hosts": {"runner-uuid": "remote.host"},
                "ssh_users": {"runner-uuid": "alice"},
                "ssh_key_paths": {"runner-uuid": "~/.ssh/id_rsa"},
                "ssh_ports": {"runner-uuid": 22},
                "remote_workdirs": {"runner-uuid": "/tmp/yuki-workflows"},
            }, f)

    def _make_job(self, uuid_full, status_value="prelude",  # pylint: disable=too-many-arguments,too-many-positional-arguments
                  is_input=False, job_type_value="task", files=None):
        job = MagicMock()
        job.uuid = uuid_full
        job.is_input = is_input
        job.path = "/fake/" + uuid_full
        job.job_type.return_value = job_type_value
        job.status.return_value = status_value
        job.short_uuid.return_value = uuid_full[:7]
        job.files.return_value = files or []
        job.environment.return_value = "docker.io/reanahub/reana-env-root6:6.18.04"
        return job

    def _prepare_real_storage_jobs(self):
        """Create the Storage layout for two real jobs: one with a run
        config (the finished job), one without (the pending job).

        Returns (done_job_dir, pending_job_dir).
        """
        storage = os.path.join(self.tmpdir, ".Yuki", "Storage", self.project_uuid)
        done_job_dir = os.path.join(storage, "a" * 32)
        os.makedirs(os.path.join(done_job_dir, "runner-uuid"), exist_ok=True)
        with open(os.path.join(done_job_dir, "config.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"object_type": "task"}, f)
        with open(os.path.join(done_job_dir, "runner-uuid", "config.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"workflow": self.workflow_uuid}, f)
        pending_job_dir = os.path.join(storage, "b" * 32)
        os.makedirs(pending_job_dir, exist_ok=True)
        with open(os.path.join(pending_job_dir, "config.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"object_type": "task"}, f)
        return done_job_dir, pending_job_dir

    def _add_runner_to_runners_list(self):
        """Append the fake runner to the runners list in the config."""
        config_path = os.path.join(self.tmpdir, ".Yuki", "config.json")
        with open(config_path, encoding="utf-8") as f:
            conf = json.load(f)
        conf["runners"] = ["myrunner"]
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(conf, f)

    def _write_workflow_backend_config(self):
        """Write the workflow config so update_distribution's runner
        context resolves the ssh backend."""
        workflow_dir = os.path.join(self.tmpdir, ".Yuki", "Workflows",
                                    self.project_uuid, self.workflow_uuid)
        os.makedirs(workflow_dir, exist_ok=True)
        with open(os.path.join(workflow_dir, "config.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"backend_type": "ssh",
                       "machine_id": "runner-uuid"}, f)

    @patch("paramiko.SSHClient")
    def test_start_snakemake_failure_includes_wrapper_log_tail(self, mock_ssh_cls):
        """A failed remote launch surfaces the wrapper log tail."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/yuki-wrapper.log"] = b"boom"
        self.mock_client.exec_command.side_effect = [
            (MagicMock(), _MockStdout(""), _MockStderr("")),           # chmod +x
            (MagicMock(), _MockStdout("", exit_code=1), _MockStderr("")),  # wrapper
            (MagicMock(), _MockStdout("MissingInputException in rule select"),
             _MockStderr("")),                                          # log tail
        ]

        with self.assertRaises(RuntimeError) as ctx:
            self.workflow._start_remote_snakemake()

        self.assertIn("MissingInputException", str(ctx.exception))
        self.assertIn("(exit 1)", str(ctx.exception))

    @patch("paramiko.SSHClient")
    def test_start_remote_snakemake_backgrounds_wrapper(self, mock_ssh_cls):
        """The wrapper is started detached so the submit returns immediately."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_client.exec_command.return_value = (
            MagicMock(), _MockStdout("started"), _MockStderr("")
        )

        with patch.object(self.workflow, "_confirm_remote_start"):
            self.workflow._start_remote_snakemake()

        commands = [call[0][0]
                    for call in self.mock_client.exec_command.call_args_list]
        start_cmd = next(command for command in commands if "setsid" in command)
        self.assertNotIn("nohup", start_cmd)
        self.assertIn("bash ./yuki_run.sh", start_cmd)
        self.assertNotIn("echo started", start_cmd)
        self.assertIn("test -r yuki_run.sh", start_cmd)
        # All three fds must leave the channel, or sshd keeps the channel
        # open and recv_exit_status blocks forever (submit stuck running).
        self.assertIn("< /dev/null", start_cmd)
        # setsid moves the wrapper into its own session: it survives the
        # sshd session teardown (SIGHUP) without nohup, and the exec
        # session may actually close once no members remain.
        self.assertIn("setsid", start_cmd)

    @patch("paramiko.SSHClient")
    def test_start_remote_snakemake_verifies_silent_remote(self, mock_ssh_cls):
        """A silent SSH channel succeeds only after marker verification."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_client.exec_command.return_value = (
            MagicMock(), _MockStdout("started"), _MockStderr("")
        )
        from Yuki.kernel.ssh_workflow import SSHStartNotConfirmed

        with patch("Yuki.kernel.ssh_workflow._SshConnection.exec_start_detached",
                   side_effect=SSHStartNotConfirmed("no confirmation")), \
                patch.object(self.workflow, "_confirm_remote_start") as confirm:
            self.workflow._start_remote_snakemake()

        confirm.assert_called_once()

    def test_confirm_remote_start_requires_marker(self):
        """Channel completion alone cannot prove that the wrapper started."""
        ssh = MagicMock()
        ssh.exists.return_value = False

        with patch("Yuki.kernel.ssh_workflow.time.monotonic",
                   side_effect=[0.0, 1.0]), \
                patch("Yuki.kernel.ssh_workflow.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "yuki.started"):
                self.workflow._confirm_remote_start(ssh, timeout=0.5)

    def test_confirm_remote_start_accepts_live_wrapper(self):
        """A valid startup marker and live supervisor confirm the launch."""
        ssh = MagicMock()
        started_path = f"{self.workflow.remote_exec_path}/yuki.started"
        ssh.exists.side_effect = lambda path: path == started_path

        def exec_side_effect(command, timeout=300):  # pylint: disable=unused-argument
            if command.startswith("cat"):
                return "1234 1235", "", 0
            if command.startswith("kill -0"):
                return "", "", 0
            return "", "", 1

        ssh.exec.side_effect = exec_side_effect

        self.workflow._confirm_remote_start(ssh, timeout=0)

    def test_confirm_remote_start_rejects_immediate_failure(self):
        """A nonzero atomic exit marker fails submission immediately."""
        ssh = MagicMock()
        exit_path = f"{self.workflow.remote_exec_path}/yuki.exit"
        ssh.exists.side_effect = lambda path: path == exit_path
        ssh.exec.return_value = "127", "", 0

        with self.assertRaisesRegex(RuntimeError, "code 127"):
            self.workflow._confirm_remote_start(ssh, timeout=0)

    def test_exec_start_detached_raises_on_silent_remote(self):
        """A silent remote raises SSHStartNotConfirmed.

        The channel is left for the connection teardown: closing it
        directly blocks on hosts whose sshd never confirms the close.
        """
        from Yuki.kernel.ssh_workflow import _SshConnection, SSHStartNotConfirmed
        conn = _SshConnection("host", "user")
        conn._client = MagicMock()
        stdout = MagicMock()
        stdout.channel = _MockChannel(ready=False)
        stderr = MagicMock()
        conn._client.exec_command.return_value = (MagicMock(), stdout, stderr)

        with self.assertRaises(SSHStartNotConfirmed):
            conn.exec_start_detached("cmd", timeout=30, grace=0)

        self.assertFalse(stdout.channel._closed)  # pylint: disable=protected-access

    def test_exec_start_detached_returns_result_when_reported(self):
        """A remote that reports promptly returns (out, err, code).

        The outputs come from the channel's buffered data, not from
        waiting for EOF (the channel may never reach EOF when the
        session keeps a background job alive).
        """
        from Yuki.kernel.ssh_workflow import _SshConnection
        conn = _SshConnection("host", "user")
        conn._client = MagicMock()
        stdout = MagicMock()
        stdout.channel = _MockChannel(ready=True, data=b"started")
        stderr = MagicMock()
        conn._client.exec_command.return_value = (MagicMock(), stdout, stderr)

        result = conn.exec_start_detached("cmd", timeout=30, grace=0)

        self.assertEqual(result, ("started", "", 0))
        stdout.read.assert_not_called()

    def test_exec_bounds_the_wait_with_channel_timeout(self):
        """exec polls for readiness before fetching the exit status."""
        from Yuki.kernel.ssh_workflow import _SshConnection
        conn = _SshConnection("host", "user")
        conn._client = MagicMock()
        stdout = MagicMock()
        stdout.channel = MagicMock()
        stdout.channel.exit_status_ready.side_effect = [False, False, True]
        stdout.channel.recv_exit_status.return_value = 0
        stdout.read.return_value = b"ok"
        stderr = MagicMock()
        stderr.read.return_value = b""
        conn._client.exec_command.return_value = (MagicMock(), stdout, stderr)

        with patch("time.sleep", return_value=None) as mock_sleep:
            out, err, code = conn.exec("true", timeout=7)

        self.assertEqual((out, err, code), ("ok", "", 0))
        stdout.channel.settimeout.assert_called_once_with(7)
        self.assertEqual(stdout.channel.exit_status_ready.call_count, 3)
        stdout.channel.recv_exit_status.assert_called_once_with()
        mock_sleep.assert_called()

    def test_exec_times_out_when_channel_never_finishes(self):
        """A stuck channel raises socket.timeout instead of hanging forever."""
        from Yuki.kernel.ssh_workflow import _SshConnection
        conn = _SshConnection("host", "user")
        conn._client = MagicMock()
        stdout = MagicMock()
        stdout.channel = MagicMock()
        stdout.channel.exit_status_ready.return_value = False
        stdout.read.return_value = b""
        stderr = MagicMock()
        stderr.read.return_value = b""
        conn._client.exec_command.return_value = (MagicMock(), stdout, stderr)

        with patch("time.sleep", return_value=None), \
                patch("time.monotonic", side_effect=[0.0, 10.0]):
            with self.assertRaises(socket.timeout):
                conn.exec("true", timeout=1)

    @patch("paramiko.SSHClient")
    def test_wrapper_records_exit_code_on_failure(self, mock_ssh_cls):
        """yuki.exit is written even when snakemake exits nonzero."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_client.exec_command.return_value = (
            MagicMock(), _MockStdout("started"), _MockStderr("")
        )

        with patch.object(self.workflow, "_confirm_remote_start"):
            self.workflow._start_remote_snakemake()

        wrapper = self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/yuki_run.sh"].decode("utf-8")
        self.assertIn("trap record_exit EXIT", wrapper)
        self.assertIn('atomic_write "$rc" yuki.exit', wrapper)
        self.assertIn('atomic_write "$$ $snakemake_pid" yuki.started', wrapper)
        self.assertIn('wait "$snakemake_pid"', wrapper)

    def test_generated_wrapper_has_valid_bash_syntax(self):
        """The generated supervision script must parse as Bash."""
        wrapper = self.workflow._build_remote_wrapper()

        result = subprocess.run(
            ["bash", "-n"], input=wrapper, text=True,
            capture_output=True, check=False)

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_generated_wrapper_writes_atomic_runtime_markers(self):
        """The supervisor records startup PIDs and the final exit code."""
        self.workflow.ssh_config["snakemake_path"] = "/usr/bin/true"
        wrapper_path = os.path.join(self.tmpdir, "yuki_run.sh")
        with open(wrapper_path, "w", encoding="utf-8") as wrapper_file:
            wrapper_file.write(self.workflow._build_remote_wrapper())

        result = subprocess.run(
            ["bash", wrapper_path], text=True,
            capture_output=True, check=False)

        self.assertEqual(result.returncode, 0, result.stderr)
        with open(os.path.join(self.tmpdir, "yuki.started"),
                  encoding="utf-8") as started_file:
            self.assertEqual(len(started_file.read().split()), 2)
        with open(os.path.join(self.tmpdir, "yuki.exit"),
                  encoding="utf-8") as exit_file:
            self.assertEqual(exit_file.read().strip(), "0")
        self.assertFalse(any(".tmp." in name for name in os.listdir(self.tmpdir)))

    def test_generated_wrapper_quotes_configured_command(self):
        """Runner command paths and core values are shell-quoted."""
        self.workflow.ssh_config.update({
            "snakemake_path": "/opt/yuki tools/snakemake",
            "cores": "8; false",
        })

        wrapper = self.workflow._build_remote_wrapper()

        self.assertIn("'/opt/yuki tools/snakemake'", wrapper)
        self.assertIn("--cores '8; false'", wrapper)

    def test_launch_command_does_not_mask_cd_failure(self):
        """Preflight failure must be visible before anything is backgrounded."""
        self.workflow.remote_exec_path = "/definitely/not/a/yuki path"
        command = self.workflow._build_remote_launch_command()

        result = subprocess.run(
            ["bash", "-c", command], text=True,
            capture_output=True, check=False)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cd '/definitely/not/a/yuki path'", command)

    @patch("paramiko.SSHClient")
    def test_wrapper_conda_binary_dir_on_path(self, mock_ssh_cls):
        """conda_path is a binary path; its directory lands on PATH."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_client.exec_command.return_value = (
            MagicMock(), _MockStdout("started"), _MockStderr("")
        )
        self.workflow.ssh_config["conda_path"] = \
            "/home/zhaomr/workdir/miniconda3/bin/conda"

        with patch.object(self.workflow, "_confirm_remote_start"):
            self.workflow._start_remote_snakemake()

        wrapper = self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/yuki_run.sh"].decode("utf-8")
        self.assertIn('CONDA_BIN=/home/zhaomr/workdir/miniconda3/bin',
                      wrapper)
        self.assertIn('export PATH="${CONDA_BIN:+$CONDA_BIN:}'
                      '$HOME/.local/bin', wrapper)
        self.assertNotIn("miniconda3/bin/bin", wrapper)

    @patch("paramiko.SSHClient")
    def test_wrapper_conda_fallback_without_conda_path(self, mock_ssh_cls):
        """Without conda_path the wrapper falls back to conda info --base."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_client.exec_command.return_value = (
            MagicMock(), _MockStdout("started"), _MockStderr("")
        )

        with patch.object(self.workflow, "_confirm_remote_start"):
            self.workflow._start_remote_snakemake()

        wrapper = self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/yuki_run.sh"].decode("utf-8")
        self.assertIn("conda info --base", wrapper)
        self.assertIn('CONDA_BIN="${CONDA_BIN:+$CONDA_BIN/bin}"', wrapper)

    @patch("paramiko.SSHClient")
    def test_execute_backend_uploads_snakefile_and_starts_command(self, mock_ssh_cls):
        """_execute_backend uploads the Snakefile and runs the wrapper."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_client.exec_command.return_value = (
            MagicMock(), _MockStdout("12345"), _MockStderr("")
        )

        # Prepare a local Snakefile to upload.
        snakefile_path = os.path.join(self.workflow.path, "Snakefile")
        os.makedirs(self.workflow.path, exist_ok=True)
        with open(snakefile_path, "w", encoding="utf-8") as f:
            f.write("rule test: shell: 'echo ok'")
        self.workflow.snakefile_path = snakefile_path

        with patch.object(self.workflow, "_confirm_remote_start"):
            self.workflow._execute_backend()

        remote_snakefile = f"{self.workflow.remote_exec_path}/Snakefile"
        self.assertIn(remote_snakefile, self.mock_sftp.files)
        self.mock_client.exec_command.assert_called()
        cmd = self.mock_client.exec_command.call_args[0][0]
        self.assertIn("yuki_run.sh", cmd)

    @patch("paramiko.SSHClient")
    def test_update_workflow_status_without_local_workflow_info(self, mock_ssh_cls):
        """SSH status update must work without local workflow_info.json.

        workflow_info.json is uploaded to the remote host by
        _create_remote_structure; it is not written locally. The status update
        should derive the job list from self.jobs (loaded from local
        config.json jobs_info) instead of requiring the remote-only file.
        """
        mock_ssh_cls.return_value = self.mock_client

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        # Ensure the local workflow directory exists (setUp already creates it).
        os.makedirs(self.workflow.path, exist_ok=True)

        done_path = f"{self.workflow.remote_exec_path}/{job.short_uuid()}.done"
        self.mock_sftp.files[done_path] = b""
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)

        self.workflow.update_workflow_status()

        results_path = os.path.join(self.workflow.path, "results.json")
        self.assertTrue(os.path.exists(results_path))
        with open(results_path, encoding="utf-8") as f:
            results = json.load(f)
        self.assertEqual(results["results"]["status"], "finished")
        job.set_status.assert_called_with("finished", "Remote execution completed")

    @patch("paramiko.SSHClient")
    def test_update_workflow_status_reports_finished_when_done_files_exist(self, mock_ssh_cls):
        """A done file on the runner yields a finished workflow status."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)
        short = "a" * 7
        self.mock_sftp.files[f"{self.workflow.remote_exec_path}/{short}.done"] = b""

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        os.makedirs(self.workflow.path, exist_ok=True)

        self.workflow.update_workflow_status()

        results_path = os.path.join(self.workflow.path, "results.json")
        with open(results_path, encoding="utf-8") as f:
            results = json.load(f)
        self.assertEqual(results["results"]["status"], "finished")
        self.assertEqual(results["results"]["progress"]["completed"], 1)

    @patch("paramiko.SSHClient")
    def test_update_workflow_status_terminal_transition_records_distributions(
            self, mock_ssh_cls):
        """The poll that first observes finished records the data registry."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)
        short = "a" * 7
        self.mock_sftp.files[f"{self.workflow.remote_exec_path}/{short}.done"] = b""

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        os.makedirs(self.workflow.path, exist_ok=True)

        self.workflow.update_workflow_status()

        self.mock_refresh.assert_called_once_with(
            self.project_uuid, self.workflow, "finished")

    @patch("paramiko.SSHClient")
    def test_update_workflow_status_repeated_terminal_poll_skips_refresh(
            self, mock_ssh_cls):
        """A poll that finds the workflow already terminal does not re-record."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)
        short = "a" * 7
        self.mock_sftp.files[f"{self.workflow.remote_exec_path}/{short}.done"] = b""

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        os.makedirs(self.workflow.path, exist_ok=True)
        results_path = os.path.join(self.workflow.path, "results.json")
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump({"results": {"status": "finished"}}, f)

        self.workflow.update_workflow_status()

        self.mock_refresh.assert_not_called()

    @patch("paramiko.SSHClient")
    def test_update_workflow_status_running_does_not_refresh(
            self, mock_ssh_cls):
        """A running workflow never triggers the distribution refresh."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        os.makedirs(self.workflow.path, exist_ok=True)

        self.workflow.update_workflow_status()

        self.mock_refresh.assert_not_called()

    @patch("paramiko.SSHClient")
    def test_update_workflow_status_detects_remote_exit_nonzero(self, mock_ssh_cls):
        """A nonzero remote yuki.exit marks the workflow failed with the
        snakemake log tail, and never-run jobs fail with a skip message."""
        from Yuki.kernel.status_constants import FAILED

        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)
        self.mock_sftp.files[f"{self.workflow.remote_exec_path}/yuki.exit"] = b"1"
        self.mock_sftp.files[f"{self.workflow.remote_exec_path}/snakemake.log"] = b"x"

        def exec_side_effect(command, timeout=300):  # pylint: disable=unused-argument
            if "yuki.exit" in command:
                return MagicMock(), _MockStdout("1"), _MockStderr("")
            if "snakemake.log" in command:
                return (MagicMock(),
                        _MockStdout("EnvironmentNameNotFound: Could not find conda environment"),
                        _MockStderr(""))
            return MagicMock(), _MockStdout(""), _MockStderr("")

        self.mock_client.exec_command.side_effect = exec_side_effect

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        os.makedirs(self.workflow.path, exist_ok=True)

        self.workflow.update_workflow_status()

        results_path = os.path.join(self.workflow.path, "results.json")
        with open(results_path, encoding="utf-8") as f:
            results = json.load(f)
        self.assertEqual(results["results"]["status"], "failed")
        self.assertIn("EnvironmentNameNotFound", results["results"]["failure_detail"])
        job.set_status.assert_called_with(
            FAILED, "Skipped: upstream dependency failed before this job ran")

    @patch("paramiko.SSHClient")
    def test_update_workflow_status_rejects_incomplete_success(self, mock_ssh_cls):
        """Exit zero is a failure when expected completion markers are absent."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)
        self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/yuki.exit"] = b"0"
        self.mock_client.exec_command.return_value = (
            MagicMock(), _MockStdout("0"), _MockStderr(""))

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        os.makedirs(self.workflow.path, exist_ok=True)

        self.workflow.update_workflow_status()

        results_path = os.path.join(self.workflow.path, "results.json")
        with open(results_path, encoding="utf-8") as f:
            results = json.load(f)["results"]
        self.assertEqual(results["status"], "failed")
        self.assertIn(job.short_uuid(), results["failure_detail"])

    @patch("paramiko.SSHClient")
    def test_update_workflow_status_detects_dead_supervisor(self, mock_ssh_cls):
        """A dead wrapper without an exit marker cannot remain running."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)
        self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/yuki.started"] = b"1234 1235"

        def exec_side_effect(command, timeout=300):  # pylint: disable=unused-argument
            if "yuki.started" in command:
                return (MagicMock(), _MockStdout("1234 1235"),
                        _MockStderr(""))
            if command.startswith("kill -0"):
                return MagicMock(), _MockStdout("", exit_code=1), _MockStderr("")
            return MagicMock(), _MockStdout(""), _MockStderr("")

        self.mock_client.exec_command.side_effect = exec_side_effect
        self.workflow.jobs = [self._make_job("a" * 32)]
        os.makedirs(self.workflow.path, exist_ok=True)

        self.workflow.update_workflow_status()

        results_path = os.path.join(self.workflow.path, "results.json")
        with open(results_path, encoding="utf-8") as f:
            results = json.load(f)["results"]
        self.assertEqual(results["status"], "failed")
        self.assertIn("without writing yuki.exit", results["failure_detail"])

    @patch("paramiko.SSHClient")
    def test_propagate_done_jobs_become_finished(self, mock_ssh_cls):
        """Completed jobs should be stored with the legacy status 'finished'
        so the client can display [coda][finished]."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        self.mock_sftp.files[f"{self.workflow.remote_exec_path}/{job.short_uuid()}.done"] = b""

        self.workflow.propagate_job_statuses(workflow_terminal=False)

        job.set_status.assert_called_once_with("finished", "Remote execution completed")

    @patch("paramiko.SSHClient")
    def test_running_poll_records_distribution_for_finished_job(
            self, mock_ssh_cls):
        """The poll where one job finishes while the workflow is still
        running must list that job's final files and record the produced
        registry entry in distribution.json.

        The refresh must happen before the job is marked finished:
        afterwards the listing refresh skips terminal jobs, so the
        produced entry would be built from a stale (or missing) listing.
        """
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)

        done_short = "a" * 7
        self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/{done_short}.done"] = b""
        remote_stageout = f"{self.workflow.remote_exec_path}/imp{done_short}/stageout"
        self.mock_sftp.dirs.add(remote_stageout)
        self.mock_sftp.files[f"{remote_stageout}/output.root"] = b"data"

        # Real storage layout for two jobs: the finished one has a run
        # config (so its runner context resolves), the pending one does not.
        done_job_dir, pending_job_dir = self._prepare_real_storage_jobs()
        # The workflow config resolves the ssh backend for the runner
        # context built by update_distribution.
        self._write_workflow_backend_config()
        # Add the runner to the runners list (runners_id already has it).
        self._add_runner_to_runners_list()

        # update_distribution resolves paths through the server config
        # singleton; point it at the tmp HOME.
        with patch("Yuki.server.config.config", _fake_server_config(self.tmpdir)):
            from Yuki.kernel.vjob import VJob
            self.workflow.jobs = [
                VJob(done_job_dir, "runner-uuid"),
                VJob(pending_job_dir, "runner-uuid"),
            ]

            self.workflow.update_workflow_status()

        results_path = os.path.join(self.workflow.path, "results.json")
        with open(results_path, encoding="utf-8") as f:
            results = json.load(f)
        self.assertEqual(results["results"]["status"], "running")

        dist_path = os.path.join(done_job_dir, "distribution.json")
        self.assertTrue(os.path.exists(dist_path),
                        "distribution.json must record the finished job")
        with open(dist_path, encoding="utf-8") as f:
            dist = json.load(f)
        self.assertEqual(dist["produced_on"], "myrunner")
        produced = dist["locations"]["runner:myrunner"]["workflow"]
        self.assertEqual(produced["origin"], "produced")
        self.assertEqual(produced["files"], 1)

    @patch("paramiko.SSHClient")
    def test_propagate_records_distribution_on_finished_transition(
            self, mock_ssh_cls):
        """Marking a job finished must also record its data registry."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/{job.short_uuid()}.done"] = b""

        with patch("Yuki.kernel.impression_storage.ImpressionStorage") as mock_storage:
            self.workflow.propagate_job_statuses(workflow_terminal=False)

        job.set_status.assert_called_once_with("finished",
                                               "Remote execution completed")
        mock_storage.assert_called_once_with(self.project_uuid, job.uuid)
        mock_storage.return_value.update_distribution.assert_called_once_with()

    @patch("paramiko.SSHClient")
    def test_propagate_records_distribution_on_failed_transition(
            self, mock_ssh_cls):
        """A job that ran and failed must also record its produced data."""
        from Yuki.kernel.status_constants import FAILED

        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        short = job.short_uuid()
        logs_dir = f"{self.workflow.remote_exec_path}/imp{short}/logs"
        self.mock_sftp.dirs.add(logs_dir)
        self.mock_sftp.files[f"{logs_dir}/celebi_user_step0.log"] = b"err"
        self.mock_client.exec_command.return_value = (
            MagicMock(), _MockStdout("RuntimeError: segfault"), _MockStderr(""))

        with patch("Yuki.kernel.impression_storage.ImpressionStorage") as mock_storage:
            self.workflow.propagate_job_statuses(workflow_terminal=True)

        args, _kwargs = job.set_status.call_args
        self.assertEqual(args[0], FAILED)
        self.assertIn("RuntimeError", args[1])
        mock_storage.assert_called_once_with(self.project_uuid, job.uuid)
        mock_storage.return_value.update_distribution.assert_called_once_with()

    @patch("paramiko.SSHClient")
    def test_propagate_survives_distribution_failure(self, mock_ssh_cls):
        """A failing registry update must not break the status write."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.dirs.add(self.workflow.remote_exec_path)

        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/{job.short_uuid()}.done"] = b""

        with patch("Yuki.kernel.impression_storage.ImpressionStorage") as mock_storage:
            mock_storage.return_value.update_distribution.side_effect = OSError("boom")
            self.workflow.propagate_job_statuses(workflow_terminal=False)  # no raise

        job.set_status.assert_called_once_with("finished",
                                               "Remote execution completed")
        mock_storage.return_value.update_distribution.assert_called_once_with()

    @patch("paramiko.SSHClient")
    def test_download_outputs_pulls_remote_stageout_files(self, mock_ssh_cls):
        """download_outputs pulls remote stageout files into Storage."""
        mock_ssh_cls.return_value = self.mock_client
        short = "a" * 7
        remote_dir = f"{self.workflow.remote_exec_path}/imp{short}/stageout"
        self.mock_sftp.dirs.add(remote_dir)
        self.mock_sftp.files[f"{remote_dir}/output.root"] = b"data"

        self.workflow.download_outputs(impression="a" * 32)

        local_file = os.path.join(
            self.tmpdir, ".Yuki", "Storage", self.project_uuid,
            "a" * 32, "runner-uuid", "stageout", "output.root"
        )
        self.assertTrue(os.path.exists(local_file))
        with open(local_file, "rb") as f:
            self.assertEqual(f.read(), b"data")

    @patch("paramiko.SSHClient")
    def test_kill_sends_signal_to_remote_pid(self, mock_ssh_cls):
        """kill sends a signal to the remote pid from yuki.pid."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.files[f"{self.workflow.remote_exec_path}/yuki.pid"] = b"12345"

        def exec_side_effect(command, timeout=300):  # pylint: disable=unused-argument
            if command.startswith("cat"):
                return MagicMock(), _MockStdout("12345"), _MockStderr("")
            return MagicMock(), _MockStdout(""), _MockStderr("")

        self.mock_client.exec_command.side_effect = exec_side_effect

        self.workflow.kill()

        cmds = [call[0][0] for call in self.mock_client.exec_command.call_args_list]
        self.assertTrue(any("kill -TERM -- 12345" in cmd for cmd in cmds))

    @patch("paramiko.SSHClient")
    def test_kill_sends_signal_to_remote_process_group(self, mock_ssh_cls):
        """New workflows terminate the complete supervised process group."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_sftp.files[
            f"{self.workflow.remote_exec_path}/yuki.started"] = b"1234 1235"

        def exec_side_effect(command, timeout=300):  # pylint: disable=unused-argument
            if command.startswith("cat"):
                return (MagicMock(), _MockStdout("1234 1235"),
                        _MockStderr(""))
            return MagicMock(), _MockStdout(""), _MockStderr("")

        self.mock_client.exec_command.side_effect = exec_side_effect

        self.workflow.kill()

        commands = [call[0][0]
                    for call in self.mock_client.exec_command.call_args_list]
        self.assertIn("kill -TERM -- -1234", commands)

    @patch("paramiko.SSHClient")
    def test_ping_returns_true_when_remote_echo_succeeds(self, mock_ssh_cls):
        """ping returns True when the remote echo succeeds."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_client.exec_command.return_value = (
            MagicMock(), _MockStdout("ok"), _MockStderr("")
        )

        self.assertTrue(self.workflow.ping())

    @patch("paramiko.SSHClient")
    def test_ping_returns_false_on_connection_failure(self, mock_ssh_cls):
        """ping returns False when the connection is refused."""
        mock_ssh_cls.return_value = self.mock_client
        self.mock_client.connect.side_effect = OSError("Connection refused")

        self.assertFalse(self.workflow.ping())

    def _prepare_snakefile(self):
        """Create a local Snakefile so _upload_files_remote has one to upload."""
        os.makedirs(self.workflow.path, exist_ok=True)
        snakefile_path = os.path.join(self.workflow.path, "Snakefile")
        with open(snakefile_path, "w", encoding="utf-8") as f:
            f.write("rule all:\n    shell: 'true'\n")
        self.workflow.snakefile_path = snakefile_path

    @patch("paramiko.SSHClient")
    def test_upload_files_remote_uploads_input_stageout_files(self, mock_ssh_cls):
        """Input-job data in local Storage must be copied to the remote stageout.

        Downstream jobs resolve their inputs through a ``gen -> ../imp<short>``
        symlink, so the producer's ``imp<short>/stageout/<file>`` must exist on
        the remote host before Snakemake runs. Recording it in a manifest is not
        enough: nothing processes that manifest remotely.
        """
        mock_ssh_cls.return_value = self.mock_client

        def exec_side_effect(command, timeout=300):  # pylint: disable=unused-argument
            if command.startswith("test -d"):
                return MagicMock(), _MockStdout("", exit_code=1), _MockStderr("")
            return MagicMock(), _MockStdout(""), _MockStderr("")

        self.mock_client.exec_command.side_effect = exec_side_effect

        job = self._make_job("a" * 32, is_input=True)
        job.machine_id = "runner-uuid"
        self.workflow.jobs = [job]

        src_stageout = os.path.join(
            self.tmpdir, ".Yuki", "Storage", self.project_uuid,
            "a" * 32, "runner-uuid", "stageout",
        )
        os.makedirs(src_stageout, exist_ok=True)
        with open(os.path.join(src_stageout, "data.root"), "wb") as f:
            f.write(b"input-bytes")

        self._prepare_snakefile()

        self.workflow._upload_files_remote()

        cache_dir = (f"/tmp/yuki-workflows/impressions/"
                     f"{self.project_uuid}/{"a" * 32}")
        expected = f"{cache_dir}/data.root"
        self.assertIn(expected, self.mock_sftp.files)
        self.assertEqual(self.mock_sftp.files[expected], b"input-bytes")

    @patch("paramiko.SSHClient")
    def test_upload_files_remote_preserves_nested_input_structure(self, mock_ssh_cls):
        """Nested subdirectories inside input stageout must be uploaded recursively."""
        mock_ssh_cls.return_value = self.mock_client

        def exec_side_effect(command, timeout=300):  # pylint: disable=unused-argument
            if command.startswith("test -d"):
                return MagicMock(), _MockStdout("", exit_code=1), _MockStderr("")
            return MagicMock(), _MockStdout(""), _MockStderr("")

        self.mock_client.exec_command.side_effect = exec_side_effect

        job = self._make_job("a" * 32, is_input=True)
        job.machine_id = "runner-uuid"
        self.workflow.jobs = [job]

        src_stageout = os.path.join(
            self.tmpdir, ".Yuki", "Storage", self.project_uuid,
            "a" * 32, "runner-uuid", "stageout",
        )
        nested_dir = os.path.join(src_stageout, "data")
        os.makedirs(nested_dir, exist_ok=True)
        with open(os.path.join(nested_dir, "data.root"), "wb") as f:
            f.write(b"input-bytes")

        self._prepare_snakefile()
        self.workflow._upload_files_remote()

        cache_dir = (f"/tmp/yuki-workflows/impressions/"
                     f"{self.project_uuid}/{"a" * 32}")
        expected = f"{cache_dir}/data/data.root"
        self.assertIn(expected, self.mock_sftp.files)
        self.assertEqual(self.mock_sftp.files[expected], b"input-bytes")

    @patch("paramiko.SSHClient")
    def test_collect_remote_artifacts_preserves_nested_stageout(self, mock_ssh_cls):
        """Nested remote stageout files must be downloaded preserving structure."""
        mock_ssh_cls.return_value = self.mock_client

        impression = "i" * 32
        short = impression[:7]
        remote_stageout = f"{self.workflow.remote_exec_path}/imp{short}/stageout"
        self.mock_sftp.dirs.add(remote_stageout)
        self.mock_sftp.dirs.add(f"{remote_stageout}/plots")
        self.mock_sftp.files[f"{remote_stageout}/plots/mass.png"] = b"img"

        report = self.workflow._collect_remote_artifacts(
            impression, "stageout", "stageout.downloaded", "output"
        )

        local_stageout = os.path.join(
            self.tmpdir, ".Yuki", "Storage", self.project_uuid,
            impression, self.workflow.machine_id, "stageout"
        )
        self.assertTrue(os.path.exists(os.path.join(local_stageout, "plots", "mass.png")))
        self.assertIn("plots/mass.png", report["collected"])

    @patch("paramiko.SSHClient")
    def test_collect_remote_artifacts_refresh_overwrites_existing_log(self, mock_ssh_cls):
        """refresh=True must overwrite a local log that has grown remotely."""
        mock_ssh_cls.return_value = self.mock_client

        impression = "i" * 32
        short = impression[:7]
        remote_logs = f"{self.workflow.remote_exec_path}/imp{short}/logs"
        self.mock_sftp.dirs.add(remote_logs)
        self.mock_sftp.files[f"{remote_logs}/celebi_user_step0.log"] = b"grown"

        # Seed a stale local snapshot.
        local_log = os.path.join(
            self.tmpdir, ".Yuki", "Storage", self.project_uuid,
            impression, self.workflow.machine_id, "logs", "celebi_user_step0.log")
        os.makedirs(os.path.dirname(local_log), exist_ok=True)
        with open(local_log, "wb") as f:
            f.write(b"stale")

        report = self.workflow._collect_remote_artifacts(
            impression, "logs", "logs.downloaded", "log", refresh=True
        )

        with open(local_log, "rb") as f:
            self.assertEqual(f.read(), b"grown")
        self.assertIn("celebi_user_step0.log", report["collected"])

    @patch("paramiko.SSHClient")
    def test_collect_remote_artifacts_without_refresh_skips_existing(self, mock_ssh_cls):
        """Without refresh an already-downloaded log is skipped."""
        mock_ssh_cls.return_value = self.mock_client

        impression = "i" * 32
        short = impression[:7]
        remote_logs = f"{self.workflow.remote_exec_path}/imp{short}/logs"
        self.mock_sftp.dirs.add(remote_logs)
        self.mock_sftp.files[f"{remote_logs}/celebi_user_step0.log"] = b"grown"

        local_log = os.path.join(
            self.tmpdir, ".Yuki", "Storage", self.project_uuid,
            impression, self.workflow.machine_id, "logs", "celebi_user_step0.log")
        os.makedirs(os.path.dirname(local_log), exist_ok=True)
        with open(local_log, "wb") as f:
            f.write(b"stale")

        report = self.workflow._collect_remote_artifacts(
            impression, "logs", "logs.downloaded", "log"
        )

        with open(local_log, "rb") as f:
            self.assertEqual(f.read(), b"stale")
        self.assertEqual(
            report["skipped"],
            [{"file": "celebi_user_step0.log", "reason": "already in Yuki"}])

    @patch("paramiko.SSHClient")
    def test_list_runner_files_remote_returns_relative_paths(self, mock_ssh_cls):
        """list_runner_files must return relative paths for nested files."""
        mock_ssh_cls.return_value = self.mock_client

        impression = "i" * 32
        short = impression[:7]
        remote_stageout = f"{self.workflow.remote_exec_path}/imp{short}/stageout"
        self.mock_sftp.dirs.add(remote_stageout)
        self.mock_sftp.dirs.add(f"{remote_stageout}/data")
        self.mock_sftp.files[f"{remote_stageout}/data/ntuple.root"] = b"data"

        out = self.workflow.list_runner_files(impression, "stageout")
        names = {f["name"] for f in out}
        self.assertIn("data/ntuple.root", names)

    @patch("paramiko.SSHClient")
    def test_download_selected_remote_matches_relative_path(self, mock_ssh_cls):
        """download_selected predicate must see relative paths for nested files."""
        mock_ssh_cls.return_value = self.mock_client

        impression = "i" * 32
        short = impression[:7]
        remote_stageout = f"{self.workflow.remote_exec_path}/imp{short}/stageout"
        self.mock_sftp.dirs.add(remote_stageout)
        self.mock_sftp.dirs.add(f"{remote_stageout}/plots")
        self.mock_sftp.files[f"{remote_stageout}/plots/mass.png"] = b"img"
        self.mock_sftp.files[f"{remote_stageout}/ntuple.root"] = b"data"

        from Yuki.kernel import file_types
        report = self.workflow.download_selected(
            impression, file_types.make_predicate("plots/*.png"), "stageout"
        )

        local_stageout = os.path.join(
            self.tmpdir, ".Yuki", "Storage", self.project_uuid,
            impression, self.workflow.machine_id, "stageout"
        )
        self.assertTrue(os.path.exists(os.path.join(local_stageout, "plots", "mass.png")))
        self.assertFalse(os.path.exists(os.path.join(local_stageout, "ntuple.root")))
        self.assertIn("plots/mass.png", report["collected"])


if __name__ == "__main__":
    unittest.main()
