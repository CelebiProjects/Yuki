"""Exercise real detached processes with no SSH host or Celery broker."""
import json
import fcntl
import os
import shlex
import signal
import subprocess
import sys
import time

from Yuki.kernel.detached_copy import launch_command, read_state


class LocalSsh:  # pylint: disable=too-few-public-methods
    """Execute only the test's status probe locally."""

    @staticmethod
    def exec(command, timeout):
        """Match the SSH result tuple without requiring a remote runner."""
        result = subprocess.run(shlex.split(command), timeout=timeout,
                                check=False, capture_output=True, text=True)
        return result.stdout, result.stderr, result.returncode


def _wait_state(path, status):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if path.exists():
            state = json.loads(path.read_text())
            if state["status"] == status:
                return state
        time.sleep(0.02)
    raise AssertionError(f"Copy did not reach {status}: {path}")


def test_copy_survives_worker_exit_and_duplicate_launch(tmp_path):
    """Killing the launching session leaves exactly one independent copy."""
    state_path = tmp_path / "copy state.json"
    gate = tmp_path / "release"
    output = tmp_path / "copied"
    script = (
        "import pathlib,sys,time\n"
        "gate,output = map(pathlib.Path,sys.argv[1:])\n"
        "deadline = time.monotonic()+10\n"
        "while not gate.exists():\n"
        "    if time.monotonic()>deadline: sys.exit(1)\n"
        "    time.sleep(0.02)\n"
        "with output.open('a') as stream: stream.write('copied\\n')\n"
    )
    copy = shlex.join([sys.executable, "-c", script, str(gate), str(output)])
    launch = shlex.split(launch_command(str(state_path), copy))
    worker = subprocess.Popen(  # pylint: disable=consider-using-with
        [sys.executable, "-c",
         "import subprocess,sys,time; subprocess.run(sys.argv[1:],check=True); time.sleep(30)",
         *launch], start_new_session=True,
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        state = _wait_state(state_path, "copying")
        assert os.getsid(state["pid"]) == state["pid"]
        assert state["pid"] != worker.pid
        assert read_state(LocalSsh(), str(state_path))["active"] is True
        os.killpg(worker.pid, signal.SIGTERM)
        worker.wait(timeout=5)
        subprocess.run(launch, check=True, timeout=5, capture_output=True)
        assert not output.exists()
        gate.touch()
        assert _wait_state(state_path, "done")["exit_code"] == 0
        assert read_state(LocalSsh(), str(state_path))["status"] == "done"
        # A redelivery after completion also cannot copy again.
        subprocess.run(launch, check=True, timeout=5, capture_output=True)
        assert output.read_text() == "copied\n"
    finally:
        gate.touch()
        if worker.poll() is None:
            os.killpg(worker.pid, signal.SIGTERM)
        worker.wait(timeout=5)


def test_detached_copy_records_failure_and_log(tmp_path):
    """Copy exit status and diagnostics survive the launcher process."""
    state_path = tmp_path / "state.json"
    subprocess.run(shlex.split(launch_command(
        str(state_path), "echo 'disk full' >&2; exit 23")),
        check=True, timeout=5, capture_output=True)
    state = _wait_state(state_path, "failed")
    assert state["exit_code"] == 23
    assert "remote copy failed" in state["error"]
    assert "disk full" in (tmp_path / "state.json.log").read_text()


def test_status_probe_distinguishes_missing_active_and_abandoned(tmp_path):
    """The lock is authoritative even before the first state write."""
    state_path = tmp_path / "copy.json"
    ssh = LocalSsh()
    assert read_state(ssh, str(state_path)) is None
    with open(str(state_path) + ".lock", "a", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        assert read_state(ssh, str(state_path)) == {"status": "copying", "active": True}
        state_path.write_text(json.dumps({"status": "copying", "pid": 123}))
        assert read_state(ssh, str(state_path))["active"] is True
    state = read_state(ssh, str(state_path))
    assert state["status"] == "copying"
    assert state["active"] is False


def test_probe_reads_progress_alongside_process_state(tmp_path):
    """One remote command returns both process state and byte progress."""
    state_path = tmp_path / "copy.json"
    progress_path = tmp_path / "progress.json"
    state_path.write_text(json.dumps({"status": "copying"}))
    progress = {"stage": "copying", "bytes_done": 42, "bytes_total": 100}
    progress_path.write_text(json.dumps(progress))
    state = read_state(LocalSsh(), str(state_path), str(progress_path))
    assert state["progress"] == progress
    progress_path.unlink()
    assert read_state(LocalSsh(), str(state_path), str(progress_path))["progress"] is None
