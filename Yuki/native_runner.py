"""Host agent for native workflows in the shared Yuki directory."""
import fcntl
import json
import os
import subprocess
import sys
import threading
import time
from contextlib import contextmanager

import click
from CelebiChrono.utils.metadata import ConfigFile

from Yuki.kernel.runners import config as runner_config
from Yuki.kernel.runners import project_environments
from Yuki.kernel.execution.local import execute_workflow, timestamp_logger


def _root():
    return os.path.abspath(os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki")))


@contextmanager
def _lock(path, blocking=False):
    """Hold a persistent advisory lock; leave its file for future users."""
    with open(path, "a+", encoding="utf-8") as lock_file:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(lock_file, flags)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _state(workflow_path):
    return ConfigFile(os.path.join(workflow_path, "results.json"))


def _set_state(workflow_path, state, error=None):
    file = _state(workflow_path)
    results = file.read_variable("results", {})
    results["status"] = state
    if error:
        results["error"] = error
    file.write_variable("results", results)


def _workflows(root):
    parent = os.path.join(root, "Workflows")
    if not os.path.isdir(parent):
        return
    for project in sorted(os.listdir(parent)):
        project_path = os.path.join(parent, project)
        if not os.path.isdir(project_path):
            continue
        for workflow_uuid in sorted(os.listdir(project_path)):
            path = os.path.join(project_path, workflow_uuid)
            if os.path.isfile(os.path.join(path, "config.json")):
                yield workflow_uuid, path


def _pid_alive(pid_path):
    try:
        with open(pid_path, encoding="utf-8") as source:
            pid = int(source.read().strip())
        if pid <= 0:
            return False
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def _execution_dir(root, workflow_path, workflow_uuid):
    project_uuid = os.path.basename(os.path.dirname(workflow_path))
    machine_id = ConfigFile(os.path.join(workflow_path, "config.json")) \
        .read_variable("machine_id", "")
    settings = runner_config.get_runner_settings(
        ConfigFile(os.path.join(root, "config.json")), machine_id)
    return runner_config.native_workflow_path(
        settings, project_uuid, workflow_uuid, yuki_dir=root)


def _recover_interrupted(root):
    """Return True while an orphaned Snakemake run may still be active."""
    orphan_alive = False
    for workflow_uuid, path in _workflows(root):
        claim = os.path.join(path, "native-runner.claim")
        if not os.path.exists(claim):
            continue
        with _lock(os.path.join(path, "native-runner.lock")) as acquired:
            if not acquired:
                continue
            state = _state(path).read_variable("results", {}).get("status")
            pid_path = os.path.join(
                _execution_dir(root, path, workflow_uuid),
                "native-runner.snakemake.pid")
            # The workflow state can already be terminal when cancellation or
            # another server-side action races with an interrupted agent.  A
            # live Snakemake process still owns the workspace in that case, so
            # keep its claim and prevent this agent from starting more work.
            if _pid_alive(pid_path):
                orphan_alive = True
                continue
            if state in ("running", "collecting"):
                _set_state(path, "failed",
                           "Native runner was interrupted; inspect workflow logs before retrying")
            os.unlink(claim)
    return orphan_alive


def _run_ready(root):
    """Run at most one claimed workflow and return whether work was found."""
    for workflow_uuid, path in _workflows(root):
        if _state(path).read_variable("results", {}).get("status") != \
                "ready_for_local_execution":
            continue
        with _lock(os.path.join(path, "native-runner.lock")) as claimed:
            if not claimed:
                continue
            if _state(path).read_variable("results", {}).get("status") != \
                    "ready_for_local_execution":
                continue
            claim_path = os.path.join(path, "native-runner.claim")
            with open(claim_path, "w", encoding="utf-8") as claim_file:
                claim_file.write(str(os.getpid()))
            _set_state(path, "running")
            try:
                code = execute_workflow(workflow_uuid, logger=timestamp_logger)
                final = _state(path).read_variable("results", {}).get("status")
                if not code and final not in ("finished", "coda"):
                    _set_state(path, "failed", "Local runner finished without completion metadata")
                elif code:
                    # The monitor normally writes the error first. Keep a
                    # terminal state even if it could not write its result.
                    if final not in ("failed", "stopped"):
                        _set_state(path, "failed", f"Local runner exited with {code}")
            except Exception as exc:  # pylint: disable=broad-exception-caught
                timestamp_logger(f"[LOCAL] {workflow_uuid} failed: {exc}")
                _set_state(path, "failed", str(exc))
            finally:
                os.unlink(claim_path)
            return True
    return False


def _run_environment_request(root):
    """Run at most one queued native environment installation."""
    request_dir = os.path.join(
        root, project_environments.NATIVE_REQUESTS_DIR)
    if not os.path.isdir(request_dir):
        return False
    for filename in sorted(os.listdir(request_dir)):
        if not filename.endswith(".json"):
            continue
        path = os.path.join(request_dir, filename)
        try:
            with open(path, encoding="utf-8") as request_file:
                request = json.load(request_file)
            if request.get("status") != "queued":
                continue
            request["status"] = "running"
            project_environments._write_json_atomic(  # pylint: disable=protected-access
                path, request)
            try:
                settings = dict(request.get("settings") or {},
                                backend_type="native")
                result = project_environments.install_native_environment_local(
                    settings, request["project_uuid"], request["name"],
                    request["yaml"])
                request.update(status="finished", result=result, error=None)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                request.update(status="failed", result=None,
                               error=str(exc) or type(exc).__name__)
            project_environments._write_json_atomic(  # pylint: disable=protected-access
                path, request)
            return True
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            timestamp_logger(f"[LOCAL] Invalid environment request {path}: {exc}")
    return False


def _agent_paths(root):
    directory = os.path.join(root, "NativeRunner")
    os.makedirs(directory, exist_ok=True)
    return (os.path.join(directory, "agent.lock"),
            os.path.join(directory, "stop.request"),
            os.path.join(directory, "runner.log"))


def _is_running(lock_path):
    with _lock(lock_path) as acquired:
        return not acquired


@contextmanager
def _heartbeat(root):
    """Publish liveness where cross-filesystem advisory locks are invisible."""
    path = os.path.join(root, "NativeRunner", "agent.heartbeat")
    stopped = threading.Event()

    def beat():
        while not stopped.is_set():
            with open(path, "a", encoding="utf-8"):
                os.utime(path, None)
            stopped.wait(2)

    thread = threading.Thread(target=beat, name="native-runner-heartbeat",
                              daemon=True)
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join(timeout=3)
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def _serve(root, once=False):
    lock_path, stop_path, _log_path = _agent_paths(root)
    with _lock(lock_path) as acquired:
        if not acquired:
            raise click.ClickException("Native runner is already running")
        with _heartbeat(root):
            timestamp_logger(f"[LOCAL] Native runner started; storage={root}")
            while True:
                if os.path.exists(stop_path):
                    os.unlink(stop_path)
                    break
                orphan_alive = _recover_interrupted(root)
                did_work = False if orphan_alive else _run_environment_request(root)
                if not orphan_alive and not did_work:
                    did_work = _run_ready(root)
                if once:
                    break
                if not did_work:
                    time.sleep(2)
            timestamp_logger("[LOCAL] Native runner stopped")


@click.group()
def cli():
    """Execute native workflows on the host using shared Yuki storage."""


@cli.command()
@click.option("--foreground", is_flag=True, help="Stay attached to this terminal.")
@click.option("--yuki-dir", envvar="YUKIDIR", default="~/.Yuki",
              help="Shared Yuki directory (default: ~/.Yuki).")
def start(foreground, yuki_dir):
    """Start the local workflow agent."""
    root = os.path.abspath(os.path.expanduser(yuki_dir))
    if not os.path.isdir(root):
        raise click.ClickException(f"Yuki directory does not exist: {root}")
    lock_path, stop_path, log_path = _agent_paths(root)
    if _is_running(lock_path):
        raise click.ClickException("Native runner is already running")
    if os.path.exists(stop_path):
        os.unlink(stop_path)
    if foreground:
        os.environ["YUKIDIR"] = root
        _serve(root)
        return
    env = dict(os.environ, YUKIDIR=root)
    with open(log_path, "a", encoding="utf-8") as log:
        process = subprocess.Popen(  # pylint: disable=consider-using-with
            [sys.executable, "-m", "Yuki.native_runner", "serve"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True, env=env)
    click.echo(f"Native runner starting (PID {process.pid}); log: {log_path}")


@cli.command(hidden=True)
def serve():
    """Internal detached agent entry point."""
    _serve(_root())


@cli.command()
@click.option("--yuki-dir", envvar="YUKIDIR", default="~/.Yuki")
def status(yuki_dir):
    """Show whether the host agent is running."""
    root = os.path.abspath(os.path.expanduser(yuki_dir))
    lock_path, _stop_path, _log_path = _agent_paths(root)
    click.echo("running" if _is_running(lock_path) else "stopped")


@cli.command()
@click.option("--yuki-dir", envvar="YUKIDIR", default="~/.Yuki")
def stop(yuki_dir):
    """Stop after the current workflow finishes."""
    root = os.path.abspath(os.path.expanduser(yuki_dir))
    lock_path, stop_path, _log_path = _agent_paths(root)
    if not _is_running(lock_path):
        click.echo("Native runner is not running")
        return
    with open(stop_path, "w", encoding="utf-8") as request:
        request.write("stop\n")
    click.echo("Native runner will stop after its current workflow")


@cli.command()
@click.option("--yuki-dir", envvar="YUKIDIR", default="~/.Yuki")
def logs(yuki_dir):
    """Print the agent log."""
    _lock_path, _stop_path, log_path = _agent_paths(
        os.path.abspath(os.path.expanduser(yuki_dir)))
    if os.path.exists(log_path):
        with open(log_path, encoding="utf-8") as log:
            click.echo(log.read(), nl=False)


if __name__ == "__main__":
    cli()
