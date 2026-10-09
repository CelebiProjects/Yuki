"""Runner-side registration copies independent of Celery and SSH lifetimes."""
import json
import shlex


COPY_SCRIPT = r'''
import fcntl, json, os, subprocess, sys, tempfile

state_path, command = sys.argv[1:]
lock = open(state_path + ".lock", "a")
try:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    sys.exit(0)
if os.path.exists(state_path):
    with open(state_path) as stream:
        if json.load(stream).get("status") in ("done", "failed"):
            sys.exit(0)

def write_state(status, error=None, exit_code=None):
    fd, path = tempfile.mkstemp(dir=os.path.dirname(state_path))
    with os.fdopen(fd, "w") as stream:
        json.dump({"status": status, "pid": os.getpid(),
                   "error": error, "exit_code": exit_code}, stream)
    os.replace(path, state_path)

write_state("copying")
try:
    # The copy inherits the lock: killing this supervisor cannot allow a
    # second copy to write the destination while the first is still running.
    code = subprocess.call(command, shell=True, pass_fds=(lock.fileno(),))
    write_state("done" if code == 0 else "failed",
                None if code == 0 else "remote copy failed; see " + state_path + ".log",
                code)
except Exception as error:
    write_state("failed", str(error))
'''

LAUNCH_SCRIPT = r'''
import os, subprocess, sys
state_path, worker, command = sys.argv[1:]
os.makedirs(os.path.dirname(state_path), exist_ok=True)
with open(os.devnull, "rb") as source, open(state_path + ".log", "ab") as log:
    subprocess.Popen([sys.executable, "-c", worker, state_path, command],
                     stdin=source, stdout=log, stderr=log,
                     start_new_session=True, close_fds=True)
'''

READ_STATE_SCRIPT = r'''
import fcntl, json, os, sys
path = sys.argv[1]
try:
    with open(path) as stream:
        state = json.load(stream)
except FileNotFoundError:
    state = None
active = False
try:
    lock = open(path + ".lock", "r")
except FileNotFoundError:
    pass
else:
    with lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            active = True
if state is None and active:
    state = {"status": "copying"}
if isinstance(state, dict):
    state["active"] = active
    if len(sys.argv) > 2:
        try:
            with open(sys.argv[2]) as stream:
                progress = json.load(stream)
            state["progress"] = progress if isinstance(progress, dict) else None
        except (OSError, ValueError):
            state["progress"] = None
print(json.dumps(state))
'''


def launch_command(state_path, copy_command):
    """Redirect all descriptors and create a new session on the runner."""
    return "python3 -c " + " ".join(shlex.quote(value) for value in (
        LAUNCH_SCRIPT, state_path, COPY_SCRIPT, copy_command))


def read_state(ssh, state_path, progress_path=None):
    """Observe persisted state and the process lock without starting a copy."""
    args = [READ_STATE_SCRIPT, state_path]
    if progress_path is not None:
        args.append(progress_path)
    command = "python3 -c " + " ".join(shlex.quote(value) for value in args)
    out, err, code = ssh.exec(command, timeout=15)
    if code != 0:
        # Never confuse a failed probe with an absent process and relaunch.
        raise RuntimeError(f"Cannot read remote copy state: {err or out}")
    state = json.loads(out)
    if state is None:
        return None
    if not isinstance(state, dict) or state.get("status") not in (
            "copying", "done", "failed"):
        raise ValueError(f"Invalid remote copy state: {state_path}")
    return state
