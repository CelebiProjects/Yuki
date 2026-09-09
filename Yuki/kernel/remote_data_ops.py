"""Remote-side data operations for register-ssh-data.

Helpers that build shell commands executed ON an ssh runner, plus the
local Yuki-side storage paths for registration job state.
"""
import datetime
import json
import os
import shlex
import shutil
import tempfile

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.status_constants import CODA
from . import liveness

REMOTE_MD5_SCRIPT = r'''
import hashlib, json, os, sys

def md5sum(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()

def list_files(root):
    result = []
    for cur, dirs, files in os.walk(root):
        dirs[:] = sorted(d for d in dirs if not d.startswith("."))
        result.extend(
            os.path.join(cur, name)
            for name in sorted(f for f in files if not f.startswith(".")))
    return result

def dir_md5(root, progress_path=None):
    files = list_files(root)
    total = sum(os.path.getsize(path) for path in files)
    if progress_path:
        os.makedirs(os.path.dirname(progress_path), exist_ok=True)
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump({"stage": "hashing", "bytes_done": 0,
                       "bytes_total": total}, f)
    hasher = hashlib.md5()
    done = 0
    for path in files:
        hasher.update(md5sum(path).encode("utf-8"))
        done += os.path.getsize(path)
        if progress_path:
            with open(progress_path, "w", encoding="utf-8") as f:
                json.dump({"stage": "hashing", "bytes_done": done,
                           "bytes_total": total}, f)
    return hasher.hexdigest()

if len(sys.argv) >= 3:
    print(dir_md5(sys.argv[1], sys.argv[2]))
else:
    print(dir_md5(sys.argv[1]))
'''


def _yuki_dir():
    """Yuki data root ($YUKIDIR or ~/.Yuki)."""
    return os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))


def remote_md5_command(remote_path, progress_path=None):
    """SSH command computing the dir md5 on the remote host.

    With progress_path, the script also writes cumulative byte progress
    there ({"stage": "hashing", "bytes_done": n, "bytes_total": t}).
    """
    args = [REMOTE_MD5_SCRIPT, remote_path]
    if progress_path:
        args.append(progress_path)
    return "python3 -c " + " ".join(shlex.quote(a) for a in args)


def build_remote_fast_copy_command(src, dst, progress_path=None):
    """Copy src into dst on the remote host, fastest mechanism first.

    Mirrors yuki_create_data.fast_copy_tree: reflink -> hardlink ->
    rsync -> plain copy. The copied data is then made read-only
    (write-once cache); find handles empty dirs where a glob would fail.

    With progress_path, the copy runs backgrounded under a watcher that
    writes dst's byte count into the progress file every 3s (stage
    "copying", bytes_total read from the file left by the md5 stage).
    The chain's exit code is preserved and the progress file removed.
    """
    chain = (
        f"(cp -a --reflink=auto {shlex.quote(src)}/. {shlex.quote(dst)}/ || "
        f"cp -al {shlex.quote(src)}/. {shlex.quote(dst)}/ || "
        f"rsync -a {shlex.quote(src)}/ {shlex.quote(dst)}/ || "
        f"cp -r {shlex.quote(src)}/. {shlex.quote(dst)}/)"
    )
    chmod_ro = (f"find {shlex.quote(dst)} -mindepth 1 -maxdepth 1 "
                f"-exec chmod -R a-w -- {{}} +")
    if not progress_path:
        return f"mkdir -p {shlex.quote(dst)} && {chain} && {chmod_ro}"
    progress_reader = (
        "python3 -c 'import json,sys;"
        "print(json.load(open(sys.argv[1]))[\"bytes_total\"])'"
    )
    return (
        f"mkdir -p {shlex.quote(dst)} && "
        f"{chain} && {chmod_ro} & "
        f"_pid=$!; "
        f"_total=$({progress_reader} {shlex.quote(progress_path)}); "
        f"while kill -0 $_pid 2>/dev/null; do "
        f"_done=$(du -sb {shlex.quote(dst)} 2>/dev/null | cut -f1); "
        f"_done=${{_done:-0}}; "
        f'printf \'{{"stage": "copying", "bytes_done": %s, "bytes_total": %s}}\' '
        f'"$_done" "$_total" > {shlex.quote(progress_path)}; '
        f"sleep 3; "
        f"done; "
        f"wait $_pid; "
        f"_code=$?; "
        f"rm -f {shlex.quote(progress_path)}; "
        f"exit $_code"
    )


JOBS_DIR_NAME = "register-jobs"


def _jobs_dir(yuki_dir):
    return os.path.join(yuki_dir, JOBS_DIR_NAME)


def write_job_state(yuki_dir, job_id, state, jobs_dir_name=JOBS_DIR_NAME):
    """Persist a job's state to $YUKIDIR/<jobs_dir_name>/<id>.json."""
    jobs_dir = os.path.join(yuki_dir, jobs_dir_name)
    os.makedirs(jobs_dir, exist_ok=True)
    with open(os.path.join(jobs_dir, f"{job_id}.json"),
              "w", encoding="utf-8") as f:
        json.dump(state, f)


def read_job_state(yuki_dir, job_id, jobs_dir_name=JOBS_DIR_NAME):
    """Read a job's state, or None."""
    path = os.path.join(yuki_dir, jobs_dir_name, f"{job_id}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    return data


def _impression_md5(imp_dir):
    from CelebiChrono.utils import metadata
    yaml_file = metadata.YamlFile(os.path.join(imp_dir, "contents", "celebi.yaml"))
    return yaml_file.read_variable("uuid", "")


def find_existing_registration(yuki_dir, runner_id, remote_path):
    """Return the completed registration for (runner, path), else None."""
    storage = os.path.join(yuki_dir, "Storage")
    if not os.path.isdir(storage):
        return None
    for proj in os.listdir(storage):
        proj_dir = os.path.join(storage, proj)
        if not os.path.isdir(proj_dir):
            continue
        for imp in os.listdir(proj_dir):
            marker_path = os.path.join(proj_dir, imp, "remote.json")
            if not os.path.exists(marker_path):
                continue
            marker = ConfigFile(marker_path)
            if marker.read_variable("host_runner_id", "") == runner_id and \
                    marker.read_variable("source_path", "") == remote_path:
                imp_dir = os.path.join(proj_dir, imp)
                # Failed registrations can be re-registered (synthesis
                # overwrites the record); running ones have their copy in
                # flight, so re-registration re-routes through the live job.
                status = ConfigFile(
                    os.path.join(imp_dir, "status.json")
                ).read_variable("status", "")
                if status in ("failed", "running"):
                    continue
                from CelebiChrono.utils import metadata
                yaml_file = metadata.YamlFile(
                    os.path.join(imp_dir, "contents", "celebi.yaml"))
                return {"result": {
                    "uuid": _impression_md5(imp_dir),
                    "impression_uuid": imp,
                    "descriptor": yaml_file.read_variable("descriptor", ""),
                }}
    return None


def find_inflight_job(yuki_dir, runner_id, remote_path):
    """Return the job id of an in-flight registration for (runner, path)."""
    jobs_dir = _jobs_dir(yuki_dir)
    if not os.path.isdir(jobs_dir):
        return None
    for name in os.listdir(jobs_dir):
        if not name.endswith(".json"):
            continue
        state = read_job_state(yuki_dir, name[:-5])
        if state is None:
            continue
        if state.get("runner_id") == runner_id and \
                state.get("remote_path") == remote_path and \
                state.get("status") not in ("done", "failed"):
            return name[:-5]
    return None


def find_job_by_impression(yuki_dir, impression_uuid):
    """Return (job_id, state) for the job whose result references the
    impression, or None."""
    jobs_dir = _jobs_dir(yuki_dir)
    if not os.path.isdir(jobs_dir):
        return None
    for name in sorted(os.listdir(jobs_dir), reverse=True):
        if not name.endswith(".json"):
            continue
        job_id = name[:-5]
        state = read_job_state(yuki_dir, job_id)
        if state is None:
            continue
        result = state.get("result") or {}
        if result.get("impression_uuid") == impression_uuid:
            return job_id, state
    return None


def _ssh_settings(runner_id, yuki_dir=None):
    """Merged ssh settings for a runner (yuki_dir injects the config root)."""
    from Yuki.kernel import runner_config
    config_file = runner_config.open_config()
    if yuki_dir:
        config_file = ConfigFile(os.path.join(yuki_dir, "config.json"))
    return runner_config.get_ssh_settings(config_file, runner_id)


def _ssh_connection(runner_id):
    """An _SshConnection for the runner (paramiko import stays lazy)."""
    from Yuki.kernel.ssh_workflow import _SshConnection
    settings = _ssh_settings(runner_id)
    return _SshConnection(host=settings.get("host", ""),
                          user=settings.get("user", ""),
                          key_path=settings.get("key_path"),
                          port=settings.get("port", 22))


def list_managed_files(runner_id, managed_path):
    """List files in a runner's managed impressions dir via SSH.

    Returns [{"name": rel_path, "size": bytes}, ...] (flat, sorted by
    traversal order).
    """
    print(f"[list_managed_files] runner_id={runner_id} path={managed_path}")
    with _ssh_connection(runner_id) as ssh:
        files = [{"name": rel, "size": size}
                 for rel, _rpath, size in ssh.walk_files(managed_path)]
    print(f"[list_managed_files] found {len(files)} files")
    return files


def list_cache_files(runner_id, project_uuid, impression):
    """List files in an ssh runner's impressions cache for an impression."""
    settings = _ssh_settings(runner_id)
    cache_dir = (f"{settings.get('remote_workdir', '/tmp/yuki-workflows')}"
                 f"/impressions/{project_uuid}/{impression}")
    print(f"[list_cache_files] runner_id={runner_id} cache_dir={cache_dir}")
    return list_managed_files(runner_id, cache_dir)


def _runner_name(runner_id, yuki_dir=None):
    """Runner name for a runner id (fallback: the id itself)."""
    from Yuki.kernel import runner_config
    cfg = runner_config.open_config()
    if yuki_dir:
        cfg = ConfigFile(os.path.join(yuki_dir, "config.json"))
    runners_id = cfg.read_variable("runners_id", {})
    for name, rid in runners_id.items():
        if rid == runner_id:
            return name
    return runner_id


def progress_file_path(runner_id, job_id):
    """Progress file path on the runner for a registration job."""
    settings = _ssh_settings(runner_id)
    remote_workdir = settings.get("remote_workdir", "/tmp/yuki-workflows")
    return f"{remote_workdir}/register-progress/{job_id}.json"


def read_remote_progress(runner_id, job_id):
    """Read the runner-side progress file, or None on any failure."""
    try:
        with _ssh_connection(runner_id) as ssh:
            out, _err, code = ssh.exec(
                f"cat {shlex.quote(progress_file_path(runner_id, job_id))}",
                timeout=15)
        if code != 0:
            return None
        data = json.loads(out)
        if not isinstance(data, dict):
            return None
        return data
    except Exception:  # pylint: disable=broad-exception-caught
        return None


def remove_remote_progress_file(runner_id, job_id):
    """Best-effort removal of a job's progress file on the runner."""
    try:
        with _ssh_connection(runner_id) as ssh:
            ssh.exec(
                f"rm -f {shlex.quote(progress_file_path(runner_id, job_id))}",
                timeout=15)
    except Exception:  # pylint: disable=broad-exception-caught
        pass


def purge_runner_cache(runner_id, project=None, impression=None,  # pylint: disable=too-many-locals,too-many-branches,too-many-arguments,too-many-positional-arguments
                       dry_run=False, echo=None, yuki_dir=None,
                       superseded=False):
    """Evict cache entries from an ssh runner's impressions cache.

    With superseded=True, only cache entries whose impressions are
    explicitly marked superseded in the project's live set are selected
    (project/impression filters must not be set). Deletes matching
    ``<remote_workdir>/impressions/<project>/<impression>`` directories
    (chmod'd writable first; cached data is stored read-only) and clears
    the local bookkeeping that pointed at them.

    Impressions whose registration is still running are skipped.

    Returns {"purged": [...], "skipped": [...], "dry_run": bool}.
    """
    echo = echo or print
    yuki_dir = yuki_dir or _yuki_dir()
    settings = _ssh_settings(runner_id, yuki_dir)
    cache_root = (f"{settings.get('remote_workdir', '/tmp/yuki-workflows')}"
                  f"/impressions")

    purged, skipped = [], []
    with _ssh_connection(runner_id) as ssh:
        for proj in ssh.listdir(cache_root):
            if project and proj != project:
                continue
            proj_dir = f"{cache_root}/{proj}"
            for imp in ssh.listdir(proj_dir):
                if impression and imp != impression:
                    continue
                remote_dir = f"{proj_dir}/{imp}"
                imp_local = os.path.join(yuki_dir, "Storage", proj, imp)
                if superseded:
                    live = liveness.impression_live(
                        proj, imp, yuki_dir)
                    if live is not False:
                        continue
                status_file = os.path.join(imp_local, "status.json")
                if os.path.isfile(status_file):
                    status = ConfigFile(status_file).read_variable("status", "")
                    if status == "running":
                        skipped.append({
                            "project": proj, "impression": imp,
                            "reason": "registration still running"})
                        echo(f"[SKIP] {remote_dir} — registration still running")
                        continue
                kind = "cache"
                if os.path.isfile(os.path.join(imp_local, "remote.json")):
                    kind = "registered"
                echo(f"{'Would purge' if dry_run else 'Purging'} "
                     f"{remote_dir} ({kind})")
                if not dry_run:
                    cmd = (f"chmod -R u+w -- {shlex.quote(remote_dir)} "
                           f"2>/dev/null || true && "
                           f"rm -rf {shlex.quote(remote_dir)}")
                    _out, _err, code = ssh.exec(cmd, timeout=3600)
                    if code != 0:
                        skipped.append({
                            "project": proj, "impression": imp,
                            "reason": "remote delete failed"})
                        continue
                    _clear_local_markers(imp_local, runner_id, yuki_dir)
                purged.append({"project": proj, "impression": imp,
                               "kind": kind, "remote_dir": remote_dir})
    return {"purged": purged, "skipped": skipped, "dry_run": dry_run}


def _clear_local_markers(imp_dir, runner_id, yuki_dir=None):
    """Drop the impression's registration markers and the purged runner's
    distribution cache entry."""
    for name in ("remote.json", "status.json"):
        path = os.path.join(imp_dir, name)
        if os.path.isfile(path):
            os.remove(path)
    dist_path = os.path.join(imp_dir, "distribution.json")
    if not os.path.isfile(dist_path):
        return
    with open(dist_path, encoding="utf-8") as fh:
        dist = json.load(fh)
    locations = dist.get("locations", {})
    key = f"runner:{_runner_name(runner_id, yuki_dir)}"
    if key in locations:
        locations[key].pop("cache", None)
        if not locations[key]:
            del locations[key]
        with open(dist_path, "w", encoding="utf-8") as fh:
            json.dump(dist, fh, indent=2)


def cache_results_job(runner_id, project_uuid, impression,  # pylint: disable=too-many-arguments,too-many-positional-arguments
                      update, yuki_dir=None):
    """Cache the impression's workflow stageout on its runner.

    Manual version of the workflow's own cache rule: fast-copies
    <remote_workdir>/workflows/<project>/<workflow>/imp<short>/stageout
    into the runner's managed impressions cache (read-only) and records
    a 'transferred' cache entry in distribution.json.

    update(state) is called with each progress state; the caller owns
    the job-state file.
    """
    yuki_dir = yuki_dir or _yuki_dir()
    imp_dir = os.path.join(yuki_dir, "Storage", project_uuid, impression)
    from Yuki.kernel.vjob import VJob
    job = VJob(imp_dir, runner_id)
    workflow_id = job.workflow_id()
    if not workflow_id:
        result = {"cached": 0, "reason": "no workflow on this runner"}
        update({"status": "done", "result": result, "error": None})
        return result

    status = job.status(musical=True)
    if status != CODA:
        result = {"cached": 0,
                  "reason": f"job status is {status} — nothing to cache"}
        update({"status": "done", "result": result, "error": None})
        return result

    settings = _ssh_settings(runner_id, yuki_dir)
    base = settings.get("remote_workdir", "/tmp/yuki-workflows")
    src = (f"{base}/workflows/{project_uuid}/{workflow_id}"
           f"/imp{job.short_uuid()}/stageout")
    cache_dir = f"{base}/impressions/{project_uuid}/{impression}"

    update({"status": "copying", "result": None, "error": None})
    with _ssh_connection(runner_id) as ssh:
        if not ssh.exists(src):
            result = {"cached": 0, "reason": "no stageout on the runner"}
            update({"status": "done", "result": result, "error": None})
            return result
        out, err, code = ssh.exec(
            build_remote_fast_copy_command(src, cache_dir), timeout=10800)
        if code != 0:
            raise RuntimeError(f"remote copy failed: {err or out}")
        files = [{"name": rel, "size": size}
                 for rel, _rpath, size in ssh.walk_files(cache_dir)]
    _record_cache_distribution(imp_dir, runner_id, files, yuki_dir)
    result = {"cached": len(files),
              "bytes": sum(f.get("size", 0) for f in files)}
    update({"status": "done", "result": result, "error": None})
    return result


def _record_cache_distribution(imp_dir, runner_id, files, yuki_dir=None):
    """Record the cached copy in distribution.json (origin 'transferred')."""
    dist_path = os.path.join(imp_dir, "distribution.json")
    dist = {}
    if os.path.isfile(dist_path):
        try:
            with open(dist_path, encoding="utf-8") as fh:
                dist = json.load(fh)
        except (OSError, ValueError):
            dist = {}
    entry = {
        "origin": "transferred",
        "files": len(files),
        "bytes": sum(f.get("size", 0) for f in files),
        "updated": datetime.datetime.now(
            datetime.timezone.utc).isoformat(),
    }
    locations = dist.setdefault("locations", {})
    key = f"runner:{_runner_name(runner_id, yuki_dir)}"
    locations.setdefault(key, {})["cache"] = entry
    os.makedirs(imp_dir, exist_ok=True)
    with open(dist_path, "w", encoding="utf-8") as fh:
        json.dump(dist, fh, indent=2)


def verify_registered_data(project_uuid, impression_uuid):  # pylint: disable=too-many-locals
    """Recompute the data md5 and compare it with the registered uuid.

    Remote-hosted data is hashed on the host runner's managed dir; local
    data is hashed from Storage/<project>/<impression>/rawdata/.
    Returns {"match", "expected", "actual", "location", "error"}.
    """
    yuki_dir = _yuki_dir()
    imp_dir = os.path.join(yuki_dir, "Storage", project_uuid, impression_uuid)
    expected = _impression_md5(imp_dir)
    if not expected:
        return {"match": False, "expected": "", "actual": "",
                "location": "", "error": "no uuid registered for impression"}

    marker_path = os.path.join(imp_dir, "remote.json")
    if os.path.exists(marker_path):
        marker = ConfigFile(marker_path)
        host_runner = marker.read_variable("host_runner_id", "")
        managed_path = marker.read_variable("remote_path", "")
        # SSH connections to the runner flake occasionally (banner timeouts);
        # retry once, then report a proper error instead of a 500.
        last_exc = None
        for _attempt in range(2):
            try:
                with _ssh_connection(host_runner) as ssh:
                    out, err, code = ssh.exec(remote_md5_command(managed_path),
                                              timeout=3600)
                last_exc = None
                break
            except Exception as exc:  # pylint: disable=broad-exception-caught
                last_exc = exc
        if last_exc is not None:
            return {"match": False, "expected": expected, "actual": "",
                    "location": f"runner {_runner_name(host_runner)}",
                    "error": f"ssh verify failed: "
                             f"{str(last_exc) or type(last_exc).__name__}"}
        if code != 0:
            return {"match": False, "expected": expected, "actual": "",
                    "location": "", "error": f"remote md5 failed: {err or out}"}
        actual = out.strip()
        return {"match": actual == expected, "expected": expected,
                "actual": actual, "location": f"runner {_runner_name(host_runner)}",
                "error": None}

    data_dir = os.path.join(imp_dir, "rawdata")
    if not os.path.isdir(data_dir):
        return {"match": False, "expected": expected, "actual": "",
                "location": "yuki storage",
                "error": f"no local data directory: {data_dir}"}
    from CelebiChrono.utils.file_utils import dir_md5
    actual = dir_md5(data_dir)
    return {"match": actual == expected, "expected": expected,
            "actual": actual, "location": "yuki storage", "error": None}


def synthesize_impression(project_uuid, impression_uuid, data_md5, descriptor,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
                          runner_id, source_path, managed_dir,
                          status="running"):
    """Create the impression record in Yuki Storage (data stays remote).

    Replicates yuki_create_data's layout (contents/ + config.json +
    status.json) and adds remote.json marking the hosting runner.
    The status is "running" while the data copy is in flight; the caller
    flips it to "archived" (or "failed") when the copy settles.
    """
    from CelebiChrono.utils import metadata
    from Yuki.cli.yuki_create_data import (
        create_canonical_rawdata_task, build_impression_config,
    )

    impression_dir = os.path.join(_yuki_dir(), "Storage",
                                  project_uuid, impression_uuid)
    os.makedirs(impression_dir, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="yuki_register_") as tmp:
        create_canonical_rawdata_task(tmp, descriptor, data_md5)
        contents_dir = os.path.join(impression_dir, "contents")
        if os.path.exists(contents_dir):
            shutil.rmtree(contents_dir)
        shutil.copytree(tmp, contents_dir)
        impression_config = build_impression_config(
            project_uuid, impression_uuid, tmp)

    config_file = metadata.ConfigFile(os.path.join(impression_dir, "config.json"))
    for key, value in impression_config.items():
        config_file.write_variable(key, value)

    set_impression_status(project_uuid, impression_uuid, status)

    remote_file = metadata.ConfigFile(os.path.join(impression_dir, "remote.json"))
    remote_file.write_variable("host_runner_id", runner_id)
    remote_file.write_variable("source_path", source_path)
    remote_file.write_variable("remote_path", managed_dir)


def set_impression_status(project_uuid, impression_uuid, status):
    """Write the impression's status.json."""
    from CelebiChrono.utils import metadata
    status_file = metadata.ConfigFile(os.path.join(
        _yuki_dir(), "Storage", project_uuid, impression_uuid, "status.json"))
    status_file.write_variable("status", status)


def register_remote_data_job(job_id, runner_id, remote_path, project_uuid,  # pylint: disable=too-many-locals,too-many-arguments,too-many-positional-arguments
                             descriptor, update):
    """Hash phase of a registration: hash -> synthesize -> dispatch copy.

    Ends with the job state "copying" and the registration result; the
    copy runs in task_copy_remote_data.

    ``update(state: dict)`` is called with each progress state.
    """
    from CelebiChrono.kernel.vimpression import VImpression
    from Yuki.cli.yuki_create_data import create_canonical_rawdata_task

    if not hasattr(VImpression, "generate_imp_uuid"):
        raise RuntimeError(
            "installed CelebiChrono is too old (missing "
            "VImpression.generate_imp_uuid); upgrade celebichrono or mount "
            "a local checkout via CELEBI_DIR")

    progress_path = progress_file_path(runner_id, job_id)
    update({"status": "hashing", "result": None, "error": None})
    with _ssh_connection(runner_id) as ssh:
        out, err, code = ssh.exec(remote_md5_command(remote_path, progress_path),
                                  timeout=3600)
        if code != 0:
            raise RuntimeError(f"remote md5 failed: {err or out}")
        data_md5 = out.strip()
        if not data_md5:
            raise RuntimeError("remote md5 returned empty result")

        existing = find_existing_registration(_yuki_dir(), runner_id,
                                              remote_path)
        if existing and existing["result"]["uuid"] == data_md5:
            # Unchanged data: reuse the archived registration (the managed
            # copy is still valid). No synthesis, no copy dispatch.
            result = existing["result"]
            update({"status": "done", "result": result, "error": None})
            return result

        with tempfile.TemporaryDirectory(prefix="yuki_register_") as tmp:
            create_canonical_rawdata_task(tmp, descriptor, data_md5)
            # Skip VImpression.__init__: it requires a Celebi project
            # context (csys.project_path()), which the celery worker does
            # not have. generate_imp_uuid uses no instance state.
            impression_uuid = VImpression.__new__(VImpression).generate_imp_uuid(
                project_uuid, tmp, [])

        settings = _ssh_settings(runner_id)
        managed_dir = (f"{settings.get('remote_workdir', '/tmp/yuki-workflows')}"
                       f"/impressions/{project_uuid}/{impression_uuid}")

        # The impression exists from here on: "running" while the copy is
        # in flight, so status queries gate downstream submits correctly.
        synthesize_impression(project_uuid, impression_uuid, data_md5,
                              descriptor, runner_id, remote_path, managed_dir,
                              status="running")

        result = {"uuid": data_md5, "impression_uuid": impression_uuid,
                  "descriptor": descriptor}
        update({"status": "copying", "result": result, "error": None})

    return result


def copy_remote_data_job(job_id, impression_uuid, project_uuid, runner_id,
                         remote_path):
    """Copy phase of a registration: copy data into the managed dir.

    Ends with the impression archived (the caller records job "done");
    on failure the impression is marked failed and the error raised.
    """
    settings = _ssh_settings(runner_id)
    managed_dir = (f"{settings.get('remote_workdir', '/tmp/yuki-workflows')}"
                   f"/impressions/{project_uuid}/{impression_uuid}")
    progress_path = progress_file_path(runner_id, job_id)
    with _ssh_connection(runner_id) as ssh:
        out, err, code = ssh.exec(
            build_remote_fast_copy_command(remote_path, managed_dir,
                                           progress_path),
            timeout=10800)
    if code != 0:
        set_impression_status(project_uuid, impression_uuid, "failed")
        raise RuntimeError(f"remote copy failed: {err or out}")
    set_impression_status(project_uuid, impression_uuid, "archived")
    imp_dir = os.path.join(_yuki_dir(), "Storage", project_uuid,
                           impression_uuid)
    from CelebiChrono.utils import metadata
    yaml_file = metadata.YamlFile(
        os.path.join(imp_dir, "contents", "celebi.yaml"))
    return {"uuid": _impression_md5(imp_dir),
            "impression_uuid": impression_uuid,
            "descriptor": yaml_file.read_variable("descriptor", "")}
