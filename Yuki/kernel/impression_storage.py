"""Impression storage management for Yuki kernel.

This module provides the ImpressionStorage class for managing workflow operations
and status tracking for individual impressions across different execution runners.
"""
import datetime
import os
import json
import shutil
from CelebiChrono.utils.metadata import ConfigFile
from . import file_types
from . import remote_data_ops
from .vjob import VJob
from .vworkflow import VWorkflow
from .status_constants import (
    CODA, FAILED, DISSONANCE, IN_MOVEMENT, PRE_EXECUTION_STATUSES,
    translate_to_musical)

class ImpressionStorage:
    """Storage manager for impression workflow operations and status tracking."""
    def __init__(self, project_uuid, impression):
        # Imported lazily: Yuki.server.config pulls in the server package, whose
        # routes import this module — a module-level import here would create a
        # circular import (impression_storage -> server -> routes -> impression_storage).
        from ..server.config import config
        self.project_uuid = project_uuid
        self.impression = impression
        self.job_path = config.get_job_path(project_uuid, impression)

        # Load registry of runners
        config_file = config.get_config_file()
        self.runners = config_file.read_variable("runners", [])
        self.runners_id = config_file.read_variable("runners_id", {})
        self.backend_types = config_file.read_variable("backend_types", {})

        # Metadata access
        self.job_config = ConfigFile(config.get_job_config_path(project_uuid, impression))

    def _get_runner_contexts(self):
        """Generator to yield active job/workflow pairs across all machines."""
        for machine in self.runners:
            machine_id = self.runners_id.get(machine)
            job = VJob(self.job_path, machine_id)

            if job.workflow_id():
                # Using the factory method from our previous refactor
                workflow = VWorkflow.create(self.project_uuid, [], job.workflow_id())
                yield machine, job, workflow

    def kill(self):
        """Kills all workflows associated with this storage entry."""
        for _, _, workflow in self._get_runner_contexts():
            workflow.kill()
        # Mark local record as failed
        VJob(self.job_path, None).set_status("failed")

    @staticmethod
    def _merge_reports(reports):
        """Merge a list of per-kind collect reports into one report."""
        merged = {"collected": [], "skipped": [], "failed": []}
        for report in reports:
            if not report:
                continue
            for key, value in report.items():
                if key in merged:
                    merged[key].extend(value)
        return merged

    def collect(self):
        """Light default: plots + logs on success, logs on failure."""
        report = {}
        for name, job, workflow in self._get_runner_contexts():
            job_status = job.status(musical=True)
            runner_report = {}
            if job_status == CODA:
                print(f"[{name}] Collecting plots + logs...")
                runner_report = self._merge_reports([
                    workflow.download_selected(self.impression, file_types.is_plot, "stageout"),
                    workflow.download_logs(self.impression),
                ])
            elif job_status in (FAILED, DISSONANCE):
                print(f"[{name}] Collecting logs...")
                runner_report = workflow.download_logs(self.impression)
            report[name] = runner_report
        return report

    def collect_files(self, kind, spec):
        """Download a subset of <kind> files matching a selection spec."""
        predicate = file_types.make_predicate(spec)
        report = {}
        for name, job, workflow in self._get_runner_contexts():
            if job.status(musical=True) == CODA:
                print(f"[{name}] Collecting {kind} matching {spec!r}...")
                report[name] = workflow.download_selected(self.impression, predicate, kind)
            else:
                report[name] = {"collected": [], "skipped": [], "failed": []}
        return report

    def file_status(self, kind="stageout", detailed=False):  # pylint: disable=too-many-locals
        """Merge the saved runner listing with downloaded Storage state.

        The runner is never contacted here: the listing is read from
        <machine>/<kind>.filelist.json, which the status-update path
        (refresh_job_filelists, running in Celery) refreshes. See
        _runner_files for the read policy.

        Remote-hosted data (registered via register-ssh-data) is listed from
        the host runner's managed impressions dir; see _remote_hosted_files.

        With detailed=True returns {"files": [...], "notes": [...]} where each
        note is {"runner": <name or None>, "level": info|warning|error,
        "message": str} explaining e.g. a missing listing or a persisted
        refresh failure; otherwise returns the bare files list.
        """
        result = []
        notes = []
        hosted, hosted_note = self._remote_hosted_files(kind)
        result.extend(hosted)
        if hosted_note:
            notes.append(hosted_note)
        for name, job, _workflow in self._get_runner_contexts():
            machine_id = self.runners_id.get(name)
            machine_dir = os.path.join(self.job_path, machine_id)
            storage_dir = os.path.join(machine_dir, kind)
            downloaded = set()
            if os.path.isdir(storage_dir):
                for root, _dirs, files in os.walk(storage_dir):
                    for f in files:
                        downloaded.add(os.path.relpath(os.path.join(root, f), storage_dir))

            runner_files, note = self._runner_files(job, kind, machine_dir)
            if note:
                notes.append({"runner": name, **note})

            seen = set()
            for rf in runner_files:
                seen.add(rf["name"])
                result.append({
                    "name": rf["name"],
                    "size": rf.get("size", 0),
                    "type": file_types.classify(rf["name"]),
                    "in_runner": True,
                    "in_yuki": rf["name"] in downloaded,
                })
            for fn in sorted(downloaded - seen):
                full = os.path.join(storage_dir, fn)
                result.append({
                    "name": fn,
                    "size": os.path.getsize(full) if os.path.isfile(full) else 0,
                    "type": file_types.classify(fn),
                    "in_runner": False,
                    "in_yuki": True,
                })
        if detailed:
            return {"files": result, "notes": notes}
        return result

    def _remote_hosted_files(self, kind):  # pylint: disable=too-many-locals,too-many-branches
        """Return (rows, note) for a remote-hosted data impression
        (register-ssh-data).

        Files live in the host runner's managed impressions dir. The listing
        is cached to <host_runner_id>/<kind>.filelist.json (same convention as
        _runner_files) and merged with the Storage state, so rows report
        in_runner/in_yuki like any other impression. The note is None or
        {"level", "message"} for a cached listing or an unreachable host.
        """
        marker_path = os.path.join(self.job_path, "remote.json")
        if not os.path.exists(marker_path):
            return [], None
        marker = ConfigFile(marker_path)
        host_runner = marker.read_variable("host_runner_id", "")
        managed_path = marker.read_variable("remote_path", "")
        if not host_runner or not managed_path:
            return [], None

        machine_dir = os.path.join(self.job_path, host_runner)
        cache_path = os.path.join(machine_dir, kind + ".filelist.json")

        runner_files = None
        if os.path.isfile(cache_path):
            try:
                with open(cache_path, encoding="utf-8") as fh:
                    cached = json.load(fh)
                if cached.get("workflow_id") == "remote-data":
                    runner_files = cached.get("files", [])
            except (OSError, ValueError):
                pass

        note = None
        if runner_files is None:
            try:
                runner_files = remote_data_ops.list_managed_files(
                    host_runner, managed_path)
            except Exception as exc:
                runner_files = []
                note = {
                    "level": "error",
                    "message": (f"remote host unreachable "
                                f"[{type(exc).__name__}]: {exc}"),
                }
            if runner_files:
                try:
                    os.makedirs(machine_dir, exist_ok=True)
                    with open(cache_path, "w", encoding="utf-8") as fh:
                        json.dump({"workflow_id": "remote-data",
                                   "files": runner_files}, fh)
                except OSError:
                    pass
        else:
            note = {"level": "info", "message": "cached remote listing"}

        storage_dir = os.path.join(machine_dir, kind)
        downloaded = set()
        if os.path.isdir(storage_dir):
            for root, _dirs, files in os.walk(storage_dir):
                for f in files:
                    downloaded.add(os.path.relpath(
                        os.path.join(root, f), storage_dir))

        result = []
        for rf in runner_files:
            result.append({
                "name": rf["name"],
                "size": rf.get("size", 0),
                "type": file_types.classify(rf["name"]),
                "in_runner": True,
                "in_yuki": rf["name"] in downloaded,
            })
        return result, note

    def _runner_files(self, job, kind, machine_dir):
        """Return (files, note) for the saved runner listing of <kind>.

        The runner is never contacted here: the listing is read from
        <machine_dir>/<kind>.filelist.json, which the status-update path
        (refresh_job_filelists, running in Celery) refreshes. The file is
        keyed by the job's workflow id, so a re-run starts with an empty
        listing until its first refresh. A persisted refresh error is
        reported as a warning alongside the (stale) listing.

        The note is None or {"level": info|warning, "message": str}
        explaining the outcome: no listing yet, a saved listing, or a
        failed refresh.
        """
        workflow_id = job.workflow_id()
        cache_path = os.path.join(machine_dir, kind + ".filelist.json")
        try:
            with open(cache_path, encoding="utf-8") as fh:
                cached = json.load(fh)
            if cached.get("workflow_id") != workflow_id:
                return [], {"level": "info",
                            "message": f"no {kind} listing yet"}
            files = cached.get("files", [])
        except (OSError, ValueError):
            return [], {"level": "info", "message": f"no {kind} listing yet"}

        stamp = cached.get("stamp") or datetime.datetime.fromtimestamp(
            os.path.getmtime(cache_path)).strftime("%Y-%m-%d %H:%M")
        if cached.get("error"):
            note = {"level": "warning",
                    "message": (f"listing from {stamp}, "
                                f"refresh failed: {cached['error']}")}
        elif files:
            note = {"level": "info", "message": f"listing from {stamp}"}
        else:
            note = {"level": "info",
                    "message": f"no {kind} files on the runner "
                              f"(listing from {stamp})"}
        return files, note

    @staticmethod
    def _write_filelist(machine_dir, kind, workflow_id, files, error=None):
        """Atomically write a saved runner listing (tmp + rename).

        /file-status reads the file concurrently, so writes must be atomic.
        """
        payload = {
            "workflow_id": workflow_id,
            "files": files,
            "stamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        if error:
            payload["error"] = error
        os.makedirs(machine_dir, exist_ok=True)
        cache_path = os.path.join(machine_dir, kind + ".filelist.json")
        tmp_path = cache_path + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp_path, cache_path)
        except OSError:
            pass   # best-effort; status still works without it

    @staticmethod
    def _previous_files(machine_dir, kind, workflow_id):
        """Return the previous saved listing when it matches <workflow_id>."""
        try:
            with open(os.path.join(machine_dir, kind + ".filelist.json"),
                      encoding="utf-8") as fh:
                cached = json.load(fh)
        except (OSError, ValueError):
            return None
        if cached.get("workflow_id") != workflow_id:
            return None
        return cached.get("files")

    def refresh_filelists(self, workflow, pre_execution):  # pylint: disable=too-many-locals
        """Write the saved runner listings after a status update.

        Called from the status-update path (Celery), never from a request.
        For a pre-execution status the runner cannot hold files, so empty
        listings are written locally without contacting the runner.
        Otherwise the runner is listed live (stageout and logs); a failed
        listing keeps the previous files and records the error in the
        listing file so /file-status can report it.
        """
        machine_dir = None
        workflow_id = None
        for name, job, context in self._get_runner_contexts():
            if context.uuid == workflow.uuid:
                machine_dir = os.path.join(self.job_path, self.runners_id[name])
                workflow_id = job.workflow_id()
                break
        if machine_dir is None:
            return
        for kind in ("stageout", "logs"):
            files, error = [], None
            if not pre_execution:
                try:
                    files = workflow.list_runner_files(self.impression, kind)
                except Exception as exc:  # runner unreachable
                    error = f"{type(exc).__name__}: {exc}"
                    previous = self._previous_files(machine_dir, kind,
                                                    workflow_id)
                    files = previous if previous is not None else []
            self._write_filelist(machine_dir, kind, workflow_id, files, error)

    def force_refresh_filelists(self):
        """Re-list the runner live and rewrite the saved file listings.

        The status-update path skips terminal jobs (see
        refresh_job_filelists), so a finished job's listing freezes at its
        terminal stamp. Called from /refresh-filelists, this method lists
        stageout and logs on demand for every runner context. A failed
        listing keeps the previous files and records the error; an empty
        listing also keeps a previous listing of the same workflow (the
        workspace may be gone) and records a note.
        """
        report = {}
        for name, job, workflow in self._get_runner_contexts():
            machine_dir = os.path.join(self.job_path, self.runners_id[name])
            workflow_id = job.workflow_id()
            runner_report = {}
            for kind in ("stageout", "logs"):
                error = None
                files = []
                try:
                    files = workflow.list_runner_files(self.impression, kind)
                except Exception as exc:  # runner unreachable
                    error = f"{type(exc).__name__}: {exc}"
                    previous = self._previous_files(machine_dir, kind,
                                                    workflow_id)
                    files = previous if previous is not None else []
                if not error and not files:
                    previous = self._previous_files(machine_dir, kind,
                                                    workflow_id)
                    if previous:
                        files = previous
                        error = ("runner returned no files; keeping the "
                                 "previous listing")
                self._write_filelist(machine_dir, kind, workflow_id, files,
                                     error)
                runner_report[kind] = {"files": len(files), "error": error}
            report[name] = runner_report
        return report

    def collect_outputs(self):
        """Retrieves only output files from runners."""
        report = {}
        for name, job, workflow in self._get_runner_contexts():
            if job.status(musical=True) == CODA:
                print(f"[{name}] Collecting outputs...")
                report[name] = workflow.download_outputs(self.impression)
            else:
                report[name] = {"collected": [], "skipped": [], "failed": []}
        return report

    def collect_logs(self):
        """Retrieves only logs from runners.

        Logs are refreshed (existing local copies overwritten) whenever the
        job is executing or has reached a terminal state, so live log
        following sees growing remote logs.
        """
        report = {}
        for name, job, workflow in self._get_runner_contexts():
            job_status = job.status(musical=True)
            if job_status in (CODA, FAILED, DISSONANCE, IN_MOVEMENT):
                print(f"[{name}] Collecting logs...")
                report[name] = workflow.download_logs(self.impression, refresh=True)
            else:
                report[name] = {"collected": [], "skipped": [], "failed": []}

        self.collect_engine_logs()
        return report

    def collect_engine_logs(self):
        """Retrieves engine logs from runners."""
        for name, _job, workflow in self._get_runner_contexts():
            print(f"[{name}] Collecting engine logs...")
            workflow.get_workflow_logs()

    def watermark(self):
        """Applies watermarks to the stored results."""
        for name, job, workflow in self._get_runner_contexts():
            if job.status(musical=True) == CODA:
                print(f"[{name}] Applying watermarks...")
                workflow.watermark(self.impression)

    def get_info(self):
        """Returns the location and ID of the first active runner."""
        for name, _, workflow in self._get_runner_contexts():
            return f"{name} {workflow.uuid}"
        return "UNDEFINED"

    def purge_collected_data(self, force=False):
        """Remove locally collected data (stageout/logs/watermarks) for the impression.

        Refuses — returning {"refused": reason, "running": bool} — when a
        workflow is running or, without ``force``, when the collected data
        cannot be re-collected from a runner (purged workspace, missing
        runner files, unverifiable backend). Keeps the impression
        metadata, run configs, and saved file listings, and recomputes
        the distribution registry afterwards.
        """
        report = {"purged": True, "impression": self.impression,
                  "machines": {}, "freed_bytes": 0}
        planned = []
        for name, _job, workflow in self._get_runner_contexts():
            if translate_to_musical(workflow.status()) == IN_MOVEMENT:
                return {"refused": "workflow is running; kill it first",
                        "running": True}
            machine_dir = os.path.join(
                self.job_path, self.runners_id.get(name, ""))
            kinds = self._collected_kinds(machine_dir)
            if not kinds:
                continue
            refusal = self._purge_refusal(name, workflow, kinds)
            if refusal and not force:
                return {"refused": refusal, "running": False}
            planned.append((name, machine_dir, kinds))

        for name, machine_dir, kinds in planned:
            entry = {}
            for kind, files in kinds.items():
                entry[kind] = {"files": len(files),
                               "bytes": sum(os.path.getsize(path)
                                            for _rel, path in files)}
                report["freed_bytes"] += entry[kind]["bytes"]
                shutil.rmtree(os.path.join(machine_dir, kind),
                              ignore_errors=True)
            for marker in ("stageout.downloaded", "logs.downloaded"):
                marker_path = os.path.join(machine_dir, marker)
                if os.path.isfile(marker_path):
                    os.remove(marker_path)
            report["machines"][name] = entry

        if planned:
            self.update_distribution()
        return report

    def _collected_kinds(self, machine_dir):
        """Map kind -> [(relative_name, absolute_path)] for collected files."""
        kinds = {}
        for kind in ("stageout", "logs", "watermarks"):
            root = os.path.join(machine_dir, kind)
            if not os.path.isdir(root):
                continue
            files = []
            for dirpath, _dirs, fnames in os.walk(root):
                for fname in fnames:
                    full = os.path.join(dirpath, fname)
                    files.append((os.path.relpath(full, root), full))
            if files:
                kinds[kind] = files
        return kinds

    def _purge_refusal(self, name, workflow, kinds):
        """Reason the collected data must not be purged, or None.

        The data is safe to purge only when the runner is known to still
        hold a copy: an ssh runner whose live listing covers every local
        file. Backends Yuki cannot inspect (reana) and unreachable or
        incomplete listings refuse unless the caller passes force.
        """
        workflow_config = ConfigFile(
            os.path.join(workflow.path, "config.json"))
        if workflow_config.read_variable("workspace_purged_at", ""):
            return (f"runner '{name}' workspace was purged; the collected "
                    f"data is the only copy (pass force to delete anyway)")
        backend = workflow.backend_type()
        if backend != "ssh":
            return (f"cannot verify a runner copy for backend "
                    f"'{backend}' (pass force to purge anyway)")
        for kind in ("stageout", "logs"):
            files = kinds.get(kind)
            if not files:
                continue
            try:
                remote = {row["name"] for row in
                          workflow.list_runner_files(self.impression, kind)}
            except Exception as exc:  # pylint: disable=broad-exception-caught
                return (f"runner '{name}' unreachable "
                        f"({type(exc).__name__}: {exc}); "
                        f"pass force to purge anyway")
            missing = [rel for rel, _path in files if rel not in remote]
            if missing:
                return (f"runner '{name}' no longer holds "
                        f"{len(missing)} {kind} file(s); "
                        f"pass force to purge anyway")
        return None

    def update_distribution(self, overrides=None,  # pylint: disable=too-many-locals,too-many-branches,too-many-statements,too-many-arguments,too-many-positional-arguments
                            refresh_cache=False, cache_runner_id=None):
        """Refresh and persist the impression's distribution.json registry.

        The registry records, at impression granularity, where the data
        lives and how each copy got there:
        - produced_on: the runner that ran the producing workflow
        - 'yuki': origin 'collected' (union of local stageout dirs)
        - 'runner:<name>': a block with per-state entries:
          - 'workflow': data that only exists in the workflow's storage
            (origin 'produced'), recomputed on every refresh
          - 'cache': data in the runner's managed impressions cache
            (origin 'transferred', 'registered', or 'cached')
        Transferred entries are written by the transfer task; they are
        preserved here and block recomputation of that state.
        overrides: {location: state_entry}; for 'yuki' it replaces the
        yuki entry, for a runner it replaces the 'cache' state (used by
        the transfer task to record the destination).
        refresh_cache: additionally reconcile the cache state with the
        runners: ssh caches are live-checked (verified entries), reana
        jobs that requested caching and finished record an assumed
        entry (EOS is not inspectable from Yuki).
        cache_runner_id: the runner whose ssh cache is live-checked
        (the ssh check only runs when this is provided).
        """
        dist_path = os.path.join(self.job_path, "distribution.json")
        existing = {}
        if os.path.isfile(dist_path):
            try:
                with open(dist_path, encoding="utf-8") as fh:
                    existing = json.load(fh)
            except (OSError, ValueError):
                existing = {}

        # Keep only the states that must survive a refresh: transferred
        # yuki entries and transferred runner cache entries. Legacy flat
        # runner entries are migrated into state blocks.
        locations = {}
        for loc, value in existing.get("locations", {}).items():
            if "origin" in value:  # yuki entry or legacy flat runner entry
                if loc == "yuki":
                    if value.get("origin") == "transferred":
                        locations[loc] = value
                else:
                    state = "workflow" if value.get("origin") == "produced" \
                        else "cache"
                    locations[loc] = {state: value}
            elif loc != "yuki":
                cache = value.get("cache")
                if cache and cache.get("origin") in ("transferred", "cached"):
                    locations[loc] = {"cache": cache}
        produced_on = existing.get("produced_on")

        def make_entry(origin, files):
            return {
                "origin": origin,
                "files": len(files),
                "bytes": sum(f.get("size", 0) for f in files),
                "updated": datetime.datetime.now(
                    datetime.timezone.utc).isoformat(),
            }

        for name, job, _workflow in self._get_runner_contexts():
            if produced_on is None and job.workflow_id():
                produced_on = name
            machine_id = self.runners_id.get(name)
            machine_dir = os.path.join(self.job_path, machine_id)
            files, _note = self._runner_files(job, "stageout", machine_dir)
            if files:
                locations.setdefault(f"runner:{name}", {})["workflow"] = \
                    make_entry("produced", files)

            if refresh_cache:
                self._record_assumed_reana_cache(locations, name, job,
                                                 machine_id)

        if refresh_cache and cache_runner_id:
            self._refresh_ssh_cache(locations, cache_runner_id)

        # yuki: union of local stageout dirs across machines
        if "yuki" not in locations:
            yuki_files = []
            if os.path.isdir(self.job_path):
                for machine in sorted(os.listdir(self.job_path)):
                    root = os.path.join(self.job_path, machine, "stageout")
                    if not os.path.isdir(root):
                        continue
                    for dirpath, _dirs, filenames in os.walk(root):
                        for fname in filenames:
                            full = os.path.join(dirpath, fname)
                            yuki_files.append({
                                "name": os.path.relpath(full, root),
                                "size": os.path.getsize(full),
                            })
            if yuki_files:
                locations["yuki"] = make_entry("collected", yuki_files)

        # registered (remote-hosted data via register-ssh-data)
        marker = os.path.join(self.job_path, "remote.json")
        if os.path.exists(marker):
            marker_file = ConfigFile(marker)
            host_runner = marker_file.read_variable("host_runner_id", "")
            host_name = next(
                (n for n, mid in self.runners_id.items() if mid == host_runner),
                None)
            hosted, _note = self._remote_hosted_files("stageout")
            registered = [row for row in hosted if row.get("in_runner")]
            key = f"runner:{host_name}" if host_name else None
            if registered and key:
                block = locations.setdefault(key, {})
                if "cache" not in block:
                    block["cache"] = make_entry("registered", registered)

        if overrides:
            for loc, value in overrides.items():
                if loc == "yuki":
                    locations[loc] = value
                else:
                    locations.setdefault(loc, {})["cache"] = value

        dist = {"produced_on": produced_on, "locations": locations}
        new_content = json.dumps(dist, indent=2)
        try:
            with open(dist_path, encoding="utf-8") as fh:
                if fh.read() == new_content:
                    return dist
        except OSError:
            pass
        os.makedirs(self.job_path, exist_ok=True)
        tmp_path = dist_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(new_content)
        os.replace(tmp_path, dist_path)
        return dist

    @staticmethod
    def _cache_updated_entry(origin, files, verified):
        """Build a cache entry with a UTC timestamp."""
        entry = {
            "origin": origin,
            "verified": verified,
            "files": len(files) if files is not None else None,
            "bytes": sum(f.get("size", 0) for f in files)
            if files is not None else None,
            "updated": datetime.datetime.now(
                datetime.timezone.utc).isoformat(),
        }
        return entry

    def _record_assumed_reana_cache(self, locations, name, job, machine_id):
        """Record an unverifiable cache entry for a reana job.

        The reana cache lives on EOS, which Yuki cannot inspect; a job
        that requested caching and finished is assumed to have its
        stageout cached by the workflow's own cache rule.
        """
        if self.backend_types.get(machine_id, "reana") != "reana":
            return
        if not job.cache_on_runner():
            return
        if job.status(musical=True) != CODA:
            return
        block = locations.setdefault(f"runner:{name}", {})
        if block.get("cache", {}).get("origin") == "transferred":
            return
        block["cache"] = self._cache_updated_entry("cached", None, False)

    def _refresh_ssh_cache(self, locations, cache_runner_id):
        """Live-check an ssh runner's impressions cache for this impression.

        Files present -> a verified 'cached' entry; empty/missing -> any
        stale 'cached' entry is dropped. Transferred entries and
        registered (remote.json) impressions are never touched.
        """
        print(f"[_refresh_ssh_cache] impression={self.impression} "
              f"cache_runner_id={cache_runner_id} "
              f"backend={self.backend_types.get(cache_runner_id, 'reana')}")
        if self.backend_types.get(cache_runner_id, "reana") != "ssh":
            print("[_refresh_ssh_cache] early-return: runner is not ssh")
            return
        remote_marker = os.path.join(self.job_path, "remote.json")
        if os.path.exists(remote_marker):
            print(f"[_refresh_ssh_cache] early-return: remote.json marker exists "
                  f"({remote_marker})")
            return
        name = next((n for n, rid in self.runners_id.items()
                     if rid == cache_runner_id), None)
        if not name:
            print("[_refresh_ssh_cache] early-return: no runner name for id")
            return
        print(f"[_refresh_ssh_cache] live-checking runner={name} "
              f"project={self.project_uuid} impression={self.impression}")
        try:
            files = remote_data_ops.list_cache_files(
                cache_runner_id, self.project_uuid, self.impression)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"[_refresh_ssh_cache] list_cache_files raised: {exc}")
            return  # runner unreachable: keep the current registry
        print(f"[_refresh_ssh_cache] found {len(files)} cached files")
        block = locations.setdefault(f"runner:{name}", {})
        existing = block.get("cache", {})
        print(f"[_refresh_ssh_cache] existing cache entry origin={existing.get('origin')}")
        if existing.get("origin") == "transferred":
            print("[_refresh_ssh_cache] preserving existing 'transferred' entry")
            return
        if not files:
            if existing.get("origin") == "cached":
                print("[_refresh_ssh_cache] dropping stale 'cached' entry")
                del block["cache"]
            else:
                print("[_refresh_ssh_cache] no files and no existing cached entry")
            return
        block["cache"] = self._cache_updated_entry("cached", files, True)
        print(f"[_refresh_ssh_cache] wrote 'cached' entry: "
              f"files={len(files)} bytes={sum(f.get('size', 0) for f in files)}")


def refresh_workflow_distributions(project_uuid, workflow, workflow_status):
    """Refresh every job's data-status registry once the workflow ends.

    Callers pass the workflow status they just determined: relying on
    workflow.status() here could read a stale in-memory consult cache.
    Runs only for a terminal status, and is strictly best-effort — a
    failing refresh must never fail the status update.
    """
    print(
        f"[refresh_workflow_distributions] caller=_record_terminal_distributions "
        f"reason=terminal_workflow_refresh "
        f"project={project_uuid} workflow={workflow.uuid} "
        f"status={workflow_status} total_jobs={len(workflow.jobs)}"
    )
    if translate_to_musical(workflow_status) not in (CODA, FAILED):
        print("[refresh_workflow_distributions] skipping: status is not terminal")
        return
    processed = 0
    skipped = 0
    for job in workflow.jobs:
        if job.job_type() == "algorithm":
            print(f"[refresh_workflow_distributions] skip job={job.uuid} reason=algorithm")
            skipped += 1
            continue
        if getattr(job, "is_input", False):
            print(f"[refresh_workflow_distributions] skip job={job.uuid} reason=input_job")
            skipped += 1
            continue
        short_uuid = job.short_uuid() if hasattr(job, "short_uuid") else job.uuid[:7]
        print(
            f"[refresh_workflow_distributions] process job={job.uuid} "
            f"short={short_uuid} "
            f"is_input={getattr(job, 'is_input', False)}"
        )
        processed += 1
        try:
            ImpressionStorage(project_uuid, job.uuid).update_distribution(
                refresh_cache=True, cache_runner_id=workflow.machine_id)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"[refresh_workflow_distributions] error for job={job.uuid}: {exc}")
    print(
        f"[refresh_workflow_distributions] done: processed={processed} skipped={skipped}"
    )


def refresh_job_filelists(project_uuid, workflow, workflow_status,
                          terminal_transition=False):
    """Refresh every job's saved runner listing after a status update.

    /file-status reads the saved <machine>/<kind>.filelist.json files, so
    this function — called from the status-update path, which runs in
    Celery — is the only place that lists the runner. Pre-execution
    statuses write empty listings locally; in-movement and terminal
    statuses fetch live listings. Strictly best-effort: a failing refresh
    must never fail the status update.
    """
    pre_execution = translate_to_musical(workflow_status) \
        in PRE_EXECUTION_STATUSES
    for job in workflow.jobs:
        if job.job_type() == "algorithm":
            continue
        if getattr(job, "is_input", False):
            continue
        if not terminal_transition and \
                translate_to_musical(job.status()) in (CODA, FAILED):
            continue
        try:
            ImpressionStorage(project_uuid, job.uuid).refresh_filelists(
                workflow, pre_execution)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
