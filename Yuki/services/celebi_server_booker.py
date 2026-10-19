"""Pack small collected results and relay scoped uploads to celebi-server."""
import gzip
import hashlib
import json
import re
import tarfile
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import requests

IMP = re.compile(r"^[0-9a-f]{32}$")
PROJECT = re.compile(r"^(?:[0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$")
IMAGE = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".pdf", ".eps"}
TEXT = {".txt", ".md", ".json", ".yaml", ".yml", ".csv", ".py", ".c",
        ".cpp", ".h", ".sh", ".ini", ".toml", ".xml"}
DEFAULT_MAX_FILE_SIZE = 5 * 1024 * 1024


def _safe_relative(name):
    return (isinstance(name, str) and bool(name) and not Path(name).is_absolute()
            and ".." not in Path(name).parts and "\\" not in name
            and not any(ord(char) < 32 for char in name))


def _finished_jobs(storage):
    """Read cheap local status before constructing jobs or any workflow."""
    from Yuki.kernel.jobs.base import VJob
    from Yuki.kernel.execution.status import CODA, translate_to_musical
    from Yuki.utils.locked_metadata import read_variable
    status = read_variable(str(Path(storage.job_path) / "status.json"), "status", "")
    if translate_to_musical(status) != CODA:
        return
    for name in storage.runners:
        runner = storage.runners_id.get(name)
        if not _safe_relative(runner) or "/" in runner:
            continue
        job = VJob(storage.job_path, runner)
        if job.status(musical=True) == CODA and job.workflow_id():
            yield name, job


def collect_finished_booking_outputs(project_uuid, impressions, max_file_size=DEFAULT_MAX_FILE_SIZE):
    """List and collect only small stageout files from successful attempts.

    No log listing/download methods are called. Runner/workflow provenance
    is retained so the upload route can reject attempts which changed state.
    """
    from Yuki.kernel.storage.impressions import ImpressionStorage
    from Yuki.kernel.execution.status import CODA
    from Yuki.kernel.workflows.base import VWorkflow
    validate_scope(project_uuid, impressions)
    eligible, filelists, reports = {}, {}, {}
    workflows = {}
    for uid in sorted(set(impressions)):
        storage = ImpressionStorage(project_uuid, uid)
        runners, inventory, failures = {}, [], []
        for name, job in _finished_jobs(storage):
            if job.status(musical=True) != CODA:
                continue
            runner = storage.runners_id.get(name)
            if not _safe_relative(runner) or "/" in runner:
                continue
            workflow_id = job.workflow_id()
            if workflow_id not in workflows:
                try:
                    workflows[workflow_id] = VWorkflow.create(project_uuid, [], workflow_id)
                except Exception:
                    workflows[workflow_id] = None
            workflow = workflows[workflow_id]
            if workflow is None:
                failures.append({"runner": runner, "error": "workflow-unavailable"})
                continue
            machine_dir = Path(storage.job_path) / runner
            try:
                listing = workflow.list_runner_files(uid, "stageout")
                if not isinstance(listing, list):
                    raise ValueError("Invalid runner listing")
                storage._write_filelist(str(machine_dir), "stageout", workflow_id, listing)
            except Exception:
                listing, _note = storage._runner_files(job, "stageout", str(machine_dir))
                if not listing:
                    failures.append({"runner": runner, "error": "stageout-listing-unavailable"})
                    continue
            selected, rows = set(), []
            for item in listing:
                if not isinstance(item, dict) or not _safe_relative(item.get("name")):
                    continue
                relative = item["name"]
                if Path(relative).suffix.lower() == ".log":
                    continue
                size = item.get("size")
                reason = None
                if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                    size, reason = 0, "unknown-size"
                elif size > max_file_size:
                    reason = "too-large"
                elif Path(relative).suffix.lower() not in IMAGE | TEXT:
                    reason = "unsupported-type"
                else:
                    selected.add(relative)
                rows.append({"path": "contents/" + runner + "/stageout/" + relative,
                             "size": size, "reason": reason, "runner": runner,
                             "workflow_id": workflow_id})
            try:
                # Predicate is an exact set derived from the size-checked
                # filelist. It can never select runner logs or huge outputs.
                if selected:
                    workflow.download_selected(uid, selected.__contains__, "stageout")
            except Exception:
                failures.append({"runner": runner, "error": "stageout-collection-failed"})
            if job.status(musical=True) != CODA or job.workflow_id() != workflow_id:
                failures.append({"runner": runner, "error": "job-no-longer-finished"})
                continue
            incomplete = False
            for row in rows:
                relative = row["path"].split("/stageout/", 1)[1]
                local = machine_dir / "stageout" / relative
                confined = not local.is_symlink() and local.resolve().is_relative_to(machine_dir.resolve())
                row["in_yuki"] = confined and local.is_file() and local.stat().st_size == row["size"]
                if row["reason"] is None and not row["in_yuki"]:
                    row["reason"] = "not-collected"
                    incomplete = True
            if incomplete:
                failures.append({"runner": runner, "error": "eligible-files-not-collected"})
            runners[runner] = workflow_id
            inventory.extend(rows)
        if runners and not failures:
            eligible[uid], filelists[uid] = runners, inventory
        reports[uid] = failures
    return {"eligible": eligible, "filelists": filelists, "reports": reports}


def still_finished(project_uuid, uid, declared):
    from Yuki.kernel.storage.impressions import ImpressionStorage
    from Yuki.kernel.execution.status import CODA
    storage = ImpressionStorage(project_uuid, uid)
    actual = {storage.runners_id.get(name): job.workflow_id()
              for name, job in _finished_jobs(storage)
              if job.status(musical=True) == CODA}
    return bool(declared) and all(actual.get(runner) == workflow for runner, workflow in declared.items())

def validate_scope(project_uuid, impressions):
    if not isinstance(project_uuid, str) or not PROJECT.fullmatch(project_uuid):
        raise ValueError("Invalid project UUID")
    if not isinstance(impressions, list) or len(impressions) > 10000:
        raise ValueError("Expected an impression UUID list (maximum 10000)")
    if any(not isinstance(uid, str) or not IMP.fullmatch(uid) for uid in impressions):
        raise ValueError("Invalid impression UUID")

def file_hash(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

def pack_booking_archives(storage_path, project_uuid, impression_uuids,
                          staging_dir, max_file_size=DEFAULT_MAX_FILE_SIZE,
                          eligible=None, filelists=None):
    """Produce deterministic archives from collected runner stageout files."""
    validate_scope(project_uuid, impression_uuids)
    if not isinstance(max_file_size, int) or not 0 < max_file_size <= DEFAULT_MAX_FILE_SIZE:
        raise ValueError("max_file_size must be between 1 and 5 MiB")
    base = Path(storage_path).resolve()
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)
    packed, missing = [], []
    for uid in sorted(set(impression_uuids)):
        if eligible is not None and uid not in eligible:
            missing.append(uid)
            continue
        source = base / project_uuid / uid
        if not source.is_dir() or source.is_symlink():
            missing.append(uid)
            continue
        files, embedded, skipped = [], [], []
        inventory = (filelists or {}).get(uid)
        listed = {row["path"] for row in inventory} if inventory is not None else None
        for runner in sorted(source.iterdir()):
            if eligible is not None and runner.name not in eligible[uid]:
                continue
            stageout = runner / "stageout"
            if not runner.is_dir() or runner.is_symlink() or not stageout.is_dir() or stageout.is_symlink():
                continue
            for path in sorted(stageout.rglob("*")):
                if not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(base):
                    continue
                name = "contents/" + runner.name + "/stageout/" + path.relative_to(stageout).as_posix()
                if path.suffix.lower() == ".log" or (listed is not None and name not in listed):
                    continue
                # Some runners serialize a directory output as an extensionless
                # ZIP file.  Inspect those bundles without extracting them into
                # the persistent result store, and publish only eligible members.
                if zipfile.is_zipfile(path):
                    try:
                        with zipfile.ZipFile(path) as bundle:
                            for member in sorted(bundle.infolist(), key=lambda item: item.filename):
                                parts = Path(member.filename).parts
                                if (member.is_dir() or not parts or Path(member.filename).is_absolute()
                                        or ".." in parts or (member.external_attr >> 16) & 0o170000 == 0o120000):
                                    continue
                                # Runner directory archives normally contain
                                # <impression>/stageout/<directory>/... . Keep
                                # only the path below their stageout component.
                                if "stageout" in parts:
                                    parts = parts[parts.index("stageout") + 1:]
                                if not parts:
                                    continue
                                member_name = ("contents/" + runner.name + "/stageout/"
                                               + Path(*parts).as_posix())
                                suffix = Path(*parts).suffix.lower()
                                reason = None
                                if member.file_size > max_file_size:
                                    reason = "too-large"
                                elif suffix not in IMAGE | TEXT:
                                    reason = "unsupported-type"
                                else:
                                    data = bundle.open(member).read(max_file_size + 1)
                                    if len(data) > max_file_size:
                                        reason = "too-large"
                                    elif suffix in TEXT:
                                        try:
                                            text = data.decode("utf-8")
                                            if "\0" in text:
                                                reason = "binary"
                                        except UnicodeError:
                                            reason = "binary"
                                if reason:
                                    skipped.append({"path": member_name, "reason": reason})
                                else:
                                    embedded.append((member_name, data))
                    except (OSError, RuntimeError, zipfile.BadZipFile):
                        skipped.append({"path": name, "reason": "invalid-archive"})
                    continue
                reason = None
                if path.stat().st_size > max_file_size:
                    reason = "too-large"
                elif path.suffix.lower() not in IMAGE | TEXT:
                    reason = "unsupported-type"
                elif path.suffix.lower() in TEXT:
                    try:
                        text = path.read_bytes().decode("utf-8")
                        if "\0" in text:
                            reason = "binary"
                    except UnicodeError:
                        reason = "binary"
                if reason:
                    skipped.append({"path": name, "reason": reason})
                else:
                    files.append((name, path))
        # No published archive when results have not arrived: retry next bookkeep.
        if not files and not embedded and inventory is None:
            missing.append(uid)
            continue
        target = staging / (uid + ".tar.gz")
        with open(target, "wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                    for name, path in files:
                        info = tarfile.TarInfo(name)
                        info.size = path.stat().st_size
                        info.mode = 0o644
                        with open(path, "rb") as handle:
                            archive.addfile(info, handle)
                    import io
                    for name, data in embedded:
                        info = tarfile.TarInfo(name)
                        info.size = len(data)
                        info.mode = 0o644
                        archive.addfile(info, io.BytesIO(data))
                    listing = json.dumps({"version": 2,
                                          "files": [name for name, _ in files + embedded],
                                          "inventory": inventory or [], "skipped": skipped},
                                         sort_keys=True).encode()
                    info = tarfile.TarInfo("stageout.filelist.json")
                    info.size = len(listing)
                    info.mode = 0o644
                    archive.addfile(info, io.BytesIO(listing))
        packed.append({"uuid": uid, "sha256": file_hash(target), "size": target.stat().st_size,
                       "file_count": len(files) + len(embedded)})
    return {"packed": packed, "missing": missing}

def upload_booking_archives(server_url, project_uuid, archives, grant, staging_dir):
    """Upload declared archives using only the server's short-lived grant."""
    parsed = urlparse(server_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Expected an HTTP(S) server URL without credentials")
    validate_scope(project_uuid, [entry["uuid"] for entry in archives])
    session = requests.Session()
    session.headers["Authorization"] = "Bearer " + grant
    base = server_url.rstrip("/")
    uploaded, failed = [], []
    for entry in archives:
        uid = entry["uuid"]
        path = Path(staging_dir) / (uid + ".tar.gz")
        try:
            if path.stat().st_size != entry["size"] or file_hash(path) != entry["sha256"]:
                raise ValueError("Archive does not match declared manifest")
            # A fresh pack batch must still resume a prior interrupted upload
            # of the same project/impression/content to the same server.
            state_key = hashlib.sha256((base + entry["sha256"]).encode()).hexdigest()
            state_path = Path(staging_dir).parent / (
                project_uuid + "-" + uid + "-" + state_key + ".upload.json")
            state = json.loads(state_path.read_text()) if state_path.exists() else {}
            upload_id = state.get("upload_id") if state.get("server") == base and state.get("sha256") == entry["sha256"] else None
            received = set()
            if upload_id:
                response = session.get(base + "/api/uploads/" + upload_id + "/status", timeout=60,
                                       allow_redirects=False)
                if response.status_code == 404:
                    upload_id = None
                else:
                    response.raise_for_status()
                    status = response.json()
                    received = set(status["received"])
                    chunk_size = status["chunk_size"]
            if not upload_id:
                response = session.post(base + "/api/projects/" + project_uuid +
                    "/impressions/" + uid + "/upload/init",
                    json={"expected_size": entry["size"], "sha256": entry["sha256"]},
                    timeout=60, allow_redirects=False)
                response.raise_for_status()
                state = response.json()
                upload_id, chunk_size = state["upload_id"], state["chunk_size"]
                state_path.write_text(json.dumps({"upload_id": upload_id, "server": base,
                                                  "sha256": entry["sha256"]}))
            with open(path, "rb") as handle:
                index = 0
                while True:
                    block = handle.read(chunk_size)
                    if not block:
                        break
                    if index not in received:
                        response = session.put(base + "/api/uploads/" + upload_id + "/chunk/" + str(index),
                            data=block, headers={"X-Chunk-Sha256": hashlib.sha256(block).hexdigest()},
                            timeout=120, allow_redirects=False)
                        response.raise_for_status()
                    index += 1
            response = session.post(base + "/api/uploads/" + upload_id + "/complete",
                                    timeout=120, allow_redirects=False)
            # A concurrent publisher may have finished the same immutable archive.
            if response.status_code == 409:
                problem = response.json()
                if not problem.get("type", "").endswith("archive-exists"):
                    response.raise_for_status()
            else:
                response.raise_for_status()
            uploaded.append(uid)
            state_path.unlink(missing_ok=True)
        except (OSError, ValueError, requests.RequestException) as exc:
            failed.append({"uuid": uid, "error": type(exc).__name__})
    session.close()
    return {"uploaded": uploaded, "failed": failed}
