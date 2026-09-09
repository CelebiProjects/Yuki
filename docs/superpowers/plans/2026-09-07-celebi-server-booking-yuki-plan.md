# Yuki: celebi_server Booking Pack + Upload Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Yuki packs booking archives (per impression: `stageout.filelist.json` + plot files only, deterministic tar) from its own storage and uploads them to celebi_server through its existing resumable chunked API, exposing both as NDJSON-streamed endpoints for the celebi client.

**Architecture:** A pure kernel module `kernel/celebi_server_booker.py` (`pack_booking_archives`, `upload_booking_archives`) with no Flask/global-config dependencies (storage paths are explicit parameters, so tests use tmp dirs), plus a thin `routes/server_booking.py` blueprint that streams NDJSON progress exactly like `routes/booking.py::book_reana_stream` (queue + background thread). The kernel never follows client-supplied filesystem paths: archives are resolved from Yuki storage by uuid only.

**Tech Stack:** Flask blueprints, `requests` (already a Yuki dependency), tarfile/gzip/hashlib, pytest (UnitTest/ tests are plain pytest-style functions or unittest classes — match the file's neighbors).

**Spec:** `/Users/wave/workdir/Celebi/celebi_server/docs/superpowers/specs/2026-09-07-celebi-booking-design.md` (§Yuki additions)

## Global Constraints

- Impression uuids validated against `^[0-9a-f]{32}$` on every hop; invalid uuids → 400, never touch the filesystem.
- Yuki never follows client-supplied filesystem paths; archive source paths are built from its own `storage_path` + validated uuids.
- Yuki receives grants (≤1h, single-project, enumerated uuids, upload-only), NEVER user bearer tokens. The route body contains a `grant` field, passed verbatim as `Authorization: Bearer <grant>`; it is never logged or persisted.
- Booking archives contain `contents/<runner_id>/stageout.filelist.json` + plot files only (`Yuki.kernel.file_types.make_predicate("plots")` — png/jpg/jpeg/gif/pdf/svg/webp/eps). No data files.
- Deterministic archives: sorted members, `tarfile.USTAR_FORMAT`, member mtime=0, uid=gid=0, uname=gname="", gzip mtime=0. Re-pack of unchanged storage yields the same sha256 (resume safety).
- celebi_server upload API contract (verified, unchanged):
  - `POST {server}/api/projects/{project_uuid}/impressions/{uuid}/upload/init`, JSON `{expected_size, sha256}`, header `Authorization: Bearer <grant>` → 200 `{upload_id, chunk_size}` (chunk_size = 8 MiB)
  - `GET {server}/api/uploads/{upload_id}/status`, same auth → `{received: [int...], ...}`
  - `PUT {server}/api/uploads/{upload_id}/chunk/{n}`, header `X-Chunk-Sha256: <sha256 of chunk bytes>`, body = raw chunk bytes
  - `POST {server}/api/uploads/{upload_id}/complete` → finalized impression
  - Chunk `n` numbering starts at 0; final chunk may be short; exactly `ceil(size/chunk_size)` chunks.
- NDJSON stream shape (mirrors `/book-reana-stream`): each line `{"text": ..., "status": "normal"|"success"|"error"}`; final line `{"done": true, "success": bool, "data": {...}}` or `{"done": true, "success": false, "error": ...}`.
- Storage layout (verified): `Storage/{project_uuid}/{impression_uuid}/{runner_id}/stageout.filelist.json` and `Storage/{project_uuid}/{impression_uuid}/{runner_id}/stageout/<output files>`.

---

### Task 1: `pack_booking_archives` kernel

**Files:**
- Create: `Yuki/kernel/celebi_server_booker.py`
- Test: `UnitTest/test_celebi_server_booker.py`

**Interfaces:**
- Consumes: `Yuki.kernel.file_types.make_predicate` (existing), stdlib tarfile/gzip/hashlib
- Produces:
  - `UUID32_RE = re.compile(r"^[0-9a-f]{32}$")`
  - `class BookingPackError(Exception)`
  - `def pack_booking_archives(storage_path, project_uuid, impression_uuids, staging_dir, progress_cb=None) -> dict`
    - Returns `{"packed": [{"uuid", "path", "sha256", "size", "file_count"}], "missing": [uuid...]}`
    - `path` = `os.path.join(staging_dir, uuid + ".tar.gz")` (staging_dir created if absent)
    - An impression with no directory under `storage_path/project_uuid/` → appended to `missing`, no archive.
    - An impression whose directory exists but has NO `stageout.filelist.json` in any runner dir → treated as missing (nothing bookable).
    - Archive members: for each runner dir (sorted): `contents/<runner_id>/stageout.filelist.json` (always, when present) + every file under `<runner_dir>/stageout/` (recursive, relative arcname under `contents/<runner_id>/stageout/`) passing `make_predicate("plots")`. `file_count` counts both kinds.
    - `progress_cb(text, status="normal")` called per impression (packed/missing).
  - `def is_bookable(storage_path, project_uuid, uuid) -> bool` — helper used by routes later.

- [ ] **Step 1: Write the failing test**

```python
# UnitTest/test_celebi_server_booker.py
"""Tests for celebi_server booking pack/upload kernel."""
import hashlib
import os
import tarfile

import pytest

from Yuki.kernel import celebi_server_booker as csb


def _seed(storage, project, imp, runner, filelist, plots, data=()):
    """Create Storage/<project>/<imp>/<runner>/{filelist, stageout/...}."""
    rdir = os.path.join(storage, project, imp, runner)
    os.makedirs(os.path.join(rdir, "stageout"), exist_ok=True)
    if filelist is not None:
        with open(os.path.join(rdir, "stageout.filelist.json"), "w") as f:
            f.write(filelist)
    for rel in plots:
        p = os.path.join(rdir, "stageout", rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as f:
            f.write(b"PLOT" + rel.encode())
    for rel in data:
        p = os.path.join(rdir, "stageout", rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as f:
            f.write(b"DATA" + rel.encode())


def _member_names(path):
    with tarfile.open(path, "r:gz") as tar:
        return sorted(m.name for m in tar.getmembers() if m.isfile())


def test_pack_builds_filelist_plus_plots_archive(tmp_path):
    storage = str(tmp_path / "Storage")
    staging = str(tmp_path / "staging")
    imp = "a" * 32
    _seed(storage, "proj", imp, "r1",
          filelist='[{"path": "mass.png"}, {"path": "ntuple.root"}]',
          plots=["mass.png", "subdir/eff.svg"],
          data=["ntuple.root"])
    result = csb.pack_booking_archives(storage, "proj", [imp], staging)
    assert result["missing"] == []
    (entry,) = result["packed"]
    assert entry["uuid"] == imp
    assert entry["file_count"] == 3  # filelist + 2 plots
    assert os.path.isfile(entry["path"])
    assert entry["size"] == os.path.getsize(entry["path"])
    assert entry["sha256"] == hashlib.sha256(
        open(entry["path"], "rb").read()).hexdigest()
    names = _member_names(entry["path"])
    assert names == [
        "contents/r1/stageout.filelist.json",
        "contents/r1/stageout/mass.png",
        "contents/r1/stageout/subdir/eff.svg",
    ]


def test_pack_is_deterministic(tmp_path):
    storage = str(tmp_path / "Storage")
    imp = "a" * 32
    _seed(storage, "proj", imp, "r1", filelist="[]",
          plots=["m.png"], data=["d.root"])
    r1 = csb.pack_booking_archives(storage, "proj", [imp], str(tmp_path / "s1"))
    r2 = csb.pack_booking_archives(storage, "proj", [imp], str(tmp_path / "s2"))
    assert r1["packed"][0]["sha256"] == r2["packed"][0]["sha256"]


def test_pack_missing_impression_reported_not_packed(tmp_path):
    storage = str(tmp_path / "Storage")
    present, absent = "a" * 32, "b" * 32
    _seed(storage, "proj", present, "r1", filelist="[]", plots=["m.png"])
    result = csb.pack_booking_archives(storage, "proj", [present, absent],
                                       str(tmp_path / "s"))
    assert result["missing"] == [absent]
    assert [e["uuid"] for e in result["packed"]] == [present]


def test_pack_no_filelist_treated_as_missing(tmp_path):
    storage = str(tmp_path / "Storage")
    imp = "a" * 32
    _seed(storage, "proj", imp, "r1", filelist=None, plots=["m.png"])
    result = csb.pack_booking_archives(storage, "proj", [imp],
                                       str(tmp_path / "s"))
    assert result["missing"] == [imp]
    assert result["packed"] == []


def test_pack_multiple_runners_sorted(tmp_path):
    storage = str(tmp_path / "Storage")
    imp = "a" * 32
    _seed(storage, "proj", imp, "r2", filelist="[]", plots=["b.png"])
    _seed(storage, "proj", imp, "r1", filelist="[]", plots=["a.png"])
    result = csb.pack_booking_archives(storage, "proj", [imp],
                                       str(tmp_path / "s"))
    names = _member_names(result["packed"][0]["path"])
    assert names[0].startswith("contents/r1/")
    assert names[-1].startswith("contents/r2/")


def test_pack_rejects_invalid_uuid_without_touching_fs(tmp_path):
    storage = str(tmp_path / "Storage")
    with pytest.raises(ValueError) as exc:
        csb.pack_booking_archives(storage, "proj", ["../etc"], str(tmp_path / "s"))
    assert "../etc" in str(exc.value)
    assert not os.path.exists(tmp_path / "s")


def test_is_bookable(tmp_path):
    storage = str(tmp_path / "Storage")
    imp = "a" * 32
    _seed(storage, "proj", imp, "r1", filelist="[]", plots=[])
    assert csb.is_bookable(storage, "proj", imp) is True
    assert csb.is_bookable(storage, "proj", "b" * 32) is False
    assert csb.is_bookable(storage, "proj", "../x") is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest UnitTest/test_celebi_server_booker.py -x`
Expected: FAIL (`ModuleNotFoundError: Yuki.kernel.celebi_server_booker`)

- [ ] **Step 3: Implement**

Create `Yuki/kernel/celebi_server_booker.py`:

```python
"""Pack and upload celebi_server booking archives.

A booking archive captures ONE impression as Yuki holds it:
    contents/<runner_id>/stageout.filelist.json   (the output file list)
  + contents/<runner_id>/stageout/<plot files>    (plots only)

The file list is Yuki's own stageout.filelist.json (produced by status/
collect) — it is NOT declared in celebi.yaml and cannot be reproduced
client-side. Plots are files passing Yuki.kernel.file_types' "plots"
predicate. Data files never enter a booking archive.

Archives are deterministic (sorted members, normalized metadata, gzip
mtime=0) so re-packing unchanged storage yields the same sha256 — the
resume guarantee for the celebi_server chunked upload.
"""
import gzip
import hashlib
import os
import re
import tarfile

from . import file_types

UUID32_RE = re.compile(r"^[0-9a-f]{32}$")
FILELIST_NAME = "stageout.filelist.json"


class BookingPackError(Exception):
    """Raised for invalid booking requests (bad uuids, bad paths)."""


def _check_uuid(uuid):
    if not UUID32_RE.match(str(uuid)):
        raise ValueError(f"invalid impression uuid: {uuid!r}")


def is_bookable(storage_path, project_uuid, uuid):
    """True when storage holds a bookable impression dir for the uuid."""
    if not UUID32_RE.match(str(uuid)):
        return False
    imp_dir = os.path.join(storage_path, project_uuid, uuid)
    if not os.path.isdir(imp_dir):
        return False
    for runner_id in sorted(os.listdir(imp_dir)):
        if os.path.isfile(os.path.join(imp_dir, runner_id, FILELIST_NAME)):
            return True
    return False


def _collect_members(imp_dir):
    """Sorted [(arcname, abspath)] of filelist + plot files for one impression."""
    plot_pred = file_types.make_predicate("plots")
    members = []
    for runner_id in sorted(os.listdir(imp_dir)):
        runner_dir = os.path.join(imp_dir, runner_id)
        if not os.path.isdir(runner_dir):
            continue
        filelist = os.path.join(runner_dir, FILELIST_NAME)
        if os.path.isfile(filelist):
            members.append(
                (f"contents/{runner_id}/{FILELIST_NAME}", filelist))
        stageout = os.path.join(runner_dir, "stageout")
        if not os.path.isdir(stageout):
            continue
        for root, _dirs, files in os.walk(stageout):
            for name in sorted(files):
                abspath = os.path.join(root, name)
                rel = os.path.relpath(abspath, stageout)
                if plot_pred(rel):
                    members.append(
                        (f"contents/{runner_id}/stageout/{rel}", abspath))
    members.sort(key=lambda pair: pair[0])
    return members


def _write_deterministic_tar(members, dest):
    """Write a deterministic USTAR tar.gz; returns (sha256, size)."""
    hasher = hashlib.sha256()
    with open(dest, "wb") as raw:
        class _HashWriter:
            def write(self, data):
                hasher.update(data)
                return raw.write(data)
        with gzip.GzipFile(filename="", mode="wb",
                           fileobj=_HashWriter(), mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode="w",
                              format=tarfile.USTAR_FORMAT) as tar:
                for arcname, abspath in members:
                    info = tar.gettarinfo(abspath, arcname)
                    info.mtime = 0
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    with open(abspath, "rb") as fh:
                        tar.addfile(info, fh)
    return hasher.hexdigest(), os.path.getsize(dest)


def pack_booking_archives(storage_path, project_uuid, impression_uuids,
                          staging_dir, progress_cb=None):
    """Pack one booking archive per bookable impression.

    Returns {"packed": [{"uuid", "path", "sha256", "size", "file_count"}],
             "missing": [uuid...]}.
    """
    for uuid in impression_uuids:
        _check_uuid(uuid)
    os.makedirs(staging_dir, exist_ok=True)
    packed, missing = [], []
    for uuid in impression_uuids:
        imp_dir = os.path.join(storage_path, project_uuid, uuid)
        members = _collect_members(imp_dir) if os.path.isdir(imp_dir) else []
        if not members:
            missing.append(uuid)
            if progress_cb:
                progress_cb(f"impression {uuid}: nothing bookable — missing",
                            "error")
            continue
        dest = os.path.join(staging_dir, uuid + ".tar.gz")
        sha256, size = _write_deterministic_tar(members, dest)
        packed.append({
            "uuid": uuid, "path": dest, "sha256": sha256,
            "size": size, "file_count": len(members),
        })
        if progress_cb:
            progress_cb(f"impression {uuid}: packed {len(members)} files "
                        f"({size} bytes)", "normal")
    return {"packed": packed, "missing": missing}
```

- [ ] **Step 4: Run tests**

Run: `pytest UnitTest/test_celebi_server_booker.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add Yuki/kernel/celebi_server_booker.py UnitTest/test_celebi_server_booker.py
git commit -m "Add celebi_server booking archive packer (filelist + plots, deterministic)"
```

---

### Task 2: `upload_booking_archives` kernel (resumable chunked client)

**Files:**
- Modify: `Yuki/kernel/celebi_server_booker.py`
- Test: `UnitTest/test_celebi_server_booker_upload.py`

**Interfaces:**
- Consumes: `requests` (existing dependency), packed archive entries from `pack_booking_archives`
- Produces:
  - `class BookingUploadError(Exception)` with attribute `uuid`
  - `def upload_booking_archives(server_url, project_uuid, archives, grant,
                                progress_cb=None, verify_ssl=True,
                                session=None) -> dict`
    - `archives`: `[{"uuid", "path", "sha256", "size"}]` (from pack output)
    - For each archive: init → status (resume: skip received chunk indices) → PUT missing chunks (8 MiB by the server's chunk_size, header `X-Chunk-Sha256`, per-chunk retry ×3 with backoff) → complete.
    - `session`: optional `requests.Session` (tests inject a stub; default creates one).
    - Returns `{"uploaded": [uuid...], "failed": [{"uuid", "error"}]}`; failures are collected, not raised, so one bad impression doesn't block the rest. A `BookingUploadError` is only raised for a non-2xx init/status/complete response (with the server's response body in the message when available).
    - Grant is sent as `Authorization: Bearer <grant>` on every request; never logged.

- [ ] **Step 1: Write the failing test**

Use a stub session recording requests and returning canned responses. The stub must model celebi_server semantics: chunk n accepted, status returns received indices.

```python
# UnitTest/test_celebi_server_booker_upload.py
"""Tests for upload_booking_archives (resumable chunked client)."""
import hashlib
import json
import os

from Yuki.kernel import celebi_server_booker as csb


CHUNK = 8 * 1024 * 1024


class StubResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    @property
    def text(self):
        return json.dumps(self._payload)


class StubSession:
    """Models celebi_server's chunked-upload API in memory."""

    def __init__(self):
        self.calls = []          # [(method, url, kwargs-sans-body)]
        self.bodies = []         # raw bytes bodies, parallel to calls
        self.received = {}       # upload_id -> {n: sha256}
        self.upload_seq = 0
        self.fail_url_once = None   # (method, suffix) -> next call 500s
        self.closed = False

    def _auth(self, kwargs):
        auth = kwargs.get("headers", {}).get("Authorization", "")
        assert auth.startswith("Bearer grant-"), auth
        return auth

    def request(self, method, url, **kwargs):
        self._auth(kwargs)
        self.calls.append((method, url, {k: v for k, v in kwargs.items()
                                         if k != "data"}))
        self.bodies.append(kwargs.get("data", b""))
        if self.fail_url_once and self.fail_url_once in url:
            self.fail_url_once = None
            return StubResponse(500, {"detail": "boom"})
        if url.endswith("/upload/init"):
            self.upload_seq += 1
            uid = f"up-{self.upload_seq}"
            self.received[uid] = {}
            return StubResponse(200, {"upload_id": uid, "chunk_size": CHUNK})
        if url.endswith("/status"):
            uid = url.split("/uploads/")[1].split("/")[0]
            return StubResponse(200, {"received": sorted(self.received[uid])})
        if "/chunk/" in url:
            uid, _, n = url.split("/uploads/")[1].partition("/chunk/")
            n = int(n)
            data = kwargs["data"]
            assert len(data) <= CHUNK
            want = kwargs["headers"]["X-Chunk-Sha256"]
            assert hashlib.sha256(data).hexdigest() == want
            self.received[uid][n] = want
            return StubResponse(200, {"received": n})
        if url.endswith("/complete"):
            uid = url.split("/uploads/")[1].split("/")[0]
            return StubResponse(200, {"ok": True, "received": len(self.received[uid])})
        raise AssertionError(f"unexpected url {url}")

    def close(self):
        self.closed = True


def _make_archive(tmp_path, uuid, size):
    path = str(tmp_path / f"{uuid}.tar.gz")
    with open(path, "wb") as f:
        f.write(os.urandom(size))
    return {"uuid": uuid, "path": path,
            "sha256": hashlib.sha256(open(path, "rb").read()).hexdigest(),
            "size": size}


def _upload(session, archives, **kw):
    return csb.upload_booking_archives(
        "http://server:3320", "proj", archives, "grant-x",
        session=session, **kw)


def test_upload_single_small_archive(tmp_path):
    session = StubSession()
    arch = _make_archive(tmp_path, "a" * 32, 100)
    result = _upload(session, [arch])
    assert result["uploaded"] == ["a" * 32]
    assert result["failed"] == []
    methods = [c[0] for c in session.calls]
    assert methods == ["POST", "GET", "PUT", "POST"]  # init status chunk complete


def test_upload_multi_chunk_and_resume(tmp_path):
    session = StubSession()
    arch = _make_archive(tmp_path, "a" * 32, CHUNK + 10)
    result = _upload(session, [arch])
    assert result["uploaded"] == ["a" * 32]
    # 2 chunks uploaded (init GET returns empty received)
    puts = [c for c in session.calls if c[0] == "PUT"]
    assert len(puts) == 2
    assert len(session.bodies[session.calls.index(puts[1])]) == 10


def test_resume_skips_received_chunks(tmp_path):
    session = StubSession()
    arch = _make_archive(tmp_path, "a" * 32, 2 * CHUNK + 5)
    # simulate a previous run: server already has chunk 0 of the next upload
    orig_request = session.request

    def prefill(method, url, **kwargs):
        resp = orig_request(method, url, **kwargs)
        if url.endswith("/upload/init"):
            uid = resp.json()["upload_id"]
            with open(arch["path"], "rb") as f:
                chunk = f.read(CHUNK)
            session.received[uid][0] = hashlib.sha256(chunk).hexdigest()
        return resp

    session.request = prefill
    result = _upload(session, [arch])
    assert result["uploaded"] == ["a" * 32]
    puts = [c for c in session.calls if c[0] == "PUT"]
    assert [int(c[1].rsplit("/chunk/", 1)[1]) for c in puts] == [1, 2]


def test_failed_archive_collected_others_continue(tmp_path):
    session = StubSession()
    good = _make_archive(tmp_path, "a" * 32, 50)
    bad = _make_archive(tmp_path, "b" * 32, 50)
    session.fail_url_once = "/impressions/" + "b" * 32 + "/upload/init"
    result = _upload(session, [bad, good])
    assert result["uploaded"] == ["a" * 32]
    assert [f["uuid"] for f in result["failed"]] == ["b" * 32]
    assert "boom" in result["failed"][0]["error"]


def test_upload_validates_uuid(tmp_path):
    session = StubSession()
    arch = _make_archive(tmp_path, "not-a-uuid", 50)
    try:
        _upload(session, [arch])
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "not-a-uuid" in str(exc)
    assert session.calls == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest UnitTest/test_celebi_server_booker_upload.py -x`
Expected: FAIL (`AttributeError: module ... has no attribute 'upload_booking_archives'`)

- [ ] **Step 3: Implement**

Append to `Yuki/kernel/celebi_server_booker.py`:

```python
import math
import time

import requests

UPLOAD_INIT = "/api/projects/{project}/impressions/{imp}/upload/init"
UPLOAD_STATUS = "/api/uploads/{upload_id}/status"
UPLOAD_CHUNK = "/api/uploads/{upload_id}/chunk/{n}"
UPLOAD_COMPLETE = "/api/uploads/{upload_id}/complete"
CHUNK_PUT_RETRIES = 3
RETRY_BACKOFF = 2.0


class BookingUploadError(Exception):
    """Raised when celebi_server rejects an upload-level request."""

    def __init__(self, message, uuid=None):
        super().__init__(message)
        self.uuid = uuid


def _check_response(resp, uuid, what):
    if resp.status_code // 100 != 2:
        detail = ""
        try:
            detail = resp.text
        except Exception:  # noqa: BLE001 - best effort error context
            pass
        raise BookingUploadError(
            f"{what} failed (HTTP {resp.status_code}): {detail[:400]}",
            uuid=uuid)


def _upload_one(server_url, project_uuid, archive, grant, session,
                verify_ssl, progress_cb):
    uuid = archive["uuid"]
    _check_uuid(uuid)
    headers = {"Authorization": f"Bearer {grant}"}

    resp = session.post(
        server_url + UPLOAD_INIT.format(project=project_uuid, imp=uuid),
        json={"expected_size": archive["size"], "sha256": archive["sha256"]},
        headers=headers, verify=verify_ssl, timeout=60)
    _check_response(resp, uuid, "upload init")
    upload_id = resp.json()["upload_id"]
    chunk_size = resp.json().get("chunk_size") or 8 * 1024 * 1024

    resp = session.get(
        server_url + UPLOAD_STATUS.format(upload_id=upload_id),
        headers=headers, verify=verify_ssl, timeout=60)
    _check_response(resp, uuid, "upload status")
    done = set(resp.json().get("received", []))
    total = math.ceil(archive["size"] / chunk_size)

    with open(archive["path"], "rb") as fh:
        for n in range(total):
            fh.seek(n * chunk_size)
            data = fh.read(chunk_size)
            if n in done:
                if progress_cb:
                    progress_cb(f"impression {uuid}: chunk {n + 1}/{total} "
                                "already on server", "normal")
                continue
            sha = hashlib.sha256(data).hexdigest()
            last_err = None
            for attempt in range(1, CHUNK_PUT_RETRIES + 1):
                try:
                    resp = session.put(
                        server_url + UPLOAD_CHUNK.format(
                            upload_id=upload_id, n=n),
                        data=data,
                        headers={**headers, "X-Chunk-Sha256": sha},
                        verify=verify_ssl, timeout=300)
                    _check_response(resp, uuid, f"chunk {n}")
                    last_err = None
                    break
                except (requests.RequestException, BookingUploadError) as err:
                    last_err = err
                    if attempt < CHUNK_PUT_RETRIES:
                        time.sleep(RETRY_BACKOFF * attempt)
            if last_err is not None:
                raise BookingUploadError(
                    f"chunk {n} failed after {CHUNK_PUT_RETRIES} tries: "
                    f"{last_err}", uuid=uuid)
            if progress_cb:
                progress_cb(f"impression {uuid}: chunk {n + 1}/{total} "
                            "uploaded", "normal")

    resp = session.post(
        server_url + UPLOAD_COMPLETE.format(upload_id=upload_id),
        headers=headers, verify=verify_ssl, timeout=120)
    _check_response(resp, uuid, "upload complete")
    return uuid


def upload_booking_archives(server_url, project_uuid, archives, grant,
                            progress_cb=None, verify_ssl=True, session=None):
    """Upload packed archives via celebi_server's resumable chunked API.

    Returns {"uploaded": [uuid...], "failed": [{"uuid", "error"}]}.
    One archive's failure never blocks the others.
    """
    for archive in archives:
        _check_uuid(archive.get("uuid"))
    own_session = session is None
    session = session or requests.Session()
    uploaded, failed = [], []
    try:
        for archive in archives:
            try:
                uploaded.append(_upload_one(
                    server_url.rstrip("/"), project_uuid, archive, grant,
                    session, verify_ssl, progress_cb))
                if progress_cb:
                    progress_cb(f"impression {archive['uuid']}: upload "
                                "complete", "success")
            except BookingUploadError as err:
                failed.append({"uuid": archive["uuid"], "error": str(err)})
                if progress_cb:
                    progress_cb(f"impression {archive['uuid']}: {err}",
                                "error")
    finally:
        if own_session:
            session.close()
    return {"uploaded": uploaded, "failed": failed}
```

- [ ] **Step 4: Run tests**

Run: `pytest UnitTest/test_celebi_server_booker.py UnitTest/test_celebi_server_booker_upload.py -v`
Expected: 12 passed

- [ ] **Step 5: Commit**

```bash
git add Yuki/kernel/celebi_server_booker.py UnitTest/test_celebi_server_booker_upload.py
git commit -m "Add resumable chunked upload client for celebi_server booking"
```

---

### Task 3: `routes/server_booking.py` — NDJSON pack + upload endpoints

**Files:**
- Create: `Yuki/server/routes/server_booking.py`
- Modify: `Yuki/server/app.py`
- Test: `UnitTest/test_celebi_server_booking_routes.py`

**Interfaces:**
- Consumes: `pack_booking_archives`, `upload_booking_archives`, `is_bookable` (Tasks 1–2); `..config.config` (global YukiConfig, existing pattern)
- Produces:
  - `bp = Blueprint('server_booking', __name__)`
  - `POST /book-celebi-server/pack` — JSON body `{"project_uuid", "impressions": [uuid...]}`; NDJSON stream; final `data` = pack result `{"packed": [...], "missing": [...]}`. Body errors (missing fields, invalid uuids) → 400 JSON (not a stream). Staging dir: `tempfile.mkdtemp(prefix="yuki_cs_booking_")` — archives STAY after the stream ends (client uploads next; cleanup is the caller's local `book` command responsibility on the client side — Yuki staging is tmp and small: plots only).
  - `POST /book-celebi-server/upload` — JSON body `{"celebi_server_url", "project_uuid", "archives": [{"uuid", "sha256", "size"}], "grant"}`. Archive paths are resolved BY YUKI: `os.path.join(staging_root, uuid + ".tar.gz")` where `staging_root` = the pack staging dir — resolved via a `staging_dir` body field? NO — spec security constraint: never client paths. Resolution rule: the upload endpoint accepts the same pack output and looks up `<staging_dir>/<uuid>.tar.gz` where staging_dir is looked up from an in-process registry keyed by project_uuid (the pack route records it). Simplest compliant design: pack route records `{project_uuid: staging_dir}` in a module-level dict; upload route resolves paths from that dict, validating the file exists and its sha256/size match the entry. If no staging recorded → 400 "pack first". NDJSON stream; final `data` = `{"uploaded": [...], "failed": [...]}`.

- [ ] **Step 1: Write the failing test**

```python
# UnitTest/test_celebi_server_booking_routes.py
"""Route tests for /book-celebi-server/pack and /upload."""
import hashlib
import json
import os

import pytest

from Yuki.server.app import create_app
from Yuki.server.routes import server_booking
from Yuki.server.config import YukiConfig


def _seed(storage, project, imp, runner="r1", plots=("m.png",)):
    rdir = os.path.join(storage, project, imp, runner, "stageout")
    os.makedirs(rdir, exist_ok=True)
    with open(os.path.join(
            os.path.dirname(rdir), "stageout.filelist.json"), "w") as f:
        f.write("[]")
    for p in plots:
        with open(os.path.join(rdir, p), "wb") as f:
            f.write(b"PLOT")


def _read_ndjson(resp):
    lines = [json.loads(l) for l in resp.get_data(as_text=True).splitlines()]
    assert lines[-1]["done"] is True
    return lines


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    # config singleton was built at import; rebuild against the fake HOME
    monkeypatch.setattr(
        server_booking, "_config",
        YukiConfig())
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c, str(tmp_path / ".Yuki" / "Storage")


def test_pack_streams_and_records_result(client):
    c, storage = client
    _seed(storage, "proj", "a" * 32)
    resp = c.post("/book-celebi-server/pack", json={
        "project_uuid": "proj", "impressions": ["a" * 32]})
    assert resp.status_code == 200
    assert resp.mimetype == "application/x-ndjson"
    lines = _read_ndjson(resp)
    assert lines[-1]["success"] is True
    data = lines[-1]["data"]
    assert data["missing"] == []
    (entry,) = data["packed"]
    assert os.path.isfile(entry["path"])
    assert "staging" in entry["path"]
    # staging registered for the upload endpoint
    assert server_booking._staging_for("proj") == os.path.dirname(entry["path"])


def test_pack_400_on_bad_uuid(client):
    c, storage = client
    resp = c.post("/book-celebi-server/pack", json={
        "project_uuid": "proj", "impressions": ["../etc"]})
    assert resp.status_code == 400
    assert resp.mimetype != "application/x-ndjson"


def test_pack_400_missing_fields(client):
    c, storage = client
    assert c.post("/book-celebi-server/pack",
                  json={"project_uuid": "proj"}).status_code == 400


def test_upload_resolves_paths_from_staging(client, monkeypatch, tmp_path):
    c, storage = client
    _seed(storage, "proj", "a" * 32)
    pack_resp = c.post("/book-celebi-server/pack", json={
        "project_uuid": "proj", "impressions": ["a" * 32]})
    entry = _read_ndjson(pack_resp)[-1]["data"]["packed"][0]

    # stub the kernel uploader: assert it receives Yuki-resolved paths
    seen = {}

    def fake_upload(server_url, project_uuid, archives, grant,
                    progress_cb=None, verify_ssl=True, session=None):
        seen.update(server_url=server_url, project_uuid=project_uuid,
                    grant=grant, archives=archives)
        for a in archives:
            progress_cb(f"up {a['uuid']}", "normal")
        return {"uploaded": [a["uuid"] for a in archives], "failed": []}

    monkeypatch.setattr(server_booking, "upload_booking_archives",
                        fake_upload)
    resp = c.post("/book-celebi-server/upload", json={
        "celebi_server_url": "http://server:3320",
        "project_uuid": "proj",
        "archives": [{k: entry[k] for k in ("uuid", "sha256", "size")}],
        "grant": "grant-x"})
    assert resp.status_code == 200
    lines = _read_ndjson(resp)
    assert lines[-1]["success"] is True
    assert lines[-1]["data"]["uploaded"] == ["a" * 32]
    # path was resolved by Yuki, not supplied by the client
    assert seen["archives"][0]["path"] == entry["path"]
    assert seen["archives"][0]["path"].startswith(entry["path"][:-40])
    assert seen["grant"] == "grant-x"
    assert seen["server_url"] == "http://server:3320"


def test_upload_rejects_path_supplied_by_client(client, monkeypatch, tmp_path):
    c, storage = client
    _seed(storage, "proj", "a" * 32)
    pack_resp = c.post("/book-celebi-server/pack", json={
        "project_uuid": "proj", "impressions": ["a" * 32]})
    entry = _read_ndjson(pack_resp)[-1]["data"]["packed"][0]
    tampered = dict(entry)
    tampered["path"] = "/etc/passwd"
    called = []

    def boom(*a, **k):
        called.append(True)

    monkeypatch.setattr(server_booking, "upload_booking_archives", boom)
    resp = c.post("/book-celebi-server/upload", json={
        "celebi_server_url": "http://s", "project_uuid": "proj",
        "archives": [tampered], "grant": "g"})
    # either the path is stripped and upload proceeds, or 422; NEVER /etc/passwd
    if resp.status_code == 200:
        assert not called  # path was stripped -> kernel saw staging path only
    else:
        assert resp.status_code in (400, 422)


def test_upload_400_without_prior_pack(client):
    c, storage = client
    resp = c.post("/book-celebi-server/upload", json={
        "celebi_server_url": "http://s", "project_uuid": "proj",
        "archives": [{"uuid": "a" * 32, "sha256": "s", "size": 1}],
        "grant": "g"})
    assert resp.status_code == 400
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest UnitTest/test_celebi_server_booking_routes.py -x`
Expected: FAIL (`ModuleNotFoundError: Yuki.server.routes.server_booking`)

- [ ] **Step 3: Implement**

Create `Yuki/server/routes/server_booking.py`:

```python
"""celebi_server booking routes: pack archives from Yuki storage and upload
them to a celebi_server through its resumable chunked API.

Both endpoints stream NDJSON progress (see /book-reana-stream). Yuki never
follows client-supplied filesystem paths: upload archive paths are resolved
from the staging dir recorded by the pack route, keyed by project_uuid.
"""
import json
import os
import queue
import shutil
import tempfile
import threading
from logging import getLogger

from flask import Blueprint, Response, jsonify, request, stream_with_context

from ...kernel import celebi_server_booker
from ..config import config

bp = Blueprint('server_booking', __name__)
logger = getLogger("YukiLogger")

# project_uuid -> staging dir recorded by the pack route. In-process only;
# a pack run is immediately followed by its upload from the same client.
_staging = {}


def _staging_for(project_uuid):
    return _staging.get(project_uuid)


def _validate_uuids(impressions):
    if not isinstance(impressions, list) or not impressions:
        return "impressions must be a non-empty list"
    for uuid in impressions:
        if not celebi_server_booker.UUID32_RE.match(str(uuid)):
            return f"invalid impression uuid: {uuid!r}"
    return None


def _ndjson_stream(run):
    """run(progress_cb) -> final data dict; errors become failed finals."""

    def generate():
        msg_queue = queue.Queue()

        def worker():
            try:
                data = run(lambda text, status="normal": msg_queue.put(
                    {"text": text, "status": status}))
                msg_queue.put({"done": True, "success": True, "data": data})
            except Exception as err:  # noqa: BLE001 - reported in the stream
                logger.error("celebi_server booking failed: %s", err)
                msg_queue.put({"done": True, "success": False,
                               "error": str(err) or repr(err)})

        thread = threading.Thread(target=worker)
        thread.start()
        while True:
            msg = msg_queue.get()
            yield json.dumps(msg) + "\n"
            if msg.get("done"):
                break
        thread.join(timeout=5)

    return Response(stream_with_context(generate()),
                    mimetype='application/x-ndjson')


@bp.route('/book-celebi-server/pack', methods=['POST'])
def pack_celebi_server():
    """Pack booking archives for the given impressions.

    JSON body: {"project_uuid": str, "impressions": [uuid32...]}
    Returns NDJSON; final data = pack_booking_archives result. Archives stay
    in the recorded staging dir for the follow-up /upload call.
    """
    data = request.get_json(silent=True) or {}
    project_uuid = data.get("project_uuid", "")
    impressions = data.get("impressions")
    if not project_uuid:
        return jsonify({"error": "Missing project_uuid"}), 400
    err = _validate_uuids(impressions)
    if err:
        return jsonify({"error": err}), 400

    staging_dir = tempfile.mkdtemp(prefix="yuki_cs_booking_")

    def run(progress_cb):
        result = celebi_server_booker.pack_booking_archives(
            config.storage_path, project_uuid, impressions, staging_dir,
            progress_cb=progress_cb)
        if result["packed"]:
            _staging[project_uuid] = staging_dir
        return result

    return _ndjson_stream(run)


@bp.route('/book-celebi-server/upload', methods=['POST'])
def upload_celebi_server():
    """Upload staged booking archives to celebi_server.

    JSON body: {"celebi_server_url", "project_uuid",
                "archives": [{"uuid", "sha256", "size"}], "grant"}
    The grant (scoped, upload-only) is relayed as a Bearer token; it is never
    logged or stored. Archive paths are resolved from the pack staging dir —
    client-supplied "path" fields are ignored.
    """
    data = request.get_json(silent=True) or {}
    server_url = (data.get("celebi_server_url") or "").strip()
    project_uuid = data.get("project_uuid", "")
    archives = data.get("archives") or []
    grant = data.get("grant", "")
    if not server_url or not project_uuid or not grant:
        return jsonify({"error":
                        "Missing celebi_server_url/project_uuid/grant"}), 400
    if not archives:
        return jsonify({"error": "Missing archives"}), 400
    err = _validate_uuids([a.get("uuid") for a in archives])
    if err:
        return jsonify({"error": err}), 400

    staging_dir = _staging_for(project_uuid)
    if not staging_dir:
        return jsonify({"error":
                        "No staged pack for this project — call "
                        "/book-celebi-server/pack first"}), 400

    resolved = []
    for entry in archives:
        uuid = entry["uuid"]
        path = os.path.join(staging_dir, uuid + ".tar.gz")
        if not os.path.isfile(path):
            return jsonify({"error":
                            f"staged archive missing for {uuid}"}), 400
        if entry.get("sha256"):
            import hashlib
            with open(path, "rb") as fh:
                actual = hashlib.sha256(fh.read()).hexdigest()
            if actual != entry["sha256"]:
                return jsonify({"error":
                                f"staged archive sha256 mismatch for "
                                f"{uuid}"}), 400
        resolved.append({"uuid": uuid, "path": path,
                         "sha256": entry.get("sha256"),
                         "size": os.path.getsize(path)})

    def run(progress_cb):
        return celebi_server_booker.upload_booking_archives(
            server_url, project_uuid, resolved, grant,
            progress_cb=progress_cb)

    return _ndjson_stream(run)
```

In `Yuki/server/app.py`:
- Add `server_booking` to the `from .routes import (...)` list.
- Add `flask_app.register_blueprint(server_booking.bp)` after the `booking.bp` line.

Note for the test fixture: `server_booking` imports `config` at module level as `from ..config import config` — a singleton built at import time with the real HOME. The fixture above monkeypatches `server_booking._config`… but the module binds the name `config`, not `_config`. Fix the fixture (and note it in the plan): monkeypatch `server_booking.config` with a fresh `YukiConfig()` constructed after `monkeypatch.setenv("HOME", ...)`. The implementation code above uses bare `config.storage_path` — so the correct test patch target is `server_booking.config`. Adjust the fixture:

```python
@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(server_booking, "config", YukiConfig())
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c, str(tmp_path / ".Yuki" / "Storage")
```

- [ ] **Step 4: Run tests**

Run: `pytest UnitTest/test_celebi_server_booking_routes.py -v`
Expected: 6 passed

- [ ] **Step 5: Run whole suite**

Run: `pytest UnitTest -q`
Expected: all green (new blueprint must not break existing route tests; `create_app` gained one registration)

- [ ] **Step 6: Commit**

```bash
git add Yuki/server/routes/server_booking.py Yuki/server/app.py UnitTest/test_celebi_server_booking_routes.py
git commit -m "Add /book-celebi-server pack+upload NDJSON endpoints"
```

---

## Self-review notes

- **Spec coverage:** Yuki kernel pack/upload (§Yuki 1) → Tasks 1–2; NDJSON endpoints + app registration (§Yuki 2) → Task 3. ✔
- **Placeholder scan:** all steps contain exact code/commands. ✔
- **Type consistency:** `pack_booking_archives(storage_path, project_uuid, impression_uuids, staging_dir, progress_cb=None)` used identically in Task 1 and Task 3; `upload_booking_archives(server_url, project_uuid, archives, grant, progress_cb=None, verify_ssl=True, session=None)` identical in Task 2 and Task 3. ✔
- **Known friction point:** `YukiConfig` reads `HOME` at import; the Task 3 test patches `server_booking.config` (module attribute) rather than the global singleton — implementation must reference `config.storage_path` so the patch lands (code above does).
