"""Tests for ImpressionStorage.purge_collected_data."""
# pylint: disable=protected-access
import json
import os
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile

from Yuki.kernel import impression_storage as ims


def _storage(tmp_path):
    """Build an ImpressionStorage without touching the global config."""
    storage = ims.ImpressionStorage.__new__(ims.ImpressionStorage)
    storage.project_uuid = "proj-1"
    storage.impression = "imp7"
    storage.job_path = str(tmp_path / "job")
    storage.runners = ["cern"]
    storage.runners_id = {"cern": "runner-1"}
    return storage


def _workflow(tmp_path, status="finished", backend="ssh",  # pylint: disable=too-many-arguments,too-many-positional-arguments
              purged=False, remote=None, listing_error=None):
    """Build a workflow double whose mirror config lives under tmp_path."""
    wf = mock.Mock()
    wf.status.return_value = status
    wf.backend_type.return_value = backend
    wf.path = str(tmp_path / "wf-dir")
    os.makedirs(wf.path, exist_ok=True)
    if purged:
        ConfigFile(os.path.join(wf.path, "config.json")).write_variable(
            "workspace_purged_at", "2026-09-04T00:00:00+00:00")

    def list_runner_files(_impression, kind):
        if listing_error is not None:
            raise listing_error
        return [{"name": n, "size": 1} for n in (remote or {}).get(kind, [])]

    wf.list_runner_files.side_effect = list_runner_files
    return wf


def _job():
    """Job double for a runner context."""
    job = mock.Mock()
    job.workflow_id.return_value = "wf-1"
    return job


def _seed_collected(storage):
    """Create stageout/logs/watermarks plus markers and kept metadata."""
    machine_dir = os.path.join(storage.job_path, "runner-1")
    stageout = os.path.join(machine_dir, "stageout")
    plots = os.path.join(stageout, "plots")
    os.makedirs(plots, exist_ok=True)
    with open(os.path.join(stageout, "a.root"), "wb") as f:
        f.write(b"12345")
    with open(os.path.join(plots, "b.png"), "wb") as f:
        f.write(b"123")
    logs = os.path.join(machine_dir, "logs")
    os.makedirs(logs, exist_ok=True)
    with open(os.path.join(logs, "x.log"), "wb") as f:
        f.write(b"1")
    watermarks = os.path.join(machine_dir, "watermarks")
    os.makedirs(watermarks, exist_ok=True)
    with open(os.path.join(watermarks, "w.png"), "wb") as f:
        f.write(b"12")
    for marker in ("stageout.downloaded", "logs.downloaded"):
        with open(os.path.join(machine_dir, marker), "w", encoding="utf-8") as f:
            f.write("")
    # Kept metadata: a saved listing and the run config.
    with open(os.path.join(machine_dir, "stageout.filelist.json"),
              "w", encoding="utf-8") as f:
        json.dump({"workflow_id": "wf-1", "files": []}, f)
    with open(os.path.join(machine_dir, "config.json"), "w",
              encoding="utf-8") as f:
        json.dump({"workflow": "wf-1"}, f)
    return machine_dir


def _bind_contexts(storage, wf, job=None):
    """Make purge and the recompute see (cern, job, wf) contexts."""
    storage._get_runner_contexts = lambda: [("cern", job or _job(), wf)]
    storage._remote_hosted_files = lambda _kind: ([], None)


def test_purge_removes_collected_data_and_markers(tmp_path):
    """Collected dirs and markers are removed; listings/config are kept."""
    storage = _storage(tmp_path)
    wf = _workflow(tmp_path, remote={
        "stageout": ["a.root", "plots/b.png"], "logs": ["x.log"]})
    _bind_contexts(storage, wf)
    machine_dir = _seed_collected(storage)

    report = storage.purge_collected_data()

    assert report["purged"] is True
    entry = report["machines"]["cern"]
    assert entry["stageout"]["files"] == 2
    assert entry["stageout"]["bytes"] == 8
    assert entry["logs"]["files"] == 1
    assert entry["watermarks"]["files"] == 1
    assert report["freed_bytes"] == 8 + 1 + 2

    for kind in ("stageout", "logs", "watermarks"):
        assert not os.path.exists(os.path.join(machine_dir, kind))
    for marker in ("stageout.downloaded", "logs.downloaded"):
        assert not os.path.exists(os.path.join(machine_dir, marker))
    assert os.path.exists(
        os.path.join(machine_dir, "stageout.filelist.json"))
    assert os.path.exists(os.path.join(machine_dir, "config.json"))

    # The registry recompute drops the yuki entry (nothing collected).
    dist_path = os.path.join(storage.job_path, "distribution.json")
    assert os.path.exists(dist_path)
    with open(dist_path, encoding="utf-8") as f:
        dist = json.load(f)
    assert "yuki" not in dist["locations"]


def test_purge_refuses_running_workflow(tmp_path):
    """A running workflow blocks the purge even with force."""
    storage = _storage(tmp_path)
    wf = _workflow(tmp_path, status="running")
    _bind_contexts(storage, wf)
    machine_dir = _seed_collected(storage)

    report = storage.purge_collected_data(force=True)

    assert "refused" in report
    assert report["running"] is True
    assert os.path.exists(os.path.join(machine_dir, "stageout", "a.root"))


def test_purge_refuses_purged_workspace(tmp_path):
    """Without force, a purged runner workspace blocks the purge."""
    storage = _storage(tmp_path)
    wf = _workflow(tmp_path, purged=True, remote={"stageout": ["a.root"]})
    _bind_contexts(storage, wf)
    machine_dir = _seed_collected(storage)

    report = storage.purge_collected_data()

    assert "refused" in report
    assert "workspace" in report["refused"]
    assert os.path.exists(os.path.join(machine_dir, "stageout", "a.root"))

    report = storage.purge_collected_data(force=True)
    assert report["purged"] is True


def test_purge_refuses_missing_runner_files(tmp_path):
    """Files absent from the runner listing block the purge without force."""
    storage = _storage(tmp_path)
    wf = _workflow(tmp_path, remote={"stageout": ["b.root"]})
    _bind_contexts(storage, wf)
    machine_dir = _seed_collected(storage)

    report = storage.purge_collected_data()

    assert "refused" in report
    assert "stageout" in report["refused"]
    assert os.path.exists(os.path.join(machine_dir, "stageout", "a.root"))

    report = storage.purge_collected_data(force=True)
    assert report["purged"] is True


def test_purge_refuses_non_ssh_backend(tmp_path):
    """A reana backend cannot be verified; force is required."""
    storage = _storage(tmp_path)
    wf = _workflow(tmp_path, backend="reana")
    _bind_contexts(storage, wf)
    machine_dir = _seed_collected(storage)

    report = storage.purge_collected_data()

    assert "refused" in report
    assert "force" in report["refused"]
    assert os.path.exists(os.path.join(machine_dir, "stageout", "a.root"))

    report = storage.purge_collected_data(force=True)
    assert report["purged"] is True


def test_purge_refuses_unreachable_runner(tmp_path):
    """A listing failure blocks the purge without force."""
    storage = _storage(tmp_path)
    wf = _workflow(tmp_path, listing_error=OSError("connection refused"))
    _bind_contexts(storage, wf)
    machine_dir = _seed_collected(storage)

    report = storage.purge_collected_data()

    assert "refused" in report
    assert os.path.exists(os.path.join(machine_dir, "stageout", "a.root"))

    report = storage.purge_collected_data(force=True)
    assert report["purged"] is True


def test_purge_without_collected_data_is_noop(tmp_path):
    """An impression with nothing collected purges nothing."""
    storage = _storage(tmp_path)
    wf = _workflow(tmp_path)
    _bind_contexts(storage, wf)

    report = storage.purge_collected_data()

    assert report["purged"] is True
    assert report["machines"] == {}
    assert report["freed_bytes"] == 0
