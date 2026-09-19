"""Raw data remains collectable without a runner workflow ID."""
# pylint: disable=redefined-outer-name
import json
from pathlib import Path
from unittest import mock

import pytest
from flask import Flask

from Yuki.kernel.impression_storage import ImpressionStorage
from Yuki.kernel import remote_data_ops
from Yuki.server.routes import execution, upload, workflow


@pytest.fixture
def storage(tmp_path):
    """A raw-data impression with no workflow on any runner."""
    instance = ImpressionStorage.__new__(ImpressionStorage)
    instance.job_path = str(tmp_path)
    instance.project_uuid = "project"
    instance.impression = "impression"
    instance.runners = ["runner"]
    instance.runners_id = {"runner": "r1"}
    (tmp_path / "contents").mkdir()
    (tmp_path / "contents" / "celebi.yaml").write_text("environment: rawdata\n")
    return instance


def test_local_collect_and_status_through_routes(storage):
    """The CLI's collect-all and subsequent file count see uploaded data."""
    root = Path(storage.job_path) / "rawdata" / "nested"
    root.mkdir(parents=True)
    (root / "sample.root").write_bytes(b"data")
    app = Flask(__name__)
    for blueprint in (workflow.bp, execution.bp, upload.bp):
        app.register_blueprint(blueprint)
    with mock.patch.object(workflow, "ImpressionStorage", return_value=storage), \
         mock.patch.object(execution, "ImpressionStorage", return_value=storage), \
         mock.patch.object(upload.config, "get_job_path", return_value=storage.job_path):
        client = app.test_client()
        report = client.get("/collect-outputs/project/impression").get_json()
        assert report["yuki"]["skipped"] == [
            {"file": "nested/sample.root", "reason": "already in Yuki"}]
        rows = client.get("/file-status/project/impression/none").get_json()
        assert rows == [{"name": "nested/sample.root", "size": 4,
                         "type": "data", "in_runner": False, "in_yuki": True}]
        assert client.get("/export/project/impression/nested/sample.root").data == b"data"
    assert storage.file_status("logs") == []
    assert storage.file_status(machine="runner") == []


@pytest.fixture
def remote(storage):
    """A managed SSH dataset with nested data and a plot."""
    (Path(storage.job_path) / "remote.json").write_text(json.dumps({
        "host_runner_id": "r1", "remote_path": "/managed/data"}))
    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.walk_files.return_value = [
        ("nested/sample.root", "/managed/data/nested/sample.root", 4),
        ("plot.png", "/managed/data/plot.png", 3),
    ]
    ssh.get.side_effect = lambda source, dest: Path(dest).write_bytes(
        b"data" if source.endswith(".root") else b"png")
    with mock.patch.object(remote_data_ops, "_ssh_connection", return_value=ssh):
        yield storage, ssh


def test_remote_selection_repeat_and_status(remote):
    """Selectors, repeated collection and file status share the same files."""
    storage, ssh = remote
    report = storage.collect_files("stageout", "*.root")["runner"]
    assert report["collected"] == ["nested/sample.root"]
    assert not report["failed"]
    assert (Path(storage.job_path) / "r1/stageout/nested/sample.root").read_bytes() == b"data"
    rows = {row["name"]: row for row in storage.file_status()}
    assert rows["nested/sample.root"]["in_yuki"]
    assert not rows["plot.png"]["in_yuki"]
    assert storage.file_status("logs") == []
    assert not storage.collect_files("stageout", "*.root")["runner"]["collected"]
    assert ssh.get.call_count == 1
    assert storage.collect_outputs()["runner"]["collected"] == ["plot.png"]


def test_remote_light_collection_only_gets_plots(remote):
    """Default collection limits raw-data downloads to plots."""
    storage, _ssh = remote
    assert storage.collect()["runner"]["collected"] == ["plot.png"]
    assert storage.collect_files("logs", "all") == {}


def test_failed_remote_download_is_retryable(remote):
    """A partial download is removed so a retry can fetch the complete file."""
    storage, ssh = remote
    def fail_download(_source, target):
        Path(target).write_bytes(b"d")
        raise OSError("connection lost")
    ssh.get.side_effect = fail_download
    report = storage.collect_files("stageout", "*.root")["runner"]
    assert report["failed"][0]["reason"] == "connection lost"
    root = Path(storage.job_path) / "r1/stageout/nested"
    assert not list(root.iterdir())
    ssh.get.side_effect = lambda _source, target: Path(target).write_bytes(b"data")
    assert storage.collect_files("stageout", "*.root")["runner"]["collected"]


def test_remote_connection_failure_is_reported(remote):
    """SSH failures appear in the report rather than as empty success."""
    storage, ssh = remote
    ssh.__enter__.side_effect = OSError("offline")
    assert storage.collect_outputs()["runner"]["failed"] == [
        {"file": "<stageout>", "reason": "offline"}]


def test_remote_path_traversal_is_rejected(remote):
    """A malformed remote listing cannot write outside stageout."""
    storage, ssh = remote
    ssh.walk_files.return_value = [("../escape.root", "/escape.root", 4)]
    report = storage.collect_outputs()["runner"]
    assert report["failed"][0]["reason"] == "unsafe relative data path"
    ssh.get.assert_not_called()


@pytest.mark.parametrize("listing", ["offline", "empty", "stale"])
def test_collected_files_remain_visible_without_remote_listing(remote, listing):
    storage, ssh = remote
    storage.collect_outputs()
    if listing == "offline":
        ssh.__enter__.side_effect = OSError("offline")
    elif listing == "empty":
        ssh.walk_files.return_value = []
    else:
        (Path(storage.job_path) / "r1/stageout.filelist.json").write_text(
            json.dumps({"workflow_id": "remote-data", "files": [
                {"name": "plot.png", "size": 3}]}))
    result = storage.file_status(detailed=True, machine="runner")
    rows = {row["name"]: row for row in result["files"]}
    assert set(rows) == {"nested/sample.root", "plot.png"}
    assert rows["nested/sample.root"] == {
        "name": "nested/sample.root", "size": 4, "type": "data",
        "in_yuki": True, "in_runner": False}
    assert all(row["in_yuki"] for row in rows.values())
    assert storage.file_status(machine="other") == []
    if listing == "offline":
        assert result["notes"][0]["level"] == "error"


def test_purge_collected_rawdata_checks_live_copy_and_updates_distribution(remote):
    storage, _ssh = remote
    storage.collect_outputs()
    storage.update_distribution()
    root = Path(storage.job_path)
    assert "yuki" in json.loads((root / "distribution.json").read_text())["locations"]
    report = storage.purge_collected_data()
    assert report["freed_bytes"] == 7
    assert report["machines"]["runner"]["stageout"] == {"files": 2, "bytes": 7}
    assert not (root / "r1/stageout").exists()
    assert (root / "remote.json").exists()
    assert (root / "contents/celebi.yaml").exists()
    assert "yuki" not in json.loads((root / "distribution.json").read_text())["locations"]


@pytest.mark.parametrize("failure", ["offline", "missing", "truncated"])
def test_rawdata_purge_refuses_unverified_copy_unless_forced(remote, failure):
    storage, ssh = remote
    storage.collect_outputs()
    # A cached listing must not suffice to authorize deletion.
    storage.file_status()
    if failure == "offline":
        ssh.__enter__.side_effect = OSError("offline")
    elif failure == "missing":
        ssh.walk_files.return_value = []
    else:
        ssh.walk_files.return_value = [
            ("nested/sample.root", "/managed/data/nested/sample.root", 1),
            ("plot.png", "/managed/data/plot.png", 3)]
    report = storage.purge_collected_data()
    assert report["refused"]
    assert not report["running"]
    target = Path(storage.job_path) / "r1/stageout/nested/sample.root"
    assert target.read_bytes() == b"data"
    report = storage.purge_collected_data(force=True)
    assert report["freed_bytes"] == 7
    assert not target.exists()
