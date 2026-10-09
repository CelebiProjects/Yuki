"""Tests for Yuki.services.result_transfer."""
# pylint: disable=missing-function-docstring
import json
import os
import re
from unittest import mock

import pytest
from CelebiChrono.utils.metadata import ConfigFile

from Yuki.services import result_transfer


def test_resolve_yuki_dir_defaults_to_home():
    with mock.patch.dict(os.environ, {}, clear=True):
        assert result_transfer._resolve_yuki_dir() == os.path.expanduser("~/.Yuki")


def test_resolve_yuki_dir_uses_yukidir_env():
    with mock.patch.dict(os.environ, {"YUKIDIR": "/tmp/yuki-test"}):
        assert result_transfer._resolve_yuki_dir() == "/tmp/yuki-test"


def test_parse_location_yuki():
    assert result_transfer._parse_location("yuki") == ("yuki", None)


def test_parse_location_runner():
    assert result_transfer._parse_location("runner:pkufarm") == ("runner", "pkufarm")


def test_parse_location_runner_missing_id():
    with pytest.raises(ValueError):
        result_transfer._parse_location("runner:")


def test_list_local_files(tmp_path):
    (tmp_path / "a.txt").write_text("hello")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_text("world")
    files = result_transfer._list_local_files(str(tmp_path))
    assert sorted(f["name"] for f in files) == ["a.txt", "sub/b.txt"]
    size_map = {f["name"]: f["size"] for f in files}
    assert size_map["a.txt"] == 5


def test_list_local_files_missing_dir():
    files = result_transfer._list_local_files("/nonexistent/path")
    assert not files


def test_list_local_files_rejects_traversal(tmp_path, monkeypatch):
    def bad_walk(_root):
        yield str(tmp_path), [], ["../etc/passwd"]
    monkeypatch.setattr(result_transfer.os, "walk", bad_walk)
    with pytest.raises(ValueError, match="path traversal not allowed"):
        result_transfer._list_local_files(str(tmp_path))


def test_resolve_path_yuki(tmp_path, monkeypatch):
    yuki_dir = tmp_path / "yuki"
    yuki_dir.mkdir()
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))
    project_uuid = "proj-uuid"
    impression = "imp-uuid"
    path, runner_id = result_transfer._resolve_path("yuki", project_uuid, impression)
    assert path == str(yuki_dir / "Storage" / project_uuid / impression)
    assert runner_id is None


def test_list_yuki_stageout_merges_machine_dirs(tmp_path):
    job_path = tmp_path / "job"
    (job_path / "m1" / "stageout").mkdir(parents=True)
    (job_path / "m2" / "stageout").mkdir(parents=True)
    (job_path / "m1" / "stageout" / "a.txt").write_text("hello")
    (job_path / "m2" / "stageout" / "sub").mkdir()
    (job_path / "m2" / "stageout" / "sub" / "b.txt").write_text("world")
    files = result_transfer._list_yuki_stageout(str(job_path))
    assert sorted(f["name"] for f in files) == ["a.txt", "sub/b.txt"]
    full_map = {f["name"]: f["full"] for f in files}
    assert full_map["a.txt"] == str(job_path / "m1" / "stageout" / "a.txt")
    assert full_map["sub/b.txt"] == str(job_path / "m2" / "stageout" / "sub" / "b.txt")


def test_list_yuki_stageout_dedupes_by_name(tmp_path):
    job_path = tmp_path / "job"
    (job_path / "m1" / "stageout").mkdir(parents=True)
    (job_path / "m2" / "stageout").mkdir(parents=True)
    (job_path / "m1" / "stageout" / "a.txt").write_text("first")
    (job_path / "m2" / "stageout" / "a.txt").write_text("second")
    files = result_transfer._list_yuki_stageout(str(job_path))
    assert [f["name"] for f in files] == ["a.txt"]
    assert files[0]["full"] == str(job_path / "m1" / "stageout" / "a.txt")


def test_list_yuki_stageout_missing_dir():
    files = result_transfer._list_yuki_stageout("/nonexistent/job")
    assert not files


def test_resolve_path_runner(tmp_path, monkeypatch):
    yuki_dir = tmp_path / "yuki"
    yuki_dir.mkdir()
    config_path = yuki_dir / "config.json"
    config_path.write_text(json.dumps({
        "runners_id": {"pkufarm": "runner-uuid"},
        "runners": ["pkufarm"],
        "runner_settings": {
            "runner-uuid": {"ssh_host": "host", "ssh_user": "user",
                            "remote_workdir": "/remote/work"}
        }
    }))
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))
    path, runner_id = result_transfer._resolve_path(
        "runner:pkufarm", "proj-uuid", "imp-uuid")
    assert path == "/remote/work/impressions/proj-uuid/imp-uuid"
    assert runner_id == "runner-uuid"


def test_list_remote_files_uses_ssh_walk():
    ssh = mock.MagicMock()
    ssh.walk_files.return_value = [
        ("a.txt", "/remote/a.txt", 10),
        ("sub/b.txt", "/remote/sub/b.txt", 20),
    ]
    files = result_transfer._list_remote_files(ssh, "/remote/root", "*.txt")
    assert sorted(f["name"] for f in files) == ["a.txt", "sub/b.txt"]


def test_list_remote_files_rejects_traversal():
    ssh = mock.MagicMock()
    ssh.exists.return_value = True
    ssh.walk_files.return_value = [
        ("../etc/passwd", "/remote/../etc/passwd", 10),
    ]
    with pytest.raises(ValueError, match="path traversal not allowed"):
        result_transfer._list_remote_files(ssh, "/remote/root")


def test_copy_local_to_local(tmp_path):
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    (src / "a.txt").write_text("hello")
    (src / "sub").mkdir()
    (src / "sub" / "b.txt").write_text("world")
    progress = {"bytes_done": 0}
    report = {"transferred": [], "skipped": [], "failed": []}
    result_transfer._copy_local_to_local(
        str(src), str(dst), force=False,
        progress=progress, report=report)
    assert sorted(report["transferred"]) == ["a.txt", "sub/b.txt"]
    assert (dst / "a.txt").read_text() == "hello"
    assert (dst / "sub" / "b.txt").read_text() == "world"
    assert progress["bytes_done"] == 10


def test_copy_local_to_local_skips_existing(tmp_path):
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    (src / "a.txt").write_text("hello")
    (dst / "a.txt").write_text("existing")
    report = {"transferred": [], "skipped": [], "failed": []}
    result_transfer._copy_local_to_local(
        str(src), str(dst), force=False,
        progress={"bytes_done": 0}, report=report)
    assert report["skipped"] == ["a.txt"]
    assert (dst / "a.txt").read_text() == "existing"


def test_copy_local_to_remote(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("hello")
    ssh = mock.MagicMock()
    ssh.exists.return_value = False
    report = {"transferred": [], "skipped": [], "failed": []}
    result_transfer._copy_local_to_remote(
        str(src), "/remote/dst", ssh, force=False,
        progress={"bytes_done": 0}, report=report)
    ssh.put.assert_called_once()
    assert report["transferred"] == ["a.txt"]


def test_copy_remote_to_local(tmp_path):
    dst = tmp_path / "dst"
    ssh = mock.MagicMock()
    ssh.exists.return_value = True
    ssh.walk_files.return_value = [("a.txt", "/remote/a.txt", 5)]
    report = {"transferred": [], "skipped": [], "failed": []}
    result_transfer._copy_remote_to_local(
        "/remote/src", str(dst), ssh, force=False,
        progress={"bytes_done": 0}, report=report)
    ssh.get.assert_called_once_with("/remote/a.txt", mock.ANY)
    assert report["transferred"] == ["a.txt"]


def test_run_transfer_yuki_to_yuki_rejected(tmp_path, monkeypatch):
    yuki_dir = tmp_path / "yuki"
    yuki_dir.mkdir()
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))
    with pytest.raises(ValueError, match="source and destination cannot both be yuki"):
        result_transfer.run_transfer(
            "job1", "proj", "imp", "yuki", "yuki",
            pattern=None, force=False, yuki_dir=str(yuki_dir))
    progress_path = yuki_dir / "transfer-progress" / "job1.json"
    progress = json.loads(progress_path.read_text())
    assert progress["status"] == "failed"
    assert "cannot both be yuki" in progress["error"]


def test_run_transfer_invalid_location_writes_failed(tmp_path, monkeypatch):
    yuki_dir = tmp_path / "yuki"
    yuki_dir.mkdir()
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))
    with pytest.raises(ValueError, match="invalid location"):
        result_transfer.run_transfer(
            "job2", "proj", "imp", "foo", "yuki",
            pattern=None, force=False, yuki_dir=str(yuki_dir))
    progress_path = yuki_dir / "transfer-progress" / "job2.json"
    progress = json.loads(progress_path.read_text())
    assert progress["status"] == "failed"
    assert "invalid location" in progress["error"]


def _write_runner_config(yuki_dir):
    """Write a runner config registering pkufarm -> runner-uuid."""
    config_path = yuki_dir / "config.json"
    config_path.write_text(json.dumps({
        "runners_id": {"pkufarm": "runner-uuid"},
        "runners": ["pkufarm"],
        "runner_settings": {
            "runner-uuid": {"ssh_host": "host", "ssh_user": "user",
                            "remote_workdir": "/remote/work"}
        }
    }))
    return config_path


def _patch_server_config(yuki_dir, project="proj", impression="imp"):
    """Point the global server config at the temp yuki dir."""
    job_path = str(yuki_dir / "Storage" / project / impression)
    fake_config = mock.Mock()
    fake_config.get_job_path.return_value = job_path
    fake_config.get_config_file.return_value = ConfigFile(
        str(yuki_dir / "config.json"))
    fake_config.get_job_config_path.return_value = os.path.join(
        job_path, "config.json")
    return mock.patch("Yuki.server.config.config", fake_config)


class FakeSsh:
    """Dict-backed SSH stand-in: put/get mutate a fake remote filesystem."""

    def __init__(self):
        """Init."""
        self.files = {}  # remote path -> size
        self.exec_script = None  # callable(command) -> (out, err, code)

    def exec(self, command, timeout=300):
        """Run a command through the exec script if one is set."""
        if self.exec_script:
            return self.exec_script(command)
        return "", "", 0

    def exists(self, path):
        """Return whether the remote path exists (files or directories)."""
        return path in self.files or any(
            p.startswith(path) for p in self.files)

    def walk_files(self, root):
        """Yield (rel, full, size) for files under root."""
        for path, size in sorted(self.files.items()):
            if path.startswith(root):
                yield os.path.relpath(path, root), path, size

    def put(self, src, dst):
        """Copy a local file into the fake remote filesystem."""
        self.files[dst] = os.path.getsize(src)

    def get(self, src, dst):
        """Copy a fake remote file to the local filesystem."""
        with open(src, "rb") as sf, open(dst, "wb") as df:
            df.write(sf.read())


def test_run_transfer_with_mocked_remote(tmp_path, monkeypatch):
    yuki_dir = tmp_path / "yuki"
    yuki_dir.mkdir()
    storage = yuki_dir / "Storage" / "proj" / "imp" / "machine-a" / "stageout"
    storage.mkdir(parents=True)
    (storage / "a.txt").write_text("hello")
    _write_runner_config(yuki_dir)
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))

    with mock.patch("Yuki.services.result_transfer._ssh_connection") as ssh_conn, \
         _patch_server_config(yuki_dir):
        ssh = mock.MagicMock()
        ssh.exists.return_value = False
        ssh.walk_files.return_value = []
        ssh_conn.return_value.__enter__ = mock.Mock(return_value=ssh)
        ssh_conn.return_value.__exit__ = mock.Mock(return_value=False)
        report = result_transfer.run_transfer(
            "job1", "proj", "imp", "yuki", "runner:pkufarm",
            pattern=None, force=False, yuki_dir=str(yuki_dir))
        assert report["transferred"] == ["a.txt"]
        ssh.put.assert_called_once()

        progress_path = yuki_dir / "transfer-progress" / "job1.json"
        progress = json.loads(progress_path.read_text())
        assert progress["status"] == "done"
        assert progress["transferred"] == 1
        assert progress["skipped"] == 0
        assert progress["failed"] == 0
        assert progress["bytes_total"] == 5
        assert progress["report"]["transferred"] == ["a.txt"]


def test_run_transfer_updates_distribution_registry(tmp_path, monkeypatch):
    yuki_dir = tmp_path / "yuki"
    yuki_dir.mkdir()
    storage = yuki_dir / "Storage" / "proj" / "imp" / "machine-a" / "stageout"
    storage.mkdir(parents=True)
    (storage / "a.txt").write_text("hello")
    _write_runner_config(yuki_dir)
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))

    ssh = FakeSsh()
    with mock.patch("Yuki.services.result_transfer._ssh_connection") as ssh_conn, \
         _patch_server_config(yuki_dir):
        ssh_conn.return_value.__enter__ = mock.Mock(return_value=ssh)
        ssh_conn.return_value.__exit__ = mock.Mock(return_value=False)
        result_transfer.run_transfer(
            "job1", "proj", "imp", "yuki", "runner:pkufarm",
            pattern=None, force=False, yuki_dir=str(yuki_dir))

    dist_path = yuki_dir / "Storage" / "proj" / "imp" / "distribution.json"
    dist = json.loads(dist_path.read_text())
    cache = dist["locations"]["runner:pkufarm"]["cache"]
    assert cache["origin"] == "transferred"
    assert cache["files"] == 1
    assert cache["bytes"] == 5
    assert dist["locations"]["yuki"]["origin"] == "collected"


def test_run_transfer_runner_to_yuki_lands_in_machine_stageout(
        tmp_path, monkeypatch):
    yuki_dir = tmp_path / "yuki"
    yuki_dir.mkdir()
    _write_runner_config(yuki_dir)
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))

    with mock.patch("Yuki.services.result_transfer._ssh_connection") as ssh_conn, \
         _patch_server_config(yuki_dir):
        ssh = mock.MagicMock()
        ssh.exists.return_value = True
        ssh.walk_files.return_value = [("a.txt", "/remote/a.txt", 5)]
        ssh_conn.return_value.__enter__ = mock.Mock(return_value=ssh)
        ssh_conn.return_value.__exit__ = mock.Mock(return_value=False)
        report = result_transfer.run_transfer(
            "job2", "proj", "imp", "runner:pkufarm", "yuki",
            pattern=None, force=False, yuki_dir=str(yuki_dir))
        assert report["transferred"] == ["a.txt"]
        expected_dst = (yuki_dir / "Storage" / "proj" / "imp"
                        / "runner-uuid" / "stageout" / "a.txt")
        ssh.get.assert_called_once_with("/remote/a.txt", str(expected_dst))


def _write_reana_runner_config(yuki_dir):
    """Extend the runner config with a reana backend runner."""
    config_path = yuki_dir / "config.json"
    config_data = json.loads(config_path.read_text())
    config_data["runners_id"]["reanafarm"] = "reana-uuid"
    config_data["runners"].append("reanafarm")
    config_data["backend_types"] = {"runner-uuid": "ssh",
                                    "reana-uuid": "reana"}
    config_data["urls"] = {"reana-uuid": "https://reana.example"}
    config_data["tokens"] = {"reana-uuid": "tok-123"}
    config_path.write_text(json.dumps(config_data))


def _write_reana_job_config(yuki_dir):
    """Record the reana runner's workflow id in the job config."""
    job_dir = yuki_dir / "Storage" / "proj" / "8355eae8" / "reana-uuid"
    job_dir.mkdir(parents=True)
    (job_dir / "config.json").write_text(json.dumps({"workflow": "wf-1"}))


def _reana_exec_script(ssh, files):
    """Script reana-cli commands on the fake ssh host."""
    prefix = "imp8355eae/stageout/"

    def script(cmd):
        if "which reana-client" in cmd:
            return "/usr/local/bin/reana-client", "", 0
        if "ls -w 'wf-1'" in cmd:
            return json.dumps([
                {"name": prefix + rel, "size": size}
                for rel, size in files
            ]), "", 0
        match = re.search(r"cd '([^']+)'.*download -w 'wf-1' '([^']+)'", cmd)
        if match:
            dest, name = match.groups()
            rel = name[len(prefix):]
            ssh.files[os.path.join(dest, name)] = dict(files)[rel]
            return "", "", 0
        match = re.search(r"mv -f '([^']+)' '([^']+)'", cmd)
        if match:
            src, dst = match.groups()
            ssh.files[dst] = ssh.files.pop(src)
            return "", "", 0
        return "", "", 0

    return script


def test_run_transfer_reana_to_ssh_pulls_via_reana_cli(tmp_path, monkeypatch):
    yuki_dir = tmp_path / "yuki"
    yuki_dir.mkdir()
    _write_runner_config(yuki_dir)
    _write_reana_runner_config(yuki_dir)
    _write_reana_job_config(yuki_dir)
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))

    ssh = FakeSsh()
    ssh.exec_script = _reana_exec_script(ssh, [("a.txt", 5), ("sub/b.txt", 4)])

    with mock.patch("Yuki.services.result_transfer._ssh_connection") as ssh_conn, \
         _patch_server_config(yuki_dir, impression="8355eae8"):
        ssh_conn.return_value.__enter__ = mock.Mock(return_value=ssh)
        ssh_conn.return_value.__exit__ = mock.Mock(return_value=False)
        report = result_transfer.run_transfer(
            "job1", "proj", "8355eae8", "runner:reanafarm", "runner:pkufarm",
            pattern=None, force=False, yuki_dir=str(yuki_dir))

    assert sorted(report["transferred"]) == ["a.txt", "sub/b.txt"]
    dist_path = (yuki_dir / "Storage" / "proj" / "8355eae8"
                 / "distribution.json")
    dist = json.loads(dist_path.read_text())
    cache = dist["locations"]["runner:pkufarm"]["cache"]
    assert cache["origin"] == "transferred"
    assert cache["files"] == 2
    assert cache["bytes"] == 9


def test_run_transfer_reana_to_ssh_missing_cli_fails(tmp_path, monkeypatch):
    yuki_dir = tmp_path / "yuki"
    yuki_dir.mkdir()
    _write_runner_config(yuki_dir)
    _write_reana_runner_config(yuki_dir)
    _write_reana_job_config(yuki_dir)
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))

    ssh = FakeSsh()
    ssh.exec_script = lambda cmd: ("", "not found", 1) if "which" in cmd \
        else ("", "", 0)

    with mock.patch("Yuki.services.result_transfer._ssh_connection") as ssh_conn, \
         _patch_server_config(yuki_dir, impression="8355eae8"):
        ssh_conn.return_value.__enter__ = mock.Mock(return_value=ssh)
        ssh_conn.return_value.__exit__ = mock.Mock(return_value=False)
        with pytest.raises(RuntimeError, match="reana-cli"):
            result_transfer.run_transfer(
                "job2", "proj", "8355eae8", "runner:reanafarm",
                "runner:pkufarm", pattern=None, force=False,
                yuki_dir=str(yuki_dir))

    progress_path = yuki_dir / "transfer-progress" / "job2.json"
    progress = json.loads(progress_path.read_text())
    assert progress["status"] == "failed"
    assert "reana-cli" in progress["error"]
