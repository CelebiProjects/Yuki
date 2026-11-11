"""Tests for project-scoped impression sharing through an SSH runner."""

import os
import shutil

from Yuki.kernel.storage.cache import (
    CACHE_COMPLETE_MARKER,
    CACHE_IN_PROGRESS_MARKER,
)
from Yuki.services import ssh_impression_transfer as sharing


class _FakeSftp:
    def __init__(self, root):
        self.root = str(root)

    def path(self, remote):
        return os.path.join(self.root, remote.lstrip("/"))


class _FakeSsh:
    def __init__(self, root):
        self._sftp = _FakeSftp(root)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def exists(self, path):
        return os.path.exists(self._sftp.path(path))

    def put(self, source, destination):
        target = self._sftp.path(destination)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copy2(source, target)

    def get(self, source, destination):
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.copy2(self._sftp.path(source), destination)

    def walk_files(self, remote):
        root = self._sftp.path(remote)
        if not os.path.isdir(root):
            return []
        result = []
        for directory, _dirs, files in os.walk(root):
            for name in files:
                path = os.path.join(directory, name)
                rel = os.path.relpath(path, root).replace(os.sep, "/")
                result.append((rel, remote.rstrip("/") + "/" + rel,
                               os.path.getsize(path)))
        return result


def _install_fake_runner(monkeypatch, remote):
    monkeypatch.setattr(sharing, "_connection", lambda _runner: _FakeSsh(remote))
    monkeypatch.setattr(
        sharing, "_share_root",
        lambda _runner, project, _yuki: f"/shared-impressions/{project}")

    def prepare(ssh, path):
        target = ssh._sftp.path(path)
        shutil.rmtree(target, ignore_errors=True)
        os.makedirs(target)
        open(os.path.join(target, CACHE_IN_PROGRESS_MARKER), "w").close()

    def finalize(ssh, path):
        target = ssh._sftp.path(path)
        os.remove(os.path.join(target, CACHE_IN_PROGRESS_MARKER))
        open(os.path.join(target, CACHE_COMPLETE_MARKER), "w").close()

    monkeypatch.setattr(sharing, "_prepare_remote", prepare)
    monkeypatch.setattr(sharing, "_finalize_remote", finalize)


def _impression(yuki_dir, project, impression, payload=b"result"):
    root = yuki_dir / "Storage" / project / impression
    (root / "machine" / "stageout").mkdir(parents=True)
    (root / "config.json").write_text("{}", encoding="utf-8")
    (root / "machine" / "stageout" / "result.bin").write_bytes(payload)


def test_export_then_import_only_selected_matches(monkeypatch, tmp_path):
    """The runner store persists exports and import obeys its UUID allow-list."""
    yuki_dir = tmp_path / "yuki"
    remote = tmp_path / "runner"
    yuki_dir.mkdir()
    remote.mkdir()
    _install_fake_runner(monkeypatch, remote)
    project = "project"
    first, second = "a" * 32, "b" * 32
    _impression(yuki_dir, project, first, b"first")
    _impression(yuki_dir, project, second, b"second")

    exported = sharing.run_share_job(
        "export-job", "runner-id", project, "export", [first, second],
        yuki_dir=str(yuki_dir))
    assert exported["impressions"] == [first, second]

    shutil.rmtree(yuki_dir / "Storage" / project)
    imported = sharing.run_share_job(
        "import-job", "runner-id", project, "import", [second],
        yuki_dir=str(yuki_dir))
    assert imported["impressions"] == [second]
    assert not (yuki_dir / "Storage" / project / first).exists()
    result = (yuki_dir / "Storage" / project / second /
              "machine" / "stageout" / "result.bin")
    assert result.read_bytes() == b"second"
    config = yuki_dir / "Storage" / project / second / "config.json"
    assert '"origin": "imported"' in config.read_text(encoding="utf-8")


def test_export_reports_missing_yuki_impressions(monkeypatch, tmp_path):
    """Selection remains explicit when Yuki lacks one requested impression."""
    yuki_dir = tmp_path / "yuki"
    remote = tmp_path / "runner"
    yuki_dir.mkdir()
    remote.mkdir()
    _install_fake_runner(monkeypatch, remote)
    first, missing = "a" * 32, "b" * 32
    _impression(yuki_dir, "project", first)

    result = sharing.run_share_job(
        "job", "runner-id", "project", "export", [first, missing],
        yuki_dir=str(yuki_dir))
    assert result["impressions"] == [first]
    assert result["missing"] == [missing]


def test_export_excludes_local_apd_credentials(monkeypatch, tmp_path):
    """Shared impressions never contain deposited or staged APD credentials."""
    yuki_dir = tmp_path / "yuki"
    remote = tmp_path / "runner"
    yuki_dir.mkdir()
    remote.mkdir()
    _install_fake_runner(monkeypatch, remote)
    project = "project"
    impression = "a" * 32
    _impression(yuki_dir, project, impression)
    source = yuki_dir / "Storage" / project / impression
    (source / "config.local.json").write_text(
        '{"apd_token":"secret"}', encoding="utf-8")
    (source / "contents").mkdir()
    (source / "contents" / "lbapi_key.json").write_text(
        '"secret"', encoding="utf-8")
    (source / "contents" / "payload.json").write_text(
        '{"safe":true}', encoding="utf-8")

    result = sharing.run_share_job(
        "job", "runner-id", project, "export", [impression],
        yuki_dir=str(yuki_dir))

    assert result["impressions"] == [impression]
    shared = remote / "shared-impressions" / project / impression
    assert not (shared / "config.local.json").exists()
    assert not (shared / "contents" / "lbapi_key.json").exists()
    assert (shared / "contents" / "payload.json").is_file()


def test_import_ignores_credentials_from_legacy_shared_copy(monkeypatch, tmp_path):
    """Old shared stores cannot reintroduce credential files during import."""
    yuki_dir = tmp_path / "yuki"
    remote = tmp_path / "runner"
    yuki_dir.mkdir()
    remote.mkdir()
    _install_fake_runner(monkeypatch, remote)
    project = "project"
    impression = "a" * 32
    shared = remote / "shared-impressions" / project / impression
    (shared / "contents").mkdir(parents=True)
    (shared / "config.json").write_text("{}", encoding="utf-8")
    (shared / "config.local.json").write_text(
        '{"apd_token":"secret"}', encoding="utf-8")
    (shared / "contents" / "lbapi_key.json").write_text(
        '"secret"', encoding="utf-8")
    (shared / "contents" / "payload.json").write_text(
        '{"safe":true}', encoding="utf-8")
    (shared / CACHE_COMPLETE_MARKER).touch()

    result = sharing.run_share_job(
        "job", "runner-id", project, "import", [impression],
        yuki_dir=str(yuki_dir))

    assert result["impressions"] == [impression]
    imported = yuki_dir / "Storage" / project / impression
    assert not (imported / "config.local.json").exists()
    assert not (imported / "contents" / "lbapi_key.json").exists()
    assert (imported / "contents" / "payload.json").is_file()


def test_existing_shared_copy_is_protected_without_force(monkeypatch, tmp_path):
    """An existing complete shared copy is skipped unless replacement is explicit."""
    yuki_dir = tmp_path / "yuki"
    remote = tmp_path / "runner"
    yuki_dir.mkdir()
    remote.mkdir()
    _install_fake_runner(monkeypatch, remote)
    impression = "a" * 32
    _impression(yuki_dir, "project", impression, b"old")
    sharing.run_share_job(
        "first", "runner-id", "project", "export", [impression],
        yuki_dir=str(yuki_dir))
    source = (yuki_dir / "Storage" / "project" / impression /
              "machine" / "stageout" / "result.bin")
    source.write_bytes(b"new")

    skipped = sharing.run_share_job(
        "skip", "runner-id", "project", "export", [impression],
        yuki_dir=str(yuki_dir))
    assert skipped["skipped"] == [impression]

    sharing.run_share_job(
        "force", "runner-id", "project", "export", [impression], force=True,
        yuki_dir=str(yuki_dir))
    remote_result = (remote / "shared-impressions" / "project" / impression /
                     "machine" / "stageout" / "result.bin")
    assert remote_result.read_bytes() == b"new"
