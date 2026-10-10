"""Tests for backend-aware cache_on_runner command generation."""
# pylint: disable=protected-access
import json
import os
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.jobs.container import ContainerJob


def _job(machine_id="m1", is_input=False, cache=True):
    """Build a ContainerJob stub exercising cache_commands paths."""
    job = object.__new__(ContainerJob)
    job.machine_id = machine_id
    job.is_input = is_input
    job.path = "/store/proj/imp123456"
    job.project_uuid = "proj"
    job.cache_on_runner = mock.MagicMock(return_value=cache)
    job.short_uuid = mock.MagicMock(return_value="abc1234")
    job.impression = mock.MagicMock(return_value="imp123456")
    return job


def _ssh_settings(tmp_path, remote_workdir="/remote/work"):
    """Write ssh runner settings into a config file in tmp_path."""
    yuki_dir = tmp_path
    with open(yuki_dir / "config.json", "w", encoding="utf-8") as f:
        json.dump({"runner_settings": {
            "m1": {"ssh_host": "h", "ssh_user": "u",
                   "remote_workdir": remote_workdir}}}, f)
    return yuki_dir


def test_cache_commands_reana_uses_eos(monkeypatch, tmp_path):
    """REANA cache commands copy stageout into the EOS mount point."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".Yuki", exist_ok=True)
    cfg = ConfigFile(str(tmp_path / ".Yuki" / "config.json"))
    cfg.write_variable("eos_mount_point", {"m1": "/eos/home/user"})
    job = _job()
    with mock.patch.dict(os.environ, {"HOME": str(tmp_path)}):
        commands = job._cache_commands("m1", "reana")
    assert commands == [
        "mkdir -p /eos/home/user/proj/imp123456/",
        "cp -r stageout/* /eos/home/user/proj/imp123456/",
    ]


def test_cache_commands_ssh_uses_impressions_dir(monkeypatch, tmp_path):
    """SSH publication is fenced, cleaned on failure, and marked last."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _ssh_settings(tmp_path)
    job = _job()
    commands = job._cache_commands("m1", "ssh")
    assert len(commands) == 1
    command = commands[0]
    assert command.count("[ -e ../yuki.failed ]") == 2
    assert "cp -r stageout/. /remote/work/impressions/proj/imp123456/" in command
    assert "rm -rf -- /remote/work/impressions/proj/imp123456" in command
    assert "/imp123456/.yuki-cache-in-progress" in command
    assert "/imp123456/.yuki-cache-complete" in command
    assert command.index("cp -r stageout/.") < command.index(
        "/imp123456/.yuki-cache-complete")
    assert "chmod -R a-w /remote/work/impressions/proj/imp123456" in command


def test_cache_commands_ssh_execute_successfully(monkeypatch, tmp_path):
    """A successful copy replaces a read-only cache and marks completion."""
    remote = tmp_path / "remote"
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _ssh_settings(tmp_path, str(remote))
    job = _job()
    command = job._cache_commands("m1", "ssh")[0]
    work = tmp_path / "workflow" / "impabc1234"
    (work / "stageout").mkdir(parents=True)
    (work / "stageout" / "new.txt").write_text("new", encoding="utf-8")
    cache = remote / "impressions" / "proj" / "imp123456"
    cache.mkdir(parents=True)
    (cache / "old.txt").write_text("old", encoding="utf-8")
    os.chmod(cache / "old.txt", 0o444)

    import subprocess
    subprocess.run(["bash", "-c", command], cwd=work, check=True)

    assert not (cache / "old.txt").exists()
    assert (cache / "new.txt").read_text(encoding="utf-8") == "new"
    assert (cache / ".yuki-cache-complete").is_file()


def test_cache_commands_ssh_skip_failed_workflow(monkeypatch, tmp_path):
    """A fenced zombie workflow leaves the existing cache untouched."""
    remote = tmp_path / "remote"
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _ssh_settings(tmp_path, str(remote))
    job = _job()
    command = job._cache_commands("m1", "ssh")[0]
    workflow = tmp_path / "workflow"
    work = workflow / "impabc1234"
    (work / "stageout").mkdir(parents=True)
    (work / "stageout" / "new.txt").write_text("new", encoding="utf-8")
    (workflow / "yuki.failed").write_text("failed", encoding="utf-8")
    cache = remote / "impressions" / "proj" / "imp123456"
    cache.mkdir(parents=True)
    (cache / "old.txt").write_text("old", encoding="utf-8")

    import subprocess
    subprocess.run(["bash", "-c", command], cwd=work, check=True)

    assert (cache / "old.txt").read_text(encoding="utf-8") == "old"
    assert not (cache / "new.txt").exists()


def test_cache_commands_ssh_remove_failed_copy(monkeypatch, tmp_path):
    """A copy error leaves no directory that could be treated as cache."""
    remote = tmp_path / "remote"
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _ssh_settings(tmp_path, str(remote))
    command = _job()._cache_commands("m1", "ssh")[0]
    work = tmp_path / "workflow" / "impabc1234"
    work.mkdir(parents=True)
    cache = remote / "impressions" / "proj" / "imp123456"

    import subprocess
    result = subprocess.run(
        ["bash", "-c", command], cwd=work, check=False,
        capture_output=True, text=True)

    assert result.returncode != 0
    assert not cache.exists()


def test_cache_commands_native_noop(monkeypatch, tmp_path):
    """Native and dry backends generate no cache commands."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    job = _job()
    assert not job._cache_commands("m1", "native")
    assert not job._cache_commands("m1", "dry")


def test_cache_commands_disabled_noop(monkeypatch, tmp_path):
    """A job with cache_on_runner disabled emits no cache commands."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _ssh_settings(tmp_path)
    job = _job(cache=False)
    assert not job._cache_commands("m1", "ssh")


def test_cache_commands_input_job_noop(monkeypatch, tmp_path):
    """Input jobs never emit cache commands."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _ssh_settings(tmp_path)
    job = _job(is_input=True)
    assert not job._cache_commands("m1", "ssh")


def test_setup_commands_ssh_fetches_from_impressions(monkeypatch, tmp_path):
    """SSH setup commands link cached data from the remote impressions dir
    into stageout instead of copying."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _ssh_settings(tmp_path)
    job = _job()
    commands = job.setup_commands("ssh")
    assert commands == [
        "mkdir -p impabc1234/stageout",
        "ln -s /remote/work/impressions/proj/imp123456/* impabc1234/stageout/",
    ]


def test_setup_commands_native_no_cache_source(monkeypatch, tmp_path):
    """Native backends have no cache source, so only mkdir is emitted."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    job = _job()
    assert job.setup_commands("native") == ["mkdir -p impabc1234/stageout"]
