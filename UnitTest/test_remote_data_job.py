"""Tests for the register-remote-data celery job."""
import importlib
import json
import os
import shlex
from unittest import mock

import pytest

from CelebiChrono.utils.file_utils import dir_md5
from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel import remote_data_ops
from Yuki.kernel.detached_copy import LAUNCH_SCRIPT, READ_STATE_SCRIPT

config_module = importlib.import_module("Yuki.server.config")


class FakeSsh:
    """Records commands; exec returns (out, err, code)."""
    def __init__(self, md5_out):
        self.md5_out = md5_out
        self.commands = []
        self.made_dirs = []
        self.copy_state = {"status": "done"}
        self.copy_started = False

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def mkdir_p(self, path):
        """Record the created directory."""
        self.made_dirs.append(path)

    def exec(self, command, timeout=None):  # pylint: disable=unused-argument
        """Record the command and answer md5 queries from the fixture."""
        self.commands.append(command)
        if command.startswith("python3 -c"):
            if shlex.split(command)[2] == READ_STATE_SCRIPT:
                return json.dumps(self.copy_state if self.copy_started else None), "", 0
            return self.md5_out, "", 0
        return "", "", 0

    def exec_start_detached(self, command):
        """Record the launcher without running a copy in the worker."""
        self.commands.append(command)
        self.copy_started = True
        return "", "", 0


def _fixture_data(tmp_path):
    data = tmp_path / "data"
    os.makedirs(data / "sub")
    with open(data / "a.txt", "w", encoding="utf-8") as f:
        f.write("alpha")
    with open(data / "sub" / "b.txt", "w", encoding="utf-8") as f:
        f.write("beta")
    return data


def _impression_fixture(tmp_path, project="proj", imp="imp-1",  # pylint: disable=too-many-arguments,too-many-positional-arguments
                        md5="abc123", descriptor="mydata",
                        status="running", source="/src/data"):
    """A synthesized impression record for the copy/reuse phases."""
    imp_dir = tmp_path / "Storage" / project / imp
    (imp_dir / "contents").mkdir(parents=True)
    with open(imp_dir / "contents" / "celebi.yaml", "w",
              encoding="utf-8") as f:
        f.write(f"environment: rawdata\nuuid: {md5}\ndescriptor: {descriptor}\n")
    marker = ConfigFile(str(imp_dir / "remote.json"))
    marker.write_variable("host_runner_id", "r1")
    marker.write_variable("source_path", source)
    marker.write_variable("remote_path",
                          f"/tmp/yuki-workflows/impressions/{project}/{imp}")
    status_file = ConfigFile(str(imp_dir / "status.json"))
    status_file.write_variable("status", status)
    return imp_dir


def test_register_remote_data_job_end_to_end(monkeypatch, tmp_path):
    """The hash phase hashes, synthesizes, and enqueues the copy task."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    fake = FakeSsh(md5)
    updates = []

    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        result = remote_data_ops.register_remote_data_job(
            "job-1", "r1", str(data), "proj", "mydata", updates.append)

    assert result["uuid"] == md5
    assert result["descriptor"] == "mydata"

    # stage transitions end at copying — the copy is a separate task
    statuses = [u["status"] for u in updates]
    assert statuses[0] == "hashing"
    assert statuses[-1] == "copying"
    assert "done" not in statuses

    # hashing command ran; no copy command in the hash phase
    assert any(c.startswith("python3 -c") for c in fake.commands)
    assert not any(c.startswith("mkdir -p") for c in fake.commands)

    # impression synthesis, running while the copy is in flight
    imp_dir = tmp_path / "Storage" / "proj" / result["impression_uuid"]
    assert (imp_dir / "remote.json").exists()
    remote_cfg = json.loads((imp_dir / "remote.json").read_text())
    assert remote_cfg["host_runner_id"] == "r1"
    assert remote_cfg["source_path"] == str(data)
    assert remote_cfg["remote_path"].startswith(
        "/tmp/yuki-workflows/impressions/proj/")
    yaml = (imp_dir / "contents" / "celebi.yaml").read_text()
    assert f"uuid: {md5}" in yaml
    status = json.loads((imp_dir / "status.json").read_text())
    assert status["status"] == "running"


def test_task_register_remote_data_enqueue_failure(monkeypatch, tmp_path):
    """A failed copy-task enqueue marks the job failed and cleans up."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    from Yuki.server import tasks
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    fake = FakeSsh(md5)

    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake), \
            mock.patch("Yuki.server.tasks.task_copy_remote_data") as copy_task:
        copy_task.apply_async.side_effect = RuntimeError("broker down")
        tasks.task_register_remote_data.run(
            "job-1", "r1", str(data), "proj", "d")
    state = remote_data_ops.read_job_state(str(tmp_path), "job-1")
    assert state["status"] == "failed"
    assert "broker down" in state["error"]
    rm_cmds = [c for c in fake.commands if c.startswith("rm -f ")]
    assert rm_cmds, "expected the progress file cleanup"
    assert "/register-progress/job-1.json" in rm_cmds[0]


def test_register_remote_data_job_reuses_unchanged_registration(monkeypatch, tmp_path):
    """A fresh md5 equal to the archived record reuses it: done, no copy."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    _impression_fixture(tmp_path, imp="imp-old", md5=md5, status="archived",
                        source=str(data))
    fake = FakeSsh(md5)
    updates = []
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        result = remote_data_ops.register_remote_data_job(
            "job-1", "r1", str(data), "proj", "mydata", updates.append)

    assert result == {"uuid": md5, "impression_uuid": "imp-old",
                      "descriptor": "mydata"}
    assert updates[-1]["status"] == "done"
    # no copy command, no new impression directory
    assert not any(c.startswith("mkdir -p") for c in fake.commands)
    impressions = sorted(
        p.name for p in (tmp_path / "Storage" / "proj").iterdir())
    assert impressions == ["imp-old"]


def test_register_remote_data_job_changed_data_registers_fresh(monkeypatch, tmp_path):
    """A fresh md5 different from the archived record registers anew."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    _impression_fixture(tmp_path, imp="imp-old", md5="different-md5",
                        status="archived", source=str(data))
    fake = FakeSsh(md5)
    updates = []
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        result = remote_data_ops.register_remote_data_job(
            "job-1", "r1", str(data), "proj", "mydata", updates.append)

    assert result["uuid"] == md5
    assert result["impression_uuid"] != "imp-old"
    assert updates[-1]["status"] == "copying"
    impressions = sorted(
        p.name for p in (tmp_path / "Storage" / "proj").iterdir())
    assert "imp-old" in impressions
    assert result["impression_uuid"] in impressions


def test_task_register_remote_data_skips_copy_when_reused(monkeypatch, tmp_path):
    """The wrapper does not enqueue the copy when the job reused a record."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    from Yuki.server import tasks
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    _impression_fixture(tmp_path, imp="imp-old", md5=md5, status="archived",
                        source=str(data))
    fake = FakeSsh(md5)
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake), \
            mock.patch("Yuki.server.tasks.task_copy_remote_data") as copy_task:
        tasks.task_register_remote_data.run(
            "job-1", "r1", str(data), "proj", "mydata")
    copy_task.apply_async.assert_not_called()
    state = remote_data_ops.read_job_state(str(tmp_path), "job-1")
    assert state["status"] == "done"
    assert state["result"]["impression_uuid"] == "imp-old"


def test_copy_remote_data_job_archives_impression(monkeypatch, tmp_path):
    """The copy phase copies into the managed dir and archives."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _impression_fixture(tmp_path, md5="abc123", descriptor="mydata")
    fake = FakeSsh("")
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        result = remote_data_ops.copy_remote_data_job(
            "job-1", "imp-1", "proj", "r1", "/src/data")

    copy_cmds = [shlex.split(c)[-1] for c in fake.commands
                 if c.startswith("python3 -c") and shlex.split(c)[2] == LAUNCH_SCRIPT]
    assert copy_cmds, "expected a copy command"
    assert "/impressions/proj/imp-1" in copy_cmds[0]
    assert "cp -a --reflink=auto" in copy_cmds[0]
    assert "/register-progress/job-1.json" in copy_cmds[0]

    status = json.loads(
        (tmp_path / "Storage" / "proj" / "imp-1" / "status.json").read_text())
    assert status["status"] == "archived"
    assert result["status"] == "done"
    assert result["result"] == {"uuid": "abc123", "impression_uuid": "imp-1",
                                "descriptor": "mydata"}


def test_copy_remote_data_job_failure_marks_failed(monkeypatch, tmp_path):
    """A remote failure marks both the impression and registration failed."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _impression_fixture(tmp_path, md5="abc123", descriptor="mydata")
    fake = FakeSsh("")

    fake.copy_state = {"status": "failed", "error": "remote copy failed: disk full"}
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        state = remote_data_ops.copy_remote_data_job(
            "job-1", "imp-1", "proj", "r1", "/src/data")
    assert state["status"] == "failed"
    assert "disk full" in state["error"]
    status = json.loads(
        (tmp_path / "Storage" / "proj" / "imp-1" / "status.json").read_text())
    assert status["status"] == "failed"


def test_register_remote_data_job_hash_failure(monkeypatch, tmp_path):
    """A failed remote md5 raises with an actionable message."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    fake = FakeSsh("")

    def failing_exec(command, timeout=None):  # pylint: disable=unused-argument
        fake.commands.append(command)
        if command.startswith("python3 -c"):
            return "", "no such dir", 1
        return "", "", 0

    fake.exec = failing_exec
    updates = []
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        with pytest.raises(RuntimeError) as exc:
            remote_data_ops.register_remote_data_job(
                "job-1", "r1", "/missing", "proj", "d", updates.append)
    assert "md5" in str(exc.value)
    assert updates[0]["status"] == "hashing"


def test_task_register_remote_data_ends_copying_and_enqueues(monkeypatch, tmp_path):
    """The hash task ends at copying with the result and enqueues the copy."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    from Yuki.server import tasks
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    fake = FakeSsh(md5)
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake), \
            mock.patch("Yuki.server.tasks.task_copy_remote_data") as copy_task:
        tasks.task_register_remote_data.run(
            "job-1", "r1", str(data), "proj", "d")
    state = remote_data_ops.read_job_state(str(tmp_path), "job-1")
    assert state["status"] == "copying"
    assert state["result"]["uuid"] == md5
    assert state["error"] is None
    copy_task.apply_async.assert_called_once()


def test_task_register_remote_data_state_keeps_runner_fields(monkeypatch, tmp_path):
    """Task status updates must preserve the route's runner fields."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    from Yuki.server import tasks
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    remote_data_ops.write_job_state(
        str(tmp_path), "job-1",
        {"status": "hashing", "result": None, "error": None,
         "runner_id": "r1", "remote_path": str(data)})
    fake = FakeSsh(md5)
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake), \
            mock.patch("Yuki.server.tasks.task_copy_remote_data"):
        tasks.task_register_remote_data.run(
            "job-1", "r1", str(data), "proj", "d")
    state = remote_data_ops.read_job_state(str(tmp_path), "job-1")
    assert state["status"] == "copying"
    assert state["runner_id"] == "r1"
    assert state["remote_path"] == str(data)


def test_task_copy_remote_data_writes_done_and_removes_progress(monkeypatch, tmp_path):
    """The copy task writes done and cleans up the progress file."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    from Yuki.server import tasks
    _impression_fixture(tmp_path)
    fake = FakeSsh("")
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        tasks.task_copy_remote_data.run(
            "job-1", "imp-1", "proj", "r1", "/src/data")
    state = remote_data_ops.read_job_state(str(tmp_path), "job-1")
    assert state["status"] == "done"
    assert state["result"]["impression_uuid"] == "imp-1"
    assert state["result"]["uuid"] == "abc123"
    assert state["error"] is None
    rm_cmds = [c for c in fake.commands if c.startswith("rm -f ")]
    assert any("/register-progress/job-1.json" in c for c in rm_cmds)


def test_task_copy_remote_data_cleanup_failure_is_harmless(monkeypatch, tmp_path):
    """A failing cleanup ssh must not mask the copy outcome."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    from Yuki.server import tasks
    _impression_fixture(tmp_path)
    fake = FakeSsh("")
    orig_exec = fake.exec
    calls = {"rm": 0}

    def failing_cleanup(command, timeout=None):  # pylint: disable=unused-argument
        if command.startswith("rm -f "):
            calls["rm"] += 1
            raise ConnectionError("runner gone")
        return orig_exec(command, timeout)

    fake.exec = failing_cleanup
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        tasks.task_copy_remote_data.run(
            "job-1", "imp-1", "proj", "r1", "/src/data")
    assert calls["rm"] == 1
    state = remote_data_ops.read_job_state(str(tmp_path), "job-1")
    assert state["status"] == "done"
    assert state["result"]["uuid"] == "abc123"


def test_register_remote_data_job_emits_progress_paths(monkeypatch, tmp_path):
    """The remote md5 command receives the progress path."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    fake = FakeSsh(md5)
    updates = []
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        remote_data_ops.register_remote_data_job(
            "job-9", "r1", str(data), "proj", "mydata", updates.append)
    progress_path = "/tmp/yuki-workflows/register-progress/job-9.json"
    md5_cmds = [c for c in fake.commands if c.startswith("python3 -c")]
    assert any(progress_path in c for c in md5_cmds), fake.commands


def test_copy_task_releases_worker_while_remote_copy_is_pending(monkeypatch, tmp_path):
    """A pending remote copy schedules another check instead of waiting."""
    from celery.exceptions import Retry
    from Yuki.server import tasks
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    imp_dir = _impression_fixture(tmp_path)
    fake = FakeSsh("")
    fake.copy_state = {"status": "copying"}
    with mock.patch.object(remote_data_ops, "_ssh_connection", return_value=fake), \
            mock.patch.object(tasks.task_copy_remote_data, "retry",
                              side_effect=Retry()) as retry:
        with pytest.raises(Retry):
            tasks.task_copy_remote_data.run("job-1", "imp-1", "proj", "r1", "/src")
    retry.assert_called_once_with(countdown=2)
    state = remote_data_ops.read_job_state(str(tmp_path), "job-1")
    assert state["status"] == "copying"
    assert state["result"]["impression_uuid"] == "imp-1"
    assert ConfigFile(str(imp_dir / "status.json")).read_variable("status") == "running"
    assert not any(command.startswith("rm ") for command in fake.commands)
    assert tasks.task_copy_remote_data.acks_late
    assert tasks.task_copy_remote_data.reject_on_worker_lost


def test_copy_disconnect_is_retried_without_marking_failed(monkeypatch, tmp_path):
    """A disconnected launch may have succeeded on the runner."""
    from celery.exceptions import Retry
    from Yuki.server import tasks
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    imp_dir = _impression_fixture(tmp_path)
    with mock.patch.object(remote_data_ops, "_ssh_connection",
                           side_effect=ConnectionError("SSH lost")), \
            mock.patch.object(tasks.task_copy_remote_data, "retry",
                              side_effect=Retry()) as retry:
        with pytest.raises(Retry):
            tasks.task_copy_remote_data.run("job-1", "imp-1", "proj", "r1", "/src")
    assert isinstance(retry.call_args.kwargs["exc"], ConnectionError)
    assert remote_data_ops.read_job_state(str(tmp_path), "job-1")["status"] == "copying"
    assert ConfigFile(str(imp_dir / "status.json")).read_variable("status") == "running"


@pytest.mark.parametrize("outcome, impression_status", [
    ("done", "archived"), ("failed", "failed"),
])
def test_progress_poll_recovers_detached_result_without_worker(
        monkeypatch, tmp_path, outcome, impression_status):
    """A fresh observer recovers persisted remote state without relaunching."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    imp_dir = _impression_fixture(tmp_path)
    remote_data_ops.write_job_state(str(tmp_path), "job-1", {
        "status": "copying", "copy_detached": True, "project_uuid": "proj",
        "runner_id": "r1", "result": {"impression_uuid": "imp-1", "uuid": "abc123"},
    })
    fake = FakeSsh("")
    fake.copy_state = {"status": outcome, "error": "disk full" if outcome == "failed" else None}
    fake.copy_started = True
    with mock.patch.object(remote_data_ops, "_ssh_connection", return_value=fake), \
            mock.patch.object(fake, "exec_start_detached") as launch:
        remote_data_ops.read_remote_progress("r1", "job-1")
    launch.assert_not_called()
    state = remote_data_ops.read_job_state(str(tmp_path), "job-1")
    assert state["status"] == outcome
    assert state["error"] == fake.copy_state["error"]
    assert state["result"]["impression_uuid"] == "imp-1"
    assert ConfigFile(str(imp_dir / "status.json")).read_variable("status") == impression_status


def test_unconfirmed_launch_can_still_recover_completion(monkeypatch, tmp_path):
    """Lack of an SSH start acknowledgement must not hide remote success."""
    from Yuki.kernel.ssh_workflow import SSHStartNotConfirmed
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _impression_fixture(tmp_path)
    fake = FakeSsh("")

    def unconfirmed_start(_command):
        fake.copy_started = True
        raise SSHStartNotConfirmed("no acknowledgement")

    with mock.patch.object(remote_data_ops, "_ssh_connection", return_value=fake), \
            mock.patch.object(fake, "exec_start_detached",
                              side_effect=unconfirmed_start):
        state = remote_data_ops.copy_remote_data_job(
            "job-1", "imp-1", "proj", "r1", "/src")
    assert state["status"] == "done"


def test_copy_checks_do_not_relaunch_live_process(monkeypatch, tmp_path):
    """Repeated checks, including a fresh worker, only observe a live copy."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _impression_fixture(tmp_path)
    fake = FakeSsh("")
    fake.copy_state = {"status": "copying", "active": True}
    with mock.patch.object(remote_data_ops, "_ssh_connection", return_value=fake), \
            mock.patch.object(fake, "exec_start_detached",
                              wraps=fake.exec_start_detached) as launch:
        for _ in range(3):
            assert remote_data_ops.copy_remote_data_job(
                "job-1", "imp-1", "proj", "r1", "/src") is None
        launch.assert_called_once()
        fake.copy_state = {"status": "done", "active": False}
        assert remote_data_ops.copy_remote_data_job(
            "job-1", "imp-1", "proj", "r1", "/src")["status"] == "done"
        launch.assert_called_once()


def test_copy_relaunches_only_when_process_lock_is_free(monkeypatch, tmp_path):
    """A stale copying record alone must not prevent recovery."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _impression_fixture(tmp_path)
    fake = FakeSsh("")
    fake.copy_started = True
    fake.copy_state = {"status": "copying", "active": False}
    with mock.patch.object(remote_data_ops, "_ssh_connection", return_value=fake), \
            mock.patch.object(fake, "exec_start_detached",
                              wraps=fake.exec_start_detached) as launch:
        assert remote_data_ops.copy_remote_data_job(
            "job-1", "imp-1", "proj", "r1", "/src") is None
        launch.assert_called_once()


def test_failed_copy_probe_does_not_launch(monkeypatch, tmp_path):
    """An unavailable status read is not evidence that no copy is running."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _impression_fixture(tmp_path)
    fake = FakeSsh("")
    with mock.patch.object(remote_data_ops, "_ssh_connection", return_value=fake), \
            mock.patch.object(fake, "exec", return_value=("", "permission denied", 1)), \
            mock.patch.object(fake, "exec_start_detached") as launch:
        with pytest.raises(RuntimeError, match="Cannot read remote copy state"):
            remote_data_ops.copy_remote_data_job("job-1", "imp-1", "proj", "r1", "/src")
    launch.assert_not_called()


def test_old_copy_completion_cannot_archive_new_registration(monkeypatch, tmp_path):
    """Only the newest registration may change the shared impression status."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    imp_dir = _impression_fixture(tmp_path)
    state = {"status": "copying", "copy_detached": True,
             "project_uuid": "proj", "runner_id": "r1",
             "result": {"impression_uuid": "imp-1"}}
    remote_data_ops.write_job_state(str(tmp_path), "e827", dict(state, created_at_ns=100))
    remote_data_ops.write_job_state(str(tmp_path), "af177", dict(state, created_at_ns=200))
    fake = FakeSsh("")
    fake.copy_started = True
    with mock.patch.object(remote_data_ops, "_ssh_connection", return_value=fake):
        assert remote_data_ops.reconcile_remote_copy("e827", "r1")["status"] == "done"
        assert ConfigFile(str(imp_dir / "status.json")).read_variable("status") == "running"
        assert remote_data_ops.find_job_by_impression(str(tmp_path), "imp-1")[0] == "af177"
        assert remote_data_ops.reconcile_remote_copy("af177", "r1")["status"] == "done"
    assert ConfigFile(str(imp_dir / "status.json")).read_variable("status") == "archived"


def test_register_remote_data_job_without_project_context(monkeypatch, tmp_path):
    """The job must work when no Celebi project context exists (celery worker).

    VImpression.__init__ on older CelebiChrono releases does
    csys.project_path() + "/.chern/..." and crashes with TypeError when
    project_path() is None. The job must not depend on project context.
    """
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    fake = FakeSsh(md5)
    # Simulate running outside any Celebi project (docker celery worker).
    monkeypatch.setattr("CelebiChrono.utils.csys.project_path",
                        lambda: None)
    updates = []
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        result = remote_data_ops.register_remote_data_job(
            "job-1", "r1", str(data), "proj", "mydata", updates.append)
    assert result["uuid"] == md5
    assert (tmp_path / "Storage" / "proj" / result["impression_uuid"] /
            "remote.json").exists()


def test_register_remote_data_job_too_old_celebichrono(monkeypatch, tmp_path):
    """A Celebichrono without generate_imp_uuid must produce an actionable error."""
    from CelebiChrono.kernel import vimpression
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    fake = FakeSsh(md5)
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection",
                    return_value=fake):
        monkeypatch.delattr(vimpression.VImpression, "generate_imp_uuid")
        with pytest.raises(RuntimeError) as exc:
            remote_data_ops.register_remote_data_job(
                "job-1", "r1", str(data), "proj", "d", lambda s: None)
    assert "too old" in str(exc.value)
    assert "CELEBI_DIR" in str(exc.value)


class _StubConfig:
    """A config shim pointing at the test YUKIDIR."""

    def __init__(self, root):
        self.root = root

    def get_job_path(self, project, impression):
        """Return the job path under the test Storage root."""
        return str(self.root / "Storage" / project / impression)

    def get_config_file(self):
        """Return a ConfigFile rooted at the test YUKIDIR."""
        return ConfigFile(str(self.root / "config.json"))

    def get_job_config_path(self, project, impression):
        """Return the config path for the given job."""
        return str(self.root / "Storage" / project / impression / "config.json")


def test_file_status_lists_remote_hosted_files(monkeypatch, tmp_path):
    """file_status lists files over ssh and serves them from cache."""
    from Yuki.kernel.impression_storage import ImpressionStorage

    job_dir = tmp_path / "Storage" / "proj" / "imp-1"
    job_dir.mkdir(parents=True)
    marker = ConfigFile(str(job_dir / "remote.json"))
    marker.write_variable("host_runner_id", "r1")
    marker.write_variable("remote_path", "/remote/imp")
    monkeypatch.setattr(config_module, "config", _StubConfig(tmp_path))

    calls = []

    class _WalkFakeSsh:
        """Ssh shim that yields a fixed remote file listing."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def walk_files(self, path):
            """Record the walked path and yield the fixture files."""
            calls.append(path)
            yield "a.txt", "/remote/imp/a.txt", 10
            yield "sub/b.root", "/remote/imp/sub/b.root", 20

    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=_WalkFakeSsh()):
        rows = ImpressionStorage("proj", "imp-1").file_status("stageout")
    assert calls == ["/remote/imp"]
    assert [r["name"] for r in rows] == ["a.txt", "sub/b.root"]
    assert all(r["in_runner"] and not r["in_yuki"] for r in rows)
    assert rows[1]["size"] == 20

    # second call served from cache — no ssh round-trip
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection") as patched:
        rows2 = ImpressionStorage("proj", "imp-1").file_status("stageout")
        patched.assert_not_called()
    assert rows2 == rows


def test_file_status_no_remote_marker_returns_empty(monkeypatch, tmp_path):
    """A job without a remote marker yields an empty listing."""
    from Yuki.kernel.impression_storage import ImpressionStorage
    job_dir = tmp_path / "Storage" / "proj" / "imp-1"
    job_dir.mkdir(parents=True)
    monkeypatch.setattr(config_module, "config", _StubConfig(tmp_path))
    rows = ImpressionStorage("proj", "imp-1").file_status("stageout")
    assert not rows


@pytest.mark.parametrize("project, descriptor", [
    ("other-project", "mydata"), ("proj", "old-descriptor"),
])
def test_registration_does_not_reuse_other_identity(monkeypatch, tmp_path, project, descriptor):
    """Equal data on the same runner/path does not imply equal impressions."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    data = _fixture_data(tmp_path)
    md5 = dir_md5(str(data))
    old = _impression_fixture(tmp_path, project=project, imp="old-identity",
                              md5=md5, descriptor=descriptor, status="archived",
                              source=str(data))
    old_yaml = (old / "contents" / "celebi.yaml").read_bytes()
    updates = []
    with mock.patch("Yuki.kernel.ssh_workflow._SshConnection", return_value=FakeSsh(md5)):
        result = remote_data_ops.register_remote_data_job(
            "job-1", "r1", str(data), "proj", "mydata", updates.append)
    assert result["impression_uuid"] != "old-identity"
    assert result["descriptor"] == "mydata"
    assert updates[-1]["status"] == "copying"
    assert (old / "contents" / "celebi.yaml").read_bytes() == old_yaml


@pytest.mark.parametrize("suffix", ["", "/", "/."])
def test_register_managed_directory_does_not_copy_onto_itself(monkeypatch, tmp_path, suffix):
    """An already managed source is archived without launching a copy."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    imp_dir = _impression_fixture(tmp_path)
    source = "/tmp/yuki-workflows/impressions/proj/imp-1" + suffix
    with mock.patch.object(remote_data_ops, "_ssh_connection") as connect:
        result = remote_data_ops.copy_remote_data_job("job-1", "imp-1", "proj", "r1", source)
    connect.assert_not_called()
    assert result["result"]["impression_uuid"] == "imp-1"
    assert ConfigFile(str(imp_dir / "status.json")).read_variable("status") == "archived"
