"""Tests for the IHEP ``hep_sub`` workflow backend."""
# pylint: disable=protected-access,missing-function-docstring
import time
import subprocess
from unittest import mock

import pytest
from CelebiChrono.utils.metadata import ConfigFile

from Yuki.kernel.ihep_workflow import IhepWorkflow


class FakeSsh:
    """Small context-managed SSH double used by submission/status tests."""

    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.commands = []
        self.files = {}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def exec(self, command, timeout=300):  # pylint: disable=unused-argument
        self.commands.append(command)
        return self.responses.pop(0) if self.responses else ("", "", 0)

    def put_text(self, text, path, encoding="utf-8"):  # pylint: disable=unused-argument
        self.files[path] = text

    def remove(self, path):
        self.files.pop(path, None)

    def exists(self, path):
        return path in self.files


def _workflow(tmp_path):
    workflow = IhepWorkflow.__new__(IhepWorkflow)
    workflow.remote_exec_path = "/publicfs/alice/yuki/workflows/p/w"
    workflow.ssh_config = {
        "hep_sub_path": "hep_sub",
        "hep_q_path": "hep_q",
        "hep_rm_path": "hep_rm",
        "hep_group": "lhaaso",
    }
    workflow.config_file = ConfigFile(str(tmp_path / "config.json"))
    workflow.logger = mock.MagicMock()
    return workflow


def test_parse_hep_sub_output():
    assert IhepWorkflow._parse_hep_job_id(
        "1 job(s) submitted to cluster 31415") == "31415"
    with pytest.raises(RuntimeError, match="no recognizable job id"):
        IhepWorkflow._parse_hep_job_id("submission accepted")


def test_submission_uses_hep_sub_and_persists_job_id(tmp_path):
    workflow = _workflow(tmp_path)
    ssh = FakeSsh([
        ("", "", 0),
        ("1 job(s) submitted to cluster 42", "", 0),
    ])
    workflow._ssh = mock.MagicMock(return_value=ssh)
    workflow._build_remote_wrapper = mock.MagicMock(return_value="#!/bin/bash\ntrue\n")

    workflow._start_remote_snakemake()

    submit = next(command for command in ssh.commands
                  if command.startswith("cd ") and "hep_sub" in command)
    assert "hep_sub -g lhaaso" in submit
    assert submit.endswith("hep_sub -g lhaaso ./yuki_run.sh")
    assert "setsid" not in submit
    assert workflow.config_file.read_variable("hep_job_id", "") == "42"
    assert ssh.files[workflow.remote_exec_path + "/yuki.hep_job_id"] == "42\n"


def test_coordinator_submits_every_step_through_hep_sub(tmp_path):
    workflow = _workflow(tmp_path)
    workflow.ssh_config["hep_max_jobs"] = 37

    wrapper = workflow._build_remote_wrapper()
    submitter = workflow._build_hep_step_submitter()

    assert "--executor cluster-generic" in wrapper
    assert '--cluster-generic-submit-cmd "./yuki_hep_submit.sh"' in wrapper
    assert '--cluster "./yuki_hep_submit.sh"' in wrapper
    assert "--jobs 37" in wrapper
    assert 'export XDG_CACHE_HOME="$PWD/.cache"' in wrapper
    assert 'mkdir -p "$XDG_CACHE_HOME"' in wrapper
    assert '--apptainer-prefix "$PWD/.snakemake/apptainer"' in wrapper
    assert "$HOME/.local/bin" not in wrapper
    assert '> snakemake.log 2>&1' in wrapper
    assert "hep_sub -g lhaaso" in submitter
    assert "yuki.hep_children" in submitter
    assert 'job_dir="$workflow_dir/hep_jobs"' in submitter
    assert 'mktemp "$job_dir/job.XXXXXX.sh"' in submitter
    assert 'fallback_memory_mb=1024' in submitter
    assert '"mem_mb"' in submitter
    assert '-m "$memory_mb"' in submitter
    assert '-o "$job_dir" -e "$job_dir" "$jobscript"' in submitter

    for script in (wrapper, submitter, workflow._build_hep_step_canceller()):
        result = subprocess.run(
            ["bash", "-n"], input=script, text=True,
            capture_output=True, check=False)
        assert result.returncode == 0, result.stderr


def test_step_memory_fallback_is_configurable_and_invalid_values_are_safe(
        tmp_path):
    workflow = _workflow(tmp_path)
    workflow.ssh_config["hep_step_memory_mb"] = 8192
    assert "fallback_memory_mb=8192" in workflow._build_hep_step_submitter()

    workflow.ssh_config["hep_step_memory_mb"] = "invalid"
    assert "fallback_memory_mb=1024" in workflow._build_hep_step_submitter()


def test_script_tasks_activate_named_conda_environment(tmp_path):
    workflow = _workflow(tmp_path)
    snake_file = mock.MagicMock()

    workflow._write_environment_directive(snake_file, "script")

    assert snake_file.addline.call_args_list == [
        mock.call("conda:", 1),
        mock.call('"script"', 2),
    ]


@pytest.mark.parametrize(("job_resource", "expected_mb"), [
    ('"mem_mb": 256', "256"),
    ("", "8192"),
])
def test_step_submitter_uses_each_job_memory(
        tmp_path, job_resource, expected_mb):
    args_path = tmp_path / "hep_sub.args"
    hep_sub = tmp_path / "hep_sub"
    hep_sub.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > "
        f"{args_path}\n"
        "echo '1 job(s) submitted to cluster 42'\n",
        encoding="utf-8")
    hep_sub.chmod(0o755)

    workflow = _workflow(tmp_path)
    workflow.ssh_config["hep_sub_path"] = str(hep_sub)
    workflow.ssh_config["hep_step_memory_mb"] = 8192
    submitter = tmp_path / "submitter.sh"
    submitter.write_text(workflow._build_hep_step_submitter(), encoding="utf-8")
    submitter.chmod(0o755)
    jobscript = tmp_path / "snakejob.sh"
    jobscript.write_text(
        '#!/bin/sh\n# properties = {"resources": '
        f'{{{job_resource}}}}}\ntrue\n',
        encoding="utf-8")

    result = subprocess.run(
        [str(submitter), str(jobscript)], text=True,
        capture_output=True, check=False)

    assert result.returncode == 0, result.stderr
    args = args_path.read_text(encoding="utf-8").splitlines()
    assert args[args.index("-m") + 1] == expected_mb


def test_queued_job_is_running_without_login_node_pid(tmp_path):
    workflow = _workflow(tmp_path)
    workflow.config_file.write_variable("hep_job_id", "42")
    ssh = FakeSsh([("42.0 alice R yuki_run.sh", "", 0)])

    status, detail, exit_code, completed = workflow._remote_execution_state(
        ssh, ["aaaaaaa"])

    assert (status, detail, exit_code, completed) == ("running", "", None, [])
    assert ssh.commands == ["hep_q -i 42"]


def test_missing_queue_job_is_not_mistaken_for_live_job(tmp_path):
    workflow = _workflow(tmp_path)
    ssh = FakeSsh([("", "job 42 not found", 1)])

    assert workflow._hep_job_is_queued(ssh, "42") is False


def test_recent_submission_gets_queue_visibility_grace(tmp_path):
    workflow = _workflow(tmp_path)
    workflow.config_file.write_variable("hep_job_id", "42")
    workflow.config_file.write_variable("hep_submitted_at", time.time())
    ssh = FakeSsh([("", "", 0)])

    status, detail, _exit_code, _completed = workflow._remote_execution_state(
        ssh, ["aaaaaaa"])

    assert status == "running"
    assert "visible" in detail


def test_kill_uses_hep_rm(tmp_path):
    workflow = _workflow(tmp_path)
    workflow.config_file.write_variable("hep_job_id", "42")
    ssh = FakeSsh([("removed", "", 0)])
    workflow._ssh = mock.MagicMock(return_value=ssh)
    workflow._workflow_is_terminal = mock.MagicMock(return_value=False)
    workflow._finalize_stop = mock.MagicMock(return_value=True)

    assert workflow.kill() is True

    assert ssh.commands == ["hep_rm 42"]
    assert ssh.files[workflow.remote_exec_path + "/yuki.exit"] == "137\n"
    workflow._finalize_stop.assert_called_once()


def test_kill_removes_recorded_child_jobs(tmp_path):
    workflow = _workflow(tmp_path)
    workflow.config_file.write_variable("hep_job_id", "42")
    ssh = FakeSsh([
        ("removed", "", 0),
        ("43\n44.0\n", "", 0),
        ("removed", "", 0),
        ("removed", "", 0),
    ])
    ssh.files[workflow.remote_exec_path + "/yuki.hep_children"] = "43\n44.0\n"
    workflow._ssh = mock.MagicMock(return_value=ssh)
    workflow._workflow_is_terminal = mock.MagicMock(return_value=False)
    workflow._finalize_stop = mock.MagicMock(return_value=True)

    assert workflow.force_kill() is True

    assert ssh.commands == [
        "hep_rm 42",
        "cat /publicfs/alice/yuki/workflows/p/w/yuki.hep_children",
        "hep_rm 43",
        "hep_rm 44.0",
    ]
