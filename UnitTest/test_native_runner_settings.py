"""Tests for native runner settings consumption."""
import json
import os
from unittest import mock

from Yuki.kernel.execution.monitor import SnakemakeMonitor


def _monitor(tmp):
    os.makedirs(os.path.join(tmp, "wf"), exist_ok=True)
    exec_dir = os.path.join(tmp, "exec")
    os.makedirs(exec_dir, exist_ok=True)
    return SnakemakeMonitor(os.path.join(tmp, "wf"), exec_dir,
                            project_uuid="p", workflow_uuid="w")


def test_execute_snakemake_settings(tmp_path):
    """execute_snakemake honors snakemake/conda path and memory settings."""
    mon = _monitor(str(tmp_path))
    with mock.patch("subprocess.Popen") as popen:
        popen.return_value.wait.return_value = 0
        popen.return_value.returncode = 0
        try:
            mon.execute_snakemake(8, mem_mb=4096,
                                  snakemake_path="/opt/bin/snakemake",
                                  conda_path="/opt/conda/bin/conda")
        except Exception:
            pass  # status file handling is out of scope here
    cmd = popen.call_args[0][0]
    assert cmd[0] == "/opt/bin/snakemake"
    assert "--resources" in cmd and "mem_mb=4096" in cmd
    assert "-j" in cmd and "8" in cmd
    env = popen.call_args[1].get("env")
    assert env["PATH"].startswith("/opt/conda/bin" + os.pathsep)


def test_native_workflow_uses_workdir_setting(monkeypatch, tmp_path):
    """Runner workdir setting is read from the runner config."""
    yuki_dir = tmp_path / ".Yuki"
    (yuki_dir / "Storage" / "proj").mkdir(parents=True)
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))
    monkeypatch.setenv("HOME", str(tmp_path))
    with open(yuki_dir / "config.json", "w", encoding="utf-8") as f:
        json.dump({"runner_settings": {"m1": {"workdir": str(tmp_path / "custom")}}}, f)

    from Yuki.kernel.workflows.native import NativeWorkflow
    with mock.patch.object(NativeWorkflow, "__init__", lambda self, *a, **k: None):
        NativeWorkflow.__new__(NativeWorkflow)
    # exercise the path-resolution logic directly
    from Yuki.kernel.runners import config as runner_config
    settings = runner_config.get_runner_settings(runner_config.open_config(), "m1")
    assert settings["workdir"] == str(tmp_path / "custom")


def test_run_workflow_resolves_custom_workdir(monkeypatch, tmp_path):
    """run-workflow must find the exec dir under a custom runner workdir."""
    yuki_dir = tmp_path / ".Yuki"
    wf_uuid = "wf-123"
    proj = "proj"
    wf_dir = yuki_dir / "Workflows" / proj / wf_uuid
    wf_dir.mkdir(parents=True)
    with open(wf_dir / "config.json", "w", encoding="utf-8") as f:
        json.dump({"machine_id": "m1"}, f)
    custom = tmp_path / "custom"
    exec_dir = custom / wf_uuid
    exec_dir.mkdir(parents=True)
    (exec_dir / "Snakefile").write_text("rule all:\n    shell: 'true'\n")
    with open(yuki_dir / "config.json", "w", encoding="utf-8") as f:
        json.dump({"runner_settings": {"m1": {"workdir": str(custom)}}}, f)
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))
    monkeypatch.setenv("HOME", str(tmp_path))

    from click.testing import CliRunner
    from Yuki.main import cli
    with mock.patch("Yuki.kernel.execution.monitor.SnakemakeMonitor") as mon_cls, \
            mock.patch("Yuki.kernel.storage.staging.FileStager") as stager_cls:
        stager_cls.return_value.stage_in.return_value = True
        stager_cls.return_value.stage_out.return_value = True
        mon_cls.return_value.execute_snakemake.return_value = 0
        result = CliRunner().invoke(cli, ["run-workflow", wf_uuid])
    assert result.exit_code == 0, result.output
    # SnakemakeMonitor(workflow_path, local_exec_dir, ...) — 2nd positional arg
    assert mon_cls.call_args[0][1] == str(exec_dir)
