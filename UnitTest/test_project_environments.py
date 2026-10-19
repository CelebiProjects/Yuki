"""Project-scoped Conda environment behavior."""

import json
from unittest import mock

import pytest

from Yuki.kernel.runners import project_environments
from Yuki.kernel.workflows.native import NativeWorkflow
from Yuki.kernel.workflows.ssh import SshWorkflow, resolve_conda_environment


def test_environment_path_isolated_by_project():
    settings = {"remote_workdir": "/work/yuki"}

    first = project_environments.environment_path(settings, "project-a", "analysis")
    second = project_environments.environment_path(settings, "project-b", "analysis")

    assert first == "/work/yuki/environments/project-a/analysis"
    assert second == "/work/yuki/environments/project-b/analysis"
    assert first != second


def test_native_environment_path_isolated_by_project(tmp_path):
    settings = {"backend_type": "native", "workdir": str(tmp_path)}

    first = project_environments.environment_path(
        settings, "project-a", "analysis")
    second = project_environments.environment_path(
        settings, "project-b", "analysis")

    assert first == str(tmp_path / "environments" / "project-a" / "analysis")
    assert second == str(tmp_path / "environments" / "project-b" / "analysis")


def test_reserved_and_unsafe_names_are_rejected():
    with pytest.raises(ValueError, match="reserved"):
        project_environments.validate_environment_name("script")
    with pytest.raises(ValueError, match="invalid"):
        project_environments.validate_environment_name("../escape")


def test_project_resolution_keeps_explicit_global_mapping(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "conda_env_map": {"legacy": "/shared/legacy"}
    }), encoding="utf-8")
    settings = {"remote_workdir": "/work/yuki"}

    assert resolve_conda_environment(
        "analysis", str(config_path), "project-a", settings
    ) == "/work/yuki/environments/project-a/analysis"
    assert resolve_conda_environment(
        "legacy", str(config_path), "project-a", settings
    ) == "/shared/legacy"
    assert resolve_conda_environment(
        "script", str(config_path), "project-a", settings
    ) == "script"


def test_install_uses_prefix_update_and_prune(monkeypatch):
    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False
    ssh.exec.return_value = ("updated", "", 0)
    monkeypatch.setattr(project_environments, "_connection", lambda _settings: ssh)

    result = project_environments.install_project_environment(
        {"remote_workdir": "/work", "conda_path": "/opt/conda/bin/conda"},
        "project-a", "analysis", "dependencies:\n  - python\n")

    ssh.put_text.assert_called_once_with(
        "dependencies:\n  - python\n",
        "/work/environments/project-a/.specs/analysis.yaml")
    command = ssh.exec.call_args.args[0]
    assert "/opt/conda/bin/conda env update" in command
    assert "--prefix /work/environments/project-a/analysis" in command
    assert "--prune" in command
    assert result["path"] == "/work/environments/project-a/analysis"


def test_ihep_install_keeps_all_conda_state_off_afs(monkeypatch):
    """IHEP installation must not write Conda state beneath the AFS home."""
    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False
    ssh.exec.return_value = ("updated", "", 0)
    monkeypatch.setattr(project_environments, "_connection", lambda _settings: ssh)

    project_environments.install_project_environment(
        {"backend_type": "ihep", "remote_workdir": "/cefs/yuki/workflows",
         "conda_path": "/cefs/yuki/miniconda3/bin/conda"},
        "project-a", "analysis", "dependencies:\n  - python\n")

    command = ssh.exec.call_args.args[0]
    state = "/cefs/yuki/workflows/.celebi-conda"
    assert f"export HOME={state}/home" in command
    assert f"export CONDA_PKGS_DIRS={state}/pkgs" in command
    assert f"export CONDA_ENVS_PATH={state}/envs" in command
    assert f"export XDG_CACHE_HOME={state}/cache" in command
    assert f"export XDG_CONFIG_HOME={state}/config" in command
    assert "export CONDA_REGISTER_ENVS=false" in command
    assert "cd /cefs/yuki/workflows" in command
    assert "/cefs/yuki/miniconda3/bin/conda env update" in command


def test_native_install_uses_prefix_update_and_prune(monkeypatch, tmp_path):
    completed = mock.Mock(returncode=0, stdout="updated\n", stderr="")
    run = mock.Mock(return_value=completed)
    monkeypatch.setattr(project_environments.subprocess, "run", run)
    settings = {"backend_type": "native", "workdir": str(tmp_path),
                "conda_path": "/opt/conda/bin/conda"}

    result = project_environments.install_native_environment_local(
        settings, "project-a", "analysis", "dependencies:\n  - python\n")

    prefix = tmp_path / "environments" / "project-a" / "analysis"
    spec = tmp_path / "environments" / "project-a" / ".specs" / "analysis.yaml"
    assert spec.read_text(encoding="utf-8") == "dependencies:\n  - python\n"
    assert run.call_args.args[0] == [
        "/opt/conda/bin/conda", "env", "update", "--prefix", str(prefix),
        "--file", str(spec), "--prune"]
    assert result["path"] == str(prefix)


def test_native_project_environment_listing(tmp_path):
    root = tmp_path / "environments" / "project-a"
    (root / "analysis" / "conda-meta").mkdir(parents=True)
    (root / "incomplete").mkdir()

    result = project_environments.list_project_environments(
        {"backend_type": "native", "workdir": str(tmp_path)}, "project-a")

    assert result["envs"] == [{
        "name": "analysis", "path": str(root / "analysis"), "active": False}]


def test_native_install_requires_live_agent(monkeypatch, tmp_path):
    monkeypatch.setattr(project_environments, "yuki_dir", lambda: str(tmp_path))
    monkeypatch.setattr(
        project_environments.runner_config, "native_agent_running",
        lambda _root: (False, str(tmp_path / "agent.lock")))

    with pytest.raises(RuntimeError, match="yuki native-runner start"):
        project_environments.install_project_environment(
            {"backend_type": "native", "workdir": str(tmp_path)},
            "project-a", "analysis", "dependencies:\n  - python\n")


def test_missing_project_environment_fails_with_install_command(monkeypatch, tmp_path):
    """Workflow construction names the corrective command before remote launch."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    (tmp_path / "config.json").write_text(json.dumps({
        "runners_id": {"cluster": "runner-1"}
    }), encoding="utf-8")
    workflow = SshWorkflow.__new__(SshWorkflow)
    workflow.project_uuid = "project-a"
    workflow.machine_id = "runner-1"
    workflow.ssh_config = {"remote_workdir": "/work"}
    ssh = mock.MagicMock()
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False
    ssh.exists.return_value = False
    monkeypatch.setattr(workflow, "_ssh", lambda: ssh)

    with pytest.raises(RuntimeError, match=(
            "celebi-cli install-environment cluster analysis")):
        workflow._resolve_conda_environment("analysis")


def test_native_missing_project_environment_fails_before_queue(monkeypatch, tmp_path):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    (tmp_path / "config.json").write_text(json.dumps({
        "runners_id": {"local": "runner-1"}
    }), encoding="utf-8")
    workflow = NativeWorkflow.__new__(NativeWorkflow)
    workflow.project_uuid = "project-a"
    workflow.machine_id = "runner-1"
    workflow.native_settings = {
        "backend_type": "native", "workdir": str(tmp_path / "native")}

    with pytest.raises(RuntimeError, match=(
            "celebi-cli install-environment local analysis")):
        workflow._resolve_conda_environment("analysis")


def test_native_resolves_installed_project_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    (tmp_path / "config.json").write_text("{}", encoding="utf-8")
    prefix = tmp_path / "native" / "environments" / "project-a" / "analysis"
    (prefix / "conda-meta").mkdir(parents=True)
    workflow = NativeWorkflow.__new__(NativeWorkflow)
    workflow.project_uuid = "project-a"
    workflow.machine_id = "runner-1"
    workflow.native_settings = {
        "backend_type": "native", "workdir": str(tmp_path / "native")}
    workflow.local_exec_path = str(
        tmp_path / "native" / "workflows" / "project-a" / "workflow-a")

    assert workflow._resolve_conda_environment("analysis") == (
        "../../../environments/project-a/analysis")


def test_native_activates_project_environment_in_rule_shell(monkeypatch, tmp_path):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    (tmp_path / "config.json").write_text("{}", encoding="utf-8")
    prefix = tmp_path / "native" / "environments" / "project-a" / "analysis"
    (prefix / "conda-meta").mkdir(parents=True)
    workflow = NativeWorkflow.__new__(NativeWorkflow)
    workflow.project_uuid = "project-a"
    workflow.machine_id = "runner-1"
    workflow.native_settings = {
        "backend_type": "native", "workdir": str(tmp_path / "native")}
    workflow.local_exec_path = str(
        tmp_path / "native" / "workflows" / "project-a" / "workflow-a")

    snake_file = mock.Mock()
    workflow._write_environment_directive(snake_file, "analysis")
    snake_file.addline.assert_not_called()
    assert workflow._wrap_environment_commands("analysis", ["python run.py"]) == [
        'source \\"$(conda info --base)/etc/profile.d/conda.sh\\"',
        "conda activate ../../../environments/project-a/analysis",
        "python run.py",
    ]
