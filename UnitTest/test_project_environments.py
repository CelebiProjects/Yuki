"""Project-scoped Conda environment behavior."""

import json
from unittest import mock

import pytest

from Yuki.kernel.runners import project_environments
from Yuki.kernel.workflows.ssh import SshWorkflow, resolve_conda_environment


def test_environment_path_isolated_by_project():
    settings = {"remote_workdir": "/work/yuki"}

    first = project_environments.environment_path(settings, "project-a", "analysis")
    second = project_environments.environment_path(settings, "project-b", "analysis")

    assert first == "/work/yuki/environments/project-a/analysis"
    assert second == "/work/yuki/environments/project-b/analysis"
    assert first != second


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
