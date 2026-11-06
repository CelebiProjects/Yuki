"""Tests for ContainerJob output discovery and command resolution."""
import json
import stat
from unittest import mock

import pytest

from CelebiChrono.utils import metadata
from Yuki.kernel.jobs.container import (
    LB_CONDA_REANA_ENVIRONMENT,
    ContainerJob,
)


def _make_container(path, machine_id="runner-1"):
    c = ContainerJob.__new__(ContainerJob, str(path), machine_id)
    c.path = str(path)
    c.machine_id = machine_id
    return c


def _make_container_with_yaml(path, task_vars):
    """Build a ContainerJob whose yaml_file is a real celebi.yaml with task_vars."""
    contents = path / "contents"
    contents.mkdir(parents=True, exist_ok=True)
    yaml_file = metadata.YamlFile(str(contents / "celebi.yaml"))
    for key, value in task_vars.items():
        yaml_file.write_variable(key, value)
    c = _make_container(path)
    c.is_input = False
    c.yaml_file = yaml_file
    c._substitute_parameters = lambda cmd: cmd
    c._substitute_inputs = lambda cmd: cmd
    c._substitute_paths = lambda cmd: cmd
    return c


def _algorithm_with_commands(path, commands, lb_run=None, environment=None):
    """Build a mock ImageJob whose yaml_file declares the given commands."""
    alg_yaml = metadata.YamlFile(str(path / "alg.yaml"))
    alg_yaml.write_variable("commands", commands)
    alg_yaml.write_variable(
        "environment", environment or ("lb-run" if lb_run else "script"))
    if lb_run is not None:
        alg_yaml.write_variable("lb_run", lb_run)
    img = mock.Mock()
    img.yaml_file = alg_yaml
    return img


def _add_apd_token(path, token="lbapi:user:secret"):
    """Write the deposited local setting used by Yuki at workflow build time."""
    local = metadata.ConfigFile(str(path / "config.local.json"))
    local.write_variable("apd_token", token)


def test_outputs_returns_relative_paths_for_nested_stageout(tmp_path):
    """Outputs under a nested stageout directory come back relative."""
    c = _make_container(tmp_path)
    stageout = tmp_path / "runner-1" / "stageout"
    (stageout / "plots").mkdir(parents=True)
    (stageout / "mass.png").write_bytes(b"img")
    (stageout / "plots" / "fit.png").write_bytes(b"img2")
    names = set(c.outputs())
    assert names == {"mass.png", "plots/fit.png"}


def test_apd_token_file_is_private_json_and_staged_for_ordinary_task(tmp_path):
    """Ordinary tasks stage a token without embedding it in their commands."""
    c = _make_container_with_yaml(
        tmp_path, {"environment": "lb-conda/pidcalib/2026-10-15"})
    c.config_file = metadata.ConfigFile(str(tmp_path / "config.json"))
    c.config_file.write_variable("tree", [])
    _add_apd_token(tmp_path)

    files = c.files()
    key_path = tmp_path / "contents/lbapi_key.json"

    assert f"{c.short_uuid()}/lbapi_key.json" in files
    assert json.loads(key_path.read_text(encoding="utf-8")) == "lbapi:user:secret"
    assert stat.S_IMODE(key_path.stat().st_mode) == 0o600
    export = c._apd_token_export_command("lbapi_key.json")
    assert export == "export APD_USER_TOKEN_FILE=$(pwd)/lbapi_key.json"
    assert "lbapi:user:secret" not in export


def test_apd_token_file_removes_stale_secret_when_local_token_is_absent(tmp_path):
    """A secret from an earlier build is not silently reused."""
    c = _make_container_with_yaml(tmp_path, {"environment": "script"})
    key_path = tmp_path / "contents/lbapi_key.json"
    key_path.write_text('"old-token"', encoding="utf-8")

    assert c._create_apd_token_file() is None
    assert not key_path.exists()
    assert c._apd_token_export_command("lbapi_key.json") == ""


def test_apd_export_precedes_user_command_in_reana_step(tmp_path, monkeypatch):
    """The exported file path is inherited by an ordinary REANA user command."""
    c = _make_container_with_yaml(tmp_path, {"environment": "script"})
    _add_apd_token(tmp_path)
    monkeypatch.setattr(c, "_create_directory_commands", lambda: ["cd impression"])
    monkeypatch.setattr(c, "_create_symlink_commands", lambda: [])
    monkeypatch.setattr(c, "_process_user_commands_for_reana", lambda: ["run-user"])
    monkeypatch.setattr(c, "_cache_commands", lambda *_args: [])
    monkeypatch.setattr(c, "_create_reana_step_metadata", lambda: {})

    commands = c.step("runner-1")["commands"]

    export_index = commands.index(
        "export APD_USER_TOKEN_FILE=$(pwd)/lbapi_key.json")
    assert export_index < commands.index("run-user")


def test_apd_export_precedes_subtask_user_command(tmp_path, monkeypatch):
    """Each repeated task resolves the shared token from its nested workspace."""
    c = _make_container_with_yaml(
        tmp_path, {"environment": "script", "subtasks": 2})
    c.config_file = metadata.ConfigFile(str(tmp_path / "config.json"))
    c.config_file.write_variable("alias_to_impression", {})
    _add_apd_token(tmp_path)
    monkeypatch.setattr(c, "image", lambda: None)
    monkeypatch.setattr(c, "_process_user_commands", lambda: ["run-user"])
    monkeypatch.setattr(c, "_get_step_inputs", lambda: [])
    monkeypatch.setattr(c, "cvmfs", lambda: [])
    monkeypatch.setattr(c, "_create_step_metadata", lambda *_args: {})

    commands = c.subtask_rule()["commands"]

    export_index = commands.index(
        "export APD_USER_TOKEN_FILE=$(pwd)/../../lbapi_key.json")
    assert export_index < commands.index("run-user")


def test_outputs_returns_relative_paths_for_nested_rawdata(tmp_path):
    """Outputs under a nested rawdata directory come back relative."""
    c = _make_container(tmp_path, machine_id=None)
    rawdata = tmp_path / "rawdata"
    (rawdata / "data").mkdir(parents=True)
    (rawdata / "data" / "x.root").write_bytes(b"data")
    names = set(c.outputs())
    assert names == {"data/x.root"}


def test_outputs_returns_empty_when_missing(tmp_path):
    """A container with no stageout or rawdata yields no outputs."""
    c = _make_container(tmp_path)
    assert c.outputs() == []


def test_process_user_commands_prefers_inline_commands(tmp_path, monkeypatch):
    """Inline task commands win over the algorithm's, in both processing paths."""
    c = _make_container_with_yaml(tmp_path, {"commands": ["echo inline"]})
    img = _algorithm_with_commands(tmp_path, ["echo algorithm"])
    mock_image = mock.Mock(return_value=img)
    monkeypatch.setattr(c, "image", mock_image)

    for processed in (c._process_user_commands_for_reana(),
                      c._process_user_commands()):
        assert any("echo inline" in cmd for cmd in processed)
        assert not any("echo algorithm" in cmd for cmd in processed)

    mock_image.assert_not_called()


def test_process_user_commands_falls_back_to_algorithm(tmp_path, monkeypatch):
    """Without inline commands, the algorithm's commands are used."""
    c = _make_container_with_yaml(tmp_path, {})
    img = _algorithm_with_commands(tmp_path, ["echo algorithm"])
    monkeypatch.setattr(c, "image", lambda: img)

    for processed in (c._process_user_commands_for_reana(),
                      c._process_user_commands()):
        assert any("echo algorithm" in cmd for cmd in processed)


def test_process_user_commands_empty_when_neither(tmp_path, monkeypatch):
    """No inline commands and no algorithm means no user commands."""
    c = _make_container_with_yaml(tmp_path, {})
    monkeypatch.setattr(c, "image", lambda: None)

    assert c._process_user_commands_for_reana() == []
    assert c._process_user_commands() == []


def test_process_user_commands_wraps_algorithm_command_with_lb_run(
        tmp_path, monkeypatch):
    """Algorithm commands run inside the declared CVMFS LHCb release."""
    c = _make_container_with_yaml(tmp_path, {"environment": "lb-run"})
    img = _algorithm_with_commands(
        tmp_path,
        ['python code/options.py --label "signal sample"'],
        {
            "project": "Moore",
            "version": "v55r12",
            "platform": "x86_64_v3-el9-gcc13-opt",
        },
    )
    monkeypatch.setattr(c, "image", lambda: img)

    for processed in (c._process_user_commands_for_reana(),
                      c._process_user_commands()):
        command = processed[0]
        assert "/cvmfs/lhcb.cern.ch/" in command
        assert "if ! test -d /cvmfs/lhcb.cern.ch; then" in command
        assert "if ! test -x /cvmfs/lhcb.cern.ch/" in command
        assert "CVMFS repository /cvmfs/lhcb.cern.ch is not available" in command
        assert "lb-run" in command
        assert "-c x86_64_v3-el9-gcc13-opt" in command
        assert "Moore/v55r12 bash --noprofile --norc -c" in command
        assert "python code/options.py" in command


def test_process_user_commands_wraps_nightly_lb_run(tmp_path, monkeypatch):
    """A nightly slot adds the flag and checks the development repository."""
    c = _make_container_with_yaml(tmp_path, {"environment": "lb-run"})
    img = _algorithm_with_commands(
        tmp_path,
        ["gaudirun.py code/options.py"],
        {
            "project": "Gauss",
            "version": "run5_stackglue_June2026",
            "platform": "x86_64_v3-el9-gcc15-opt",
            "nightly": "lhcb-sim11-run5-gluing/74",
        },
    )
    monkeypatch.setattr(c, "image", lambda: img)

    for processed in (c._process_user_commands_for_reana(),
                      c._process_user_commands()):
        command = processed[0]
        assert "--nightly lhcb-sim11-run5-gluing/74" in command
        assert "Gauss/run5_stackglue_June2026" in command
        assert "if ! test -d /cvmfs/lhcbdev.cern.ch; then" in command
        assert "CVMFS repository /cvmfs/lhcbdev.cern.ch is not available" in command
        assert "gaudirun.py code/options.py" in command


def test_inline_commands_do_not_inherit_algorithm_lb_run(tmp_path, monkeypatch):
    """Replacing algorithm commands also replaces its lb_run declaration."""
    c = _make_container_with_yaml(tmp_path, {"commands": ["echo inline"]})
    img = _algorithm_with_commands(
        tmp_path, ["echo algorithm"],
        {"project": "Moore", "version": "v55r12"})
    mock_image = mock.Mock(return_value=img)
    monkeypatch.setattr(c, "image", mock_image)

    processed = c._process_user_commands()

    assert "lb-run" not in processed[0]
    mock_image.assert_not_called()


def test_lb_run_substitutes_task_parameters(tmp_path, monkeypatch):
    """Task parameters can select the project release and platform."""
    c = _make_container_with_yaml(tmp_path, {"environment": "lb-run"})
    img = _algorithm_with_commands(
        tmp_path, ["true"],
        {
            "project": "${project}",
            "version": "${version}",
            "platform": "${platform}",
            "nightly": "${nightly}",
        },
    )
    monkeypatch.setattr(c, "image", lambda: img)
    values = {
        "project": "DaVinci",
        "version": "v63r3",
        "platform": "x86_64_v3-el9-gcc13-opt",
        "nightly": "lhcb-head/1234",
    }
    c.parameters = lambda: (sorted(values), values)

    def substitute_parameters(value):
        for key, replacement in values.items():
            value = value.replace("${" + key + "}", replacement)
        return value

    c._substitute_parameters = substitute_parameters

    command = c._process_user_commands()[0]

    assert "DaVinci/v63r3" in command
    assert "-c x86_64_v3-el9-gcc13-opt" in command
    assert "--nightly lhcb-head/1234" in command


def test_lb_run_automatically_requires_lhcb_cvmfs(tmp_path, monkeypatch):
    """Declaring lb_run adds the central LHCb CVMFS repository once."""
    c = _make_container_with_yaml(
        tmp_path, {"environment": "lb-run", "cvmfs": ["sft.cern.ch"]})
    img = _algorithm_with_commands(
        tmp_path, ["true"],
        {"project": "Moore", "version": "v55r12"})
    monkeypatch.setattr(c, "image", lambda: img)

    assert c.cvmfs() == ["sft.cern.ch", "lhcb.cern.ch"]


def test_nightly_lb_run_requires_both_lhcb_cvmfs_repositories(
        tmp_path, monkeypatch):
    """Nightly releases need the central and development repositories."""
    c = _make_container_with_yaml(
        tmp_path, {"environment": "lb-run",
                   "cvmfs": ["sft.cern.ch", "lhcbdev.cern.ch"]})
    img = _algorithm_with_commands(
        tmp_path,
        ["true"],
        {
            "project": "Gauss",
            "version": "master",
            "nightly": "lhcb-sim11/latest",
        },
    )
    monkeypatch.setattr(c, "image", lambda: img)

    assert c.cvmfs() == [
        "sft.cern.ch", "lhcbdev.cern.ch", "lhcb.cern.ch"]


def test_lb_run_rejects_incomplete_configuration(tmp_path, monkeypatch):
    """Malformed declarations fail during workflow construction."""
    c = _make_container_with_yaml(tmp_path, {"environment": "lb-run"})
    img = _algorithm_with_commands(
        tmp_path, ["true"], {"project": "Moore"})
    monkeypatch.setattr(c, "image", lambda: img)

    try:
        c._process_user_commands()
    except ValueError as exc:
        assert str(exc) == "lb_run.version is required"
    else:
        raise AssertionError("invalid lb_run declaration was accepted")


def test_lb_run_requires_task_environment(tmp_path, monkeypatch):
    c = _make_container_with_yaml(tmp_path, {"environment": "script"})
    img = _algorithm_with_commands(
        tmp_path, ["true"], {"project": "Moore", "version": "v55r12"})
    monkeypatch.setattr(c, "image", lambda: img)

    with pytest.raises(ValueError, match="task environment 'lb-run'"):
        c._process_user_commands()


def test_lb_run_requires_algorithm_environment(tmp_path, monkeypatch):
    c = _make_container_with_yaml(tmp_path, {"environment": "lb-run"})
    img = _algorithm_with_commands(
        tmp_path, ["true"], {"project": "Moore", "version": "v55r12"},
        environment="script")
    monkeypatch.setattr(c, "image", lambda: img)

    with pytest.raises(ValueError, match="command source environment 'lb-run'"):
        c._process_user_commands()


def test_inline_lb_run_task_needs_no_algorithm(tmp_path, monkeypatch):
    c = _make_container_with_yaml(tmp_path, {
        "environment": "lb-run",
        "commands": ["echo inline"],
        "lb_run": {"project": "Moore", "version": "v55r12"},
    })
    image = mock.Mock()
    monkeypatch.setattr(c, "image", image)

    command = c._process_user_commands()[0]

    assert "lb-run" in command
    assert "echo inline" in command
    image.assert_not_called()


def test_lb_run_environment_requires_declaration(tmp_path):
    c = _make_container_with_yaml(tmp_path, {"environment": "lb-run"})

    with pytest.raises(ValueError, match="requires an lb_run declaration"):
        c._lb_run_spec(None)


def test_lb_run_uses_base_backend_environment(tmp_path, monkeypatch):
    c = _make_container_with_yaml(tmp_path, {
        "environment": "lb-run",
        "commands": ["true"],
        "lb_run": {"project": "Moore", "version": "v55r12"},
    })
    monkeypatch.setattr(c, "image", lambda: None)
    monkeypatch.setattr(c, "use_kerberos", lambda _backend=None: False)

    assert c._create_step_metadata()["environment"] == c.default_environment()


def test_lb_conda_environment_wraps_user_commands(tmp_path, monkeypatch):
    """A dated LbConda environment wraps commands independently of lb-run."""
    c = _make_container_with_yaml(
        tmp_path, {"environment": "lb-conda/default/2020-05-14"})
    img = _algorithm_with_commands(
        tmp_path, ['python code/analysis.py --label "signal sample"'])
    monkeypatch.setattr(c, "image", lambda: img)

    for processed in (c._process_user_commands_for_reana(),
                      c._process_user_commands()):
        command = processed[0]
        assert "lb-conda" in command
        assert "default/2020-05-14 bash --noprofile --norc -c" in command
        assert "python code/analysis.py" in command
        assert "lb-run" not in command


def test_lb_conda_uses_reana_environment_and_both_lhcb_cvmfs_repositories(
        tmp_path, monkeypatch):
    """REANA hosts LbConda in its compatible image with both repositories."""
    c = _make_container_with_yaml(
        tmp_path, {"environment": "lb-conda/default/2020-05-14"})
    monkeypatch.setattr(c, "image", lambda: None)
    monkeypatch.setattr(c, "use_kerberos", lambda _backend=None: False)

    assert c._create_step_metadata()["environment"] == LB_CONDA_REANA_ENVIRONMENT
    reana_metadata = c._create_reana_step_metadata()
    assert reana_metadata["environment"] == LB_CONDA_REANA_ENVIRONMENT
    assert c.cvmfs() == ["lhcb.cern.ch", "lhcbdev.cern.ch"]


def test_lb_conda_non_reana_backend_keeps_existing_base_environment(
        tmp_path, monkeypatch):
    """The REANA-specific LbConda image does not change other backends."""
    c = _make_container_with_yaml(
        tmp_path, {"environment": "lb-conda/default/2020-05-14"})
    monkeypatch.setattr(c, "image", lambda: None)

    assert (c._create_step_metadata("ssh")["environment"]
            == c.default_environment())


@pytest.mark.parametrize("environment", [
    "lb-conda",
    "lb-conda/default",
    "lb-conda/default/20200514",
    "lb-conda/default/2020-05-14/extra",
])
def test_lb_conda_rejects_invalid_environment_format(
        tmp_path, monkeypatch, environment):
    """Malformed LbConda selectors fail during workflow construction."""
    c = _make_container_with_yaml(tmp_path, {"environment": environment})
    monkeypatch.setattr(c, "image", lambda: None)

    with pytest.raises(ValueError, match="lb-conda"):
        c._create_step_metadata()


def test_lb_conda_cannot_be_combined_with_lb_run(tmp_path, monkeypatch):
    """LbConda and lb-run remain mutually independent execution modes."""
    c = _make_container_with_yaml(
        tmp_path, {"environment": "lb-conda/default/2020-05-14"})
    img = _algorithm_with_commands(
        tmp_path, ["true"], {"project": "Moore", "version": "v55r12"})
    monkeypatch.setattr(c, "image", lambda: img)

    with pytest.raises(ValueError, match="cannot be combined"):
        c._process_user_commands()
