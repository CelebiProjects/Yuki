"""Tests for ContainerJob output discovery and command resolution."""
from unittest import mock

from CelebiChrono.utils import metadata
from Yuki.kernel.jobs.container import ContainerJob


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


def _algorithm_with_commands(path, commands, lb_run=None):
    """Build a mock ImageJob whose yaml_file declares the given commands."""
    alg_yaml = metadata.YamlFile(str(path / "alg.yaml"))
    alg_yaml.write_variable("commands", commands)
    if lb_run is not None:
        alg_yaml.write_variable("lb_run", lb_run)
    img = mock.Mock()
    img.yaml_file = alg_yaml
    return img


def test_outputs_returns_relative_paths_for_nested_stageout(tmp_path):
    """Outputs under a nested stageout directory come back relative."""
    c = _make_container(tmp_path)
    stageout = tmp_path / "runner-1" / "stageout"
    (stageout / "plots").mkdir(parents=True)
    (stageout / "mass.png").write_bytes(b"img")
    (stageout / "plots" / "fit.png").write_bytes(b"img2")
    names = set(c.outputs())
    assert names == {"mass.png", "plots/fit.png"}


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
    c = _make_container_with_yaml(tmp_path, {})
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
    c = _make_container_with_yaml(tmp_path, {})
    img = _algorithm_with_commands(
        tmp_path, ["true"],
        {
            "project": "${project}",
            "version": "${version}",
            "platform": "${platform}",
        },
    )
    monkeypatch.setattr(c, "image", lambda: img)
    values = {
        "project": "DaVinci",
        "version": "v63r3",
        "platform": "x86_64_v3-el9-gcc13-opt",
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


def test_lb_run_automatically_requires_lhcb_cvmfs(tmp_path, monkeypatch):
    """Declaring lb_run adds the central LHCb CVMFS repository once."""
    c = _make_container_with_yaml(
        tmp_path, {"cvmfs": ["sft.cern.ch"]})
    img = _algorithm_with_commands(
        tmp_path, ["true"],
        {"project": "Moore", "version": "v55r12"})
    monkeypatch.setattr(c, "image", lambda: img)

    assert c.cvmfs() == ["sft.cern.ch", "lhcb.cern.ch"]


def test_lb_run_rejects_incomplete_configuration(tmp_path, monkeypatch):
    """Malformed declarations fail during workflow construction."""
    c = _make_container_with_yaml(tmp_path, {})
    img = _algorithm_with_commands(
        tmp_path, ["true"], {"project": "Moore"})
    monkeypatch.setattr(c, "image", lambda: img)

    try:
        c._process_user_commands()
    except ValueError as exc:
        assert str(exc) == "lb_run.version is required"
    else:
        raise AssertionError("invalid lb_run declaration was accepted")
