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


def _algorithm_with_commands(path, commands):
    """Build a mock ImageJob whose yaml_file declares the given commands."""
    alg_yaml = metadata.YamlFile(str(path / "alg.yaml"))
    alg_yaml.write_variable("commands", commands)
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
