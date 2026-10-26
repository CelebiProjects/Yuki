"""Default local-runner registration."""

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.runners import config as runner_config


def test_ensure_default_local_runner_registers_native_backend(tmp_path):
    config_file = ConfigFile(str(tmp_path / "config.json"))

    runner_id = runner_config.ensure_default_local_runner(config_file)

    assert config_file.read_variable("runners") == ["local"]
    assert config_file.read_variable("runners_id") == {"local": runner_id}
    assert config_file.read_variable("urls") == {runner_id: ""}
    assert config_file.read_variable("tokens") == {runner_id: ""}
    assert config_file.read_variable("backend_types") == {runner_id: "native"}


def test_ensure_default_local_runner_is_idempotent(tmp_path):
    config_file = ConfigFile(str(tmp_path / "config.json"))

    first_id = runner_config.ensure_default_local_runner(config_file)
    second_id = runner_config.ensure_default_local_runner(config_file)

    assert second_id == first_id
    assert config_file.read_variable("runners") == ["local"]


def test_ensure_default_local_runner_preserves_existing_definition(tmp_path):
    config_file = ConfigFile(str(tmp_path / "config.json"))
    config_file.write_variable("runners", ["local"])
    config_file.write_variable("runners_id", {"local": "custom-id"})
    config_file.write_variable("backend_types", {"custom-id": "ssh"})

    runner_id = runner_config.ensure_default_local_runner(config_file)

    assert runner_id == "custom-id"
    assert config_file.read_variable("backend_types") == {"custom-id": "ssh"}
