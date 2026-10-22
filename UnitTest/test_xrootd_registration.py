"""XRootD samples are inventoried remotely and stored as datalists."""
import json
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile, YamlFile
from Yuki.kernel.jobs.container import ContainerJob
from Yuki.kernel.storage import remote as remote_data_ops
from Yuki.server.routes import remote_data as remote_data_routes


class FakeSsh:
    """Return a precomputed XRootD manifest for the remote Python command."""

    def __init__(self, records):
        self.records = records
        self.commands = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def exec(self, command, timeout=None):  # pylint: disable=unused-argument
        self.commands.append(command)
        if command.startswith("rm -f "):
            return "", "", 0
        return json.dumps(self.records), "", 0


def _app():
    """Build a minimal Flask application containing the data routes."""
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(remote_data_routes.bp)
    return app


def test_xrootd_route_enqueues_ssh_inventory(monkeypatch, tmp_path):
    """The HTTP route validates the runner and queues a background job."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    config_obj = mock.MagicMock()
    config_obj.get_config_file.return_value = ConfigFile(
        str(tmp_path / "config.json"))
    config_obj.get_config_file.return_value.write_variable(
        "runners_id", {"cluster": "runner-id"})
    config_obj.get_config_file.return_value.write_variable(
        "backend_types", {"runner-id": "ssh"})
    monkeypatch.setattr(remote_data_routes, "config", config_obj)

    with mock.patch.object(
            remote_data_routes, "task_register_xrootd_data") as task:
        response = _app().test_client().post("/register-xrootd-data", json={
            "runner": "cluster",
            "root_url": "root://host//sample",
            "project_uuid": "project",
            "checksum": "required",
        })

    assert response.status_code == 200
    job_id = response.get_json()["job_id"]
    task.apply_async.assert_called_once_with(args=[
        job_id, "runner-id", "root://host//sample", "project", "sample",
        "required",
    ])
    state = remote_data_ops.read_job_state(
        str(tmp_path), job_id, jobs_dir_name="xrootd-register-jobs")
    assert state["status"] == "listing"


def test_embedded_xrootd_inventory_script_is_valid_python():
    """The Python source transported through SSH must remain syntactically valid."""
    compile(remote_data_ops.XROOTD_MANIFEST_SCRIPT,
            "<xrootd-manifest>", "exec")


def test_xrootd_registration_creates_frozen_datalist(monkeypatch, tmp_path):
    """The registration job persists sorted URLs and checksum metadata."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    records = [
        {"url": "root://host//sample/b.root", "size": 20,
         "checksum": {"algorithm": "adler32", "value": "bbbb"}},
        {"url": "root://host//sample/a.root", "size": 10,
         "checksum": {"algorithm": "adler32", "value": "aaaa"}},
    ]
    fake = FakeSsh(records)
    updates = []
    with mock.patch("Yuki.kernel.workflows.ssh._SshConnection",
                    return_value=fake), \
            mock.patch.object(remote_data_ops, "_runner_name",
                              return_value="cluster"):
        result = remote_data_ops.register_xrootd_data_job(
            "job", "runner-id", "root://host//sample", "project",
            "sample", "required", updates.append)

    assert updates[-1]["status"] == "done"
    assert result["files"] == [
        "root://host//sample/a.root", "root://host//sample/b.root"]
    assert result["manifest"]["file_count"] == 2
    assert result["manifest"]["total_size"] == 30
    impression = (tmp_path / "Storage" / "project" /
                  result["impression_uuid"])
    yaml = YamlFile(str(impression / "contents" / "celebi.yaml"))
    assert yaml.read_variable("environment") == "datalist"
    assert yaml.read_variable("data_source")["type"] == "xrootd"
    assert yaml.read_variable("datalist") == result["files"]
    assert ConfigFile(str(impression / "status.json")).read_variable(
        "status") == "pending"
    assert "XROOTD_MANIFEST_SCRIPT" not in fake.commands[0]


def test_datalist_generator_writes_both_filelist_names(tmp_path):
    """Datalist execution keeps the legacy name and adds filelist.txt."""
    contents = tmp_path / "contents"
    contents.mkdir()
    yaml = YamlFile(str(contents / "celebi.yaml"))
    yaml.write_variable("datalist", ["root://host//a.root"])
    job = ContainerJob.__new__(ContainerJob, str(tmp_path), "runner")
    job.path = str(tmp_path)
    job.yaml_file = yaml

    script = job._create_datalist_generator()  # pylint: disable=protected-access
    with open(script, encoding="utf-8") as stream:
        text = stream.read()
    assert "stageout/filelist.txt" in text
    assert "stageout/dataList.txt" in text
