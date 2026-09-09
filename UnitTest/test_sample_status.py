"""Sample readiness supports canonical rawdata and legacy uploads."""
import json
from unittest import mock

import pytest
from flask import Flask
from CelebiChrono.kernel.vtask_job import JobManager
from CelebiChrono.kernel.chern_communicator import ChernCommunicator
from Yuki.server.config import YukiConfig
from Yuki.server.routes import status as status_routes


@pytest.fixture
def sample(tmp_path, monkeypatch):
    config = YukiConfig()
    config.storage_path = str(tmp_path)
    monkeypatch.setattr(status_routes, "config", config)
    job = tmp_path / "project" / "impression"
    (job / "contents").mkdir(parents=True)
    (job / "config.json").write_text('{"object_type": "task"}')
    (job / "contents" / "celebi.yaml").write_text(
        'environment: rawdata\nuuid: abc123\n')
    app = Flask(__name__)
    app.register_blueprint(status_routes.bp)
    return job, app.test_client()


@pytest.mark.parametrize("status", ["archived", "finished", "success", "coda", "final note"])
def test_canonical_sample_returns_md5_and_client_reports_finished(sample, status):
    job, client = sample
    (job / "status.json").write_text(json.dumps({"status": status}))
    response = client.get('/sample-status/project/impression')
    assert response.status_code == 200
    assert response.text == 'abc123'
    communicator = mock.Mock()
    communicator.sample_status.return_value = response.text
    task = mock.Mock()
    task.environment.return_value = 'rawdata'
    task.input_md5.return_value = 'abc123'
    with mock.patch.object(ChernCommunicator, 'instance', return_value=communicator):
        assert JobManager.run_status(task) == 'finished'
        task.input_md5.return_value = 'different'
        assert JobManager.run_status(task) == 'unsubmitted'


@pytest.mark.parametrize("status", [None, "running", "failed", "pending", "deleted"])
def test_incomplete_registration_does_not_report_ready(sample, status):
    job, client = sample
    if status is not None:
        (job / "status.json").write_text(json.dumps({"status": status}))
    assert client.get('/sample-status/project/impression').text == ''


def test_legacy_sample_uuid_is_preserved(sample):
    job, client = sample
    (job / 'config.json').write_text('{"sample_uuid": "legacy-md5"}')
    assert client.get('/sample-status/project/impression').text == 'legacy-md5'


@pytest.mark.parametrize('contents', [None, 'environment: script\nuuid: abc123\n',
                                     'environment: rawdata\n'])
def test_missing_or_non_rawdata_metadata_is_not_ready(sample, contents):
    job, client = sample
    (job / 'status.json').write_text('{"status": "archived"}')
    yaml_file = job / 'contents' / 'celebi.yaml'
    if contents is None:
        yaml_file.unlink()
    else:
        yaml_file.write_text(contents)
    assert client.get('/sample-status/project/impression').text == ''


def test_unknown_impression_is_not_ready(sample):
    _, client = sample
    response = client.get('/sample-status/project/unknown')
    assert response.status_code == 200
    assert response.text == ''
