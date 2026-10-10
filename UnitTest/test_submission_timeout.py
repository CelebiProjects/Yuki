"""Submission timeouts survive dispatch and extend SSH operation limits."""
import io
from unittest.mock import Mock, patch

import pytest
from flask import Flask

from Yuki.server.routes import execution
from Yuki.server import tasks
from Yuki.kernel.workflows.ssh import SshWorkflow, _SshConnection
from Yuki.kernel.execution.status import SILENCE, PRELUDE, FAILED, CODA, STOPPED


@pytest.mark.parametrize('timeout', [None, '3000'])
def test_execute_dispatches_timeout(timeout, tmp_path, monkeypatch):
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    data = {'machine': 'runner', 'project_uuid': 'proj', 'cache_on_runner': '{}',
            'impressions': (io.BytesIO(b'imp'), 'impressions')}
    if timeout is not None:
        data['timeout'] = timeout
    with patch.object(execution, 'VJob') as job, \
         patch.object(execution, 'task_exec_impression') as task:
        job.return_value.job_type.return_value = 'task'
        job.return_value.status.return_value = SILENCE
        job.return_value.uuid = 'imp'
        task.apply_async.return_value.id = 'task-id'
        response = app.test_client().post('/execute', data=data)
    assert response.status_code == 202
    payload = response.get_json()
    assert payload['status'] == 'accepted'
    assert response.headers['Location'].endswith(payload['submission_id'])
    task_kwargs = {'cache_on_runner': {'imp': False}}
    if timeout:
        task_kwargs['timeout'] = 3000
    task_kwargs['submission_id'] = payload['submission_id']
    task.apply_async.assert_called_once_with(
        args=['proj', 'imp', 'runner'], kwargs=task_kwargs)


def test_execute_returns_existing_active_workflow(tmp_path, monkeypatch):
    """A known duplicate is resolved synchronously instead of silently skipped."""
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    data = {'machine': 'runner', 'project_uuid': 'proj', 'cache_on_runner': '{}',
            'impressions': (io.BytesIO(b'imp'), 'impressions')}
    with patch.object(execution, 'VJob') as job, \
         patch.object(execution, 'workflow_status', return_value='unknown'), \
         patch.object(execution, 'workflow_is_active', return_value=True), \
         patch.object(execution, 'task_exec_impression') as task:
        job.return_value.job_type.return_value = 'task'
        job.return_value.status.return_value = PRELUDE
        job.return_value.uuid = 'imp'
        job.return_value.path = '/jobs/proj/imp'
        job.return_value.workflow_id.return_value = 'wf-existing'
        response = app.test_client().post('/execute', data=data)

    assert response.status_code == 200
    assert response.get_json()['status'] == 'deduplicated'
    assert response.get_json()['workflow_id'] == 'wf-existing'
    assert response.get_json()['submission_reason'] == 'active'
    assert response.get_json()['workflow_status'] == 'unknown'
    task.apply_async.assert_not_called()


def test_execute_reuses_completed_workflow(tmp_path, monkeypatch):
    """Submitting completed work reuses its existing workflow and results."""
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    data = {'machine': 'runner', 'project_uuid': 'proj', 'cache_on_runner': '{}',
            'impressions': (io.BytesIO(b'imp'), 'impressions')}
    with patch.object(execution, 'VJob') as job, \
         patch.object(execution, 'workflow_status', return_value='finished'), \
         patch.object(execution, 'workflow_is_active', return_value=False), \
         patch.object(execution, 'task_exec_impression') as task:
        job.return_value.job_type.return_value = 'task'
        job.return_value.status.return_value = CODA
        job.return_value.uuid = 'imp'
        job.return_value.path = '/jobs/proj/imp'
        job.return_value.workflow_id.return_value = 'wf-finished'
        response = app.test_client().post('/execute', data=data)

    assert response.status_code == 200
    assert response.get_json()['status'] == 'deduplicated'
    assert response.get_json()['workflow_id'] == 'wf-finished'
    assert response.get_json()['submission_reason'] == 'completed'
    assert response.get_json()['workflow_status'] == 'finished'
    task.apply_async.assert_not_called()


@pytest.mark.parametrize('retry_status', [FAILED, STOPPED])
def test_execute_retryable_job_records_replacement_workflow(
        retry_status, tmp_path, monkeypatch):
    """A failed or stopped attempt is submitted again with predecessor metadata."""
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    data = {'machine': 'runner', 'project_uuid': 'proj', 'cache_on_runner': '{}',
            'impressions': (io.BytesIO(b'imp'), 'impressions')}
    with patch.object(execution, 'VJob') as job, \
         patch.object(execution, 'task_exec_impression') as task:
        job.return_value.job_type.return_value = 'task'
        job.return_value.status.return_value = retry_status
        job.return_value.uuid = 'imp'
        job.return_value.workflow_id.return_value = 'wf-failed'
        task.apply_async.return_value.id = 'task-id'
        response = app.test_client().post('/execute', data=data)

    assert response.status_code == 202
    payload = response.get_json()
    assert payload['submission_reason'] == 'replacement'
    assert payload['previous_workflows'] == {
        'imp': {'workflow_id': 'wf-failed', 'job_status': retry_status}}
    task.apply_async.assert_called_once()


def test_execute_with_unknown_refreshes_but_does_not_duplicate(tmp_path,
                                                               monkeypatch):
    """An inconclusive explicit refresh leaves the unknown workflow alone."""
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    data = {'machine': 'runner', 'project_uuid': 'proj', 'cache_on_runner': '{}',
            'with_unknown': 'true',
            'impressions': (io.BytesIO(b'imp'), 'impressions')}
    with patch.object(execution, 'VJob') as job, \
         patch.object(execution, 'workflow_status', return_value='unknown'), \
         patch.object(execution, '_refresh_unknown_workflow',
                      return_value=('unknown', '')), \
         patch.object(execution, 'workflow_is_active', return_value=True), \
         patch.object(execution, 'task_exec_impression') as task:
        job.return_value.job_type.return_value = 'task'
        job.return_value.status.return_value = PRELUDE
        job.return_value.uuid = 'imp'
        job.return_value.path = '/jobs/proj/imp'
        job.return_value.workflow_id.return_value = 'wf-unknown'
        response = app.test_client().post('/execute', data=data)

    payload = response.get_json()
    assert payload['submission_reason'] == 'unknown'
    assert payload['status_refreshed'] is True
    assert payload['workflow_status'] == 'unknown'
    task.apply_async.assert_not_called()


def test_execute_with_unknown_refresh_failure_does_not_submit(tmp_path,
                                                               monkeypatch):
    """A failed status refresh is reported without creating a workflow."""
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    data = {'machine': 'runner', 'project_uuid': 'proj', 'cache_on_runner': '{}',
            'with_unknown': 'true',
            'impressions': (io.BytesIO(b'imp'), 'impressions')}
    with patch.object(execution, 'VJob') as job, \
         patch.object(execution, 'workflow_status', return_value='unknown'), \
         patch.object(execution, '_refresh_unknown_workflow',
                      return_value=('unknown', 'SSH unavailable')), \
         patch.object(execution, 'task_exec_impression') as task:
        job.return_value.job_type.return_value = 'task'
        job.return_value.status.return_value = PRELUDE
        job.return_value.uuid = 'imp'
        job.return_value.path = '/jobs/proj/imp'
        job.return_value.workflow_id.return_value = 'wf-unknown'
        response = app.test_client().post('/execute', data=data)

    payload = response.get_json()
    assert payload['submission_reason'] == 'refresh_failed'
    assert payload['refresh_error'] == 'SSH unavailable'
    task.apply_async.assert_not_called()


def test_execute_with_unknown_ignores_stale_job_failure_when_workflow_runs(
        tmp_path, monkeypatch):
    """A stale failed job cannot trigger retry after its workflow refreshes running."""
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    data = {'machine': 'runner', 'project_uuid': 'proj', 'cache_on_runner': '{}',
            'with_unknown': 'true',
            'impressions': (io.BytesIO(b'imp'), 'impressions')}
    with patch.object(execution, 'VJob') as job, \
         patch.object(execution, 'workflow_status',
                      side_effect=['unknown', 'running']), \
         patch.object(execution, '_refresh_unknown_workflow',
                      return_value=('running', '')), \
         patch.object(execution, 'workflow_is_active', return_value=True), \
         patch.object(execution, 'task_exec_impression') as task:
        job.return_value.job_type.return_value = 'task'
        job.return_value.status.return_value = FAILED
        job.return_value.uuid = 'imp'
        job.return_value.path = '/jobs/proj/imp'
        job.return_value.workflow_id.return_value = 'wf-unknown'
        response = app.test_client().post('/execute', data=data)

    payload = response.get_json()
    assert payload['submission_reason'] == 'active'
    assert payload['workflow_status'] == 'running'
    task.apply_async.assert_not_called()


def test_execute_with_unknown_replaces_only_after_confirmed_failure(
        tmp_path, monkeypatch):
    """A confirmed failed refresh makes the old workflow retryable."""
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    data = {'machine': 'runner', 'project_uuid': 'proj', 'cache_on_runner': '{}',
            'with_unknown': 'true',
            'impressions': (io.BytesIO(b'imp'), 'impressions')}
    with patch.object(execution, 'VJob') as job, \
         patch.object(execution, 'workflow_status', return_value='unknown'), \
         patch.object(execution, '_refresh_unknown_workflow',
                      return_value=(FAILED, '')), \
         patch.object(execution, 'task_exec_impression') as task:
        job.return_value.job_type.return_value = 'task'
        job.return_value.status.side_effect = [PRELUDE, FAILED]
        job.return_value.uuid = 'imp'
        job.return_value.workflow_id.return_value = 'wf-unknown'
        task.apply_async.return_value.id = 'task-id'
        response = app.test_client().post('/execute', data=data)

    payload = response.get_json()
    assert response.status_code == 202
    assert payload['submission_reason'] == 'replacement'
    assert payload['status_refreshed'] is True
    assert payload['previous_workflows']['imp']['workflow_id'] == 'wf-unknown'
    assert payload['previous_workflows']['imp']['workflow_status'] == FAILED
    task.apply_async.assert_called_once()


def test_refresh_unknown_workflow_uses_shared_refresh_lock():
    """The explicit refresh shares the same exclusion lock as status polling."""
    with patch.object(execution, 'VWorkflow') as factory, \
         patch.object(execution, 'running_refresh') as refresh, \
         patch.object(execution, 'workflow_status', return_value=FAILED):
        workflow = factory.create.return_value
        workflow.path = '/workflows/proj/wf-unknown'
        refresh.return_value.__enter__.return_value = True
        status, error = execution._refresh_unknown_workflow(
            'proj', 'wf-unknown')

    assert (status, error) == (FAILED, '')
    refresh.assert_called_once_with('/workflows/proj/wf-unknown')
    workflow.update_workflow_status.assert_called_once_with()


def test_refresh_unknown_workflow_refuses_overlapping_refresh():
    """A refresh already in progress is inconclusive, never permission to retry."""
    with patch.object(execution, 'VWorkflow') as factory, \
         patch.object(execution, 'running_refresh') as refresh, \
         patch.object(execution, 'workflow_status', return_value='unknown'):
        workflow = factory.create.return_value
        workflow.path = '/workflows/proj/wf-unknown'
        refresh.return_value.__enter__.return_value = False
        status, error = execution._refresh_unknown_workflow(
            'proj', 'wf-unknown')

    assert status == 'unknown'
    assert 'already in progress' in error
    workflow.update_workflow_status.assert_not_called()


@pytest.mark.parametrize('timeout', ['0', '-1', 'nan', 'inf', '1.2', ''])
def test_invalid_timeout_rejected_before_job_changes(timeout):
    app = Flask(__name__)
    app.register_blueprint(execution.bp)
    with patch.object(execution, 'VJob') as job:
        response = app.test_client().post('/execute', data={'timeout': timeout})
    assert response.status_code == 400
    job.assert_not_called()


def test_worker_persists_timeout_before_run():
    with patch.object(tasks, 'metadata') as meta, \
         patch.object(tasks, 'VJob'), patch.object(tasks, 'VWorkflow') as factory, \
         patch.object(tasks, '_validate_remote_data_binding', return_value=[]):
        meta.ConfigFile.return_value.read_variable.return_value = {}
        workflow = factory.create.return_value
        workflow.run.side_effect = lambda: (
            workflow.config_file.write_variable.assert_called_once_with('submission_timeout', 3000)
        )
        tasks.task_exec_impression('proj', 'imp', 'runner', timeout=3000)
    workflow.run.assert_called_once()


@pytest.mark.parametrize('timeout,expected', [(None, 300), (10, 300), (3000, 3000)])
def test_reloaded_workflow_sets_lease_timeout(timeout, expected):
    workflow = object.__new__(SshWorkflow)
    workflow.ssh_config = {'host': 'host', 'user': 'user'}
    workflow.config_file = Mock()
    workflow.config_file.read_variable.return_value = timeout
    connection = workflow._ssh()
    workflow.config_file.read_variable.assert_called_once_with('submission_timeout', None)
    connection._client = Mock()
    stdout, stderr = Mock(), Mock()
    stdout.read.return_value = b'ok'
    stderr.read.return_value = b''
    stdout.channel.recv_exit_status.return_value = 0
    connection._client.exec_command.return_value = (Mock(), stdout, stderr)
    assert connection.exec('chmod +x wrapper') == ('ok', '', 0)
    connection._client.exec_command.assert_called_once_with('chmod +x wrapper', timeout=expected)
    stdout.channel.settimeout.assert_called_once_with(expected)
    assert connection._operation_timeout(3600) == 3600
    assert _SshConnection('host', 'user')._operation_timeout(300) == 300


def test_detached_launch_extends_channel_timeout():
    connection = _SshConnection('host', 'user', timeout=3000)
    connection._client = Mock()
    stdout = Mock()
    stdout.channel.recv_ready.return_value = False
    stdout.channel.recv_stderr_ready.return_value = False
    connection._client.exec_command.return_value = (Mock(), stdout, Mock())
    connection.exec_start_detached('launch')
    connection._client.exec_command.assert_called_once_with('launch', timeout=3000)
    stdout.channel.settimeout.assert_called_once_with(3000)


def test_connect_extends_handshake_limits():
    connection = _SshConnection('host', 'user', timeout=3000)
    with patch('paramiko.SSHClient') as client:
        connection._open_connection()
    kwargs = client.return_value.connect.call_args.kwargs
    assert kwargs['timeout'] == 3000
    assert kwargs['banner_timeout'] == 3000
    assert kwargs['auth_timeout'] == 3000


def test_start_extends_marker_confirmation_timeout():
    workflow = object.__new__(SshWorkflow)
    workflow.remote_exec_path = '/workflow'
    workflow.config_file = Mock()
    workflow.config_file.read_variable.return_value = 3000
    workflow.logger = Mock()
    ssh = Mock()
    ssh.exec.return_value = ('', '', 0)
    ssh.exec_start_detached.return_value = ('', '', 0)
    with patch.object(workflow, '_ssh') as connect, \
         patch.object(workflow, '_build_remote_wrapper', return_value='wrapper'), \
         patch.object(workflow, '_confirm_remote_start') as confirm:
        connect.return_value.__enter__.return_value = ssh
        workflow._start_remote_snakemake()
    confirm.assert_called_once_with(ssh, timeout=3000)
