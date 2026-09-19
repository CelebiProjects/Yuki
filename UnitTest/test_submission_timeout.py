"""Submission timeouts survive dispatch and extend SSH operation limits."""
import io
from unittest.mock import Mock, patch

import pytest
from flask import Flask

from Yuki.server.routes import execution
from Yuki.server import tasks
from Yuki.kernel.ssh_workflow import SshWorkflow, _SshConnection
from Yuki.kernel.status_constants import SILENCE


@pytest.mark.parametrize('timeout', [None, '3000'])
def test_execute_dispatches_timeout(timeout):
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
    assert response.status_code == 200
    options = {'kwargs': {'timeout': 3000}} if timeout else {}
    task.apply_async.assert_called_once_with(args=['proj', 'imp', 'runner'], **options)


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
