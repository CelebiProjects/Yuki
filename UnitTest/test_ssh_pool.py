"""Persistent SSH lease lifecycle and concurrency regression tests."""
from concurrent.futures import ThreadPoolExecutor
import threading
from unittest.mock import MagicMock, patch

import pytest

from Yuki.kernel.ssh_pool import SshPool, ssh_pool
from Yuki.kernel.ssh_workflow import _SshConnection


def connection():
    client, sftp = MagicMock(), MagicMock()
    sftp.get_channel.return_value.closed = False
    return client, sftp


@pytest.fixture(autouse=True)
def clear_pool():
    ssh_pool.close()
    yield
    ssh_pool.close()


def test_repeated_operations_reuse_authenticated_connection():
    client, sftp = connection()
    client.open_sftp.return_value = sftp
    with patch('paramiko.SSHClient', return_value=client) as factory:
        for _ in range(10):
            with _SshConnection('host', 'user') as ssh:
                assert ssh._sftp is sftp
        assert factory.call_count == 1
        client.connect.assert_called_once()
        client.get_transport().set_keepalive.assert_called_once_with(30)
        client.close.assert_not_called()


def test_dead_connection_replaced():
    pool = SshPool()
    first, second = connection(), connection()
    factory = MagicMock(side_effect=[first, second])
    pool.release('host', pool.acquire('host', factory))
    first[0].get_transport().is_active.return_value = False
    assert pool.acquire('host', factory) is second
    first[0].close.assert_called_once()
    pool.close()


def test_sftp_closed_connection_replaced():
    pool = SshPool()
    first, second = connection(), connection()
    factory = MagicMock(side_effect=[first, second])
    pool.release('host', pool.acquire('host', factory))
    first[1].get_channel().closed = True
    assert pool.acquire('host', factory) is second
    pool.close()


def test_bounded_pool_waits_for_exclusive_lease():
    pool = SshPool(limit=1)
    factory = MagicMock(side_effect=connection)
    first = pool.acquire('host', factory)
    waiting = threading.Event()

    def borrow():
        waiting.set()
        return pool.acquire('host', factory)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(borrow)
        assert waiting.wait(1)
        assert not future.done()
        pool.release('host', first)
        assert future.result(timeout=1) is first
    assert factory.call_count == 1
    pool.close()


def test_wait_timeout_and_separate_destinations():
    pool = SshPool(limit=1, wait_timeout=0.01)
    first = pool.acquire('one', connection)
    with pytest.raises(TimeoutError):
        pool.acquire('one', connection)
    assert pool.acquire('two', connection) is not first
    pool.close()


def test_failed_factory_releases_reservation():
    pool = SshPool(limit=1)
    with pytest.raises(OSError):
        pool.acquire('host', MagicMock(side_effect=OSError('offline')))
    pool.acquire('host', connection)
    pool.close()


def test_exception_discards_connection_without_replaying_operation():
    client, sftp = connection()
    client.open_sftp.return_value = sftp
    with patch('paramiko.SSHClient', return_value=client):
        with pytest.raises(RuntimeError):
            with _SshConnection('host', 'user'):
                raise RuntimeError('uncertain remote result')
    client.close.assert_called_once()
    client.connect.assert_called_once()


def test_sftp_open_failure_closes_transport():
    client, _ = connection()
    client.open_sftp.side_effect = OSError('failed')
    with patch('paramiko.SSHClient', return_value=client):
        with pytest.raises(OSError):
            _SshConnection('host', 'user').connect()
    client.close.assert_called_once()


def test_fork_discards_inherited_transports():
    pool = SshPool()
    entry = pool.acquire('host', connection)
    pool.after_fork()
    entry[0].get_transport().atfork.assert_called_once()
    entry[0].close.assert_not_called()
    assert pool.acquire('host', connection) is not entry
    pool.close()


def test_changed_credentials_do_not_reuse_connection(tmp_path):
    key = tmp_path / 'key'
    key.write_text('first')

    def make_client():
        client, sftp = connection()
        client.open_sftp.return_value = sftp
        return client

    with patch('paramiko.SSHClient', side_effect=make_client) as factory:
        with _SshConnection('host', 'alice', str(key)):
            pass
        with _SshConnection('host', 'bob', str(key)):
            pass
        key.write_text('replacement key')
        with _SshConnection('host', 'alice', str(key)):
            pass
        assert factory.call_count == 3


def test_shutdown_closes_idle_and_borrowed_connections():
    pool = SshPool()
    idle = pool.acquire('host', connection)
    borrowed = pool.acquire('host', connection)
    pool.release('host', idle)
    pool.close()
    idle[0].close.assert_called_once()
    borrowed[0].close.assert_called_once()
