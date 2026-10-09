"""Process-local, exclusive leases for persistent SSH/SFTP connections."""
import atexit
import os
import threading
import time


class SshPool:
    """Bound connections per destination; never share an SFTP session concurrently."""

    def __init__(self, limit=4, wait_timeout=30):
        self.limit = limit
        self.wait_timeout = wait_timeout
        self._condition = threading.Condition()
        self._entries = {}

    @staticmethod
    def _healthy(entry):
        client, sftp = entry
        transport = client.get_transport()
        return (transport is not None and transport.is_active()
                and transport.is_authenticated()
                and not sftp.get_channel().closed)

    @staticmethod
    def _close(entry):
        # Close the transport first: an unresponsive SFTP channel must not
        # delay teardown waiting for the server to acknowledge its close.
        entry[0].close()

    def acquire(self, key, factory):
        deadline = time.monotonic() + self.wait_timeout
        with self._condition:
            while True:
                entries = self._entries.setdefault(key, {})
                for entry, busy in list(entries.items()):
                    if busy:
                        continue
                    if self._healthy(entry):
                        entries[entry] = True
                        return entry
                    del entries[entry]
                    self._close(entry)
                if len(entries) < self.limit:
                    # Reserve a slot before releasing the lock for authentication.
                    reservation = object()
                    entries[reservation] = True
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for an SSH connection")
                self._condition.wait(remaining)
        try:
            entry = factory()
        except BaseException:
            with self._condition:
                entries.pop(reservation, None)
                self._condition.notify_all()
            raise
        with self._condition:
            entries.pop(reservation, None)
            entries[entry] = True
        return entry

    def release(self, key, entry, discard=False):
        with self._condition:
            entries = self._entries.get(key, {})
            if discard or not self._healthy(entry):
                entries.pop(entry, None)
                self._close(entry)
            else:
                entries[entry] = False
            self._condition.notify_all()

    def close(self):
        """Close pooled transports at process shutdown (or between tests)."""
        with self._condition:
            for entries in self._entries.values():
                for entry in entries:
                    if isinstance(entry, tuple):
                        self._close(entry)
            self._entries.clear()
            self._condition.notify_all()

    def after_fork(self):
        """Discard inherited sockets without sending SSH messages to the peer."""
        for entries in self._entries.values():
            for entry in entries:
                if isinstance(entry, tuple):
                    transport = entry[0].get_transport()
                    if transport is not None:
                        transport.atfork()
        self._condition = threading.Condition()
        self._entries = {}


ssh_pool = SshPool()
atexit.register(ssh_pool.close)
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=ssh_pool.after_fork)
