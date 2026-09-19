"""Best-effort progress snapshots that never wait for a slow SSH operation."""
from collections import OrderedDict
import os
import threading
import time


class ProgressCache:  # pylint: disable=too-few-public-methods
    """Bound refresh concurrency and memory independently of HTTP polling."""

    def __init__(self, limit=4, capacity=128, ttl=3, wait=0.1):
        self.limit = limit
        self.capacity = capacity
        self.ttl = ttl
        self.wait = wait
        self._reset()

    def _reset(self):
        self._lock = threading.Lock()
        self._values = OrderedDict()
        self._pending = {}

    def get(self, key, reader):
        """Return a cached snapshot or None; allow only a brief initial wait."""
        with self._lock:
            value, updated = self._values.get(key, (None, float("-inf")))
            if time.monotonic() - updated < self.ttl:
                return value
            event = self._pending.get(key)
            if event is None:
                if len(self._pending) >= self.limit:
                    return value
                event = threading.Event()
                self._pending[key] = event
                try:
                    threading.Thread(target=self._refresh,
                                     args=(key, reader, event), daemon=True).start()
                except RuntimeError:
                    self._pending.pop(key)
                    return value
        event.wait(self.wait)
        with self._lock:
            return self._values.get(key, (None, 0))[0]

    def _refresh(self, key, reader, event):
        try:
            value = reader()
        except Exception:  # pylint: disable=broad-exception-caught
            value = None
        with self._lock:
            self._values[key] = (value, time.monotonic())
            self._values.move_to_end(key)
            while len(self._values) > self.capacity:
                self._values.popitem(last=False)
            self._pending.pop(key)
            event.set()


progress_cache = ProgressCache(ttl=1, wait=1)
remote_listing_cache = ProgressCache(ttl=1, wait=1)
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=progress_cache._reset)  # pylint: disable=protected-access
    os.register_at_fork(after_in_child=remote_listing_cache._reset)  # pylint: disable=protected-access
