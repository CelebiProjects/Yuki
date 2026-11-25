"""Regression coverage for binding HTTP to an OS-assigned port."""

import socket
import time
from multiprocessing import get_context
from urllib.request import urlopen

import pytest

from Yuki.server_main import start_flask_app


def test_reported_port_is_the_live_loopback_listener(tmp_path, monkeypatch):
    """A launcher can read the port file and immediately connect to Yuki."""
    with socket.socket() as probe:
        try:
            probe.bind(("127.0.0.1", 0))
        except PermissionError:
            pytest.skip("Loopback binding is unavailable in this sandbox")
    port_file = tmp_path / "http.port"
    monkeypatch.setenv("YUKI_HTTP_PORT", "0")
    monkeypatch.setenv("YUKI_HTTP_HOST", "127.0.0.1")
    monkeypatch.setenv("YUKI_HTTP_PORT_FILE", str(port_file))
    process = get_context("spawn").Process(target=start_flask_app)
    process.start()
    try:
        deadline = time.monotonic() + 12
        while not port_file.exists() and time.monotonic() < deadline:
            assert process.is_alive()
            time.sleep(0.05)
        port = int(port_file.read_text(encoding="ascii"))
        assert 0 < port <= 65535
        with urlopen(f"http://127.0.0.1:{port}/dite-status", timeout=5) as response:
            assert response.read() == b"ok"
    finally:
        process.terminate()
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join(5)
