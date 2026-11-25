"""Credential import and distribution without real keys or external services."""
import base64
import io
import json
from pathlib import Path
from unittest.mock import Mock

from flask import Flask
import pytest
from CelebiChrono.utils.metadata import ConfigFile
from Yuki.services import secrets, secrets_reana
from Yuki.server.routes import secrets as routes, runner

KEYTAB = b"\x05\x02synthetic-keytab-for-tests"


@pytest.fixture
def client(tmp_path, monkeypatch):
    path = tmp_path / "config.json"
    cfg = ConfigFile(str(path))
    cfg.write_variable("runners", ["cern"])
    cfg.write_variable("runners_id", {"cern": "r1"})
    cfg.write_variable("backend_types", {"r1": "reana"})
    monkeypatch.setattr(routes.config, "config_path", str(path))
    app = Flask(__name__)
    app.register_blueprint(routes.bp)
    app.register_blueprint(runner.bp)
    return app.test_client(), cfg


def test_import_private_binary_and_duplicate(client, tmp_path):
    http, _ = client
    def send():
        return http.post("/secrets", data={"name": "cern-keytab", "type": "kerberos",
                         "principal": "user@CERN.CH", "file": (io.BytesIO(KEYTAB), "key")})
    response = send()
    assert response.status_code == 201
    assert set(response.json) == {"name", "type", "principal"}
    path = secrets.secret_path(tmp_path, "cern-keytab")
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert base64.b64decode(json.loads(path.read_text())["data"]) == KEYTAB
    assert send().status_code == 409
    assert http.get("/secrets/cern-keytab").status_code == 404


@pytest.mark.parametrize("name,kind,principal,content", [
    ("../escape", "kerberos", "u@CERN.CH", KEYTAB),
    ("valid", "ssh", "u@CERN.CH", KEYTAB),
    ("valid", "kerberos", "", KEYTAB),
    ("valid", "kerberos", "u@CERN.CH", b"not-a-keytab"),
    ("valid", "kerberos", "u@CERN.CH", b"\x05\x02" + b"x" * secrets.MAX_KEYTAB_BYTES),
])
def test_invalid_import(tmp_path, name, kind, principal, content):
    with pytest.raises(ValueError):
        secrets.add(tmp_path, name, kind, principal, content)
    assert not (tmp_path / "Secrets").exists()


def test_binding_only_saved_after_success(client, monkeypatch):
    http, cfg = client
    upload = Mock(side_effect=RuntimeError("sensitive backend detail"))
    monkeypatch.setattr(secrets, "bind_reana", upload)
    response = http.patch("/update-runner/cern", json={"secret": "cern-keytab"})
    assert response.status_code == 502
    assert "sensitive" not in response.text
    assert not cfg.read_variable("runner_settings", {})
    upload.side_effect = None
    assert http.patch("/update-runner/cern", json={"secret": "cern-keytab"}).status_code == 200
    assert cfg.read_variable("runner_settings")["r1"]["secret"] == "cern-keytab"
    assert cfg.read_variable("use_kerberos")["r1"] is True
    assert http.patch("/update-runner/cern", json={"secret": "x", "url": "other"}).status_code == 400
    assert upload.call_count == 2


def test_reana_payload_matches_sdk(tmp_path, monkeypatch):
    import sys
    monkeypatch.setenv("REANA_SERVER_URL", "https://original.example")
    cfg = ConfigFile(str(tmp_path / "config.json"))
    cfg.write_variable("urls", {"r1": "https://reana.example"})
    cfg.write_variable("tokens", {"r1": "test-token"})
    secrets.add(tmp_path, "cern-keytab", "kerberos", "user@CERN.CH", KEYTAB)
    sdk = Mock()
    monkeypatch.setitem(sys.modules, "reana_client.api", Mock(client=sdk))
    monkeypatch.setitem(sys.modules, "reana_commons.api_client", Mock())
    secrets_reana.upload({"config_path": cfg.file_path, "runner_id": "r1", "name": "cern-keytab"})
    payload = sdk.add_secrets.call_args.args[0]
    assert base64.b64decode(payload["yuki-cern-keytab.keytab"]["value"]) == KEYTAB
    assert base64.b64decode(payload["CERN_USER"]["value"]) == b"user"
    assert base64.b64decode(payload["CERN_KEYTAB"]["value"]) == b"yuki-cern-keytab.keytab"
    assert sdk.add_secrets.call_args.kwargs["overwrite"] is False


def test_ssh_keytab_outside_workflow_and_private(tmp_path, monkeypatch):
    from Yuki.kernel.workflows import ssh as ssh_module
    cfg = ConfigFile(str(tmp_path / "config.json"))
    secrets.add(tmp_path, "farm", "kerberos", "user@CERN.CH", KEYTAB)
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    connection.exec.side_effect = [("/home/user/.yuki-secret.random\n", "", 0), ("", "", 0)]
    def check_copy(local, remote):
        assert Path(local).read_bytes() == KEYTAB
        assert Path(local).stat().st_mode & 0o777 == 0o600
        assert remote == "/home/user/.yuki-secret.random/credential.keytab"
    connection.put.side_effect = check_copy
    monkeypatch.setattr(ssh_module, "_SshConnection", Mock(return_value=connection))
    result = secrets.bind_ssh(cfg, "farm", {"host": "host", "user": "user"})
    assert result["secret"] == "farm"
    assert result["kerberos_principal"] == "user@CERN.CH"
    assert connection.exec.call_args.args[0].startswith("chmod 600 ")


def test_eos_worker_uses_imported_key_and_cleans_up(tmp_path, monkeypatch):
    from Yuki.services import eos_download
    secrets.add(tmp_path, "cern", "kerberos", "user@CERN.CH", KEYTAB)
    paths = []
    def kinit(args, **kwargs):
        assert args[:2] == ["kinit", "-kt"]
        path = Path(args[2])
        paths.append(path)
        assert path.read_bytes() == KEYTAB
        assert path.stat().st_mode & 0o777 == 0o600
        assert args[3] == "user@CERN.CH"
        return Mock(returncode=0)
    monkeypatch.setattr(eos_download.subprocess, "run", kinit)
    with eos_download.worker_environment("cern", tmp_path) as env:
        assert env["KRB5CCNAME"].startswith("FILE:")
        assert paths[0].exists()
    assert not paths[0].exists()


def test_reana_subprocess_errors_are_redacted(tmp_path, monkeypatch):
    secrets.add(tmp_path, "cern", "kerberos", "user@CERN.CH", KEYTAB)
    execute = Mock(return_value=Mock(returncode=1, stdout="private content", stderr="private content"))
    monkeypatch.setattr(secrets.subprocess, "run", execute)
    with pytest.raises(RuntimeError) as error:
        secrets.bind_reana(tmp_path / "config.json", "r1", "cern")
    assert "private content" not in str(error.value)
    assert "data" not in json.loads(execute.call_args.kwargs["input"])


def test_symlink_store_rejected(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "Secrets").symlink_to(outside)
    with pytest.raises(ValueError, match="symlink"):
        secrets.add(tmp_path, "cern", "kerberos", "user@CERN.CH", KEYTAB)
    assert not list(outside.iterdir())
