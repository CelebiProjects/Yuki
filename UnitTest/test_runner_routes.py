"""Tests for runner management routes with SSH backend."""
import json
import os
import tempfile
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.server.routes import runner as runner_routes


def _app(bp):
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app


def _temp_config(monkeypatch):
    """Return a YukiConfig-like object pointing at a temp HOME."""
    tmp = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmp, ".Yuki"), exist_ok=True)
    config_obj = mock.MagicMock()
    config_obj.home_dir = tmp
    config_obj.config_path = os.path.join(tmp, ".Yuki", "config.json")
    config_obj.storage_path = os.path.join(tmp, ".Yuki", "Storage")
    config_obj.get_config_file.return_value = ConfigFile(config_obj.config_path)
    monkeypatch.setattr(runner_routes, "config", config_obj)
    return tmp, config_obj


def test_register_runner_stores_ssh_config(monkeypatch):
    """Registering an ssh runner persists host, user, key, and port."""
    _temp_config(monkeypatch)

    app = _app(runner_routes.bp)
    c = app.test_client()

    r = c.post("/register-runner", data={
        "runner": "mycluster",
        "url": "",
        "token": "",
        "backend_type": "ssh",
        "ssh_host": "cluster.example.com",
        "ssh_user": "alice",
        "ssh_key_path": "~/.ssh/id_rsa",
        "ssh_port": "2222",
        "remote_workdir": "/data/yuki",
    })
    assert r.status_code == 200
    assert b"successful" in r.data

    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    runner_id = cfg["runners_id"]["mycluster"]
    assert cfg["backend_types"][runner_id] == "ssh"
    assert cfg["ssh_hosts"][runner_id] == "cluster.example.com"
    assert cfg["ssh_users"][runner_id] == "alice"
    assert cfg["ssh_key_paths"][runner_id] == "~/.ssh/id_rsa"
    assert cfg["ssh_ports"][runner_id] == 2222
    assert cfg["remote_workdirs"][runner_id] == "/data/yuki"


def test_runners_config_includes_ssh_fields(monkeypatch):
    """runners-config exposes ssh fields for ssh runners."""
    _temp_config(monkeypatch)
    runner_id = "r-uuid"
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump({
            "runners": ["mycluster"],
            "runners_id": {"mycluster": runner_id},
            "urls": {runner_id: ""},
            "tokens": {runner_id: ""},
            "backend_types": {runner_id: "ssh"},
            "ssh_hosts": {runner_id: "cluster.example.com"},
            "ssh_users": {runner_id: "alice"},
            "ssh_key_paths": {runner_id: "~/.ssh/id_rsa"},
            "ssh_ports": {runner_id: 22},
            "remote_workdirs": {runner_id: "/tmp/yuki-workflows"},
        }, f)

    app = _app(runner_routes.bp)
    r = app.test_client().get("/runners-config")
    assert r.status_code == 200
    data = r.get_json()
    assert len(data) == 1
    assert data[0]["backend_type"] == "ssh"
    assert data[0]["ssh_host"] == "cluster.example.com"
    assert data[0]["ssh_user"] == "alice"


def test_update_runner_switches_to_ssh_and_stores_fields(monkeypatch):
    """Updating a runner to ssh persists the new ssh fields."""
    _temp_config(monkeypatch)
    runner_id = "r-uuid"
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump({
            "runners": ["mycluster"],
            "runners_id": {"mycluster": runner_id},
            "urls": {runner_id: "https://reana.example.com"},
            "tokens": {runner_id: "token"},
            "backend_types": {runner_id: "reana"},
        }, f)

    app = _app(runner_routes.bp)
    c = app.test_client()
    r = c.patch("/update-runner/mycluster", json={
        "backend_type": "ssh",
        "ssh_host": "cluster.example.com",
        "ssh_user": "alice",
        "ssh_key_path": "~/.ssh/id_rsa",
        "remote_workdir": "/data/yuki",
    })
    assert r.status_code == 200

    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    assert cfg["backend_types"][runner_id] == "ssh"
    assert cfg["ssh_hosts"][runner_id] == "cluster.example.com"
    assert cfg["remote_workdirs"][runner_id] == "/data/yuki"


def test_remove_runner_cleans_ssh_config(monkeypatch):
    """Removing a runner drops its legacy ssh_* entries."""
    _temp_config(monkeypatch)
    runner_id = "r-uuid"
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump({
            "runners": ["mycluster"],
            "runners_id": {"mycluster": runner_id},
            "urls": {runner_id: ""},
            "tokens": {runner_id: ""},
            "backend_types": {runner_id: "ssh"},
            "ssh_hosts": {runner_id: "cluster.example.com"},
            "ssh_users": {runner_id: "alice"},
            "ssh_key_paths": {runner_id: "~/.ssh/id_rsa"},
            "ssh_ports": {runner_id: 22},
            "remote_workdirs": {runner_id: "/tmp/yuki-workflows"},
        }, f)

    app = _app(runner_routes.bp)
    r = app.test_client().get("/remove-runner/mycluster")
    assert r.status_code == 200

    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    assert "mycluster" not in cfg["runners"]
    assert runner_id not in cfg.get("ssh_hosts", {})
    assert runner_id not in cfg.get("ssh_users", {})


def test_runner_connection_ssh_uses_paramiko_ping(monkeypatch):
    """runner-connection pings ssh runners via paramiko."""
    _temp_config(monkeypatch)
    runner_id = "r-uuid"
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump({
            "runners": ["mycluster"],
            "runners_id": {"mycluster": runner_id},
            "urls": {runner_id: ""},
            "tokens": {runner_id: ""},
            "backend_types": {runner_id: "ssh"},
            "ssh_hosts": {runner_id: "cluster.example.com"},
            "ssh_users": {runner_id: "alice"},
            "ssh_key_paths": {runner_id: "~/.ssh/id_rsa"},
            "ssh_ports": {runner_id: 22},
        }, f)

    mock_client = mock.MagicMock()
    with mock.patch("paramiko.SSHClient") as ssh_cls:
        ssh_cls.return_value = mock_client
        mock_client.exec_command.return_value = (
            mock.MagicMock(),
            mock.MagicMock(**{"read.return_value": b"ok",
                              "channel.recv_exit_status.return_value": 0}),
            mock.MagicMock(**{"read.return_value": b""}),
        )

        app = _app(runner_routes.bp)
        r = app.test_client().get("/runner-connection/mycluster")
        assert r.status_code == 200
        assert r.get_json()["status"] == "Connected"


def test_register_runner_stores_native_settings(monkeypatch):
    """Registering a native runner persists its runner_settings."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    r = c.post("/register-runner", data={
        "runner": "local", "url": "", "token": "", "backend_type": "native",
        "workdir": "/data/yuki", "cores": "8", "mem_mb": "4096",
        "conda_path": "/opt/conda/bin/conda",
    })
    assert r.status_code == 200
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    runner_id = cfg["runners_id"]["local"]
    assert cfg["runner_settings"][runner_id] == {
        "workdir": "/data/yuki", "cores": 8, "mem_mb": 4096,
        "conda_path": "/opt/conda/bin/conda",
    }


def test_register_runner_duplicate_name_409(monkeypatch):
    """Registering a duplicate runner name returns 409."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={"runner": "local", "url": "",
                                     "token": "", "backend_type": "native"})
    r = c.post("/register-runner", data={"runner": "local", "url": "",
                                         "token": "", "backend_type": "native"})
    assert r.status_code == 409
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    assert cfg["runners"].count("local") == 1


def test_register_runner_missing_field_400(monkeypatch):
    """A register request missing the runner name is rejected."""
    _temp_config(monkeypatch)
    r = _app(runner_routes.bp).test_client().post(
        "/register-runner", data={"runner": "local"})
    assert r.status_code == 400


def test_register_runner_ssh_double_writes_settings(monkeypatch):
    """Ssh runner registration writes both legacy maps and new settings."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u", "ssh_key_path": "/k",
        "ssh_port": "2222", "remote_workdir": "/remote",
    })
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    runner_id = cfg["runners_id"]["cluster"]
    # legacy maps still written
    assert cfg["ssh_hosts"][runner_id] == "h"
    # new map also written
    s = cfg["runner_settings"][runner_id]
    assert s["ssh_host"] == "h" and s["ssh_port"] == 2222
    assert s["remote_workdir"] == "/remote"


def test_update_runner_stores_settings(monkeypatch):
    """Updating a runner persists partial settings changes."""
    _temp_config(monkeypatch)
    runner_id = "r-uuid"
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump({"runners": ["local"], "runners_id": {"local": runner_id},
                   "backend_types": {runner_id: "native"}}, f)
    r = _app(runner_routes.bp).test_client().patch(
        "/update-runner/local", json={"cores": 16, "snakemake_path": "/usr/bin/snakemake"})
    assert r.status_code == 200
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    assert cfg["runner_settings"][runner_id]["cores"] == 16
    assert cfg["runner_settings"][runner_id]["snakemake_path"] == "/usr/bin/snakemake"


def test_machine_id_unknown_404(monkeypatch):
    """An unknown machine id gives 404."""
    _temp_config(monkeypatch)
    r = _app(runner_routes.bp).test_client().get("/machine-id/ghost")
    assert r.status_code == 404


def test_runners_url_tolerates_missing_entries(monkeypatch):
    """runners-url tolerates runners without ids or urls."""
    _temp_config(monkeypatch)
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump({"runners": ["a", "b"], "runners_id": {"a": "id-a"},
                   "urls": {}}, f)  # "b" has no id, "a" has no url
    r = _app(runner_routes.bp).test_client().get("/runners-url")
    assert r.status_code == 200


def test_runners_config_includes_settings_and_health(monkeypatch):
    """runners-config includes runner_settings and runner_health."""
    _temp_config(monkeypatch)
    runner_id = "r-uuid"
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump({
            "runners": ["local"], "runners_id": {"local": runner_id},
            "urls": {runner_id: ""}, "tokens": {runner_id: ""},
            "backend_types": {runner_id: "native"},
            "runner_settings": {runner_id: {"cores": 8}},
            "runner_health": {runner_id: {"status": "ok", "checks": {}}},
        }, f)
    data = _app(runner_routes.bp).test_client().get("/runners-config").get_json()
    assert data[0]["settings"] == {"cores": 8}
    assert data[0]["health"]["status"] == "ok"


def test_runners_config_defaults_without_new_maps(monkeypatch):
    """runners-config defaults settings and health when maps are absent."""
    _temp_config(monkeypatch)
    runner_id = "r-uuid"
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump({"runners": ["local"], "runners_id": {"local": runner_id},
                   "urls": {runner_id: ""}, "tokens": {runner_id: ""},
                   "backend_types": {runner_id: "native"}}, f)
    data = _app(runner_routes.bp).test_client().get("/runners-config").get_json()
    assert data[0]["settings"] == {}
    assert data[0]["health"] == {"status": "untested"}


def test_remove_runner_cleans_settings_health_and_stale_ssh(monkeypatch):
    """Removing a runner drops settings, health, and stale ssh maps."""
    _temp_config(monkeypatch)
    runner_id = "r-uuid"
    # backend flipped reana after ssh: stale ssh_* entries must still go
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump({
            "runners": ["cluster"], "runners_id": {"cluster": runner_id},
            "urls": {runner_id: ""}, "tokens": {runner_id: ""},
            "backend_types": {runner_id: "reana"},
            "ssh_hosts": {runner_id: "h"},
            "runner_settings": {runner_id: {"cores": 8}},
            "runner_health": {runner_id: {"status": "ok"}},
        }, f)
    r = _app(runner_routes.bp).test_client().get("/remove-runner/cluster")
    assert r.status_code == 200
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    assert runner_id not in cfg.get("ssh_hosts", {})
    assert runner_id not in cfg.get("runner_settings", {})
    assert runner_id not in cfg.get("runner_health", {})


def test_update_runner_partial_ssh_preserves_existing_fields(monkeypatch):
    """PATCH with only some ssh fields must not wipe the others."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u", "ssh_key_path": "/k",
        "ssh_port": "2222", "remote_workdir": "/remote",
    })
    r = c.patch("/update-runner/cluster", json={"ssh_key_path": "/newkey"})
    assert r.status_code == 200
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    runner_id = cfg["runners_id"]["cluster"]
    assert cfg["ssh_key_paths"][runner_id] == "/newkey"   # updated
    assert cfg["ssh_hosts"][runner_id] == "h"             # preserved
    assert cfg["ssh_users"][runner_id] == "u"             # preserved
    assert cfg["ssh_ports"][runner_id] == 2222            # preserved
    assert cfg["remote_workdirs"][runner_id] == "/remote"  # preserved


def test_register_runner_uploads_ssh_key(monkeypatch):
    """An uploaded ssh key is stored server-side with 600 permissions."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    r = c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u",
        "ssh_key_data": "-----BEGIN KEY-----\nfake\n-----END KEY-----",
    })
    assert r.status_code == 200
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    runner_id = cfg["runners_id"]["cluster"]
    keys_dir = os.path.join(os.path.dirname(runner_routes.config.config_path), "keys")
    key_file = os.path.join(keys_dir, runner_id)
    # key stored server-side with 600 perms
    with open(key_file, encoding="utf-8") as f:
        assert f.read().startswith("-----BEGIN KEY-----")
    assert os.stat(key_file).st_mode & 0o777 == 0o600
    # ssh_key_path points at the stored key, in both maps
    assert cfg["ssh_key_paths"][runner_id] == key_file
    assert cfg["runner_settings"][runner_id]["ssh_key_path"] == key_file


def test_update_runner_uploads_ssh_key(monkeypatch):
    """Updating a runner with ssh_key_data rewrites the stored key."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u", "ssh_key_path": "/server/side/key",
    })
    r = c.patch("/update-runner/cluster",
                json={"ssh_key_data": "new-key-content"})
    assert r.status_code == 200
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    runner_id = cfg["runners_id"]["cluster"]
    keys_dir = os.path.join(os.path.dirname(runner_routes.config.config_path), "keys")
    key_file = os.path.join(keys_dir, runner_id)
    with open(key_file, encoding="utf-8") as f:
        assert f.read().startswith("new-key-content")
    assert cfg["runner_settings"][runner_id]["ssh_key_path"] == key_file
    assert cfg["ssh_hosts"][runner_id] == "h"  # untouched


def test_remove_runner_deletes_stored_key(monkeypatch):
    """Removing a runner deletes its stored ssh key file."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u", "ssh_key_data": "key-content",
    })
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    runner_id = cfg["runners_id"]["cluster"]
    key_file = os.path.join(
        os.path.dirname(runner_routes.config.config_path), "keys", runner_id)
    assert os.path.exists(key_file)
    c.get("/remove-runner/cluster")
    assert not os.path.exists(key_file)


def test_runner_ssh_config_returns_settings_and_key(monkeypatch):
    """runner-ssh-config returns ssh settings plus the stored key content."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "cluster.example.com", "ssh_user": "alice",
        "ssh_port": "2222", "remote_workdir": "/data/yuki",
        "ssh_key_data": "-----BEGIN KEY-----\nfake\n-----END KEY-----",
    })

    r = c.get("/runner-ssh-config/cluster")
    assert r.status_code == 200
    data = r.get_json()
    assert data["host"] == "cluster.example.com"
    assert data["user"] == "alice"
    assert data["port"] == 2222
    assert data["remote_workdir"] == "/data/yuki"
    assert data["key"].startswith("-----BEGIN KEY-----")


def test_runner_ssh_config_unknown_runner_404(monkeypatch):
    """runner-ssh-config 404s for an unknown runner name."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    r = c.get("/runner-ssh-config/nosuch")
    assert r.status_code == 404
    assert "nosuch" in r.get_json()["error"]


def test_runner_ssh_config_non_ssh_runner_400(monkeypatch):
    """runner-ssh-config 400s for a runner that is not ssh-backed."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "reanacluster", "url": "https://reana.example.com",
        "token": "t", "backend_type": "reana",
    })
    r = c.get("/runner-ssh-config/reanacluster")
    assert r.status_code == 400


def test_runner_ssh_config_missing_key_file_returns_empty_key(monkeypatch):
    """runner-ssh-config returns empty key when the key file is absent."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u",
        "ssh_key_path": "/nonexistent/key",
    })
    r = c.get("/runner-ssh-config/cluster")
    assert r.status_code == 200
    data = r.get_json()
    assert data["key"] == ""
    assert data["key_path"] == "/nonexistent/key"


def test_runner_ssh_config_resolves_environment_via_map(monkeypatch):
    """An environment param is resolved through the server's conda_env_map."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u",
    })
    with open(runner_routes.config.config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["conda_env_map"] = {
        "rootproject/root:6.32.02-ubuntu22.04": "env_root_6.32.02"}
    with open(runner_routes.config.config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f)

    r = c.get("/runner-ssh-config/cluster",
              query_string={"environment": "rootproject/root:6.32.02-ubuntu22.04"})
    assert r.status_code == 200
    assert r.get_json()["conda_env"] == "env_root_6.32.02"


def test_runner_ssh_config_mangles_unmapped_environment(monkeypatch):
    """An unmapped docker image falls back to the mangled name."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u",
    })
    r = c.get("/runner-ssh-config/cluster",
              query_string={"environment": "docker.io/reanahub/env-root6:6.18.04"})
    assert r.status_code == 200
    assert r.get_json()["conda_env"] == "reanahub_env-root6_6.18.04"


def test_runner_ssh_config_skips_environment_for_script_tasks(monkeypatch):
    """script/rawdata environments yield no conda_env."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u",
    })
    r = c.get("/runner-ssh-config/cluster",
              query_string={"environment": "script"})
    assert r.status_code == 200
    assert "conda_env" not in r.get_json()


def test_runner_ssh_config_without_environment_param(monkeypatch):
    """Without the environment param, no conda_env is returned."""
    _temp_config(monkeypatch)
    c = _app(runner_routes.bp).test_client()
    c.post("/register-runner", data={
        "runner": "cluster", "url": "", "token": "", "backend_type": "ssh",
        "ssh_host": "h", "ssh_user": "u",
    })
    r = c.get("/runner-ssh-config/cluster")
    assert r.status_code == 200
    assert "conda_env" not in r.get_json()


def _write_runner_config(config_obj, variables):
    with open(config_obj.config_path, "w", encoding="utf-8") as f:
        json.dump(variables, f)


def test_runner_data_returns_inventory(monkeypatch):
    """/runner-data serves the inventory for an ssh runner."""
    _temp_config(monkeypatch)
    _write_runner_config(runner_routes.config, {
        "runners_id": {"pkufarm": "r1"},
        "backend_types": {"r1": "ssh"},
    })
    inventory = {
        "cache": {"total_files": 1, "total_bytes": 10, "entries": [
            {"project": "p", "impression": "i", "files": 1,
             "bytes": 10, "known": "registered"}]},
        "workflows": {"total_files": 0, "total_bytes": 0, "entries": []},
    }
    with mock.patch.object(runner_routes.runner_inventory,
                           "inventory_runner",
                           return_value=inventory) as inv:
        r = _app(runner_routes.bp).test_client().get(
            "/runner-data/pkufarm")

    assert r.status_code == 200
    body = r.get_json()
    assert body["runner"] == "pkufarm"
    assert body["backend_type"] == "ssh"
    assert body["cache"]["entries"][0]["known"] == "registered"
    inv.assert_called_once_with("r1", "ssh")


def test_runner_data_unknown_runner_404(monkeypatch):
    """Unknown runners get a 404."""
    _temp_config(monkeypatch)
    _write_runner_config(runner_routes.config, {
        "runners_id": {"pkufarm": "r1"},
        "backend_types": {"r1": "ssh"},
    })
    r = _app(runner_routes.bp).test_client().get("/runner-data/nope")
    assert r.status_code == 404


def test_runner_data_reana_backend_400(monkeypatch):
    """Reana runners have no listable data and get a 400."""
    _temp_config(monkeypatch)
    _write_runner_config(runner_routes.config, {
        "runners_id": {"cern": "r2"},
        "backend_types": {"r2": "reana"},
    })
    r = _app(runner_routes.bp).test_client().get("/runner-data/cern")
    assert r.status_code == 400
    assert "error" in r.get_json()


def test_runner_data_inventory_failure_500(monkeypatch):
    """An inventory failure surfaces as a 500 with the error."""
    _temp_config(monkeypatch)
    _write_runner_config(runner_routes.config, {
        "runners_id": {"pkufarm": "r1"},
        "backend_types": {"r1": "ssh"},
    })
    with mock.patch.object(runner_routes.runner_inventory,
                           "inventory_runner",
                           side_effect=OSError("down")):
        r = _app(runner_routes.bp).test_client().get(
            "/runner-data/pkufarm")
    assert r.status_code == 500
    assert "down" in r.get_json()["error"]


def test_yuki_overview_returns_runner_inventory(monkeypatch, tmp_path):
    """/yuki-overview summarizes the current project's distribution data."""
    _temp_config(monkeypatch)
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    impression = "a" * 32
    yuki_dir = tmp_path / "Storage" / "proj"
    imp_dir = yuki_dir / impression
    imp_dir.mkdir(parents=True)
    with open(imp_dir / "distribution.json", "w", encoding="utf-8") as f:
        json.dump({
            "locations": {
                "runner:pkufarm": {
                    "cache": {"files": 1, "bytes": 10},
                    "workflow": {"files": 2, "bytes": 30},
                },
                "yuki": {"origin": "collected", "files": 3, "bytes": 40},
            }
    }, f)
    from Yuki.kernel import liveness
    liveness.save_live_set("proj", [impression], [], yuki_dir=str(tmp_path))
    r = _app(runner_routes.bp).test_client().get(
        "/yuki-overview?project_uuid=proj")

    assert r.status_code == 200
    body = r.get_json()
    assert body["project_uuid"] == "proj"
    assert body["distribution"]["live_bytes"] == 80
    assert body["distribution"]["stale_bytes"] == 0
    assert body["distribution"]["runners"]["pkufarm"]["bytes"] == 40


def test_refresh_distribution_route(monkeypatch, tmp_path):
    """/refresh-distribution refreshes the current impression registry."""
    _temp_config(monkeypatch)
    imp_dir = tmp_path / "Storage" / "proj" / "imp-1"
    imp_dir.mkdir(parents=True)
    fake = mock.MagicMock()
    fake.update_distribution.return_value = {"live": 1, "superseded": 0,
                                             "live_workflows": 1}
    from Yuki.server.routes import status as status_routes
    with mock.patch("Yuki.kernel.impression_storage.ImpressionStorage",
                    return_value=fake) as storage:
        app = _app(status_routes.bp)
        r = app.test_client().post("/refresh-distribution/proj/imp-1")

    assert r.status_code == 200
    assert r.get_json()["live"] == 1
    storage.assert_called_once_with("proj", "imp-1")
    fake.update_distribution.assert_called_once_with()
