"""Tests for runner settings/health config helpers."""
import os
import tempfile

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.runners import config as runner_config


def _cfg():
    tmp = tempfile.mkdtemp()
    return ConfigFile(os.path.join(tmp, "config.json"))


def test_runner_settings_roundtrip():
    """Runner settings merge incrementally and read back whole."""
    cfg = _cfg()
    assert not runner_config.get_runner_settings(cfg, "r1")
    runner_config.merge_runner_settings(cfg, "r1", {"workdir": "/data", "cores": 8})
    runner_config.merge_runner_settings(cfg, "r1", {"mem_mb": 4096})
    assert runner_config.get_runner_settings(cfg, "r1") == {
        "workdir": "/data", "cores": 8, "mem_mb": 4096,
    }


def test_ssh_settings_prefer_new_map():
    """The runner_settings map wins over the legacy ssh_* maps."""
    cfg = _cfg()
    cfg.write_variable("ssh_hosts", {"r1": "old.example.com"})
    cfg.write_variable("ssh_users", {"r1": "olduser"})
    cfg.write_variable("runner_settings", {"r1": {"ssh_host": "new.example.com",
                                                  "cores": 4}})
    s = runner_config.get_ssh_settings(cfg, "r1")
    assert s["host"] == "new.example.com"   # new map wins
    assert s["user"] == "olduser"           # falls back to old map
    assert s["port"] == 22                  # default
    assert s["remote_workdir"] == "/tmp/yuki-workflows"  # default
    assert s["cores"] == 4


def test_ssh_settings_old_runner_no_migration():
    """Legacy ssh_* maps are consumed without rewriting the config."""
    cfg = _cfg()
    cfg.write_variable("ssh_hosts", {"r1": "h"})
    cfg.write_variable("ssh_users", {"r1": "u"})
    cfg.write_variable("ssh_key_paths", {"r1": "/k"})
    cfg.write_variable("ssh_ports", {"r1": 2222})
    cfg.write_variable("remote_workdirs", {"r1": "/remote"})
    s = runner_config.get_ssh_settings(cfg, "r1")
    assert s == {"host": "h", "user": "u", "key_path": "/k", "port": 2222,
                 "remote_workdir": "/remote", "cores": "all",
                 "conda_path": "", "snakemake_path": ""}


def test_runner_health_roundtrip():
    """Runner health defaults to untested and persists updates."""
    cfg = _cfg()
    assert runner_config.get_runner_health(cfg, "r1") == {"status": "untested"}
    runner_config.set_runner_health(cfg, "r1", {"status": "ok", "checks": {}})
    assert runner_config.get_runner_health(cfg, "r1")["status"] == "ok"
