"""Helpers for per-runner settings and health stored in ~/.Yuki/config.json.

New-style data lives in the nested ``runner_settings`` / ``runner_health``
maps keyed by runner id. Legacy ssh fields live in the flat ``ssh_*`` maps;
``get_ssh_settings`` reads new-map-first and falls back to the legacy maps so
old runners keep working without migration.
"""
import os

from CelebiChrono.utils.metadata import ConfigFile

LEGACY_SSH_KEYS = ("ssh_hosts", "ssh_users", "ssh_key_paths",
                   "ssh_ports", "remote_workdirs")
SSH_BACKENDS = frozenset(("ssh", "ihep"))


def is_ssh_backend(backend_type):
    """Return whether a backend uses the shared SSH storage/transport layer."""
    return backend_type in SSH_BACKENDS


def open_config():
    """Open the Yuki config file ($YUKIDIR/config.json)."""
    yuki_dir = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
    return ConfigFile(os.path.join(yuki_dir, "config.json"))


def get_runner_settings(config_file, runner_id):
    """Return the runner_settings entry for runner_id ({} if absent)."""
    settings = config_file.read_variable("runner_settings", {})
    return dict(settings.get(runner_id, {}))


def merge_runner_settings(config_file, runner_id, updates):
    """Merge updates into runner_settings[runner_id] and persist."""
    settings = config_file.read_variable("runner_settings", {})
    entry = dict(settings.get(runner_id, {}))
    entry.update(updates)
    settings[runner_id] = entry
    config_file.write_variable("runner_settings", settings)


def get_ssh_settings(config_file, runner_id):
    """Return merged ssh settings, new map preferred over legacy maps."""
    s = get_runner_settings(config_file, runner_id)
    legacy = {key: config_file.read_variable(key, {}) for key in LEGACY_SSH_KEYS}

    def pick(new_key, legacy_key, default):
        if s.get(new_key):
            return s[new_key]
        return legacy[legacy_key].get(runner_id, default)

    return {
        "host": pick("ssh_host", "ssh_hosts", ""),
        "user": pick("ssh_user", "ssh_users", ""),
        "key_path": pick("ssh_key_path", "ssh_key_paths", ""),
        "port": pick("ssh_port", "ssh_ports", 22),
        "remote_workdir": pick("remote_workdir", "remote_workdirs",
                               "/tmp/yuki-workflows"),
        "cores": s.get("cores", "all"),
        "conda_path": s.get("conda_path", ""),
        "snakemake_path": s.get("snakemake_path", ""),
    }


def get_ihep_settings(config_file, runner_id):
    """Return SSH transport settings extended with IHEP HepJob options."""
    settings = get_ssh_settings(config_file, runner_id)
    runner_settings = get_runner_settings(config_file, runner_id)
    for key in ("hep_sub_path", "hep_q_path", "hep_rm_path", "hep_group",
                "hep_max_jobs", "hep_step_memory_mb"):
        if runner_settings.get(key) is not None:
            settings[key] = runner_settings[key]
    return settings


def get_runner_health(config_file, runner_id):
    """Return the persisted health entry, or {'status': 'untested'}."""
    health = config_file.read_variable("runner_health", {})
    return dict(health.get(runner_id, {"status": "untested"}))


def set_runner_health(config_file, runner_id, health):
    """Persist a health entry for runner_id."""
    all_health = config_file.read_variable("runner_health", {})
    all_health[runner_id] = health
    config_file.write_variable("runner_health", all_health)
