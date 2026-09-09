"""Tests for purge_runner_cache (remote cache eviction for ssh runners)."""
# pylint: disable=protected-access
import json
import os
from unittest import mock

from click.testing import CliRunner

from Yuki.kernel.remote_data_ops import purge_runner_cache
from Yuki.main import cli


class _FakeSsh:
    """Records exec calls and answers listdir from a canned tree.

    exec returns ("", "", 0) for every command; exec_calls records the
    command strings so tests can assert on the emitted remote commands.
    """

    def __init__(self, tree=None):
        self.tree = tree or {}
        self.exec_calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def listdir(self, remote_path):
        """Return the canned children of remote_path, or []."""
        node = self.tree
        for part in (p for p in remote_path.split("/") if p):
            node = node.get(part, {}) if isinstance(node, dict) else {}
        return sorted(node) if isinstance(node, dict) else []

    def exec(self, command, timeout=300):
        """Record the command and answer success."""
        self.exec_calls.append(command)
        return "", "", 0


def _write_runner_config(tmp_path, runner_id="r1", remote_workdir="/remote/work"):
    """Write a Yuki config.json with an ssh runner into tmp_path."""
    with open(tmp_path / "config.json", "w", encoding="utf-8") as f:
        json.dump({
            "runners_id": {"farm": runner_id},
            "backend_types": {runner_id: "ssh"},
            "runner_settings": {
                runner_id: {"ssh_host": "h", "ssh_user": "u",
                            "remote_workdir": remote_workdir},
            },
        }, f)


def _make_registered(tmp_path, project, impression, runner_id="r1",
                     status="archived"):
    """Create a Storage impression with remote.json/status.json markers."""
    imp_dir = tmp_path / "Storage" / project / impression
    os.makedirs(imp_dir, exist_ok=True)
    with open(imp_dir / "remote.json", "w", encoding="utf-8") as f:
        json.dump({"host_runner_id": runner_id,
                   "source_path": "/data/src",
                   "remote_path": f"/remote/work/impressions/{project}/{impression}"}, f)
    with open(imp_dir / "status.json", "w", encoding="utf-8") as f:
        json.dump({"status": status}, f)
    return imp_dir


def _make_dist(tmp_path, project, impression, locations):
    """Write a distribution.json for a Storage impression."""
    imp_dir = tmp_path / "Storage" / project / impression
    os.makedirs(imp_dir, exist_ok=True)
    with open(imp_dir / "distribution.json", "w", encoding="utf-8") as f:
        json.dump({"produced_on": None, "locations": locations}, f)


def test_purge_deletes_matching_remote_dirs(tmp_path):
    """Matching remote cache dirs are chmod'd writable then removed."""
    _write_runner_config(tmp_path)
    fake = _FakeSsh(tree={"remote": {"work": {"impressions": {
        "proj1": {"imp-a": {}, "imp-b": {}}, "proj2": {"imp-c": {}},
    }}}})
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=fake):
        summary = purge_runner_cache("r1", project="proj1", yuki_dir=str(tmp_path))

    assert summary["dry_run"] is False
    assert {e["impression"] for e in summary["purged"]} == {"imp-a", "imp-b"}
    rm_calls = [c for c in fake.exec_calls if "rm -rf" in c]
    assert len(rm_calls) == 2
    for call in rm_calls:
        assert "chmod -R u+w --" in call
        assert "|| true && rm -rf" in call
        assert "impressions/proj1/" in call


def test_purge_skips_running_registration(tmp_path):
    """Impressions with status 'running' are skipped, not deleted."""
    _write_runner_config(tmp_path)
    _make_registered(tmp_path, "proj1", "imp-running", status="running")
    fake = _FakeSsh(tree={"remote": {"work": {"impressions": {
        "proj1": {"imp-running": {}},
    }}}})
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=fake):
        summary = purge_runner_cache("r1", project="proj1", yuki_dir=str(tmp_path))

    assert not summary["purged"]
    assert {s["impression"] for s in summary["skipped"]} == {"imp-running"}
    assert not any("rm -rf" in c for c in fake.exec_calls)
    # Local markers survive the skip.
    assert (tmp_path / "Storage" / "proj1" / "imp-running" /
            "remote.json").exists()


def test_purge_clears_local_markers(tmp_path):
    """Registered impressions lose remote.json and status.json."""
    _write_runner_config(tmp_path)
    _make_registered(tmp_path, "proj1", "imp-reg")
    fake = _FakeSsh(tree={"remote": {"work": {"impressions": {
        "proj1": {"imp-reg": {}},
    }}}})
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=fake):
        summary = purge_runner_cache("r1", project="proj1", yuki_dir=str(tmp_path))

    entry = summary["purged"][0]
    assert entry["kind"] == "registered"
    imp_dir = tmp_path / "Storage" / "proj1" / "imp-reg"
    assert not (imp_dir / "remote.json").exists()
    assert not (imp_dir / "status.json").exists()


def test_purge_drops_distribution_cache_entries(tmp_path):
    """distribution.json loses the purged runner's cache state."""
    _write_runner_config(tmp_path)
    _make_dist(tmp_path, "proj1", "imp-cache", {
        "runner:farm": {"cache": {"origin": "transferred", "files": 2}},
        "runner:other": {"cache": {"origin": "transferred", "files": 1}},
        "yuki": {"origin": "collected", "files": 1},
    })
    fake = _FakeSsh(tree={"remote": {"work": {"impressions": {
        "proj1": {"imp-cache": {}},
    }}}})
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=fake):
        purge_runner_cache("r1", project="proj1", yuki_dir=str(tmp_path))

    dist_path = tmp_path / "Storage" / "proj1" / "imp-cache" / "distribution.json"
    with open(dist_path, encoding="utf-8") as f:
        dist = json.load(f)
    assert "runner:farm" not in dist["locations"]
    assert "runner:other" in dist["locations"]
    assert "yuki" in dist["locations"]


def test_purge_dry_run_changes_nothing(tmp_path):
    """Dry run lists the plan but deletes nothing, remote or local."""
    _write_runner_config(tmp_path)
    _make_registered(tmp_path, "proj1", "imp-reg")
    fake = _FakeSsh(tree={"remote": {"work": {"impressions": {
        "proj1": {"imp-reg": {}},
    }}}})
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=fake):
        summary = purge_runner_cache("r1", project="proj1", dry_run=True,
                                     yuki_dir=str(tmp_path))

    assert summary["dry_run"] is True
    assert len(summary["purged"]) == 1
    assert not any("rm -rf" in c for c in fake.exec_calls)
    assert (tmp_path / "Storage" / "proj1" / "imp-reg" / "remote.json").exists()


def test_purge_filters_by_impression(tmp_path):
    """--impression restricts the purge to the named impression."""
    _write_runner_config(tmp_path)
    fake = _FakeSsh(tree={"remote": {"work": {"impressions": {
        "proj1": {"imp-a": {}, "imp-b": {}},
    }}}})
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=fake):
        summary = purge_runner_cache("r1", project="proj1",
                                     impression="imp-a", yuki_dir=str(tmp_path))

    assert [e["impression"] for e in summary["purged"]] == ["imp-a"]


# ---------------- CLI ---------------- #

def _invoke(monkeypatch, tmp_path, args, user_input=None):
    """Invoke the purge CLI with $YUKIDIR pointed at tmp_path."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    return CliRunner().invoke(cli, args, input=user_input)


def _summary(purged, skipped=None, dry_run=False):
    """A canned purge_runner_cache result."""
    return {"purged": purged, "skipped": skipped or [], "dry_run": dry_run}


def test_cli_rejects_unknown_runner(monkeypatch, tmp_path):
    """An unknown runner name fails with a clear message."""
    _write_runner_config(tmp_path)
    result = _invoke(monkeypatch, tmp_path,
                     ["purge-ssh-runner-cache", "nope", "--yes"])
    assert result.exit_code == 1
    assert "runner 'nope' not found" in result.output


def test_cli_rejects_non_ssh_runner(monkeypatch, tmp_path):
    """A non-ssh backend runner is refused."""
    _write_runner_config(tmp_path)
    with open(tmp_path / "config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["backend_types"]["r1"] = "native"
    with open(tmp_path / "config.json", "w", encoding="utf-8") as f:
        json.dump(cfg, f)
    result = _invoke(monkeypatch, tmp_path,
                     ["purge-ssh-runner-cache", "farm", "--yes"])
    assert result.exit_code == 1
    assert "not an ssh runner" in result.output


def test_cli_purges_with_resolved_runner_id(monkeypatch, tmp_path):
    """The CLI resolves the runner name and calls the kernel purge."""
    _write_runner_config(tmp_path)
    planned = _summary([{"project": "proj1", "impression": "imp-a",
                         "kind": "registered",
                         "remote_dir": "/remote/work/impressions/proj1/imp-a"}])
    with mock.patch("Yuki.kernel.remote_data_ops.purge_runner_cache",
                    return_value=planned) as m_purge:
        result = _invoke(monkeypatch, tmp_path,
                         ["purge-ssh-runner-cache", "farm", "--yes"])
    assert result.exit_code == 0, result.output
    m_purge.assert_called_once_with("r1", project=None, impression=None,
                                    dry_run=False, echo=mock.ANY)
    assert "✓ Purged 1 cache entr" in result.output


def test_cli_dry_run_skips_confirmation(monkeypatch, tmp_path):
    """--dry-run prints the plan without prompting."""
    _write_runner_config(tmp_path)
    planned = _summary([{"project": "proj1", "impression": "imp-a",
                         "kind": "cache",
                         "remote_dir": "/remote/work/impressions/proj1/imp-a"}],
                       dry_run=True)
    with mock.patch("Yuki.kernel.remote_data_ops.purge_runner_cache",
                    return_value=planned) as m_purge:
        result = _invoke(monkeypatch, tmp_path,
                         ["purge-ssh-runner-cache", "farm",
                          "--project", "proj1", "--dry-run"])
    assert result.exit_code == 0, result.output
    m_purge.assert_called_once_with("r1", project="proj1", impression=None,
                                    dry_run=True, echo=mock.ANY)
    assert "dry run" in result.output.lower()


def test_cli_confirmation_aborts(monkeypatch, tmp_path):
    """Answering 'n' aborts before the kernel purge runs."""
    _write_runner_config(tmp_path)
    with mock.patch("Yuki.kernel.remote_data_ops.purge_runner_cache",
                    return_value=_summary([])) as m_purge:
        result = _invoke(monkeypatch, tmp_path,
                         ["purge-ssh-runner-cache", "farm"], user_input="n\n")
    assert result.exit_code == 1
    assert not m_purge.called


def test_purge_runner_cache_superseded_scope(tmp_path):
    """superseded=True selects only explicitly-superseded impressions."""
    from Yuki.kernel import liveness
    _write_runner_config(tmp_path)
    live_a, old_b = "a" * 32, "b" * 32
    liveness.save_live_set("proj1", [live_a], [old_b],
                           yuki_dir=str(tmp_path))
    fake = _FakeSsh(tree={"remote": {"work": {"impressions": {
        "proj1": {live_a: {}, old_b: {}},
    }}}})
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=fake):
        summary = purge_runner_cache("r1", superseded=True, dry_run=True,
                                     yuki_dir=str(tmp_path))

    assert {e["impression"] for e in summary["purged"]} == {old_b}
    assert summary["dry_run"] is True
    assert not any("rm -rf" in c for c in fake.exec_calls)


def test_purge_superseded_never_touches_unknown(tmp_path):
    """Impressions without a synced set are skipped (unknown is live)."""
    _write_runner_config(tmp_path)
    live_a = "a" * 32
    fake = _FakeSsh(tree={"remote": {"work": {"impressions": {
        "proj1": {live_a: {}},
    }}}})
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=fake):
        summary = purge_runner_cache("r1", superseded=True, dry_run=True,
                                     yuki_dir=str(tmp_path))
    assert summary["purged"] == []


def _purge_route_app(monkeypatch, tmp_path):
    """A Flask app with the remote_data blueprint and a temp config."""
    from Yuki.server.routes import remote_data as remote_data_routes
    from CelebiChrono.utils.metadata import ConfigFile
    from flask import Flask
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    app = Flask(__name__)
    app.register_blueprint(remote_data_routes.bp)
    config_obj = mock.MagicMock()
    config_obj.config_path = str(tmp_path / "config.json")
    config_obj.get_config_file.return_value = ConfigFile(
        config_obj.config_path)
    with open(config_obj.config_path, "w", encoding="utf-8") as f:
        json.dump({"runners_id": {"farm": "r1"},
                   "backend_types": {"r1": "ssh"}}, f)
    monkeypatch.setattr(remote_data_routes, "config", config_obj)
    return app


def test_purge_route_superseded_with_impression_400(monkeypatch, tmp_path):
    """superseded combined with an impression filter is rejected."""
    r = _purge_route_app(monkeypatch, tmp_path).test_client().post(
        "/purge-runner-cache",
        json={"runner": "farm", "superseded": True, "impression": "i1"})
    assert r.status_code == 400
    assert "cannot be combined" in r.get_json()["error"]


def test_purge_route_superseded_with_project_passes_through(monkeypatch,
                                                           tmp_path):
    """superseded combined with a project filter is allowed (scoped purge)."""
    from Yuki.server.routes import remote_data as remote_data_routes
    app = _purge_route_app(monkeypatch, tmp_path)
    with mock.patch.object(remote_data_routes.remote_data_ops,
                           "purge_runner_cache",
                           return_value={"purged": [], "skipped": [],
                                         "dry_run": True}) as purge:
        r = app.test_client().post(
            "/purge-runner-cache",
            json={"runner": "farm", "superseded": True, "project": "p1",
                  "dry_run": True})
    assert r.status_code == 200
    assert purge.call_args[1]["superseded"] is True
    assert purge.call_args[1]["project"] == "p1"


def test_purge_route_superseded_passes_through(monkeypatch, tmp_path):
    """The route forwards superseded and dry_run to the kernel purge."""
    from Yuki.server.routes import remote_data as remote_data_routes
    app = _purge_route_app(monkeypatch, tmp_path)
    with mock.patch.object(remote_data_routes.remote_data_ops,
                           "purge_runner_cache",
                           return_value={"purged": [], "skipped": [],
                                         "dry_run": True}) as purge:
        r = app.test_client().post(
            "/purge-runner-cache",
            json={"runner": "farm", "superseded": True, "dry_run": True})
    assert r.status_code == 200
    assert purge.call_args[1]["superseded"] is True
    assert purge.call_args[1]["dry_run"] is True
