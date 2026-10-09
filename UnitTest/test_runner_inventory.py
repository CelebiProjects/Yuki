"""Tests for the runner data inventory (cache + workflow workspaces)."""
import json
from unittest import mock

from Yuki.services import runner_inventory


def _write_config(tmp_path, **variables):
    """Write $YUKIDIR/config.json with the given top-level variables."""
    with open(tmp_path / "config.json", "w", encoding="utf-8") as fh:
        json.dump(variables, fh)


def _fake_ssh():
    """An _SshConnection replacement with listdir/walk_files stubs."""
    ssh = mock.MagicMock()
    # The real _SshConnection.__enter__ returns self.
    ssh.__enter__.return_value = ssh
    ssh.__exit__.return_value = False

    def listdir(path):
        return {
            "/remote/impressions": ["p1", "p2"],
            "/remote/impressions/p1": ["imp1", "imp3"],
            "/remote/impressions/p2": ["imp2"],
            "/remote/workflows": ["p1"],
            "/remote/workflows/p1": ["wf1", "wf2"],
        }.get(path, [])

    def walk_files(path):
        return {
            "/remote/impressions/p1/imp1": [
                ("a.root", "x", 10), ("b.root", "y", 20)],
            "/remote/impressions/p1/imp3": [("c.root", "z", 5)],
            "/remote/impressions/p2/imp2": [("d.root", "w", 7)],
            "/remote/workflows/p1/wf1": [
                ("snakemake.log", "s", 3), ("a.done", "d", 1)],
            "/remote/workflows/p1/wf2": [],
        }.get(path, [])

    ssh.listdir.side_effect = listdir
    ssh.walk_files.side_effect = walk_files
    return ssh


def test_ssh_inventory_lists_cache_and_workflows(monkeypatch, tmp_path):
    """Cache and workflow dirs are walked, sized, and classified."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _write_config(tmp_path, runners=["pkufarm"],
                  runners_id={"pkufarm": "r1"},
                  backend_types={"r1": "ssh"},
                  runner_settings={"r1": {
                      "ssh_host": "h", "ssh_user": "u",
                      "remote_workdir": "/remote"}})

    # Local knowledge: a registered impression, a distribution-recorded
    # one, and the workflow mirror for wf1.
    imp1 = tmp_path / "Storage" / "p1" / "imp1"
    imp1.mkdir(parents=True)
    (imp1 / "remote.json").write_text(
        json.dumps({"host_runner_id": "r1"}))
    imp2 = tmp_path / "Storage" / "p2" / "imp2"
    imp2.mkdir(parents=True)
    (imp2 / "distribution.json").write_text(json.dumps({
        "locations": {"runner:pkufarm": {
            "cache": {"origin": "transferred"}}}}))
    wf1 = tmp_path / "Workflows" / "p1" / "wf1"
    wf1.mkdir(parents=True)
    (wf1 / "results.json").write_text(
        json.dumps({"results": {"status": "finished"}}))

    ssh = _fake_ssh()
    with mock.patch("Yuki.services.runner_inventory._SshConnection",
                    return_value=ssh):
        result = runner_inventory.inventory_runner("r1", "ssh")

    cache = result["cache"]
    assert cache["total_files"] == 4
    assert cache["total_bytes"] == 42
    by_imp = {(e["project"], e["impression"]): e for e in cache["entries"]}
    assert by_imp[("p1", "imp1")]["known"] == "registered"
    assert by_imp[("p1", "imp1")]["files"] == 2
    assert by_imp[("p1", "imp1")]["bytes"] == 30
    assert by_imp[("p2", "imp2")]["known"] == "recorded"
    assert by_imp[("p1", "imp3")]["known"] == "orphan"

    workflows = result["workflows"]
    assert workflows["total_files"] == 2
    assert workflows["total_bytes"] == 4
    by_wf = {e["workflow"]: e for e in workflows["entries"]}
    assert by_wf["wf1"]["project"] == "p1"
    assert by_wf["wf1"]["status"] == "finished"
    assert by_wf["wf1"]["known"] is True
    assert by_wf["wf2"]["project"] == "p1"
    assert by_wf["wf2"]["status"] is None
    assert by_wf["wf2"]["known"] is False
    assert by_wf["wf2"]["files"] == 0


def test_native_inventory_lists_local_workflows(monkeypatch, tmp_path):
    """Native workflows are walked locally and matched to their projects."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    _write_config(tmp_path, runners=["local"],
                  runners_id={"local": "r1"},
                  backend_types={"r1": "native"},
                  runner_settings={"r1": {
                      "workdir": str(tmp_path / "localwf")}})

    wf1 = tmp_path / "localwf" / "wf1"
    wf1.mkdir(parents=True)
    (wf1 / "a.done").write_bytes(b"xx")
    (tmp_path / "localwf" / "wf2").mkdir(parents=True)
    mirror = tmp_path / "Workflows" / "p1" / "wf1"
    mirror.mkdir(parents=True)
    (mirror / "results.json").write_text(
        json.dumps({"results": {"status": "coda"}}))

    result = runner_inventory.inventory_runner("r1", "native")

    assert result["cache"] == {"total_files": 0, "total_bytes": 0,
                               "entries": []}
    workflows = result["workflows"]
    assert workflows["total_files"] == 1
    assert workflows["total_bytes"] == 2
    by_wf = {e["workflow"]: e for e in workflows["entries"]}
    assert by_wf["wf1"]["project"] == "p1"
    assert by_wf["wf1"]["status"] == "coda"
    assert by_wf["wf1"]["known"] is True
    assert by_wf["wf2"]["project"] is None
    assert by_wf["wf2"]["status"] is None
    assert by_wf["wf2"]["known"] is False
