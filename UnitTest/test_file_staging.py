"""Tests for the file staging helpers."""
# pylint: disable=protected-access
import json
import os

from Yuki.kernel.storage.staging import FileStager, walk_files


def test_walk_files_yields_relative_paths(tmp_path):
    """walk_files yields paths relative to the walked root."""
    root = tmp_path / "root"
    root.mkdir()
    (root / "a.txt").write_text("a")
    (root / "sub").mkdir()
    (root / "sub" / "b.txt").write_text("b")
    result = {rel: os.path.basename(abs_path) for rel, abs_path in walk_files(str(root))}
    assert result == {"a.txt": "a.txt", "sub/b.txt": "b.txt"}


def test_process_stage_manifest_reconstructs_nested_paths(tmp_path):
    """Stage manifest entries restore nested rawdata and input trees."""
    home = tmp_path / "home"
    yuki_home = home / ".Yuki"
    workflow_path = tmp_path / "workflow"
    local_exec = tmp_path / "local"
    storage = yuki_home / "Storage" / "proj-1"

    # Build a stage_manifest.json with nested rawdata and input entries.
    job_uuid = "7654321abcdef"
    manifest = {
        "entries": [
            {
                "type": "rawdata",
                "job_uuid": job_uuid,
                "src_path": str(storage / job_uuid / "rawdata" / "data" / "x.root"),
                "dst_rel": f"imp{job_uuid[:7]}/stageout/data/x.root",
            },
            {
                "type": "input",
                "job_uuid": job_uuid,
                "machine_id": "runner-1",
                "src_path": str(storage / job_uuid / "runner-1" / "stageout" / "plots" / "y.png"),
                "dst_rel": f"imp{job_uuid[:7]}/stageout/plots/y.png",
            },
        ]
    }
    local_exec.mkdir(parents=True)
    with open(local_exec / "stage_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f)

    # Place the actual source files in Storage.
    (storage / job_uuid / "rawdata" / "data").mkdir(parents=True)
    (storage / job_uuid / "rawdata" / "data" / "x.root").write_text("x")
    (storage / job_uuid / "runner-1" / "stageout" / "plots").mkdir(parents=True)
    (storage / job_uuid / "runner-1" / "stageout" / "plots" / "y.png").write_text("y")

    stager = FileStager(
        str(workflow_path), str(local_exec), "proj-1", logger=lambda *_a, **_k: None
    )
    stager.yuki_home = str(yuki_home)
    stager.storage_dir = str(storage)
    stager._process_stage_manifest()

    assert (local_exec / f"imp{job_uuid[:7]}" / "stageout" / "data" / "x.root").exists()
    assert (local_exec / f"imp{job_uuid[:7]}" / "stageout" / "plots" / "y.png").exists()
    assert not (local_exec / "stage_manifest.json").exists()
