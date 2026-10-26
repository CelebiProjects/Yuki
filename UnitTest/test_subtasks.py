"""Exercise generated wildcard rules using a real Snakemake process."""
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from Yuki.kernel.jobs.container import ContainerJob
from Yuki.kernel.workflows.native import NativeWorkflow


def make_job(tmp_path, letter, config, aliases=None):
    path = tmp_path / "Storage" / ("p" * 32) / (letter * 32)
    (path / "contents").mkdir(parents=True)
    (path / "contents/celebi.yaml").write_text(yaml.safe_dump({
        "environment": "test", "memory_limit": "64Mi", **config,
    }))
    (path / "config.json").write_text(json.dumps({
        "object_type": "task", "dependencies": [],
        "alias_to_impression": aliases or {},
    }))
    job = ContainerJob(str(path), "runner")
    job.image = lambda: None
    return job


def workflow_for(tmp_path, monkeypatch, jobs, backend=None):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    workflow = NativeWorkflow("p" * 32, [], None)
    workflow.jobs = jobs
    if backend:
        workflow.backend_type = lambda: backend
    workflow._write_environment_directive = lambda *args: None
    workflow._wrap_environment_commands = lambda env, commands: commands
    workflow.construct_snake_file()
    for job in jobs:
        shutil.copytree(Path(job.path) / "contents",
                        Path(workflow.path) / f"imp{job.short_uuid()}")
    return workflow


def run_snake(workflow):
    return subprocess.run([
        sys.executable, "-m", "snakemake", "--cores", "2", "--rerun-incomplete",
        "--snakefile", "Snakefile",
    ], cwd=workflow.path, text=True, capture_output=True, timeout=90)


def test_bulk_chain_and_ordinary_reducer(tmp_path, monkeypatch):
    upstream = make_job(tmp_path, "a", {"subtasks": 3, "commands": [
        "printf '%s' ${index} > stageout/index/a.root",
    ]})
    downstream = make_job(tmp_path, "b", {"subtasks": 3, "commands": [
        "cat input/stageout/${index}/a.root > stageout/index/b.root",
        "test -f ${input}/stageout/${index}/a.root",
        "python -c 'assert isinstance(${index}, int)'",
    ]}, {"input": upstream.uuid})
    reducer = make_job(tmp_path, "c", {"commands": [
        "cat input/stageout/*/b.root > stageout/all.root",
    ]}, {"input": downstream.uuid})
    workflow = workflow_for(tmp_path, monkeypatch, [upstream, downstream, reducer])
    result = run_snake(workflow)
    assert result.returncode == 0, result.stdout + result.stderr
    root = Path(workflow.path)
    for index in range(3):
        assert (root / f"impbbbbbbb/stageout/{index}/b.root").read_text() == str(index)
        assert (root / f"impbbbbbbb/logs/{index}/status").read_text().strip() == "finished"
        assert (root / f"impbbbbbbb/subtasks/{index}/stageout/index").resolve() == (
            root / f"impbbbbbbb/stageout/{index}")
    assert (root / "impccccccc/stageout/all.root").read_text() == "012"
    assert not (root / "impbbbbbbb/stageout/index").exists()
    assert len(workflow.steps) == 3  # Not one Yuki job per execution.


def test_failed_subtask_can_resume_without_repeating_success(tmp_path, monkeypatch):
    job = make_job(tmp_path, "a", {"subtasks": 2, "commands": [
        "echo run >> attempts",
        "if [ ${index} -eq 1 ] && [ ! -e retry ]; then touch retry; exit 1; fi",
        "echo ${index} > stageout/index/a.root",
    ]})
    workflow = workflow_for(tmp_path, monkeypatch, [job])
    first = run_snake(workflow)
    assert first.returncode != 0
    root = Path(workflow.path)
    assert not (root / "aaaaaaa.done").exists()
    assert (root / "impaaaaaaa/logs/1/status").read_text().strip() == "failed"
    second = run_snake(workflow)
    assert second.returncode == 0, second.stdout + second.stderr
    assert (root / "impaaaaaaa/subtasks/0/attempts").read_text() == "run\n"
    assert (root / "impaaaaaaa/subtasks/1/attempts").read_text() == "run\nrun\n"
    assert (root / "aaaaaaa.done").exists()


def test_single_repeat_keeps_numbered_output(tmp_path, monkeypatch):
    job = make_job(tmp_path, "a", {"subtasks": 1, "commands": [
        "echo ${index} > stageout/index/a.root",
    ]})
    workflow = workflow_for(tmp_path, monkeypatch, [job])
    result = run_snake(workflow)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (Path(workflow.path) / "impaaaaaaa/stageout/0/a.root").read_text() == "0\n"


@pytest.mark.parametrize("count", [None, True, 0, -1, "3", 2.5])
def test_invalid_subtasks_fail_during_construction(tmp_path, monkeypatch, count):
    job = make_job(tmp_path, "a", {"subtasks": count})
    with pytest.raises(ValueError, match="positive integer"):
        workflow_for(tmp_path, monkeypatch, [job])


def test_cache_is_published_only_after_all_subtasks(tmp_path, monkeypatch):
    from Yuki.kernel.storage.cache import CACHE_COMPLETE_MARKER

    cache = tmp_path / "runner-cache"
    monkeypatch.setattr(ContainerJob, "_cache_source", lambda *args: str(cache) + "/")
    job = make_job(tmp_path, "a", {"subtasks": 2, "commands": [
        "if [ ${index} -eq 1 ] && [ ! -e retry ]; then touch retry; exit 1; fi",
        "echo ${index} > stageout/index/a.root",
    ]})
    job.set_cache_on_runner(True)
    workflow = workflow_for(tmp_path, monkeypatch, [job], backend="ssh")
    result = run_snake(workflow)
    assert result.returncode != 0
    assert not cache.exists()
    result = run_snake(workflow)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (cache / CACHE_COMPLETE_MARKER).is_file()
    assert (cache / "0/a.root").read_text() == "0\n"
    assert (cache / "1/a.root").read_text() == "1\n"
    assert not (cache / "index").exists()


def test_subtask_log_can_be_read_by_index_and_offset(tmp_path):
    job = make_job(tmp_path, "a", {"subtasks": 2})
    log = Path(job.path) / "runner/logs/1/celebi_user_step0.log"
    log.parent.mkdir(parents=True)
    log.write_text("hello world")
    assert job.log(0, subtask=1, offset=6) == "world"
    assert job.log(0, subtask=0) == ""
    with pytest.raises(ValueError):
        job.log(0, subtask="../other")


def test_large_repeat_count_uses_one_wildcard_rule(tmp_path, monkeypatch):
    job = make_job(tmp_path, "a", {"subtasks": 100000, "commands": [
        "echo ${index} > stageout/index/a.root",
    ]})
    workflow = workflow_for(tmp_path, monkeypatch, [job])
    snakefile = Path(workflow.snakefile_path).read_text()
    assert "index=range(100000)" in snakefile
    assert snakefile.count("rule subtask") == 1
    assert len(snakefile) < 10000
    assert len(workflow.steps) == 1


def test_log_route_selects_subtask():
    from flask import Flask
    from unittest.mock import Mock, patch
    from Yuki.server.routes import status

    app = Flask(__name__)
    app.register_blueprint(status.bp)
    job = Mock()
    job.log.return_value = "hello"
    with patch.object(status, "VJob", return_value=job):
        client = app.test_client()
        response = client.get("/error-log/project/impression/2?subtask=7&offset=4")
        assert response.status_code == 200
        job.log.assert_called_once_with("2", offset=4, subtask=7)
        for bad in ("-1", "invalid", "../other"):
            response = client.get("/error-log/project/impression/2", query_string={
                "subtask": bad,
            })
            assert response.status_code == 400
