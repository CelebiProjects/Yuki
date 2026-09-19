"""Reject incomplete impression metadata before launching a workflow."""
from unittest.mock import MagicMock
import json
from pathlib import Path

import pytest

from Yuki.kernel.native_workflow import NativeWorkflow
from Yuki.kernel.status_constants import DISSONANCE


@pytest.mark.parametrize("object_type", ["", "directory", None])
def test_invalid_job_fails_before_backend_launch(tmp_path, monkeypatch, object_type):
    monkeypatch.setenv("YUKIDIR", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    workflow = NativeWorkflow("p" * 32, [], None)
    job = MagicMock()
    job.uuid = "237cc83243783cf94d9004c2112d61f9"
    job.path = str(tmp_path / "Storage" / ("p" * 32) / job.uuid)
    job.object_type.return_value = object_type
    job.job_type.return_value = object_type
    job.is_input = False
    job.status.return_value = "finished"
    job.workflow_id.return_value = ""
    workflow.jobs = [job]
    workflow.construct_workflow_jobs = MagicMock()
    workflow._wait_for_dependencies = MagicMock(return_value=True)
    workflow._execute_backend = MagicMock()

    with pytest.raises(ValueError, match="Invalid object_type") as error:
        workflow.run()

    detail = str(error.value)
    assert job.path + "/config.json" in detail
    assert "Restore the impression metadata" in detail
    workflow._execute_backend.assert_not_called()
    workflow_path = Path(workflow.path)
    assert not (workflow_path / "Snakefile").exists()
    job.set_status.assert_any_call(DISSONANCE, f"Workflow construction failed: {detail}")
    assert json.loads((workflow_path / "results.json").read_text())["results"]["status"] == "failed"
    assert detail in (workflow_path / "workflow.log").read_text()
