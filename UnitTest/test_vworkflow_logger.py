"""Tests for VWorkflow.logger console emission and file append."""
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from Yuki.kernel.workflows.base import VWorkflow


class _DummyWorkflow(VWorkflow):
    """Minimal concrete workflow for exercising logger()."""

    def _execute_backend(self):
        pass

    def _sync_external_job_status(self, job):
        pass

    def update_workflow_status(self):
        pass


class TestVWorkflowLogger(unittest.TestCase):
    """VWorkflow.logger emits on the Yuki.workflow channel and keeps the
    per-workflow file append."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._home_patcher = patch.dict(os.environ, {"HOME": self.tmpdir})
        self._home_patcher.start()
        self.workflow = _DummyWorkflow("p" * 32, [], machine_id="runner-1")

    def tearDown(self):
        self._home_patcher.stop()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_logger_emits_on_workflow_channel(self):
        """The message reaches the Yuki.workflow logging channel."""
        with self.assertLogs("Yuki.workflow", level="INFO") as captured:
            self.workflow.logger("hello channel")
        self.assertTrue(
            any("hello channel" in message for message in captured.output))

    def test_logger_appends_timestamped_line_to_workflow_log(self):
        """The message is still appended to workflow.log with a timestamp."""
        self.workflow.logger("file line")
        with open(self.workflow.log_path, encoding="utf-8") as fh:
            content = fh.read()
        self.assertIn("file line", content)
        self.assertTrue(content.strip().startswith("["))


if __name__ == "__main__":
    unittest.main()
