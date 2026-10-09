"""Tests for VWorkflow.create backend-type resolution and persistence."""
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from Yuki.kernel.workflows.base import VWorkflow


class TestVWorkflowCreate(unittest.TestCase):
    """Verify the factory picks the right subclass and persists the choice."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._home_patcher = patch.dict(os.environ, {"HOME": self.tmpdir})
        self._home_patcher.start()
        self.project_uuid = "p" * 32

    def tearDown(self):
        self._home_patcher.stop()
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_global_config(self, backend_types):
        config_path = os.path.join(self.tmpdir, ".Yuki", "config.json")
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump({"backend_types": backend_types}, f)

    def _make_job(self, machine_id="runner-1"):
        job = MagicMock()
        job.machine_id = machine_id
        return job

    def test_create_persists_backend_type_for_ssh(self):
        """When an SSH workflow is created, its backend_type is stored locally."""
        self._write_global_config({"runner-1": "ssh"})

        workflow = VWorkflow.create(
            self.project_uuid, [self._make_job("runner-1")], None, mode="ssh"
        )

        self.assertIsInstance(workflow, VWorkflow)
        self.assertEqual(workflow.__class__.__name__, "SshWorkflow")
        stored = workflow.config_file.read_variable("backend_type", "")
        self.assertEqual(stored, "ssh")

    def test_create_persists_backend_type_for_native(self):
        """When a native workflow is created, its backend_type is stored locally."""
        self._write_global_config({"runner-1": "native"})

        workflow = VWorkflow.create(
            self.project_uuid, [self._make_job("runner-1")], None, mode="native"
        )

        self.assertEqual(workflow.__class__.__name__, "NativeWorkflow")
        stored = workflow.config_file.read_variable("backend_type", "")
        self.assertEqual(stored, "native")

    def test_create_persists_backend_type_for_ihep(self):
        """IHEP workflows resolve to the hep_sub-aware SSH subclass."""
        self._write_global_config({"runner-1": "ihep"})

        workflow = VWorkflow.create(
            self.project_uuid, [self._make_job("runner-1")], None, mode="ihep"
        )

        self.assertEqual(workflow.__class__.__name__, "IhepWorkflow")
        stored = workflow.config_file.read_variable("backend_type", "")
        self.assertEqual(stored, "ihep")

    def test_reload_uses_stored_backend_type_over_global_lookup(self):
        """A saved SSH workflow must reload as SshWorkflow even if the global
        backend_types mapping is missing or points to a different backend.

        This is the root cause of the status-update bug: status/file-status
        routes call VWorkflow.create(uuid, [], workflow_id) without an explicit
        mode. If the factory falls back to a global lookup keyed by machine_id,
        a key mismatch makes it instantiate ReanaWorkflow instead, causing
        REANA API calls against an SSH-only host.
        """
        # Create an SSH workflow with backend_type persisted.
        self._write_global_config({"runner-1": "ssh"})
        original = VWorkflow.create(
            self.project_uuid, [self._make_job("runner-1")], None, mode="ssh"
        )
        workflow_id = original.uuid

        # Now corrupt the global mapping (e.g. keyed by runner name, not UUID,
        # or simply missing). Reload should still produce an SshWorkflow.
        self._write_global_config({})

        reloaded = VWorkflow.create(self.project_uuid, [], workflow_id)

        self.assertEqual(reloaded.__class__.__name__, "SshWorkflow")

    def test_legacy_workflow_without_stored_backend_type_falls_back_to_global(self):
        """Workflows created before backend_type persistence still resolve via
        machine_id + backend_types for backward compatibility.
        """
        self._write_global_config({"runner-1": "native"})

        # Create a workflow explicitly as native; it will persist backend_type.
        original = VWorkflow.create(
            self.project_uuid, [self._make_job("runner-1")], None, mode="native"
        )
        # Strip the stored backend_type to simulate a legacy workflow.
        original.config_file.write_variable("backend_type", "")

        reloaded = VWorkflow.create(self.project_uuid, [], original.uuid)

        self.assertEqual(reloaded.__class__.__name__, "NativeWorkflow")


if __name__ == "__main__":
    unittest.main()
