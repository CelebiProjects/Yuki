"""Unit tests for NativeWorkflow per-job status propagation."""
# pylint: disable=protected-access
import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch


class TestNativeWorkflowPropagation(unittest.TestCase):
    """Test NativeWorkflow.propagate_job_statuses and _read_job_log_tail."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        # Patch HOME so NativeWorkflow rooted file ops land in tmpdir.
        self._home_patcher = patch.dict(os.environ, {"HOME": self.tmpdir})
        self._home_patcher.start()

        # Use fixed-length fake UUIDs (32 chars) for predictable short_uuid.
        self.project_uuid = "p" * 32
        self.workflow_uuid = "w" * 32

        # Construct NativeWorkflow against the temp HOME.
        from Yuki.kernel.workflows.native import NativeWorkflow
        self.workflow = NativeWorkflow(self.project_uuid, [], None)
        # Override generated uuid -> known value so paths are predictable.
        self.workflow.uuid = self.workflow_uuid
        self.workflow.local_exec_path = os.path.join(
            self.tmpdir, ".Yuki", "LocalWorkflows", self.workflow_uuid
        )
        os.makedirs(self.workflow.local_exec_path, exist_ok=True)
        self.workflow.jobs = []

    def tearDown(self):
        self._home_patcher.stop()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    # -- helpers ----------------------------------------------------------

    def _make_job(self, uuid_full, status_value="prelude",
                  is_input=False, job_type_value="task"):
        """Build a MagicMock VJob with the methods propagate_job_statuses uses."""
        job = MagicMock()
        job.uuid = uuid_full
        job.is_input = is_input
        job.path = "/fake/" + uuid_full
        job.job_type.return_value = job_type_value
        # status() takes a 'musical' kwarg; we return the same value either way.
        job.status.return_value = status_value
        job.short_uuid.return_value = uuid_full[:7]
        return job

    def _touch_done(self, short_uuid):
        path = os.path.join(self.workflow.local_exec_path, short_uuid + ".done")
        with open(path, "w", encoding="utf-8"):
            pass
        return path

    def _write_user_log(self, short_uuid, step_index, content):
        logs_dir = os.path.join(
            self.workflow.local_exec_path, "imp" + short_uuid, "logs"
        )
        os.makedirs(logs_dir, exist_ok=True)
        path = os.path.join(logs_dir, f"celebi_user_step{step_index}.log")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    # -- scaffolding sanity check ----------------------------------------

    def test_scaffolding_constructs_workflow(self):
        """The workflow scaffold lands in the patched temp HOME."""
        self.assertTrue(os.path.isdir(self.workflow.local_exec_path))
        self.assertEqual(self.workflow.uuid, self.workflow_uuid)

    def test_propagate_done_jobs_become_finished(self):
        """Completed jobs should be stored with the legacy status 'finished'
        so the client can display [coda][finished]."""
        job_a = self._make_job("a" * 32)
        job_b = self._make_job("b" * 32)
        self.workflow.jobs = [job_a, job_b]

        self._touch_done(job_a.short_uuid())
        self._touch_done(job_b.short_uuid())

        self.workflow.propagate_job_statuses(workflow_terminal=False)

        job_a.set_status.assert_called_once_with("finished", "Local execution completed")
        job_b.set_status.assert_called_once_with("finished", "Local execution completed")

    def test_propagate_in_flight_leaves_jobs_unchanged(self):
        """Jobs without a .done marker are left untouched."""
        job = self._make_job("a" * 32, status_value="in movement")
        self.workflow.jobs = [job]
        # No .done file written.

        self.workflow.propagate_job_statuses(workflow_terminal=False)

        job.set_status.assert_not_called()

    def test_propagate_missing_done_no_logs_becomes_failed_with_skip_message(self):
        """A terminal workflow marks a never-run job as failed with skip detail."""
        from Yuki.kernel.execution.status import FAILED
        job = self._make_job("a" * 32)
        self.workflow.jobs = [job]
        # No .done, no imp<short>/logs/ directory.

        self.workflow.propagate_job_statuses(workflow_terminal=True)

        job.set_status.assert_called_once_with(
            FAILED,
            "Skipped: upstream dependency failed before this job ran",
        )

    def test_read_job_log_tail_returns_empty_when_no_logs(self):
        """Reading a log tail for a job with no logs yields an empty string."""
        # imp<short>/logs/ does not exist.
        tail = self.workflow._read_job_log_tail("a" * 7)
        self.assertEqual(tail, "")

    def test_read_job_log_tail_picks_highest_step_index(self):
        """The log tail comes from the highest-numbered step log."""
        short = "a" * 7
        self._write_user_log(short, 0, "first step output")
        self._write_user_log(short, 1, "second step output")
        self._write_user_log(short, 2, "third step boom: traceback here")

        tail = self.workflow._read_job_log_tail(short)
        self.assertIn("third step boom", tail)
        self.assertNotIn("first step", tail)

    def test_propagate_missing_done_terminal_becomes_failed(self):
        """A job that ran and failed carries the traceback into its status."""
        from Yuki.kernel.execution.status import FAILED
        short = "a" * 7
        # 32-char uuid whose first 7 chars match `short`.
        job_uuid = short + "z" * 25
        job = self._make_job(job_uuid)
        self.workflow.jobs = [job]

        self._write_user_log(
            short, 0,
            "running step\nTraceback (most recent call last):\n  ZeroDivisionError\n",
        )

        self.workflow.propagate_job_statuses(workflow_terminal=True)

        self.assertEqual(job.set_status.call_count, 1)
        status_arg, detail_arg = job.set_status.call_args.args
        self.assertEqual(status_arg, FAILED)
        self.assertIn("Local execution failed", detail_arg)
        self.assertIn("ZeroDivisionError", detail_arg)

    def test_propagate_skips_input_and_algorithm_jobs(self):
        """Input and algorithm jobs are never promoted by propagation."""
        input_job = self._make_job("a" * 32, is_input=True)
        algo_job = self._make_job("b" * 32, job_type_value="algorithm")
        self.workflow.jobs = [input_job, algo_job]

        # Both have .done — propagation would otherwise promote them.
        self._touch_done(input_job.short_uuid())
        self._touch_done(algo_job.short_uuid())

        self.workflow.propagate_job_statuses(workflow_terminal=True)

        input_job.set_status.assert_not_called()
        algo_job.set_status.assert_not_called()

    def test_propagate_does_not_churn_terminal_status(self):
        """Terminal statuses are never overwritten by propagation."""
        from Yuki.kernel.execution.status import CODA, FINAL_NOTE, FAILED, STOPPED, DELETED
        coda_job = self._make_job("a" * 32, status_value=CODA)
        final_job = self._make_job("b" * 32, status_value=FINAL_NOTE)
        failed_job = self._make_job("c" * 32, status_value=FAILED)
        stopped_job = self._make_job("d" * 32, status_value=STOPPED)
        deleted_job = self._make_job("e" * 32, status_value=DELETED)
        self.workflow.jobs = [coda_job, final_job, failed_job, stopped_job, deleted_job]

        # All have .done so propagate WOULD touch them otherwise.
        for j in self.workflow.jobs:
            self._touch_done(j.short_uuid())

        self.workflow.propagate_job_statuses(workflow_terminal=True)

        coda_job.set_status.assert_not_called()
        final_job.set_status.assert_not_called()
        failed_job.set_status.assert_not_called()
        stopped_job.set_status.assert_not_called()
        deleted_job.set_status.assert_not_called()

    def test_copy_files_local_writes_nested_stage_manifest(self):
        """Nested rawdata/input files must be recorded with full relative paths."""
        job = self._make_job("a" * 32)
        job.environment.return_value = "rawdata"
        job.path = os.path.join(self.tmpdir, "jobs", "a" * 32)
        rawdata_dir = os.path.join(job.path, "rawdata")
        os.makedirs(os.path.join(rawdata_dir, "data"), exist_ok=True)
        with open(os.path.join(rawdata_dir, "data", "x.root"), "wb") as f:
            f.write(b"x")

        self.workflow.jobs = [job]
        self.workflow.snakefile_path = os.path.join(
            self.tmpdir, "Snakefile"
        )
        with open(self.workflow.snakefile_path, "w", encoding="utf-8") as f:
            f.write("rule test: shell: 'echo ok'")

        self.workflow.copy_files_local()

        manifest_path = os.path.join(self.workflow.local_exec_path, "stage_manifest.json")
        self.assertTrue(os.path.exists(manifest_path))
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)
        dst_rels = {e["dst_rel"] for e in manifest["entries"]}
        self.assertIn("impaaaaaaa/stageout/data/x.root", dst_rels)

    # -- live log collection ----------------------------------------------

    def test_collect_artifacts_refresh_overwrites_existing_log(self):
        """refresh=True overwrites a local log snapshot with newer content."""
        impression = "i" * 32
        self.workflow.machine_id = "runner-uuid"
        src_logs = os.path.join(
            self.workflow.local_exec_path, f"imp{impression[:7]}", "logs")
        os.makedirs(src_logs, exist_ok=True)
        with open(os.path.join(src_logs, "celebi_user_step0.log"),
                  "w", encoding="utf-8") as f:
            f.write("grown")

        dst_logs = os.path.join(
            self.tmpdir, ".Yuki", "Storage", self.project_uuid,
            impression, "runner-uuid", "logs")
        os.makedirs(dst_logs, exist_ok=True)
        local_log = os.path.join(dst_logs, "celebi_user_step0.log")
        with open(local_log, "w", encoding="utf-8") as f:
            f.write("stale")

        report = self.workflow._collect_artifacts(
            impression, "logs", "logs.downloaded", "log", refresh=True)

        with open(local_log, encoding="utf-8") as f:
            self.assertEqual(f.read(), "grown")
        self.assertIn("celebi_user_step0.log", report["collected"])

    def test_collect_artifacts_without_refresh_skips_existing_log(self):
        """Without refresh an already-downloaded log is left untouched."""
        impression = "i" * 32
        self.workflow.machine_id = "runner-uuid"
        src_logs = os.path.join(
            self.workflow.local_exec_path, f"imp{impression[:7]}", "logs")
        os.makedirs(src_logs, exist_ok=True)
        with open(os.path.join(src_logs, "celebi_user_step0.log"),
                  "w", encoding="utf-8") as f:
            f.write("grown")

        dst_logs = os.path.join(
            self.tmpdir, ".Yuki", "Storage", self.project_uuid,
            impression, "runner-uuid", "logs")
        os.makedirs(dst_logs, exist_ok=True)
        local_log = os.path.join(dst_logs, "celebi_user_step0.log")
        with open(local_log, "w", encoding="utf-8") as f:
            f.write("stale")

        report = self.workflow._collect_artifacts(
            impression, "logs", "logs.downloaded", "log")

        with open(local_log, encoding="utf-8") as f:
            self.assertEqual(f.read(), "stale")
        self.assertEqual(
            report["skipped"],
            [{"file": "celebi_user_step0.log", "reason": "already in Yuki"}])


class TestWaitForDependenciesFailFast(unittest.TestCase):
    """_wait_for_dependencies must fail fast (and loudly) on terminal failures."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._home_patcher = patch.dict(os.environ, {"HOME": self.tmpdir})
        self._home_patcher.start()

        self.project_uuid = "p" * 32
        self.workflow_uuid = "w" * 32

        from Yuki.kernel.workflows.native import NativeWorkflow
        self.workflow = NativeWorkflow(self.project_uuid, [], None)
        self.workflow.uuid = self.workflow_uuid
        self.workflow.local_exec_path = os.path.join(
            self.tmpdir, ".Yuki", "LocalWorkflows", self.workflow_uuid
        )
        os.makedirs(self.workflow.path, exist_ok=True)
        self.workflow.jobs = []

    def tearDown(self):
        self._home_patcher.stop()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_job(self, uuid_full, status_value="prelude",  # pylint: disable=too-many-arguments,too-many-positional-arguments
                  is_input=False, job_type_value="task"):
        job = MagicMock()
        job.uuid = uuid_full
        job.is_input = is_input
        job.path = "/fake/" + uuid_full
        job.job_type.return_value = job_type_value
        job.status.return_value = status_value
        job.short_uuid.return_value = uuid_full[:7]
        job.workflow_id.return_value = ""
        return job

    def _workflow_results(self):
        results_path = os.path.join(self.workflow.path, "results.json")
        self.assertTrue(os.path.exists(results_path))
        with open(results_path, encoding="utf-8") as f:
            return json.load(f)

    def test_failed_input_fails_fast_without_polling(self):
        """A permanently failed input immediately fails the workflow and its
        execution jobs, naming the blocking input."""
        from Yuki.kernel.execution.status import FAILED

        input_job = self._make_job("a" * 32, status_value="failed", is_input=True)
        exec_job = self._make_job("b" * 32)
        self.workflow.jobs = [input_job, exec_job]

        # Polling dependency workflows would be pointless: fail fast must not
        # reach VWorkflow.create.
        with patch("Yuki.kernel.workflows.base.VWorkflow.create",
                   side_effect=AssertionError("should not poll workflows")):
            result = self.workflow._wait_for_dependencies()

        self.assertIs(result, False)
        self.assertEqual(self._workflow_results()["results"]["status"], "failed")
        status_arg, detail_arg = exec_job.set_status.call_args.args
        self.assertEqual(status_arg, FAILED)
        self.assertIn("Blocked", detail_arg)
        self.assertIn(input_job.short_uuid(), detail_arg)

    def test_dependency_wait_timeout_fails_loudly(self):
        """Exhausting the wait window marks the workflow and jobs failed with
        a message naming the still-pending inputs."""
        from Yuki.kernel.execution.status import FAILED

        input_job = self._make_job("a" * 32, status_value="prelude", is_input=True)
        exec_job = self._make_job("b" * 32)
        self.workflow.jobs = [input_job, exec_job]

        dep_workflow = MagicMock()
        dep_workflow.update_workflow_status.return_value = None

        with patch("Yuki.kernel.workflows.base.VWorkflow.create",
                   return_value=dep_workflow), \
             patch("Yuki.kernel.workflows.base.time.sleep"):
            result = self.workflow._wait_for_dependencies()

        self.assertIs(result, False)
        self.assertEqual(self._workflow_results()["results"]["status"], "failed")
        status_arg, detail_arg = exec_job.set_status.call_args.args
        self.assertEqual(status_arg, FAILED)
        self.assertIn("timed out", detail_arg)
        self.assertIn(input_job.short_uuid(), detail_arg)

    def test_backend_failure_message_includes_exception(self):
        """The generic backend failure message carries the exception text."""
        from Yuki.kernel.execution.status import FAILED

        exec_job = self._make_job("b" * 32)
        self.workflow.jobs = [exec_job]
        self.workflow.construct_workflow_jobs = MagicMock()
        self.workflow._wait_for_dependencies = MagicMock(return_value=True)
        self.workflow.construct_snake_file = MagicMock()
        self.workflow._execute_backend = MagicMock(
            side_effect=RuntimeError("remote snakemake died"))

        with self.assertRaises(RuntimeError):
            self.workflow.run()

        exec_job.set_status.assert_any_call(
            FAILED, "Backend execution failed: remote snakemake died")

    def test_backend_failure_preserves_terminal_job_status(self):
        """Backend failures only mark non-terminal jobs failed.

        A job the backend already moved to a terminal state (e.g. the
        ssh handler's dissonance on remote-start failure, or a coda from
        an earlier run) must not be clobbered with a generic failure.
        """
        from Yuki.kernel.execution.status import DISSONANCE, FAILED

        dissonant_job = self._make_job("a" * 32, status_value=DISSONANCE)
        running_job = self._make_job("b" * 32, status_value="in_movement")
        self.workflow.jobs = [dissonant_job, running_job]
        self.workflow.construct_workflow_jobs = MagicMock()
        self.workflow._wait_for_dependencies = MagicMock(return_value=True)
        self.workflow.construct_snake_file = MagicMock()
        self.workflow._execute_backend = MagicMock(
            side_effect=RuntimeError("remote snakemake died"))

        with self.assertRaises(RuntimeError):
            self.workflow.run()

        dissonant_calls = [call.args[0] for call in
                           dissonant_job.set_status.call_args_list]
        self.assertNotIn(FAILED, dissonant_calls)
        running_job.set_status.assert_any_call(
            FAILED, "Backend execution failed: remote snakemake died")

    def test_successful_wait_returns_true(self):
        """Fully finished inputs let the wait complete and return True."""
        from Yuki.kernel.execution.status import CODA

        input_job = self._make_job("a" * 32, status_value=CODA, is_input=True)
        exec_job = self._make_job("b" * 32)
        self.workflow.jobs = [input_job, exec_job]

        with patch("Yuki.kernel.workflows.base.VWorkflow.create",
                   side_effect=AssertionError("should not poll workflows")):
            result = self.workflow._wait_for_dependencies()

        self.assertIs(result, True)
        exec_job.set_status.assert_not_called()


if __name__ == "__main__":
    unittest.main()
