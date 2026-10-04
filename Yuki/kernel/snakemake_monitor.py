"""
Snakemake execution monitor for local workflow status tracking.

This module provides utilities to monitor snakemake workflow execution,
extract execution status and logs, and update workflow results.json
in the same format as REANA workflows.
"""
import fcntl
import os
import json
import subprocess
import time
import signal
from .status_constants import (
    IN_MOVEMENT, CODA, FAILED, translate_to_musical, is_terminal_status)


class SnakemakeMonitor:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """Monitor snakemake execution and update workflow status."""

    def __init__(self, workflow_path, local_exec_path,
                 project_uuid=None, workflow_uuid=None):
        """
        Initialize snakemake monitor.

        Args:
            workflow_path: Path to ~/.Yuki/Workflows/<project>/<uuid>/
            local_exec_path: Path to ~/.Yuki/LocalWorkflows/<uuid>/
            project_uuid: Project UUID (needed to instantiate NativeWorkflow
                          for per-job status propagation). Optional; if
                          omitted, derived from workflow_path layout.
            workflow_uuid: Workflow UUID (same rationale).
        """
        self.workflow_path = workflow_path
        self.local_exec_path = local_exec_path
        self.results_file = os.path.join(workflow_path, "results.json")
        self.log_file = os.path.join(workflow_path, "log.json")
        self.snakemake_log = os.path.join(local_exec_path, "snakemake.log")
        self.snakemake_report = os.path.join(local_exec_path, "report.json")
        self.cancel_path = os.path.join(local_exec_path, "native-runner.cancel")
        self.pid_path = os.path.join(local_exec_path, "native-runner.snakemake.pid")

        if workflow_uuid is None:
            workflow_uuid = os.path.basename(workflow_path.rstrip("/"))
        if project_uuid is None:
            project_uuid = os.path.basename(
                os.path.dirname(workflow_path.rstrip("/"))
            )
        self.project_uuid = project_uuid
        self.workflow_uuid = workflow_uuid

    def execute_snakemake(self, cores, logger=None, mem_mb=None,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-branches
                          snakemake_path=None, conda_path=None,
                          defer_success=False):
        """
        Execute snakemake and monitor progress.

        Args:
            cores: Number of cores to use
            logger: Optional logger function
            mem_mb: Optional memory limit in MB; adds --resources mem_mb=<n>
            snakemake_path: Optional path replacing the `snakemake` binary
            conda_path: Optional conda binary path; its directory is
                        prepended to PATH in the subprocess environment

        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        if logger:
            logger(f"[SNAKEMAKE] Starting snakemake execution in {self.local_exec_path}")

        # Initial status: IN_MOVEMENT
        self._update_results(IN_MOVEMENT, 0, 0, {})

        # Build snakemake command
        cmd = [
            snakemake_path or "snakemake",
            "--use-conda",
            "--conda-frontend", "conda",
            "--keep-going",
            "-j", str(cores)
        ]
        if mem_mb:
            cmd += ["--resources", f"mem_mb={int(mem_mb)}"]
        env = None
        if conda_path:
            env = dict(os.environ)
            env["PATH"] = (os.path.dirname(conda_path) + os.pathsep
                           + env.get("PATH", ""))

        try:
            # Execute snakemake with output capture
            with open(self.snakemake_log, "w", encoding='utf-8') as log_f:
                with subprocess.Popen(
                    cmd,
                    cwd=self.local_exec_path,
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                    start_new_session=True,
                ) as process:
                    with open(self.pid_path, "w", encoding="utf-8") as pid_file:
                        pid_file.write(str(process.pid))
                    # Monitor execution
                    while process.poll() is None:
                        if os.path.exists(self.cancel_path):
                            os.killpg(process.pid, signal.SIGTERM)
                            try:
                                process.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                os.killpg(process.pid, signal.SIGKILL)
                                process.wait()
                            self._update_results("stopped", 0, 0,
                                                 {"error": "Cancelled by user"})
                            return 1
                        time.sleep(2)  # Check every 2 seconds
                        self._update_progress(logger)

                    exit_code = process.returncode

        except Exception as e:
            if logger:
                logger(f"[SNAKEMAKE] Execution error: {e}")
            self._update_results(FAILED, 0, 0, {"error": str(e)})
            return 1
        finally:
            if os.path.exists(self.pid_path):
                os.unlink(self.pid_path)

        # Final status update
        if exit_code == 0:
            if defer_success:
                self._update_results("collecting", 0, 0, {})
            else:
                self._finalize_results(logger)
            return 0
        self._handle_failure(logger)
        return exit_code

    def _update_progress(self, logger=None):
        """Update progress from snakemake execution."""
        try:
            # Count .done marker files as progress indicator
            self._count_completed_jobs(logger)
        except Exception as e:
            if logger:
                logger(f"[SNAKEMAKE] Error updating progress: {e}")

    def _process_snakemake_report(self, report, logger=None):
        """Process snakemake JSON report to extract status and progress."""
        try:
            # Extract job information
            jobs = report.get("jobs", {})
            total_jobs = len(jobs)
            completed_jobs = sum(
                1 for job in jobs.values()
                if job.get("status") == "completed"
            )

            # Build detailed logs from jobs
            detailed_logs = {}
            for job_id, job_info in jobs.items():
                detailed_logs[str(job_id)] = {
                    "status": job_info.get("status"),
                    "log": job_info.get("log", ""),
                    "shell": job_info.get("shell", ""),
                    "benchmark": job_info.get("benchmark", {}),
                }

            # Update results with progress
            self._update_results(
                IN_MOVEMENT,
                total_jobs,
                completed_jobs,
                detailed_logs
            )

            if logger:
                logger(f"[SNAKEMAKE] Progress: {completed_jobs}/{total_jobs} jobs completed")

        except Exception as e:
            if logger:
                logger(f"[SNAKEMAKE] Error processing report: {e}")

    def _count_completed_jobs(self, logger=None):
        """Count completed jobs using .done marker files."""
        try:
            # Get job list from workflow_info.json
            workflow_info_path = os.path.join(self.local_exec_path, "workflow_info.json")
            if not os.path.exists(workflow_info_path):
                return

            with open(workflow_info_path, 'r', encoding='utf-8') as f:
                workflow_info = json.load(f)

            steps = workflow_info.get("workflow", {}).get("specification", {}).get("steps", [])
            total_jobs = len(steps)

            # Count .done files
            completed_jobs = 0
            for step in steps:
                job_uuid = step.get("name", "")[4:]  # Remove "step_" prefix
                done_file = os.path.join(self.local_exec_path, f"{job_uuid}.done")
                if os.path.exists(done_file):
                    completed_jobs += 1

            self._update_results(IN_MOVEMENT, total_jobs, completed_jobs, {})

            if logger:
                logger(f"[SNAKEMAKE] Progress: {completed_jobs}/{total_jobs} jobs completed")

        except Exception as e:
            if logger:
                logger(f"[SNAKEMAKE] Error counting jobs: {e}")

    def _write_results(self, results):
        """Atomically write results without reversing a terminal state."""
        os.makedirs(os.path.dirname(self.results_file), exist_ok=True)
        with open(self.results_file, "a+", encoding="utf-8") as result_file:
            fcntl.flock(result_file, fcntl.LOCK_EX)
            try:
                result_file.seek(0)
                contents = result_file.read()
                data = json.loads(contents) if contents.strip() else {}
                current = data.get("results", {}) or {}
                current_status = translate_to_musical(
                    current.get("status", "unknown"))
                new_status = translate_to_musical(
                    results.get("status", "unknown"))
                if (is_terminal_status(current_status) and
                        current_status != new_status):
                    return False
                data["results"] = results
                result_file.seek(0)
                result_file.truncate()
                json.dump(data, result_file)
                result_file.flush()
                os.fsync(result_file.fileno())
                return True
            finally:
                fcntl.flock(result_file, fcntl.LOCK_UN)

    def _update_results(self, status, total, completed, logs):
        """Update results.json with current status."""
        try:
            self._write_results({
                "status": translate_to_musical(status),
                "progress": {
                    "total": total,
                    "completed": completed
                },
                "logs": logs
            })
        except Exception as e:
            print(f"[SNAKEMAKE] Error writing results: {e}")

    def _propagate_per_job_status(self, logger=None):
        """Reconcile each VJob's status with on-disk markers."""
        try:
            from .vworkflow import VWorkflow
            workflow = VWorkflow.create(
                self.project_uuid, [],
                uuid=self.workflow_uuid, mode="native",
            )
            workflow.propagate_job_statuses(workflow_terminal=True)
        except Exception as e:
            if logger:
                logger(f"[SNAKEMAKE] Per-job propagation failed: {e}")

    def _finalize_results(self, logger=None):
        """Finalize results after successful execution."""
        try:
            # Count final job count
            total_jobs = 0
            workflow_info_path = os.path.join(self.local_exec_path, "workflow_info.json")
            if os.path.exists(workflow_info_path):
                with open(workflow_info_path, 'r', encoding='utf-8') as f:
                    workflow_info = json.load(f)
                    steps = workflow_info.get("workflow", {}).get(
                        "specification", {}
                    ).get("steps", [])
                    total_jobs = len(steps)

            # Read snakemake log
            snakemake_log_content = ""
            if os.path.exists(self.snakemake_log):
                with open(self.snakemake_log, 'r', encoding='utf-8') as f:
                    snakemake_log_content = f.read()

            # Update results with CODA status
            results = {
                "status": CODA,
                "progress": {
                    "total": total_jobs,
                    "completed": total_jobs
                },
                "execution_time": self._get_execution_time(),
                "snakemake_log": snakemake_log_content[-3000:] if snakemake_log_content else ""
            }

            if not self._write_results(results):
                return

            if logger:
                logger("[SNAKEMAKE] Execution completed successfully")

            self._propagate_per_job_status(logger)

        except Exception as e:
            if logger:
                logger(f"[SNAKEMAKE] Error finalizing results: {e}")

    def _handle_failure(self, logger=None):  # pylint: disable=too-many-locals
        """Handle snakemake execution failure."""
        try:
            # Read snakemake log for error details
            snakemake_log_content = ""
            if os.path.exists(self.snakemake_log):
                with open(self.snakemake_log, 'r', encoding='utf-8') as f:
                    snakemake_log_content = f.read()

            # Count how many jobs completed before failure
            completed_jobs = 0
            total_jobs = 0
            workflow_info_path = os.path.join(self.local_exec_path, "workflow_info.json")
            if os.path.exists(workflow_info_path):
                with open(workflow_info_path, 'r', encoding='utf-8') as f:
                    workflow_info = json.load(f)
                    steps = workflow_info.get("workflow", {}).get(
                        "specification", {}
                    ).get("steps", [])
                    total_jobs = len(steps)
                    for step in steps:
                        job_uuid = step.get("name", "")[4:]
                        done_file = os.path.join(self.local_exec_path, f"{job_uuid}.done")
                        if os.path.exists(done_file):
                            completed_jobs += 1

            # Extract error information from log
            error_msg = self._extract_error_from_log(snakemake_log_content)

            results = {
                "status": FAILED,
                "progress": {
                    "total": total_jobs,
                    "completed": completed_jobs
                },
                "error": error_msg,
                "snakemake_log": snakemake_log_content[-3000:]  # Last 3000 chars
            }

            if not self._write_results(results):
                return

            if logger:
                logger(f"[SNAKEMAKE] Execution failed: {error_msg}")

            self._propagate_per_job_status(logger)

        except Exception as e:
            if logger:
                logger(f"[SNAKEMAKE] Error handling failure: {e}")

    def _get_execution_time(self):
        """Get total execution time from snakemake log."""
        try:
            if not os.path.exists(self.snakemake_log):
                return None

            with open(self.snakemake_log, 'r', encoding='utf-8') as f:
                content = f.read()
                # Look for "Finished at" or "Finished job" lines
                for line in content.split('\n'):
                    if "Finished at" in line or "seconds)" in line:
                        return line.strip()
            return None
        except Exception:
            return None

    def _extract_error_from_log(self, log_content):
        """Extract error message from snakemake log."""
        try:
            lines = log_content.split('\n')
            for i, line in enumerate(lines):
                if 'error' in line.lower() or 'failed' in line.lower():
                    # Return this line and a few following lines for context
                    context = '\n'.join(lines[i:min(i+3, len(lines))])
                    return context
            return "Snakemake execution failed - see logs for details"
        except Exception:
            return "Unknown error occurred"
