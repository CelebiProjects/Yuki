"""Workflow abstraction and helpers for constructing, executing and monitoring
workflows composed of VJob objects.

This module defines the abstract VWorkflow class which:
- Builds a DAG of VJob instances (non-recursive, stack-based traversal).
- Produces a Snakemake Snakefile for execution.
- Waits for input/dependency workflows to finish.
- Provides hooks for backend execution and status synchronization implemented by subclasses.
"""
# pylint: disable=cyclic-import

import logging
import os
import time
from abc import ABC, abstractmethod
from PIL import Image, ImageDraw, ImageFont

from CelebiChrono.utils import csys, metadata
from CelebiChrono.kernel.chern_cache import ChernCache
from Yuki.kernel.vjob import VJob
from Yuki.kernel.container_job import ContainerJob
from Yuki.kernel.image_job import ImageJob
from Yuki.kernel.status_constants import (
    PRELUDE, IN_MOVEMENT, DISSONANCE, FAILED,
    CODA, FINAL_NOTE, STOPPED, DELETED,
    translate_to_musical, is_terminal_status
)
from Yuki.utils import snakefile
from .file_staging import walk_files

CHERN_CACHE = ChernCache.instance()


def _yuki_dir():
    """Return the configured Yuki data root."""
    return os.path.expanduser(os.environ.get("YUKIDIR") or "~/.Yuki")


class VWorkflow(ABC):  # pylint: disable=too-many-instance-attributes
    """Abstract base class representing a workflow.

    Parameters:
    - project_uuid: UUID of the project/storage area for inputs/outputs.
    - jobs: root job(s) to build the workflow from.
    - uuid: workflow UUID (optional; generated if not provided).
    - machine_id: id of the execution machine/runner (optional).
    """
    def __init__(self, project_uuid, jobs, uuid=None, machine_id=None):
        self.project_uuid = project_uuid
        self.uuid = uuid or csys.generate_uuid()
        self.path = os.path.join(
            _yuki_dir(), "Workflows", self.project_uuid, self.uuid)
        os.makedirs(self.path, exist_ok=True)

        self.config_file = metadata.ConfigFile(os.path.join(self.path, "config.json"))
        self.jobs = []
        self.dependencies = {}
        self.steps = []
        self.snakefile_path = os.path.join(self.path, "Snakefile")
        self.log_path = os.path.join(self.path, "workflow.log")

        if uuid:
            self.start_job = None
            self.machine_id = self.config_file.read_variable("machine_id", machine_id or "")
            # load the jobs from the config file
            jobs_info = self.config_file.read_variable("jobs_info", {})
            for job_uuid, info in jobs_info.items():
                job_path = os.path.join(_yuki_dir(), "Storage",
                                         self.project_uuid, job_uuid)
                job = VJob(job_path, self.machine_id)
                job.is_input = info.get("is_input", False)
                self.jobs.append(job)
        else:
            self.start_job = jobs.copy() if isinstance(jobs, list) else [jobs]
            self.machine_id = self.start_job[0].machine_id if self.start_job else (machine_id or "")
            self.config_file.write_variable("machine_id", self.machine_id)

    def logger(self, message):
        """Log message with timestamp to both console and the workflow log file."""
        timestamp = time.strftime("[%Y-%m-%d %H:%M:%S]", time.localtime())
        log_message = f"{timestamp} {message}"
        logging.getLogger("Yuki.workflow").info(message)
        with open(self.log_path, "a", encoding='utf-8') as f:
            f.write(log_message + "\n")

    @staticmethod
    def create(project_uuid, jobs, uuid=None, mode=None):
        """Factory method to instantiate the appropriate workflow subclass.

        Behavior:
        - If mode is provided, use it.
        - Otherwise, for an existing workflow, prefer the backend_type stored
          in the workflow's own config.json; fall back to the global
          backend_types mapping keyed by the workflow's machine_id.
        - For a new workflow, persist the resolved backend_type so that later
          status/file-status calls reload the correct subclass even when the
          global mapping is missing or uses a different key.
        """
        if not mode and uuid:
            workflow_path = os.path.join(_yuki_dir(),
                                          "Workflows", project_uuid, uuid)
            workflow_config = metadata.ConfigFile(
                os.path.join(workflow_path, "config.json"))
            stored_mode = workflow_config.read_variable("backend_type", "")
            if stored_mode:
                mode = stored_mode
            else:
                runner_id = workflow_config.read_variable("machine_id", "")
                config = metadata.ConfigFile(os.path.join(
                    _yuki_dir(), "config.json"))
                backend_types = config.read_variable("backend_types", {})
                mode = backend_types.get(runner_id, "reana")
        if not mode:
            mode = "reana"

        # Accept both "native" (new) and "dry" (legacy/deprecated) for backward compatibility
        if mode in ("native", "dry"):
            from .native_workflow import NativeWorkflow
            workflow = NativeWorkflow(project_uuid, jobs, uuid)
        elif mode == "ssh":
            from .ssh_workflow import SshWorkflow
            workflow = SshWorkflow(project_uuid, jobs, uuid)
        else:
            from .reana_workflow import ReanaWorkflow
            workflow = ReanaWorkflow(project_uuid, jobs, uuid)

        logging.getLogger("Yuki.workflow").info(
            f"[VWorkflow.create] selected backend={mode} "
            f"class={type(workflow).__name__} uuid={uuid or 'new'}")

        # Persist backend_type on creation so reloads are independent of the
        # global backend_types mapping.
        if not uuid:
            workflow.config_file.write_variable("backend_type", mode)

        return workflow

    def backend_type(self):
        """The workflow's backend type (persisted at creation)."""
        return self.config_file.read_variable("backend_type", "reana")

    def get_name(self):
        """Get a human-readable name for the workflow."""
        return f"w-{self.project_uuid[:8]}-{self.uuid[:8]}"

    def _write_environment_directive(self, snake_file, environment, indent=1):
        """Write the backend-specific environment directive into the Snakefile.

        The default implementation writes a Docker ``container:`` directive
        suitable for REANA execution. Subclasses (e.g. NativeWorkflow) may
        override this to emit ``conda:``, ``apptainer:``, etc.
        """
        snake_file.addline("container:", indent)
        snake_file.addline(f'"docker://{environment}"', indent + 1)

    def run(self):  # pylint: disable=too-many-branches,too-many-statements
        """Common execution flow for workflows.

        High level steps:
        1. Construct the full job list and dependency graph.
        2. Mark non-input, non-algorithm jobs as waiting.
        3. Wait for external input workflows (dependencies) to finish.
        4. Assign workflow id and mark jobs running.
        5. Construct Snakefile and hand off to backend for execution.
        Errors during construction/execution set the workflow and jobs to 'failed'.
        """
        self.logger("Constructing the workflow")
        self.logger(f"Start job: {self.start_job}")
        if isinstance(self.start_job, list):
            self.construct_workflow_jobs(self.start_job)
        else:
            self.construct_workflow_jobs([self.start_job] if self.start_job else [])

        self.logger(f"Jobs after the construction: {self.jobs}")

        # Set all the jobs to be the waiting status
        total_jobs = len(self.jobs)
        for i, job in enumerate(self.jobs):
            self.logger(f"[{i+1}/{total_jobs}] job: {job}, is input: {job.is_input}, "
                         f"job status: {job.status()}, job type: {job.job_type()}")

        # Save the jobs info to the config file
        jobs_info = {}
        for job in self.jobs:
            jobs_info[job.uuid] = {
                "is_input": job.is_input,
                "job_type": job.job_type(),
                "status": job.status(),
                "workflow_id": job.workflow_id()
            }
        self.config_file.write_variable("jobs_info", jobs_info)

        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            job.set_status(
                PRELUDE,
                "Constructing the workflow: 1/3. waiting for the unfinished dependencies"
            )

        # Wait for dependencies
        if not self._wait_for_dependencies():
            return

        # Set workflow IDs for jobs
        active_jobs = [j for j in self.jobs if not j.is_input and j.job_type() != "algorithm"]
        total_active = len(active_jobs)
        for i, job in enumerate(active_jobs):
            self.logger(f"[{i+1}/{total_active}] Set workflow id to job {job}")
            job.set_workflow_id(self.uuid)
            job.set_status(PRELUDE, "Constructing the workflow: 2/3. workflow created and assigned")

        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            job.set_status(PRELUDE, "Constructing the workflow: 3/3. Constructing the snakefile")

        # Prepare and Execute
        self.logger("Constructing")
        try:
            self.logger("Constructing the snakefile")
            self.construct_snake_file()
        except Exception:
            self.logger("Failed to construct the snakefile")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status(DISSONANCE, "Workflow construction failed: snakefile creation error")
            raise

        try:
            self.logger("Executing backend")
            self._execute_backend()
        except Exception as e:
            self.logger("Failed to execute backend")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                if is_terminal_status(job.status(musical=True)):
                    # Preserve the backend's own terminal marking (e.g. the
                    # ssh handler's dissonance on remote-start failure) and
                    # never clobber a previously completed status.
                    continue
                job.set_status(FAILED, f"Backend execution failed: {e}")
            raise

    @abstractmethod
    def _execute_backend(self):
        pass

    @abstractmethod
    def _sync_external_job_status(self, job):
        pass

    def _wait_for_dependencies(self):  # pylint: disable=too-many-branches
        """Waits for all input-dependency workflows to reach a terminal 'finished' state.

        Notes:
        - Polls the statuses of workflows referred to by input jobs.
        - Fails fast when an input is in a terminal failure state: the workflow
          and its execution jobs are marked failed, naming the blocking inputs.
        - Uses a bounded number of retries to avoid infinite wait; exhausting the
          window also fails loudly instead of leaving jobs pending forever.
        """
        def _pending_input_jobs():
            return [j for j in self.jobs if j.is_input
                    and j.status(musical=False) not in (FINAL_NOTE, CODA)
                    and j.status(musical=True) != "archived"
                    and j.job_type() != "algorithm"]

        def _failed_inputs(input_jobs):
            return [j for j in input_jobs
                    if j.status(musical=True) in (FAILED, DISSONANCE, STOPPED, DELETED)]

        def _describe(job):
            name = job.short_uuid()
            try:
                path = job.config_file.read_variable("current_path", "")
                if isinstance(path, str) and path:
                    name = f"{name} ({path})"
            except Exception:
                pass
            return name

        def _fail(message):
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.status(musical=True) == "archived":
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status(FAILED, message)
            self.logger(message)

        all_finished = False
        # First, check whether the dependencies are satisfied
        for i_tries in range(60):
            self.logger(f"Checking finished (Attempt {i_tries+1}/60)")
            input_jobs = _pending_input_jobs()
            failed_inputs = _failed_inputs(input_jobs)
            if failed_inputs:
                names = ", ".join(_describe(j) for j in failed_inputs)
                _fail(f"Blocked: upstream input {names} is failed "
                      f"- fix the upstream task and resubmit")
                return False

            all_finished = True
            workflow_list = []
            for j in input_jobs:
                logging.getLogger("Yuki.workflow").info(
                    "%s %s %s %s", j, j.status(musical=True),
                    j.status(musical=False), j.job_type())
            total_inputs = len(input_jobs)

            for i, job in enumerate(input_jobs):
                workflow = VWorkflow.create(self.project_uuid, [], job.workflow_id())
                self.logger(f"[{i+1}/{total_inputs}] Checking dependency: Job {job.uuid} "
                             f"workflow {workflow.uuid}")
                if workflow and workflow not in workflow_list:
                    workflow_list.append(workflow)

            for workflow in workflow_list:
                if is_terminal_status(workflow.status()):
                    self.logger(f"[dependencies] workflow={workflow.uuid} "
                                f"status={workflow.status()} already terminal; "
                                "skipping update_workflow_status")
                    continue
                workflow.update_workflow_status()

            for job in self.jobs:
                if not job.is_input:
                    continue
                if job.status(musical=True) == FINAL_NOTE:
                    continue
                if job.status(musical=True) == CODA:
                    continue
                if job.status(musical=True) == "archived":
                    continue
                if job.job_type() == "algorithm":
                    continue
                workflow = VWorkflow.create(self.project_uuid, [], job.workflow_id())
                if workflow in workflow_list:
                    job.update_status_from_workflow(
                        os.path.join(
                            _yuki_dir(),
                            "Workflows",
                            self.project_uuid,
                            job.workflow_id()
                            ),
                        self.logger
                        )

                job_status = job.status(musical=True)
                # self.logger(f"Job {job.short_uuid()} status: {job_status}")
                if job_status != CODA:
                    all_finished = False
                    # We continue checking other jobs to update their status as well

            if all_finished:
                break
            time.sleep(10)
        self.logger("All done")

        if not all_finished:
            pending = _pending_input_jobs()
            names = ", ".join(_describe(j) for j in pending) or "unknown inputs"
            _fail(f"Dependency wait timed out after 60 attempts "
                  f"- inputs still not finished: {names}")
            return False

        return True

    def construct_snake_file(self):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """Construct the Snakemake Snakefile describing rules for all jobs.

        Each job becomes a rule 'step<short_uuid>' that:
        - Declares inputs and a single '.done' output marker.
        - References a container image and resources.
        - Provides a shell command combining the job's commands.
        """
        config = metadata.ConfigFile(os.path.join(_yuki_dir(), "config.json"))
        use_kerberos = config.read_variable("use_kerberos", {}).get(self.machine_id, False)
        for job in self.jobs:
            self.logger(f"Job in the workflow: {job}, is input: {job.is_input}, "
                         f"job type: {job.job_type()}")

        self.snakefile_path = os.path.join(self.path, "Snakefile")
        snake_file = snakefile.SnakeFile(os.path.join(self.path, "Snakefile"))

        self.dependencies = {}
        self.steps = []

        snake_file.addline("rule all:", 0)
        snake_file.addline("input:", 1)
        self.dependencies["all"] = []
        snake_file.addline('"finalize.done",', 2)
        self.dependencies["all"].append("finalize")

        backend_type = self.backend_type()
        setup_commands = []
        setup_kerberos = False
        for job in self.jobs:
            if job.object_type() != "task" or not job.is_input:
                continue
            if backend_type == "ssh":
                # ssh inputs are always cached on the runner (auto-cache);
                # the setup rule copies them from the impressions cache.
                container = ContainerJob(job.path, job.machine_id)
                setup_commands.extend(
                    container.setup_commands("ssh", self.machine_id))
            elif job.cache_on_runner() and job.machine_id == self.machine_id:
                container = ContainerJob(job.path, job.machine_id)
                setup_commands.extend(container.setup_commands(backend_type))
                # On reana the cache lives on EOS, so fetching it in the
                # setup rule requires Kerberos.
                setup_kerberos = setup_kerberos or backend_type == "reana"

        finalize_commands = []
        for job in self.jobs:
            if job.object_type() != "task" or not job.is_input:
                continue
            if backend_type == "ssh":
                container = ContainerJob(job.path, job.machine_id)
                finalize_commands.extend(
                    container.finalize_commands("ssh"))
            elif job.cache_on_runner() and job.machine_id == self.machine_id:
                container = ContainerJob(job.path, job.machine_id)
                finalize_commands.extend(
                    container.finalize_commands(backend_type))

        snake_file.addline("\n", 0)
        snake_file.addline("rule setup:", 0)
        snake_file.addline("input:", 1)
        snake_file.addline("output:", 1)
        snake_file.addline('"setup.done",', 2)
        self._write_environment_directive(
            snake_file, "docker.io/reanahub/reana-env-root6:6.18.04", 1)
        snake_file.addline("resources:", 1)
        if setup_commands and (use_kerberos or setup_kerberos):
            snake_file.addline('kerberos=True,', 2)
        snake_file.addline('kubernetes_memory_limit="1Gi"', 2)
        snake_file.addline("shell:", 1)
        if setup_commands:
            snake_file.addline(f'"{" && ".join(setup_commands)} && touch setup.done"', 2)
        else:
            snake_file.addline('"touch setup.done"', 2)
        self.dependencies["setup"] = []

        snake_file.addline("\n", 0)
        snake_file.addline("rule finalize:", 0)
        snake_file.addline("input:", 1)
        self.dependencies["finalize"] = []
        for job in self.jobs:
            snake_file.addline(f'"{job.short_uuid()}.done",', 2)
            self.dependencies["finalize"].append(f"step{job.short_uuid()}")

        snake_file.addline("output:", 1)
        snake_file.addline('"finalize.done"', 2)
        self._write_environment_directive(
            snake_file, "docker.io/reanahub/reana-env-root6:6.18.04", 1)
        snake_file.addline("resources:", 1)
        snake_file.addline('kubernetes_memory_limit="1Gi"', 2)
        snake_file.addline("shell:", 1)
        if finalize_commands:
            snake_file.addline(f'"{" && ".join(finalize_commands)} && touch finalize.done"', 2)
        else:
            snake_file.addline('"touch finalize.done"', 2)

        total_jobs = len(self.jobs)
        for i, job in enumerate(self.jobs):
            start_time = time.time()
            self.logger(f"[{i+1}/{total_jobs}] Processing job: {job}")
            snakemake_rule = None
            step = None
            if job.object_type() == "algorithm":
                # In this case, if the command is compile, we need to compile it
                image = ImageJob(job.path, job.machine_id)
                image.is_input = job.is_input
                snakemake_rule = image.snakemake_rule(self.machine_id)
                step = image.step(self.machine_id)

                # In this case, we also need to run the "touch"
            elif job.object_type() == "task":
                container = ContainerJob(job.path, job.machine_id)
                container.is_input = job.is_input
                snakemake_rule = container.snakemake_rule(self.machine_id, backend_type)
                step = container.step(self.machine_id, backend_type)
            else:
                # Unknown job type, skip or handle
                self.logger(f"Unknown job type {job.object_type()} for job {job}, skipping")
                continue
            self.logger(f"[{i+1}/{total_jobs}] Get the step at time "
                         f"{time.time() - start_time:.4f}s")

            snake_file.addline("\n", 0)
            snake_file.addline(f"rule step{job.short_uuid()}:", 0)
            snake_file.addline("input:", 1)
            for input_file in snakemake_rule["inputs"]:
                snake_file.addline(f'"{input_file}",', 2)
            # Add the dependencies
            self.dependencies[f"step{job.short_uuid()}"] = []
            for dep in job.dependencies():
                dep_job = VJob(
                    os.path.join(
                        _yuki_dir(),
                        "Storage",
                        self.project_uuid,
                        dep
                    ),
                    self.machine_id
                )
                self.dependencies[f"step{job.short_uuid()}"].append(f"step{dep_job.short_uuid()}")
            self.logger(f"[{i+1}/{total_jobs}] Added inputs and dependencies at time "
                         f"{time.time() - start_time:.4f}s")

            snake_file.addline("output:", 1)
            snake_file.addline(f'"{job.short_uuid()}.done"', 2)
            self._write_environment_directive(
                snake_file, snakemake_rule["environment"], 1)
            snake_file.addline("resources:", 1)
            compute_backend = snakemake_rule["compute_backend"]
            resource_lines = []
            if job.use_kerberos(backend_type):
                resource_lines.append('kerberos=True')
            if compute_backend == "htcondorcern":
                resource_lines.append(f'compute_backend="{snakemake_rule["compute_backend"]}"')
                resource_lines.append('htcondor_max_runtime="espresso"')
                resource_lines.append('kerberos=True')
            else:
                resource_lines.append(f'kubernetes_memory_limit="{snakemake_rule["memory"]}"')
            cvmfs_repos = snakemake_rule.get("cvmfs", [])
            if cvmfs_repos:
                resource_lines.append(f'cvmfs="{",".join(cvmfs_repos)}"')
            for i, line in enumerate(resource_lines):
                suffix = "," if i < len(resource_lines) - 1 else ""
                snake_file.addline(line + suffix, 2)
            snake_file.addline("shell:", 1)
            snake_file.addline(f'"{" && ".join(snakemake_rule["commands"])}"', 2)
            self.logger(f"[{i+1}/{total_jobs}] Added shell and resources at time "
                         f"{time.time() - start_time:.4f}s")

            self.steps.append(step)
            self.logger(f"[{i+1}/{total_jobs}] Appended step at time "
                         f"{time.time() - start_time:.4f}s")

        snake_file.write()
        self.logger(f"Snakefile written to {self.snakefile_path}")

    def construct_workflow_jobs(self, root_jobs):
        """
        Construct workflow jobs iteratively including dependencies (DAG-safe, no recursion).

        Implementation note:
        - Uses an explicit stack for DFS-like traversal.
        - Each stack entry is (VJob, expanded_flag). On first visit we push it back as expanded
          and push its dependencies; on the second visit we append it to self.jobs.
        - This avoids recursion and handles DAGs safely.
        """
        self.jobs = []  # rebuild the full job set; keeps repeated walks idempotent
        visited = set()
        # Initialize stack with all root jobs, marked as not expanded
        stack = [(job, False) for job in root_jobs]

        while stack:
            job, expanded = stack.pop()

            # Skip if already processed (only if first time)
            if job.path in visited and not expanded:
                continue

            # Ensure job has machine_id
            if job.machine_id is None:
                job = VJob(job.path, self.machine_id)
                if job.machine_id is None:
                    continue

            status = job.status(musical=True)
            obj_type = job.object_type()

            # For jobs already in active or terminal states, add immediately
            # Note: job.status() returns musical names, so we check for those
            musical_status = translate_to_musical(status)
            if musical_status in (CODA, FAILED, DISSONANCE, IN_MOVEMENT, FINAL_NOTE):
                if obj_type == "task":
                    job.is_input = True
                self.jobs.append(job)
                visited.add(job.path)
                continue

            if expanded:
                # Second time we pop the job: all dependencies are done
                self.jobs.append(job)
                visited.add(job.path)
                continue

            # Otherwise, expand dependencies first
            stack.append((job, True))  # mark job to add after deps
            for dep in job.dependencies():
                dep_path = os.path.join(_yuki_dir(), "Storage",
                                         self.project_uuid, dep)
                dep_job = VJob(dep_path, None)
                if dep_job.path not in visited:
                    stack.append((dep_job, False))

    def status(self):
        """Get the current workflow status."""
        status, last_consult_time = CHERN_CACHE.consult_table.get(self.uuid, ("unknown", -1))
        if time.time() - last_consult_time < 1:
            return status

        path = os.path.join(self.path, "results.json")
        if not os.path.exists(path):
            return "unknown"

        results_file = metadata.ConfigFile(path)
        results = results_file.read_variable("results", {})
        # print("Results:", results)
        try:
            status = results.get("status", "unknown")
            CHERN_CACHE.consult_table[self.uuid] = (status, time.time())
            return status
        except Exception:
            self.logger("Failed to get the status")
        return "unknown"

    def set_workflow_status(self, status):
        """Set the workflow status in the results file."""
        path = os.path.join(self.path, "results.json")
        results_file = metadata.ConfigFile(path)
        results = results_file.read_variable("results", {})
        results["status"] = status
        results_file.write_variable("results", results)

    def _entered_terminal_state(self, new_status):
        """True when new_status is terminal but the recorded status isn't.

        Detects the status write that first observes the workflow finished
        (or failed), so the distribution refresh runs exactly once.
        """
        if translate_to_musical(new_status) not in (CODA, FAILED):
            return False
        results_file = metadata.ConfigFile(
            os.path.join(self.path, "results.json"))
        previous = results_file.read_variable("results", {})
        return translate_to_musical(previous.get("status", "")) \
            not in (CODA, FAILED)

    def _record_terminal_distributions(self, status):
        """Record data whereabouts once the workflow reaches a terminal state.

        Best-effort: a failing refresh must never fail the status update.
        """
        self.logger(
            f"[_record_terminal_distributions] calling refresh_workflow_distributions "
            f"project={self.project_uuid} workflow={self.uuid} status={status} "
            f"reason=workflow_entered_terminal_state"
        )
        try:
            # Lazy import: impression_storage imports this module at module
            # level, so importing it here avoids a circular import.
            from Yuki.kernel.impression_storage import \
                refresh_workflow_distributions
            refresh_workflow_distributions(self.project_uuid, self, status)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.logger(f"Failed to refresh distributions: {exc}")

    def _refresh_job_filelists(self, status, terminal_transition=False):
        """Refresh saved runner file listings after a status update.

        /file-status reads the saved listings, so they are refreshed here
        (the status-update path, running in Celery) rather than on request.
        Best-effort: a failing refresh must never fail the status update.
        """
        try:
            # Lazy import: impression_storage imports this module at module
            # level, so importing it here avoids a circular import.
            from Yuki.kernel.impression_storage import refresh_job_filelists
            refresh_job_filelists(self.project_uuid, self, status,
                                  terminal_transition)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.logger(f"Failed to refresh file listings: {exc}")

    def watermark(self, impression=None):  # pylint: disable=too-many-locals
        """Add watermark to PNG images for a given impression.

        Procedure:
        1. Verify stageout has been downloaded.
        2. Iterate PNG outputs and draw a small textual watermark (top-right).
        3. Save watermarked copies under a 'watermarks' directory.

        The function operates in-place on copies and will silently skip non-PNG files.
        """
        self.logger(f"Watermarking impression: {impression}")
        if impression:
            path = os.path.join(_yuki_dir(), "Storage",
                                 self.project_uuid, impression, self.machine_id)
            if not os.path.exists(os.path.join(path, "stageout.downloaded")):
                self.logger(f"Stageout not downloaded for impression {impression}, "
                             f"skipping watermarking")
                return
            outputs_path = os.path.join(path, "stageout")
            self.logger(f"Outputs path: {outputs_path}")
            watermark_path = os.path.join(path, "watermarks")
            os.makedirs(watermark_path, exist_ok=True)
            filelist = [
                (rel_path, abs_path)
                for rel_path, abs_path in walk_files(outputs_path)
                if rel_path.endswith(".png")
            ]
            total_files = len(filelist)
            self.logger(f"Files to watermark: {[r for r, _ in filelist]}")

            # Water mark the png files
            for i, (rel_path, abs_path) in enumerate(filelist):
                filename = os.path.basename(rel_path)
                # 1. Open the image and convert it to RGBA.
                # The watermark will be drawn directly onto this image object.
                image = Image.open(abs_path).convert("RGBA")

                # 2. Create the drawing context directly on the image
                draw = ImageDraw.Draw(image)

                # --- Watermark Setup ---

                # Use the same logic for sizing the font (using the original code's approach)
                font_size = int(min(image.size) / 20)
                try:
                    font = ImageFont.truetype("arial.tt", font_size)
                except (IOError, OSError):
                    font = ImageFont.load_default()

                text = f"Imp:{impression}"
                self.logger(f"Watermark text: {text}")

                # Use the positioning logic from the original code (top-right corner)
                textwidth = draw.textlength(text, font=font)

                # The original code's positioning (using y=10)
                x = image.size[0] - textwidth - 10
                y = 10

                # Define the color and opacity
                # fill_color = (255, 255, 255, 50)
                # fill_color = (255, 255, 255, 255)
                fill_color = (0, 0, 0, 255)

                # 3. Draw the text directly onto the image object
                draw.text((x, y), text, font=font, fill=fill_color)

                # 4. Save the resulting image.
                dst_name = os.path.join(
                    os.path.dirname(rel_path),
                    f"imp{impression[:8]}_{filename}"
                )
                dst_path = os.path.join(watermark_path, dst_name)
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                image.save(dst_path, format="PNG")
                self.logger(f"[{i+1}/{total_files}] Saved watermarked image: {rel_path}")

    @abstractmethod
    def update_workflow_status(self):
        """Update workflow status - must be implemented by subclass."""

    def kill(self):
        """Kill the workflow execution - must be implemented by subclass.

        Raises:
        - NotImplementedError if subclass does not implement termination behavior.
        """
        # ...existing code...

    def delete_workspace(self):
        """Delete the runner-side workspace - must be implemented by subclass.

        Raises:
        - NotImplementedError if subclass does not implement workspace deletion.
        """
        raise NotImplementedError

    def force_kill(self):
        """Force-stop the workflow - must be implemented by subclass.

        Raises:
        - NotImplementedError if subclass does not implement force-killing.
        """
        raise NotImplementedError
