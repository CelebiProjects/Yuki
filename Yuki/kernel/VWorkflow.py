"""Workflow abstraction and helpers for constructing, executing and monitoring
workflows composed of VJob objects.

This module defines the abstract VWorkflow class which:
- Builds a DAG of VJob instances (non-recursive, stack-based traversal).
- Produces a Snakemake Snakefile for execution.
- Waits for input/dependency workflows to finish.
- Provides hooks for backend execution and status synchronization implemented by subclasses.
"""

import os
import time
import json
from abc import ABC, abstractmethod
from PIL import Image, ImageDraw, ImageFont

from CelebiChrono.utils import csys, metadata
from CelebiChrono.kernel.chern_cache import ChernCache
from Yuki.kernel.VJob import VJob
from Yuki.kernel.VContainer import VContainer
from Yuki.kernel.VImage import VImage
from Yuki.utils import snakefile

CHERN_CACHE = ChernCache.instance()

class VWorkflow(ABC):
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
        self.path = os.path.join(os.environ["HOME"], ".Yuki", "Workflows", self.project_uuid, self.uuid)
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
        else:
            self.start_job = jobs.copy() if isinstance(jobs, list) else [jobs]
            self.machine_id = self.start_job[0].machine_id if self.start_job else (machine_id or "")
            self.config_file.write_variable("machine_id", self.machine_id)

    def logger(self, message):
        """Log message with timestamp to both console and the workflow log file."""
        timestamp = time.strftime("[%Y-%m-%d %H:%M:%S]", time.localtime())
        log_message = f"{timestamp} {message}"
        print(log_message)
        with open(self.log_path, "a") as f:
            f.write(log_message + "\n")

    @staticmethod
    def create(project_uuid, jobs, uuid=None, mode=None):
        """Factory method to instantiate the appropriate workflow subclass.

        Behavior:
        - If mode is provided, use it.
        - Otherwise determine mode from stored configuration of the workflow runner.
        """
        if not mode:
            workflow_path = os.path.join(os.environ["HOME"], ".Yuki", "Workflows", uuid)
            runner_id = metadata.ConfigFile(os.path.join(workflow_path, "config.json")).read_variable("machine_id", "")
            config = metadata.ConfigFile(os.path.join(os.environ["HOME"], ".Yuki", "config.json"))
            backend_types = config.read_variable("backend_types", {})
            mode = backend_types.get(runner_id, "reana")
        if mode == "dry":
            from .dry_workflow import DryWorkflow
            return DryWorkflow(project_uuid, jobs, uuid)
        else:
            from .reana_workflow import ReanaWorkflow
            return ReanaWorkflow(project_uuid, jobs, uuid)

    def get_name(self):
        return f"w-{self.project_uuid[:8]}-{self.uuid[:8]}"

    def run(self):
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
            self.logger(f"[{i+1}/{total_jobs}] job: {job}, is input: {job.is_input}, job status: {job.status()}, job type: {job.job_type()}")

        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            job.set_status("waiting")

        # Wait for dependencies
        if not self._wait_for_dependencies():
            return

        # Set workflow IDs for jobs
        active_jobs = [j for j in self.jobs if not j.is_input and j.job_type() != "algorithm"]
        total_active = len(active_jobs)
        for i, job in enumerate(active_jobs):
            self.logger(f"[{i+1}/{total_active}] Set workflow id to job {job}")
            job.set_workflow_id(self.uuid)
            job.set_status("running")

        # Prepare and Execute
        self.logger("Constructing")
        try:
            self.logger("Constructing the snakefile")
            self.construct_snake_file()
        except:
            self.logger("Failed to construct the snakefile")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

        try:
            self.logger("Executing backend")
            self._execute_backend()
        except:
            self.logger("Failed to execute backend")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

    @abstractmethod
    def _execute_backend(self):
        pass

    @abstractmethod
    def _sync_external_job_status(self, job):
        pass

    def _wait_for_dependencies(self):
        """Waits for all input-dependency workflows to reach a terminal 'finished' state.

        Notes:
        - Polls the statuses of workflows referred to by input jobs.
        - Uses a bounded number of retries to avoid infinite wait.
        - If dependencies do not finish within the retry window, resets job states and returns False.
        """
        # First, check whether the dependencies are satisfied
        for iTries in range(60):
            self.logger(f"Checking finished (Attempt {iTries+1}/60)")
            all_finished = True
            workflow_list = []
            input_jobs = [j for j in self.jobs if j.is_input and j.status() not in ("archived", "finished") and j.job_type() != "algorithm"]
            total_inputs = len(input_jobs)
            
            for i, job in enumerate(input_jobs):
                workflow = VWorkflow.create(self.project_uuid, [], job.workflow_id())
                self.logger(f"[{i+1}/{total_inputs}] Checking dependency: Job {job.uuid} workflow {workflow.uuid}")
                if workflow and workflow not in workflow_list:
                    workflow_list.append(workflow)
                # FIXME: may check if some of the dependence fail

            for workflow in workflow_list:
                workflow.update_workflow_status()

            for i, job in enumerate(self.jobs):
                if not job.is_input:
                    continue
                if job.status() == "archived":
                    continue
                if job.status() == "finished":
                    continue
                if job.job_type() == "algorithm":
                    continue
                workflow = VWorkflow.create(self.project_uuid, [], job.workflow_id())
                if workflow in workflow_list:
                    job.update_status_from_workflow(
                        os.path.join(
                            os.environ["HOME"],
                            ".Yuki",
                            "Workflows",
                            self.project_uuid,
                            job.workflow_id()
                            ),
                        self.logger
                        )
                
                job_status = job.status()
                # self.logger(f"Job {job.short_uuid()} status: {job_status}")
                if job_status != "finished":
                    all_finished = False
                    # We continue checking other jobs to update their status as well

            if all_finished:
                break
            time.sleep(10)
        self.logger("All done")

        if not all_finished:
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("raw")
            self.logger("Some dependencies are not finished yet.")
            return False

        return True

    def construct_snake_file(self):
        """Construct the Snakemake Snakefile describing rules for all jobs.

        Each job becomes a rule 'step<short_uuid>' that:
        - Declares inputs and a single '.done' output marker.
        - References a container image and resources.
        - Provides a shell command combining the job's commands.
        """
        config = metadata.ConfigFile(os.path.join(os.environ["HOME"], ".Yuki", "config.json"))
        use_kerberos = config.read_variable("use_kerberos", {}).get(self.machine_id, False)
        for job in self.jobs:
            self.logger(f"Job in the workflow: {job}, is input: {job.is_input}, job type: {job.job_type()}")

        self.snakefile_path = os.path.join(self.path, "Snakefile")
        snake_file = snakefile.SnakeFile(os.path.join(self.path, "Snakefile"))

        self.dependencies = {}
        self.steps = []

        snake_file.addline("rule all:", 0)
        snake_file.addline("input:", 1)
        self.dependencies["all"] = []
        snake_file.addline(f'"finalize.done",', 2)
        self.dependencies["all"].append("finalize")

        setup_commands = []
        for job in self.jobs:
            if job.object_type() == "task" and job.is_input:
                if not job.use_eos() or job.machine_id != self.machine_id:
                    continue
                container = VContainer(job.path, job.machine_id)
                setup_commands.extend(container.setup_commands())

        finalize_commands = []
        for job in self.jobs:
            if job.object_type() == "task" and job.is_input:
                if not job.use_eos() or job.machine_id != self.machine_id:
                    continue
                container = VContainer(job.path, job.machine_id)
                finalize_commands.extend(container.finalize_commands())

        snake_file.addline("\n", 0)
        snake_file.addline("rule setup:", 0)
        snake_file.addline("input:", 1)
        snake_file.addline("output:", 1)
        snake_file.addline(f'"setup.done",', 2)
        snake_file.addline("container:", 1)
        snake_file.addline(f'"docker://docker.io/reanahub/reana-env-root6:6.18.04"', 2)
        snake_file.addline("resources:", 1)
        if setup_commands and use_kerberos:
            snake_file.addline(f'kerberos=True,', 2)
        snake_file.addline(f'kubernetes_memory_limit="1Gi"', 2)
        snake_file.addline("shell:", 1)
        if setup_commands:
            snake_file.addline(f'"{" && ".join(setup_commands)} && touch setup.done"', 2)
        else:
            snake_file.addline(f'"touch setup.done"', 2)
        self.dependencies["setup"] = []

        snake_file.addline("\n", 0)
        snake_file.addline("rule finalize:", 0)
        snake_file.addline("input:", 1)
        self.dependencies["finalize"] = []
        for job in self.jobs:
            snake_file.addline(f'"{job.short_uuid()}.done",', 2)
            self.dependencies["finalize"].append(f"step{job.short_uuid()}")

        snake_file.addline("output:", 1)
        snake_file.addline(f'"finalize.done"', 2)
        snake_file.addline("container:", 1)
        snake_file.addline(f'"docker://docker.io/reanahub/reana-env-root6:6.18.04"', 2)
        snake_file.addline("resources:", 1)
        snake_file.addline(f'kubernetes_memory_limit="1Gi"', 2)
        snake_file.addline("shell:", 1)
        if finalize_commands:
            snake_file.addline(f'"{" && ".join(finalize_commands)} && touch finalize.done"', 2)
        else:
            snake_file.addline(f'"touch finalize.done"', 2)

        total_jobs = len(self.jobs)
        for i, job in enumerate(self.jobs):
            start_time = time.time()
            self.logger(f"[{i+1}/{total_jobs}] Processing job: {job}")
            if job.object_type() == "algorithm":
                # In this case, if the command is compile, we need to compile it
                image = VImage(job.path, job.machine_id)
                image.is_input = job.is_input
                snakemake_rule = image.snakemake_rule(self.machine_id)
                step = image.step(self.machine_id)

                # In this case, we also need to run the "touch"
            if job.object_type() == "task":
                container = VContainer(job.path, job.machine_id)
                container.is_input = job.is_input
                snakemake_rule = container.snakemake_rule(self.machine_id)
                step = container.step(self.machine_id)
            self.logger(f"[{i+1}/{total_jobs}] Get the step at time {time.time() - start_time:.4f}s")

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
                        os.environ["HOME"],
                        ".Yuki",
                        "Storage",
                        self.project_uuid,
                        dep
                    ),
                    self.machine_id
                )
                self.dependencies[f"step{job.short_uuid()}"].append(f"step{dep_job.short_uuid()}")
            self.logger(f"[{i+1}/{total_jobs}] Added inputs and dependencies at time {time.time() - start_time:.4f}s")

            snake_file.addline("output:", 1)
            snake_file.addline(f'"{job.short_uuid()}.done"', 2)
            snake_file.addline("container:", 1)
            snake_file.addline(f'"docker://{snakemake_rule["environment"]}"', 2)
            snake_file.addline("resources:", 1)
            compute_backend = snakemake_rule["compute_backend"]
            if job.use_eos() and use_kerberos:
                snake_file.addline(f'kerberos=True,', 2)
            if compute_backend == "htcondorcern":
                snake_file.addline(f'compute_backend="{snakemake_rule["compute_backend"]}",', 2)
                snake_file.addline(f'htcondor_max_runtime="espresso",', 2)
                snake_file.addline(f'kerberos=True,', 2)
            else:
                snake_file.addline(f'kubernetes_memory_limit="{snakemake_rule["memory"]}"', 2)
            snake_file.addline("shell:", 1)
            snake_file.addline(f'"{" && ".join(snakemake_rule["commands"])}"', 2)
            self.logger(f"[{i+1}/{total_jobs}] Added shell and resources at time {time.time() - start_time:.4f}s")

            self.steps.append(step)
            self.logger(f"[{i+1}/{total_jobs}] Appended step at time {time.time() - start_time:.4f}s")

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

            status = job.status()
            obj_type = job.object_type()

            # For terminal jobs, add immediately
            if status in ("finished", "failed", "pending", "running", "archived"):
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
                dep_path = os.path.join(os.environ["HOME"], ".Yuki", "Storage", self.project_uuid, dep)
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
        except:
            self.logger("Failed to get the status")
        return "unknown"

    def set_workflow_status(self, status):
        path = os.path.join(self.path, "results.json")
        results_file = metadata.ConfigFile(path)
        results = results_file.read_variable("results", {})
        results["status"] = status
        results_file.write_variable("results", results)

    def watermark(self, impression=None):
        """Add watermark to PNG images for a given impression.

        Procedure:
        1. Verify stageout has been downloaded.
        2. Iterate PNG outputs and draw a small textual watermark (top-right).
        3. Save watermarked copies under a 'watermarks' directory.

        The function operates in-place on copies and will silently skip non-PNG files.
        """
        self.logger(f"Watermarking impression: {impression}")
        if impression:
            path = os.path.join(os.environ["HOME"], ".Yuki", "Storage", self.project_uuid, impression, self.machine_id)
            if not os.path.exists(os.path.join(path, "stageout.downloaded")):
                return False
            outputs_path = os.path.join(path, "stageout")
            self.logger(f"Outputs path: {outputs_path}")
            watermark_path = os.path.join(path, "watermarks")
            os.makedirs(watermark_path, exist_ok=True)
            filelist = [f for f in os.listdir(outputs_path) if f.endswith(".png")]
            total_files = len(filelist)
            self.logger(f"Files to watermark: {filelist}")

            # Water mark the png files
            for i, filename in enumerate(filelist):
                # 1. Open the image and convert it to RGBA.
                # The watermark will be drawn directly onto this image object.
                image = Image.open(os.path.join(outputs_path, filename)).convert("RGBA")

                # 2. Create the drawing context directly on the image
                draw = ImageDraw.Draw(image)

                # --- Watermark Setup ---

                # Use the same logic for sizing the font (using the original code's approach)
                font_size = int(min(image.size) / 20)
                try:
                    font = ImageFont.truetype("arial.ttf", font_size)
                except:
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
                image.save(os.path.join(watermark_path, f"imp{impression[:8]}_{filename}"), format="PNG")
                self.logger(f"[{i+1}/{total_files}] Saved watermarked image: {filename}")

    @abstractmethod
    def update_workflow_status(self):
        """Update workflow status - must be implemented by subclass."""
        pass

    def kill(self):
        """Kill the workflow execution - must be implemented by subclass.

        Raises:
        - NotImplementedError if subclass does not implement termination behavior.
        """
        # ...existing code...


