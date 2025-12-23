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
    def __init__(self, project_uuid, jobs, uuid=None, machine_id=None):
        self.project_uuid = project_uuid
        self.uuid = uuid or csys.generate_uuid()
        self.path = os.path.join(os.environ["HOME"], ".Yuki", "Workflows", self.uuid)
        os.makedirs(self.path, exist_ok=True)

        self.config_file = metadata.ConfigFile(os.path.join(self.path, "config.json"))
        self.jobs = []
        self.dependencies = {}
        self.steps = []
        self.snakefile_path = os.path.join(self.path, "Snakefile")

        if uuid:
            self.start_job = None
            self.machine_id = self.config_file.read_variable("machine_id", machine_id or "")
        else:
            self.start_job = jobs.copy() if isinstance(jobs, list) else [jobs]
            self.machine_id = self.start_job[0].machine_id if self.start_job else (machine_id or "")
            self.config_file.write_variable("machine_id", self.machine_id)

    @staticmethod
    def create(project_uuid, jobs, uuid=None, mode=None):
        """Factory method to instantiate the correct workflow type."""
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
        return f"w-{self.uuid[:8]}"

    def run(self):
        """Common execution flow."""
        print("Constructing the workflow")
        print(f"Start job: {self.start_job}")
        if isinstance(self.start_job, list):
            self.construct_workflow_jobs(self.start_job)
        else:
            self.construct_workflow_jobs([self.start_job] if self.start_job else [])

        print(f"Jobs after the construction: {self.jobs}")

        # Set all the jobs to be the waiting status
        for job in self.jobs:
            print(f"job: {job}, is input: {job.is_input}, job status: {job.status()}, job type: {job.job_type()}")

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
        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            print(f"Set workflow id to job {job}")
            job.set_workflow_id(self.uuid)
            job.set_status("running")

        # Prepare and Execute
        print("Constructing")
        try:
            print("Constructing the snakefile")
            self.construct_snake_file()
        except:
            print("Failed to construct the snakefile")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

        try:
            print("Executing backend")
            self._execute_backend()
        except:
            print("Failed to execute backend")
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
        """Wait for dependencies to be satisfied."""
        # First, check whether the dependencies are satisfied
        for iTries in range(60):
            print("Checking finished")
            all_finished = True
            workflow_list = []
            for job in self.jobs:
                # print(f"Check the job {job}", job)
                if not job.is_input:
                    continue
                if job.status() == "archived":
                    continue
                # print(f"Check the status of workflow {job.workflow_id()}")
                if job.status() == "finished":
                    continue
                if job.job_type() == "algorithm":
                    continue
                workflow = VWorkflow.create(self.project_uuid, [], job.workflow_id())
                if workflow and workflow not in workflow_list:
                    workflow_list.append(workflow)
                # FIXME: may check if some of the dependence fail

            for workflow in workflow_list:
                workflow.update_workflow_status()

            for job in self.jobs:
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
                        os.path.join(os.environ["HOME"], ".Yuki", "Workflows", job.workflow_id())
                        )
                if job.status() != "finished":
                    all_finished = False
                    break

            if all_finished:
                break
            time.sleep(10)
        print("All done")

        if not all_finished:
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("raw")
            print("Some dependencies are not finished yet.")
            return False

        return True

    def construct_snake_file(self):
        """Construct snakemake file for workflow execution."""
        self.snakefile_path = os.path.join(self.path, "Snakefile")
        snake_file = snakefile.SnakeFile(os.path.join(self.path, "Snakefile"))

        self.dependencies = {}
        self.steps = []

        snake_file.addline("rule all:", 0)
        snake_file.addline("input:", 1)
        self.dependencies["all"] = []
        for job in self.jobs:
            snake_file.addline(f'"{job.short_uuid()}.done",', 2)
            self.dependencies["all"].append(f"step{job.short_uuid()}")

        for job in self.jobs:
            if job.object_type() == "algorithm":
                # In this case, if the command is compile, we need to compile it
                image = VImage(job.path, job.machine_id)
                image.is_input = job.is_input
                snakemake_rule = image.snakemake_rule()
                step = image.step()

                # In this case, we also need to run the "touch"
            if job.object_type() == "task":
                container = VContainer(job.path, job.machine_id)
                container.is_input = job.is_input
                snakemake_rule = container.snakemake_rule()
                step = container.step()

            snake_file.addline("\n", 0)
            snake_file.addline(f"rule step{job.short_uuid()}:", 0)
            snake_file.addline("input:", 1)
            for input_file in snakemake_rule["inputs"]:
                snake_file.addline(f'"{input_file}",', 2)
            snake_file.addline("output:", 1)
            snake_file.addline(f'"{job.short_uuid()}.done"', 2)
            snake_file.addline("container:", 1)
            snake_file.addline(f'"docker://{snakemake_rule["environment"]}"', 2)
            snake_file.addline("resources:", 1)
            compute_backend = snakemake_rule["compute_backend"]
            if compute_backend == "htcondorcern":
                snake_file.addline(f'compute_backend="{snakemake_rule["compute_backend"]}",', 2)
                snake_file.addline(f'htcondor_max_runtime="espresso",', 2)
                snake_file.addline(f'kerberos=True,', 2)
            else:
                snake_file.addline(f'kubernetes_memory_limit="{snakemake_rule["memory"]}"', 2)
            snake_file.addline("shell:", 1)
            snake_file.addline(f'"{" && ".join(snakemake_rule["commands"])}"', 2)

            self.steps.append(step)

        snake_file.write()

    def construct_workflow_jobs(self, root_jobs):
        """
        Construct workflow jobs iteratively including dependencies (DAG-safe, no recursion).
        root_jobs: list of VJob
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
            print("Failed to get the status")
        return "unknown"

    def set_workflow_status(self, status):
        path = os.path.join(self.path, "results.json")
        results_file = metadata.ConfigFile(path)
        results = results_file.read_variable("results", {})
        results["status"] = status
        results_file.write_variable("results", results)

    def watermark(self, impression=None):
        """Add watermark to PNG images."""
        print("Called", impression)
        if impression:
            path = os.path.join(os.environ["HOME"], ".Yuki", "Storage", self.project_uuid, impression, self.machine_id)
            if not os.path.exists(os.path.join(path, "stageout.downloaded")):
                return False
            outputs_path = os.path.join(path, "stageout")
            print(outputs_path)
            watermark_path = os.path.join(path, "watermarks")
            os.makedirs(watermark_path, exist_ok=True)
            filelist = os.listdir(outputs_path)
            print(filelist)

            # Water mark the png files
            for filename in filelist:
                if not filename.endswith(".png"):
                    continue

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
                print(text)

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

    @abstractmethod
    def update_workflow_status(self):
        """Update workflow status - must be implemented by subclass."""
        pass

    def kill(self):
        """Kill the workflow execution - must be implemented by subclass."""
        raise NotImplementedError("Subclass must implement kill method")


