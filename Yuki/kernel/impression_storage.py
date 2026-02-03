import os
from CelebiChrono.utils.metadata import ConfigFile
from .vjob import VJob
from .vworkflow import VWorkflow
from ..server.config import config

class ImpressionStorage:
    def __init__(self, project_uuid, impression):
        self.project_uuid = project_uuid
        self.impression = impression
        self.job_path = config.get_job_path(project_uuid, impression)

        # Load registry of runners
        config_file = config.get_config_file()
        self.runners = config_file.read_variable("runners", [])
        self.runners_id = config_file.read_variable("runners_id", {})

        # Metadata access
        self.job_config = ConfigFile(config.get_job_config_path(project_uuid, impression))

    def _get_runner_contexts(self):
        """Generator to yield active job/workflow pairs across all machines."""
        for machine in self.runners:
            machine_id = self.runners_id.get(machine)
            job = VJob(self.job_path, machine_id)

            if job.workflow_id():
                # Using the factory method from our previous refactor
                workflow = VWorkflow.create(self.project_uuid, [], job.workflow_id())
                yield machine, job, workflow

    def kill(self):
        """Kills all workflows associated with this storage entry."""
        for _, _, workflow in self._get_runner_contexts():
            workflow.kill()
        # Mark local record as failed
        VJob(self.job_path, None).set_status("failed")

    def collect(self):
        """Retrieves files or logs from runners."""
        for name, job, workflow in self._get_runner_contexts():
            if job.status() == "finished":
                print(f"[{name}] Collecting results...")
                workflow.download(self.impression)
                # workflow.download_outputs(self.impression)
            elif job.status() == "failed":
                print(f"[{name}] Collecting logs...")
                workflow.download_logs(self.impression)

    def collect_outputs(self):
        """Retrieves only output files from runners."""
        for name, job, workflow in self._get_runner_contexts():
            if job.status() == "finished":
                print(f"[{name}] Collecting outputs...")
                workflow.download_outputs(self.impression)

    def collect_logs(self):
        """Retrieves only logs from runners."""
        for name, job, workflow in self._get_runner_contexts():
            if job.status() == "finished" or job.status() == "failed":
                print(f"[{name}] Collecting logs...")
                workflow.download_logs(self.impression)

    def watermark(self):
        """Applies watermarks to the stored results."""
        for name, job, workflow in self._get_runner_contexts():
            if job.status() == "finished":
                print(f"[{name}] Applying watermarks...")
                workflow.watermark(self.impression)

    def get_info(self):
        """Returns the location and ID of the first active runner."""
        for name, _, workflow in self._get_runner_contexts():
            return f"{name} {workflow.uuid}"
        return "UNDEFINED"
