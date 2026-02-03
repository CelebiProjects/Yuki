"""
Virtual Job module for Yuki kernel.

This module contains the VJob abstract base class which represents a virtual job object
that can include VVolume, ImageJob, ContainerJob and other related entities.
"""
import os
import time
from abc import ABC, abstractmethod

from CelebiChrono.utils import metadata

class VJob(ABC):
    """Abstract base class for virtual job objects, including VVolume, ImageJob, ContainerJob."""

    def __init__(self, path, machine_id):
        """Initialize the project with the only **information** of an object instance."""
        self._use_eos = None
        self._use_kerberos = None
        self._environment = None
        self._workflow = None

        self.is_input = False
        self.path = path
        self.uuid = path[-32:]
        self.project_uuid = path[-64-1:-32-1]
        self.machine_id = machine_id
        self.config_file = metadata.ConfigFile(
            os.path.join(self.path, "config.json")
            )
        self.yaml_file = metadata.YamlFile(
            os.path.join(self.path, "contents", "celebi.yaml")
        )
        if self.environment() == "rawdata":
            self.is_input = True
        if machine_id is None:
            status_file = metadata.ConfigFile(
                os.path.join(self.path, "status.json")
                )
            self.machine_id = status_file.read_variable("machine_id", None)
        if self.machine_id is not None:
            self.run_path = os.path.join(self.path, self.machine_id, "run")
            self.run_config_file = metadata.ConfigFile(
                os.path.join(self.path, self.machine_id, "config.json")
                )

    def __new__(cls, path, machine_id):
        """Factory method to automatically create suitable ImageJob or ContainerJob."""
        # If VJob is being directly instantiated, determine the correct subclass
        if cls is VJob:
            # Create a temporary instance to read the object type
            temp_instance = object.__new__(cls)
            temp_instance.__init__(path, machine_id)
            job_type = temp_instance.job_type()

            # Import here to avoid circular imports
            from ..kernel.image_job import ImageJob
            from ..kernel.container_job import ContainerJob

            # Create the appropriate subclass instance
            if job_type == "algorithm":
                return object.__new__(ImageJob)
            elif job_type == "task":
                return object.__new__(ContainerJob)
            else:
                # Default to ContainerJob for unknown types
                return object.__new__(ContainerJob)
        else:
            # Normal instantiation for subclasses
            return object.__new__(cls)

    def __str__(self):
        """Define the behavior of print(vobject)."""
        return self.path

    def __repr__(self):
        """Define the behavior of print(vobject)."""
        return self.path

    def relative_path(self, path):
        """Return a path relative to the path of this object."""
        return os.path.relpath(path, self.path)

    def job_type(self):
        """Return the type of the object under a specific path."""
        # print("job_type", self.config_file.read_variable("object_type", ""))
        return self.config_file.read_variable("object_type", "")

    def object_type(self):
        """Return the type of the object under a specific path."""
        return self.config_file.read_variable("object_type", "")

    def is_zombie(self):
        """Check if this job is a zombie (empty job type)."""
        return self.job_type() == ""

    def set_runid(self, runid):
        """Set the run ID for this job."""
        self.run_config_file.write_variable("runid", runid)

    def runid(self):
        """Get the run ID for this job."""
        return self.run_config_file.read_variable("runid", "")

    def set_workflow_id(self, workflow_uuid):
        """Set the workflow ID for this job."""
        self.run_config_file.write_variable("workflow", workflow_uuid)

    def workflow_id(self):
        """Get the workflow ID for this job."""
        # print("The machine id is:", self.machine_id)
        if self._workflow is not None:
            return self._workflow
        self._workflow = self.run_config_file.read_variable("workflow", "")
        return self._workflow

    def environment(self):
        """Get the environment type from the YAML configuration."""
        if self._environment is not None:
            return self._environment
        yaml_file = metadata.YamlFile(
            os.path.join(self.path, "contents", "celebi.yaml")
            )
        self._environment = yaml_file.read_variable("environment", "")
        return self._environment

    def status(self):
        """Get the current status of the job."""
        config_file = metadata.ConfigFile(os.path.join(self.path, "status.json"))
        status = config_file.read_variable("status", "raw")
        if status != "raw":
            return status
        return "raw"

    def set_use_eos(self, use: bool):
        """Set whether the job should use EOS storage."""
        self.run_config_file.write_variable("use_eos", use)

    def use_eos(self):
        """Check if the job is set to use EOS storage."""
        if self._use_eos is not None:
            return self._use_eos
        self._use_eos = self.run_config_file.read_variable("use_eos", False)
        return self._use_eos

    def use_kerberos(self):
        if self._use_kerberos is not None:
            return self._use_kerberos
        config = metadata.ConfigFile(os.path.join(os.environ["HOME"], ".Yuki", "config.json"))
        self._use_kerberos = config.read_variable("use_kerberos", {}).get(self.machine_id, False)
        return self._use_kerberos

    def set_status(self, status):
        """Set the status of the job."""
        config_file = metadata.ConfigFile(os.path.join(self.path, "status.json"))
        config_file.write_variable("status", status)

    def update_data_status(self, status):
        """Update the data status of the job."""
        config_file = metadata.ConfigFile(os.path.join(self.path, "status.json"))
        config_file.write_variable("status", status)

    def update_status(self, status):
        """Update the status based on workflow status."""
        config_file = metadata.ConfigFile(os.path.join(self.path, "status.json"))
        if status == "PENDING":
            config_file.write_variable("status", "running")
        if status == "SUCCESS":
            config_file.write_variable("status", "success")

    def _read_workflow_results(self, workflow_path, logger=None):
        """Read workflow results and return status."""
        try:
            results_file = metadata.ConfigFile(os.path.join(workflow_path, "results.json"))
            results = results_file.read_variable("results", {})
            return results.get("status", "unknown")
        except Exception:
            if logger:
                logger("Update status from workflow failed.")
            else:
                print("Update status from workflow failed.")
            return None

    def _find_matched_step(self, workflow_path, logger=None):
        """Find the matched step in workflow log for this job."""
        try:
            log_file = metadata.ConfigFile(os.path.join(workflow_path, "log.json"))
            log = log_file.read_variable("logs", {})
            for step in log.values():
                if step.get("job_name", "") == f"step{self.short_uuid()}":
                    return step
        except Exception:
            if logger:
                logger(f"No log file found at {workflow_path}")
            else:
                print("No log file found.")
        return None

    def _write_step_logs(self, matched_step):
        """Write step logs to the job's log directory."""
        if matched_step and matched_step.get("status") in ("finished", "failed"):
            logs = matched_step.get("logs", "")
            log_dir = os.path.join(self.path, self.machine_id, "logs")
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            with open(os.path.join(log_dir, "celebi.stdout"), "w", encoding='utf-8') as f:
                f.write(logs)
            config_file = metadata.ConfigFile(os.path.join(self.path, self.machine_id, "status.json"))
            # print(matched_step)
            start_time = matched_step.get("started_at", "")
            end_time = matched_step.get("finished_at", "")
            config_file.write_variable("status", matched_step.get("status", ""))
            config_file.write_variable("started_at", start_time)
            config_file.write_variable("finished_at", end_time)
            # Format of times:
            # 2026-01-21T16:40:34
            # 2026-01-21T16:40:41
            time_format = "%Y-%m-%dT%H:%M:%S"
            # Get the end_time - start_time in seconds
            start_struct = time.strptime(start_time, time_format)
            end_struct = time.strptime(end_time, time_format)
            start_epoch = time.mktime(start_struct)
            end_epoch = time.mktime(end_struct)
            duration = int(end_epoch - start_epoch)
            config_file.write_variable("duration", duration)

    def _update_job_status(self, config_file, current_status, step_status, full_workflow_status, logger=None):
        """Update job status based on current status, step status, and workflow status."""
        if logger:
            logger(f"Job {self.short_uuid()} new status: {step_status}")
        else:
            print("New status:", step_status)

        if current_status == "raw":
            if len(step_status) < 20:
                config_file.write_variable("status", step_status)
        elif current_status == "running":
            if step_status == "success":
                config_file.write_variable("status", "success")
            elif step_status == "finished":
                config_file.write_variable("status", "finished")
            elif step_status in ("failed", "stopped"):
                config_file.write_variable("status", "failed")
            elif full_workflow_status == "failed":
                config_file.write_variable("status", "failed")
        elif current_status in ('stopped', 'deleted'):
            config_file.write_variable("status", "failed")
        elif current_status in ('finished', 'success', 'failed'):
            pass
        else:
            if len(step_status) < 20:
                config_file.write_variable("status", step_status)
            else:
                config_file.write_variable("status", "unknown")

    def update_status_from_workflow(self, workflow_path, logger=None):
        """Update job status based on workflow status."""
        if self.job_type() == "algorithm":
            return

        config_file = metadata.ConfigFile(os.path.join(self.path, "status.json"))
        current_status = config_file.read_variable("status", "raw")
        config_file.write_variable("machine_id", self.machine_id)
        if logger:
            logger(f"Job {self.short_uuid()} current status: {current_status}")
        else:
            print("Current status is: ", current_status)

        if current_status in ('finished', 'success', 'failed'):
            return

        # Read workflow results
        full_workflow_status = self._read_workflow_results(workflow_path, logger)
        if full_workflow_status is None:
            return

        if logger:
            logger(f"Full workflow status: {full_workflow_status}")
        else:
            print("Full workflow status:", full_workflow_status)

        # Find matched step
        matched_step = self._find_matched_step(workflow_path, logger)

        # Determine status from matched step or use full workflow status
        status = full_workflow_status
        if matched_step:
            status = matched_step.get("status", "unknown")
            if logger:
                logger(f"Status from matched step: {status}")
            else:
                print("status from matched step:", status)
            self._write_step_logs(matched_step)

        # Update job status
        self._update_job_status(config_file, current_status, status, full_workflow_status, logger)

    def error(self):
        """Get error message if any."""
        error_path = self.path + "/error"
        if os.path.exists(error_path):
            with open(error_path, encoding='utf-8') as f:
                return f.read()
        return ""

    def append_error(self, message):
        """Append an error message to the error file."""
        with open(self.path + "/error", "w", encoding='utf-8') as f:
            f.write(message)
            f.write("\n")

    def dependencies(self):
        """Return the predecessor of the object."""
        return self.config_file.read_variable("dependencies", [])

    def files(self):
        """Get list of files in this job."""
        file_list = []
        tree = self.config_file.read_variable("tree", [])
        for dirpath, _, filenames in tree:
            for f in filenames:
                if f == "celebi.yaml":
                    continue
                name = f"{self.short_uuid()}"
                if dirpath == ".":
                    name = os.path.join(name, f)
                else:
                    name = os.path.join(name, dirpath, f)
                file_list.append(name)
        return file_list

    def predecessors(self):
        """Get predecessor VJob objects."""
        dep = self.dependencies()
        # path = os.path.join(os.environ["HOME"], ".Yuki", "Storage", self.project_uuid)
        path = self.path[:-32]
        return [VJob(os.path.join(path, x), self.machine_id) for x in dep]

    def impression(self):
        """Get the impression UUID."""
        impression = self.path[-32:]
        return impression

    def short_uuid(self):
        """Get the short UUID (first 7 characters)."""
        return self.impression()[:7]
