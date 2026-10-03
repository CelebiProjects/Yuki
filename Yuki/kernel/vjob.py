"""
Virtual Job module for Yuki kernel.

This module contains the VJob abstract base class which represents a virtual job object
that can include VVolume, ImageJob, ContainerJob and other related entities.
"""
# pylint: disable=cyclic-import
import os
import time
from abc import ABC

from CelebiChrono.utils import metadata
from .locked_metadata import read_variable as read_locked_variable

from .status_constants import (
    translate_to_musical, translate_to_legacy, is_valid_status,
    get_detailed_status_message, is_terminal_status,
    SILENCE, PRELUDE, IN_MOVEMENT, COMPOSING, ORCHESTRATING,
    TUNING, DISSONANCE, CODA, FINAL_NOTE,
    FAILED, STOPPED, DELETED, PENDING, LEGACY_SUCCESS
)

class VJob(ABC):  # pylint: disable=too-many-instance-attributes,too-many-public-methods
    """Abstract base class for virtual job objects, including VVolume, ImageJob, ContainerJob."""

    def __init__(self, path, machine_id):
        """Initialize the project with the only **information** of an object instance."""
        self._cache_on_runner = None
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
            if job_type == "task":
                return object.__new__(ContainerJob)
            # Default to ContainerJob for unknown types
            return object.__new__(ContainerJob)
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

    def status(self, musical=False):
        """Get the current status of the job.

        Args:
            legacy: If True, return legacy status name. If False, return musical status name.

        Returns:
            Status name (musical by default, legacy if specified)
        """
        status = read_locked_variable(os.path.join(self.path, "status.json"),
                                      "status", SILENCE)

        if not musical:
            return status
        return translate_to_musical(status)

    def set_cache_on_runner(self, use: bool):
        """Set whether the job's outputs should be cached on the runner."""
        self.run_config_file.write_variable("cache_on_runner", use)

    def cache_on_runner(self):
        """Check if the job is set to cache outputs on the runner."""
        if self._cache_on_runner is not None:
            return self._cache_on_runner
        self._cache_on_runner = self.run_config_file.read_variable(
            "cache_on_runner", False)
        return self._cache_on_runner

    def use_kerberos(self, backend_type=None):
        """Check if the job is set to use Kerberos authentication.

        Also enables Kerberos automatically if any predecessor task is an
        LHCb AP data list, since AP datasets are stored on EOS.

        When ``backend_type`` is "reana", a job whose outputs are cached on
        the runner (``cache_on_runner``) also needs Kerberos, because the
        reana cache lives on EOS. Other backends cache on the runner's own
        filesystem, which does not imply Kerberos.
        """
        if self._use_kerberos is not None:
            return self._use_kerberos
        config = metadata.ConfigFile(os.path.join(os.environ["HOME"], ".Yuki", "config.json"))
        self._use_kerberos = config.read_variable("use_kerberos", {}).get(self.machine_id, False)
        if not self._use_kerberos:
            try:
                for pred in self.predecessors():
                    if pred.environment() == "lhcb_ap_datalist":
                        self._use_kerberos = True
                        break
            except Exception:
                pass
        if not self._use_kerberos and backend_type == "reana":
            return self.cache_on_runner()
        return self._use_kerberos

    def set_status(self, status, detailed_message=None):
        """Set the status of the job.

        Args:
            status: Status name (can be either musical or legacy name)
            detailed_message: Optional detailed status message. If None, a default
                             message will be generated based on the status.
        """
        # Validate the status
        print("Set the job status to ", status)
        if not is_valid_status(status):
            raise ValueError(f"Invalid status: {status}")

        # Translate to musical name for storage
        musical_status = translate_to_musical(status)

        config_file = metadata.ConfigFile(os.path.join(self.path, "status.json"))
        config_file.write_variable("status", status)

        # Store legacy name for backward compatibility
        legacy_status = translate_to_legacy(musical_status)
        config_file.write_variable("status_legacy", legacy_status)

        # Set detailed status message
        if detailed_message is None:
            detailed_message = get_detailed_status_message(musical_status)
        config_file.write_variable("detailed_status", detailed_message)

    def detailed_status(self):
        """Get the detailed status message for the job.

        Returns:
            Detailed status message string, or empty string if not set.
        """
        return read_locked_variable(os.path.join(self.path, "status.json"),
                                    "detailed_status", "")

    def set_detailed_status(self, message):
        """Set the detailed status message for the job.

        Args:
            message: Detailed status message string
        """
        config_file = metadata.ConfigFile(os.path.join(self.path, "status.json"))
        config_file.write_variable("detailed_status", message)

    def update_data_status(self, status):
        """Update the data status of the job."""
        self.set_status(status)

    def update_status(self, status, detailed_message=None):
        """Update the status based on workflow status.

        Args:
            status: Workflow status (e.g., "PENDING", "SUCCESS")
            detailed_message: Optional detailed status message
        """
        if status == "PENDING":
            self.set_status(PENDING, detailed_message)
        elif status == "SUCCESS":
            self.set_status(LEGACY_SUCCESS, detailed_message)
        else:
            # For other statuses, pass through with translation
            self.set_status(status, detailed_message)

    def _read_workflow_results(self, workflow_path, logger=None):
        """Read workflow results and return status."""
        try:
            results = read_locked_variable(os.path.join(workflow_path, "results.json"),
                                           "results", {})
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
            log = read_locked_variable(os.path.join(workflow_path, "log.json"),
                                       "logs", {})
            for step in log.values():
                if step.get("job_name", "") == f"step{self.short_uuid()}":
                    return step
        except Exception:
            if logger:
                logger(f"No log file found at {workflow_path}")
            else:
                print("No log file found.")
        return None

    def _write_step_logs(self, matched_step):  # pylint: disable=too-many-locals
        """Write step logs to the job's log directory."""
        if matched_step and matched_step.get("status") in ("finished", "failed"):
            logs = matched_step.get("logs", "")
            log_dir = os.path.join(self.path, self.machine_id, "logs")
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            with open(os.path.join(log_dir, "celebi.stdout"), "w", encoding='utf-8') as f:
                f.write(logs)
            config_file = metadata.ConfigFile(
                os.path.join(self.path, self.machine_id, "status.json"))
            # print(matched_step)
            start_time = matched_step.get("started_at", "")
            end_time = matched_step.get("finished_at", "")

            # Translate status for machine-specific status file
            step_status = matched_step.get("status", "")
            musical_status = translate_to_musical(step_status)
            config_file.write_variable("status", musical_status)
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

    def _update_job_status(self, _config_file, current_status, step_status,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-branches
                           full_workflow_status, logger=None):
        """Update job status based on current status, step status, and workflow status."""
        if logger:
            logger(f"Job {self.short_uuid()} new status: {step_status}")
        else:
            print("New status:", step_status)

        # Translate current status to musical name for comparison
        current_musical = translate_to_musical(current_status)
        step_musical = translate_to_musical(step_status)
        workflow_musical = translate_to_musical(full_workflow_status)

        # Determine if this is a construction failure vs execution failure
        # Construction failures happen before execution starts (pre-submit statuses)
        is_construction_failure = (
            step_musical == FAILED and
            current_musical in (SILENCE, PRELUDE, COMPOSING, ORCHESTRATING, TUNING)
        )

        if current_musical == SILENCE:
            if len(step_status) < 20:
                self.set_status(step_status)
        elif current_musical == IN_MOVEMENT:
            if step_musical == CODA:
                self.set_status(step_status, "Workflow step completed successfully")
            elif step_musical == "finished":  # Legacy finished status
                self.set_status(step_status, "Workflow step finished")
            elif step_musical == FAILED:
                # Execution failure during IN_MOVEMENT
                self.set_status(FAILED, "Backend execution failed during runtime")
            elif step_musical == STOPPED:
                self.set_status(STOPPED, "Job execution was manually stopped")
            elif workflow_musical == FAILED:
                self.set_status(FAILED, "Workflow execution failed")
        elif current_musical in (STOPPED, DELETED):
            # Job was stopped or deleted, mark as failed for backward compatibility
            self.set_status(FAILED, f"Job was {current_musical.lower()}")
        elif current_musical in (CODA, FINAL_NOTE, FAILED, DISSONANCE):
            # Terminal states, no change
            pass
        else:
            # Other statuses (PRELUDE, COMPOSING, ORCHESTRATING, TUNING)
            if len(step_status) < 20:
                if step_musical == FAILED and is_construction_failure:
                    # Construction failure - use Dissonance
                    self.set_status(DISSONANCE, "Workflow construction failed")
                else:
                    self.set_status(step_status)
            else:
                self.set_status("unknown", "Status cannot be determined")

    def update_status_from_workflow(self, workflow_path, logger=None):
        """Update job status based on workflow status."""
        print(
            f"[update_status_from_workflow] job={self.short_uuid()} "
            f"path={self.path} workflow_path={workflow_path} "
            f"machine_id={self.machine_id}"
        )
        if self.job_type() == "algorithm":
            return

        config_file = metadata.ConfigFile(os.path.join(self.path, "status.json"))
        current_status = read_locked_variable(os.path.join(self.path, "status.json"),
                                              "status", SILENCE)
        if read_locked_variable(os.path.join(self.path, "status.json"),
                                "machine_id") != self.machine_id:
            config_file.write_variable("machine_id", self.machine_id)
        if logger:
            logger(f"Job {self.short_uuid()} current status: {current_status}")
        else:
            print("Current status is: ", current_status)

        # Normalize storage convention: a completed job should store the legacy
        # status name "finished" rather than the musical name "coda". This
        # migrates workflows that were marked complete before the convention
        # change so the client displays [coda][finished] instead of [coda][coda].
        current_musical = translate_to_musical(current_status)
        if current_musical == CODA and current_status != "finished":
            self.set_status("finished", self.detailed_status())
            current_status = "finished"

        # Check if current status is terminal (no further updates)
        if is_terminal_status(current_musical):
            print(
                f"[update_status_from_workflow] job={self.short_uuid()} "
                f"already terminal current_status={current_status} "
                f"current_musical={current_musical}"
            )
            return

        # Read workflow results
        full_workflow_status = self._read_workflow_results(workflow_path, logger)
        print(
            f"[update_status_from_workflow] job={self.short_uuid()} "
            f"full_workflow_status={full_workflow_status}"
        )
        if full_workflow_status is None:
            print(
                f"[update_status_from_workflow] job={self.short_uuid()} "
                f"missing workflow results"
            )
            return
        if not is_valid_status(full_workflow_status):
            print(
                f"[update_status_from_workflow] job={self.short_uuid()} "
                f"invalid workflow status={full_workflow_status}"
            )
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
        print(
            f"[update_status_from_workflow] job={self.short_uuid()} "
            f"current_status={current_status} matched_status={status} "
            f"workflow_status={full_workflow_status}"
        )
        self._update_job_status(config_file, current_status, status, full_workflow_status, logger)

    def log(self, index, offset=0):
        """Get log content for this job.

        Args:
            index: Step index of the log file to read.
            offset: Number of bytes to skip from the start of the file.

        Returns:
            Log content from the given offset, or "" if the file is missing
            or the offset is at/past the end of the file.
        """
        log_path = os.path.join(self.path, self.machine_id, "logs", f"celebi_user_step{index}.log")
        if not os.path.exists(log_path):
            return ""
        with open(log_path, encoding='utf-8') as f:
            f.seek(offset)
            return f.read()

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
