"""
Dry/Local workflow implementation.

This module provides the DryWorkflow class which implements workflow execution
by copying files to a local directory for manual/local execution instead of
submitting to a remote REANA server.
"""
import os
import shutil
import json
from .vworkflow import VWorkflow
from CelebiChrono.utils import metadata


class DryWorkflow(VWorkflow):
    """Local/Dry-run implementation of VWorkflow."""

    def __init__(self, project_uuid, jobs, uuid=None):
        """Initialize local workflow."""
        super().__init__(project_uuid, jobs, uuid)
        # Create a local execution directory
        self.local_exec_path = os.path.join(
                os.path.join(
                    os.environ["HOME"],
                    ".Yuki",
                    "LocalWorkflows",
                    self.uuid,
                    )
                )
        os.makedirs(self.local_exec_path, exist_ok=True)

    def _execute_backend(self):
        """Execute workflow using local backend (copy files locally)."""
        try:
            self.logger("[LOCAL] Creating workflow structure")
            self.create_local_structure()
        except Exception as e:
            self.logger(f"[LOCAL] Failed to create workflow structure: {e}")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

        try:
            self.logger("[LOCAL] Copying files")
            self.copy_files_local()
        except Exception as e:
            self.logger(f"[LOCAL] Failed to copy files: {e}")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

        # Set status to ready for local execution
        self.set_workflow_status("ready_for_local_execution")
        self.logger(f"[LOCAL] Workflow prepared in: {self.local_exec_path}")
        self.logger(f"[LOCAL] Snakefile: {os.path.join(self.local_exec_path, 'Snakefile')}")
        self.logger("[LOCAL] You can now run: snakemake --cores all")

    def _sync_external_job_status(self, job):
        """Poll local status for external dependency."""
        # In local mode, check if files exist
        job.update_status_from_workflow(self.path, self.logger)

    def create_local_structure(self):
        """Create local workflow structure."""
        workflow_info = {
            "workflow": {
                "uuid": self.uuid,
                "name": self.get_name(),
                "specification": {
                    "job_dependencies": self.dependencies,
                    "steps": self.steps,
                },
                "type": "snakemake",
                "file": "Snakefile"
            }
        }

        # Write workflow info
        self.logger(f"[LOCAL] Writing workflow info to {os.path.join(self.local_exec_path, 'workflow_info.json')}")
        with open(os.path.join(self.local_exec_path, "workflow_info.json"), "w") as f:
            json.dump(workflow_info, f, indent=2)

    def copy_files_local(self):
        """Copy all files to local execution directory."""
        total_jobs = len(self.jobs)
        for j_idx, job in enumerate(self.jobs):
            job_dir = os.path.join(self.local_exec_path, f"imp{job.short_uuid()}")

            # Copy job files
            files = job.files()
            total_files = len(files)
            for f_idx, name in enumerate(files):
                src_path = os.path.join(job.path, "contents", name[8:])
                dst_path = os.path.join(self.local_exec_path, "imp" + name)
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                if os.path.exists(src_path):
                    shutil.copy2(src_path, dst_path)
                    self.logger(f"[LOCAL] [Job {j_idx+1}/{total_jobs}] Copied file {f_idx+1}/{total_files}: {name}")

            # Handle rawdata environment
            if job.environment() == "rawdata":
                rawdata_path = os.path.join(job.path, "rawdata")
                if os.path.exists(rawdata_path):
                    filelist = os.listdir(rawdata_path)
                    total_raw = len(filelist)
                    for f_idx, filename in enumerate(filelist):
                        src_path = os.path.join(rawdata_path, filename)
                        dst_path = os.path.join(
                            self.local_exec_path,
                            f"imp{job.short_uuid()}",
                            "stageout",
                            filename
                        )
                        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                        shutil.copy2(src_path, dst_path)
                        self.logger(f"[LOCAL] [Job {j_idx+1}/{total_jobs}] Copied rawdata {f_idx+1}/{total_raw}: {filename}")

            # Handle input jobs (copy from dependency workflows)
            elif job.is_input:
                impression = job.path.split("/")[-1]
                src_stageout = os.path.join(
                    os.environ["HOME"],
                    ".Yuki",
                    "Storage",
                    self.project_uuid,
                    impression,
                    job.machine_id,
                    "stageout"
                )

                if os.path.exists(src_stageout):
                    filelist = os.listdir(src_stageout)
                    total_input = len(filelist)
                    for f_idx, filename in enumerate(filelist):
                        src_path = os.path.join(src_stageout, filename)
                        dst_path = os.path.join(
                            self.local_exec_path,
                            f"imp{job.short_uuid()}",
                            "stageout",
                            filename
                        )
                        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                        shutil.copy2(src_path, dst_path)
                        self.logger(f"[LOCAL] [Job {j_idx+1}/{total_jobs}] Copied input {f_idx+1}/{total_input}: {filename}")

        # Copy Snakefile
        shutil.copy2(
            self.snakefile_path,
            os.path.join(self.local_exec_path, "Snakefile")
        )
        self.logger(f"[LOCAL] Copied: Snakefile")

    def update_workflow_status(self):
        """Update workflow status from local execution."""
        try:
            # Check if all output files exist
            all_done = True
            self.logger("[LOCAL] Checking jobs status...")

            # Get jobs from json
            workflow_info_json = os.path.join(self.local_exec_path, "workflow_info.json")
            with open(workflow_info_json, "r") as f:
                workflow_info = json.load(f)
            jobs = []
            for step in workflow_info["workflow"]["specification"]["steps"]:
                name = step["name"]
                jobs.append(name[4:])

            for job_uuid in jobs:
                done_file = os.path.join(
                    self.local_exec_path,
                    f"{job_uuid}.done"
                    )

                if not os.path.exists(done_file):
                    all_done = False
                    break

            if all_done:
                status = "finished"
            else:
                # Check if workflow is running
                status = "running"

            results = {
                "status": status,
                "progress": {
                    "total": len(jobs),
                    "completed": sum(
                        1 for job_uuid in jobs
                        if os.path.exists(
                            os.path.join(
                                self.local_exec_path,
                                f"{job_uuid}.done"
                            )
                        )
                    )
                }
            }
            self.logger(f"[LOCAL] Workflow status: {status}, Progress: {results['progress']['completed']}/{results['progress']['total']}")

            path = os.path.join(self.path, "results.json")
            results_file = metadata.ConfigFile(path)
            results_file.write_variable("results", results)

        except Exception as e:
            self.logger(f"[LOCAL] Failed to update workflow status: {e}")

    def check_status(self):
        """Check the status of local workflow execution."""
        self.logger("[LOCAL] Checking status...")
        self.update_workflow_status()
        return self.status()

    def kill(self):
        """Kill local workflow execution."""
        self.logger("[LOCAL] Killing local workflow (manual intervention required)")
        self.set_workflow_status("killed")
        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            job.set_status("failed")

    def download(self, impression=None):
        """Download/collect results from local execution."""
        self.logger("[LOCAL] Collecting results from local execution")
        if impression:
            src_path = os.path.join(
                self.local_exec_path,
                f"imp{impression[0:7]}",
                "stageout"
            )
            dst_path = os.path.join(
                os.environ["HOME"],
                ".Yuki",
                "Storage",
                self.project_uuid,
                impression,
                self.machine_id,
                "stageout"
            )

            if os.path.exists(src_path):
                os.makedirs(dst_path, exist_ok=True)
                filelist = os.listdir(src_path)
                total_files = len(filelist)
                for i, filename in enumerate(filelist):
                    src_file = os.path.join(src_path, filename)
                    dst_file = os.path.join(dst_path, filename)
                    shutil.copy2(src_file, dst_file)
                    self.logger(f"[LOCAL] [{i+1}/{total_files}] Collected: {filename}")

                # Mark as downloaded
                open(os.path.join(os.path.dirname(dst_path), "stageout.downloaded"), "w").close()

    def download_outputs(self, impression=None):
        """Download outputs from local execution."""
        self.download(impression)

    def download_logs(self, impression=None):
        """Download logs from local execution."""
        self.logger("[LOCAL] Collecting logs from local execution")
        if impression:
            src_path = os.path.join(
                self.local_exec_path,
                f"imp{impression[0:7]}",
                "logs"
            )
            dst_path = os.path.join(
                os.environ["HOME"],
                ".Yuki",
                "Storage",
                self.project_uuid,
                impression,
                self.machine_id,
                "logs"
            )

            if os.path.exists(src_path):
                os.makedirs(dst_path, exist_ok=True)
                filelist = os.listdir(src_path)
                total_logs = len(filelist)
                for i, filename in enumerate(filelist):
                    src_file = os.path.join(src_path, filename)
                    dst_file = os.path.join(dst_path, filename)
                    shutil.copy2(src_file, dst_file)
                    self.logger(f"[LOCAL] [{i+1}/{total_logs}] Collected log: {filename}")

                # Mark as downloaded
                open(os.path.join(os.path.dirname(dst_path), "logs.downloaded"), "w").close()

    def ping(self):
        """Ping local system."""
        self.logger("[LOCAL] Local workflow system is available")
        return True

