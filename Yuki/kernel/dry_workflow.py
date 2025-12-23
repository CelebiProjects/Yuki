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
            print("[LOCAL] Creating workflow structure")
            self.create_local_structure()
        except Exception as e:
            print(f"[LOCAL] Failed to create workflow structure: {e}")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

        try:
            print("[LOCAL] Copying files")
            self.copy_files_local()
        except Exception as e:
            print(f"[LOCAL] Failed to copy files: {e}")
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
        print(f"[LOCAL] Workflow prepared in: {self.local_exec_path}")
        print(f"[LOCAL] Snakefile: {os.path.join(self.local_exec_path, 'Snakefile')}")
        print("[LOCAL] You can now run: snakemake --cores all")

    def _sync_external_job_status(self, job):
        """Poll local status for external dependency."""
        # In local mode, check if files exist
        job.update_status_from_workflow(self.path)

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
        with open(os.path.join(self.local_exec_path, "workflow_info.json"), "w") as f:
            json.dump(workflow_info, f, indent=2)

    def copy_files_local(self):
        """Copy all files to local execution directory."""
        for job in self.jobs:
            job_dir = os.path.join(self.local_exec_path, f"imp{job.short_uuid()}")

            # Copy job files
            for name in job.files():
                src_path = os.path.join(job.path, "contents", name[8:])
                dst_path = os.path.join(self.local_exec_path, "imp" + name)
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                if os.path.exists(src_path):
                    shutil.copy2(src_path, dst_path)
                    print(f"[LOCAL] Copied: {name}")

            # Handle rawdata environment
            if job.environment() == "rawdata":
                rawdata_path = os.path.join(job.path, "rawdata")
                if os.path.exists(rawdata_path):
                    filelist = os.listdir(rawdata_path)
                    for filename in filelist:
                        src_path = os.path.join(rawdata_path, filename)
                        dst_path = os.path.join(
                            self.local_exec_path,
                            f"imp{job.short_uuid()}",
                            "stageout",
                            filename
                        )
                        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                        shutil.copy2(src_path, dst_path)
                        print(f"[LOCAL] Copied rawdata: {filename}")

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
                    for filename in filelist:
                        src_path = os.path.join(src_stageout, filename)
                        dst_path = os.path.join(
                            self.local_exec_path,
                            f"imp{job.short_uuid()}",
                            "stageout",
                            filename
                        )
                        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                        shutil.copy2(src_path, dst_path)
                        print(f"[LOCAL] Copied input: {filename}")

        # Copy Snakefile
        shutil.copy2(
            self.snakefile_path,
            os.path.join(self.local_exec_path, "Snakefile")
        )
        print(f"[LOCAL] Copied: Snakefile")

    def update_workflow_status(self):
        """Update workflow status from local execution."""
        try:
            # Check if all output files exist
            all_done = True
            print("The jobs are:")

            # Get jobs from json
            workflow_info_json = os.path.join(self.local_exec_path, "workflow_info.json")
            with open(workflow_info_json, "r") as f:
                workflow_info = json.load(f)
            jobs = []
            for step in workflow_info["workflow"]["specification"]["steps"]:
                name = step["name"]
                jobs.append(name[4:])

            print(self.jobs)
            for job in jobs:
                done_file = os.path.join(
                    self.local_exec_path,
                    f"{job}.done"
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
                        1 for job in jobs
                        if os.path.exists(
                            os.path.join(
                                self.local_exec_path,
                                f"{job}.done"
                            )
                        )
                    )
                }
            }

            path = os.path.join(self.path, "results.json")
            results_file = metadata.ConfigFile(path)
            results_file.write_variable("results", results)

        except Exception as e:
            print(f"[LOCAL] Failed to update workflow status: {e}")

    def check_status(self):
        """Check the status of local workflow execution."""
        print("[LOCAL] Checking status...")
        self.update_workflow_status()
        return self.status()

    def kill(self):
        """Kill local workflow execution."""
        print("[LOCAL] Killing local workflow (manual intervention required)")
        self.set_workflow_status("killed")
        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            job.set_status("failed")

    def download(self, impression=None):
        """Download/collect results from local execution."""
        print("[LOCAL] Collecting results from local execution")
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
                for filename in os.listdir(src_path):
                    src_file = os.path.join(src_path, filename)
                    dst_file = os.path.join(dst_path, filename)
                    shutil.copy2(src_file, dst_file)
                    print(f"[LOCAL] Collected: {filename}")

                # Mark as downloaded
                open(os.path.join(os.path.dirname(dst_path), "stageout.downloaded"), "w").close()

    def download_outputs(self, impression=None):
        """Download outputs from local execution."""
        self.download(impression)

    def download_logs(self, impression=None):
        """Download logs from local execution."""
        print("[LOCAL] Collecting logs from local execution")
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
                for filename in os.listdir(src_path):
                    src_file = os.path.join(src_path, filename)
                    dst_file = os.path.join(dst_path, filename)
                    shutil.copy2(src_file, dst_file)
                    print(f"[LOCAL] Collected log: {filename}")

                # Mark as downloaded
                open(os.path.join(os.path.dirname(dst_path), "logs.downloaded"), "w").close()

    def ping(self):
        """Ping local system."""
        print("[LOCAL] Local workflow system is available")
        return True

