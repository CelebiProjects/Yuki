"""
REANA workflow implementation.

This module provides the ReanaWorkflow class which implements workflow execution
through the REANA workflow management system.
"""
# pylint: disable=cyclic-import
import os
import time
import json
from CelebiChrono.utils import metadata
from Yuki.kernel.status_constants import ORCHESTRATING
from .vworkflow import VWorkflow, _yuki_dir
from . import file_types  # pylint: disable=unused-import  # re-exported for tests
from .file_staging import walk_files

# Try to import reana_client, but it might not be available
try:
    from reana_client.api import client
    REANA_AVAILABLE = True
except ImportError:
    REANA_AVAILABLE = False
    client = None

class ReanaWorkflow(VWorkflow):
    """REANA implementation of VWorkflow."""

    def __init__(self, project_uuid, jobs, uuid=None):
        """Initialize REANA workflow."""
        super().__init__(project_uuid, jobs, uuid)
        self.set_environment(self.machine_id)
        self.access_token = self.get_access_token(self.machine_id)

    def _execute_backend(self):  # pylint: disable=too-many-branches
        """Execute workflow using REANA backend."""

        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            job.set_status(ORCHESTRATING, "Create the workflow at backend")

        try:
            self.logger("Creating the workflow")
            self.create_workflow()
        except Exception as e:
            self.logger(f"Failed to create the workflow: {e}")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

        for job in self.jobs:
            if job.is_input:
                continue
            if job.job_type() == "algorithm":
                continue
            job.set_status(ORCHESTRATING, "Upload the dependencies and the Snakefile")

        try:
            self.logger("Upload file")
            self.upload_file()
        except Exception as e:
            self.logger(f"Failed to upload the files: {type(e).__name__}: {e}")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

        job.set_status(ORCHESTRATING, "Start the workflow")
        try:
            self.start_workflow()
        except:
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

    def _sync_external_job_status(self, job):
        """Poll REANA for external dependency status."""
        self.update_workflow_status()
        job.update_status_from_workflow(self.path, self.logger)

    def create_workflow(self):
        """Create a workflow using REANA client."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)

        reana_json = {"workflow": {}}
        reana_json["workflow"]["specification"] = {
                "job_dependencies": self.dependencies,
                "steps": self.steps,
                }
        reana_json["workflow"]["type"] = "snakemake"
        reana_json["workflow"]["file"] = "Snakefile"

        # Collect unique CVMFS repositories from all steps and add them as
        # workflow-level resources (required by REANA for CVMFS mounting).
        cvmfs_repos = set()
        for step in self.steps:
            for repo in step.get("cvmfs", []):
                cvmfs_repos.add(repo)
        if cvmfs_repos:
            reana_json["workflow"]["resources"] = {
                "cvmfs": sorted(cvmfs_repos)
            }

        self.logger(f"reana_json: {json.dumps(reana_json, indent=2)}")
        client.create_workflow(
                reana_json,
                self.get_name(),
                self.get_access_token(self.machine_id)
                )

    def set_environment(self, machine_id):
        """Set the environment variable for REANA server URL."""
        # Set the environment variable
        path = os.path.join(_yuki_dir(), "config.json")
        config_file = metadata.ConfigFile(path)
        urls = config_file.read_variable("urls", {})
        url = urls.get(machine_id, "")
        self.logger(f"machine_id = {machine_id}")
        self.logger(f"reana_url = {url}")
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        from reana_commons.api_client import BaseAPIClient
        os.environ["REANA_SERVER_URL"] = url
        BaseAPIClient("reana-server")

    def get_access_token(self, machine_id):
        """Get access token for the specified machine."""
        path = os.path.join(_yuki_dir(), "config.json")
        config_file = metadata.ConfigFile(path)
        tokens = config_file.read_variable("tokens", {})
        token = tokens.get(machine_id, "")
        return token

    def create_reana_workflow(self):
        """Create REANA workflow (deprecated - use create_workflow)."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        reana_json = {
            "workflow": {
                "specification": {"job_dependencies": self.dependencies, "steps": self.steps},
                "type": "snakemake",
                "file": "Snakefile"
            }
        }
        client.create_workflow(reana_json, self.get_name(), self.get_access_token(self.machine_id))

    def start_workflow(self):
        """Start the workflow execution."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        client.start_workflow(
            self.get_name(),
            self.get_access_token(self.machine_id),
            {}
        )

    def check_status(self):
        """Check the status of the workflow periodically."""
        # Check the status of the workflow
        # Check whether the workflow is finished, every 5 seconds
        counter = 0
        while True:
            # Check the status every minute
            if counter % 60 == 0:
                self.update_workflow_status()

            status = self.status()
            if status in ('finished', 'failed'):
                return status
            time.sleep(1)
            counter += 1

    def kill(self):
        """Kill the workflow execution."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        client.stop_workflow(
            self.get_name(),
            False,
            self.get_access_token(self.machine_id)
        )

    def force_kill(self):
        """Force-stop the online workflow on the REANA server."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        client.stop_workflow(
            self.get_name(),
            True,
            self.get_access_token(self.machine_id)
        )
        self.set_workflow_status("killed")

    def writeline(self, line):
        """Write a line to the YAML file."""
        self.yaml_file.writeline(line)  # pylint: disable=no-member

    def upload_file(self):  # pylint: disable=too-many-locals
        """Upload files to REANA workflow."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        total_jobs = len(self.jobs)
        for j_idx, job in enumerate(self.jobs):
            files = job.files()
            total_files = len(files)
            for f_idx, name in enumerate(files):
                self.logger(f"[Job {j_idx+1}/{total_jobs}] Uploading file "
                            f"{f_idx+1}/{total_files}: {name}")
                with open(os.path.join(job.path, "contents", name[8:]), "rb") as f:
                    client.upload_file(
                        self.get_name(),
                        f,
                        "imp" + name,
                        self.get_access_token(self.machine_id)
                    )
            if job.environment() == "rawdata":
                rawdata_dir = os.path.join(job.path, "rawdata")
                filelist = list(walk_files(rawdata_dir))
                total_raw = len(filelist)
                for f_idx, (rel_path, rawdata_path) in enumerate(filelist):
                    with open(rawdata_path, "rb") as f:
                        self.logger(f"[Job {j_idx+1}/{total_jobs}] Uploading rawdata "
                                    f"{f_idx+1}/{total_raw}: {rel_path}")
                        client.upload_file(
                            self.get_name(),
                            f,
                            "imp" + job.short_uuid() + "/stageout/" + rel_path,
                            self.get_access_token(self.machine_id)
                        )
            elif job.is_input:
                if job.cache_on_runner() and job.machine_id == self.machine_id:
                    continue
                impression = job.path.split("/")[-1]
                path = os.path.join(_yuki_dir(), "Storage",
                                    self.project_uuid, impression, job.machine_id)
                if not os.path.exists(os.path.join(path, "stageout")):
                    workflow = ReanaWorkflow(self.project_uuid, [], job.workflow_id())
                    workflow.download_outputs(impression)

                # Reset the id
                self.set_environment(self.machine_id)
                stageout_dir = os.path.join(path, "stageout")
                filelist = list(walk_files(stageout_dir))
                total_input = len(filelist)
                for f_idx, (rel_path, file_path) in enumerate(filelist):
                    with open(file_path, "rb") as f:
                        self.logger(f"[Job {j_idx+1}/{total_jobs}] Uploading input "
                                    f"{f_idx+1}/{total_input}: {rel_path}")
                        client.upload_file(
                            self.get_name(),
                            f,
                            "imp"+job.short_uuid() + "/stageout/" + rel_path,
                            self.get_access_token(self.machine_id)
                        )

        with open(self.snakefile_path, "rb") as f:
            self.logger("Uploading Snakefile")
            client.upload_file(
                self.get_name(),
                f,
                "Snakefile",
                self.get_access_token(self.machine_id)
            )
        yaml_file = metadata.YamlFile(os.path.join(self.path, "reana.yaml"))
        workflow_def = {
            "type": "snakemake",
            "file": "Snakefile",
        }
        cvmfs_repos = set()
        for step in self.steps:
            for repo in step.get("cvmfs", []):
                cvmfs_repos.add(repo)
        if cvmfs_repos:
            workflow_def["resources"] = {"cvmfs": sorted(cvmfs_repos)}
        yaml_file.write_variable("workflow", workflow_def)
        with open(os.path.join(self.path, "reana.yaml"), "rb") as f:
            self.logger("Uploading reana.yaml")
            client.upload_file(
                self.get_name(),
                f,
                "reana.yaml",
                self.get_access_token(self.machine_id)
            )

    def update_workflow_status(self):
        """Update workflow status from REANA."""
        try:
            if not REANA_AVAILABLE:
                raise ImportError("reana_client is not available")
            self.logger(
                f"[REANA] update_workflow_status workflow={self.uuid} "
                f"path={self.path} machine_id={self.machine_id} "
                f"jobs={[(j.short_uuid(), j.is_input, j.job_type()) for j in self.jobs]}"
            )
            self.set_environment(self.machine_id)
            results = client.get_workflow_status(
                self.get_name(),
                self.get_access_token(self.machine_id))
            status = results.get("status", "unknown")
            # Checked before the write: after it, the recorded status is
            # already terminal and the transition would be invisible.
            entered_terminal = self._entered_terminal_state(status)
            path = os.path.join(self.path, "results.json")
            results_file = metadata.ConfigFile(path)
            results_file.write_variable("results", results)
            logpath = os.path.join(self.path, "log.json")
            log_file = metadata.ConfigFile(logpath)
            logstring = results.get("logs", "{}")
            # decode the logstring with json
            log = json.loads(logstring)
            log_file.write_variable("logs", log)
            self.logger(f"Workflow status: {results.get('status', 'unknown')}")
            # Refresh listings first: the terminal distribution recording
            # below reads them and must see the final file set.
            self._refresh_job_filelists(status, entered_terminal)
            if entered_terminal:
                self.logger(
                    f"[REANA] workflow={self.uuid} entered terminal status={status} "
                    "recording distributions"
                )
                self._record_terminal_distributions(status)
        except Exception as e:
            self.logger(f"[REANA] Failed to update the workflow status: {e}")

    def download(self, impression=None):  # pylint: disable=too-many-locals
        """Download workflow results."""
        # self.logger("Downloading the files")
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        if impression:
            path = os.path.join(_yuki_dir(), "Storage",
                                self.project_uuid, impression, self.machine_id)
            try: # try to download the files
                if not os.path.exists(os.path.join(path, "stageout.downloaded")):
                    files = client.list_files(
                        self.get_name(),
                        self.get_access_token(self.machine_id),
                        "imp"+impression[0:7]+"/stageout"
                    )
                    os.makedirs(os.path.join(path, "stageout"), exist_ok=True)
                    # self.logger(f"Files: {files}")
                    total_files = len(files)
                    for i, file in enumerate(files):
                        name = file["name"]
                        self.logger(f'[{i+1}/{total_files}] Downloading stageout: {name}')
                        output = client.download_file(
                            self.get_name(),
                            name,
                            self.get_access_token(self.machine_id),
                        )
                        prefix = f"imp{impression[0:7]}/stageout/"
                        rel = name[len(prefix):] if name.startswith(prefix) else name
                        filename = os.path.join(path, "stageout", rel)
                        os.makedirs(os.path.dirname(filename), exist_ok=True)
                        with open(filename, "wb") as f:
                            f.write(output[0])
                    # all done, make a finish file
                    finish_file = os.path.join(path, "stageout.downloaded")
                    with open(finish_file, "w", encoding='utf-8') as f:
                        pass
            except Exception as e:
                self.logger(f"Failed to download stageout: {e}")

            try:
                if not os.path.exists(os.path.join(path, "logs.downloaded")):
                    files = client.list_files(
                        self.get_name(),
                        self.get_access_token(self.machine_id),
                        "imp"+impression[0:7]+"/logs"
                    )
                    os.makedirs(os.path.join(path, "logs"), exist_ok=True)
                    total_logs = len(files)
                    for i, file in enumerate(files):
                        name = file["name"]
                        self.logger(f'[{i+1}/{total_logs}] Downloading log: {name}')
                        output = client.download_file(
                            self.get_name(),
                            name,
                            self.get_access_token(self.machine_id),
                        )
                        prefix = f"imp{impression[0:7]}/logs/"
                        rel = name[len(prefix):] if name.startswith(prefix) else name
                        filename = os.path.join(path, "logs", rel)
                        os.makedirs(os.path.dirname(filename), exist_ok=True)
                        with open(filename, "wb") as f:
                            f.write(output[0])
                    # all done, make a finish file
                    with open(os.path.join(path, "logs.downloaded"), "w", encoding='utf-8') as f:
                        pass
            except Exception as e:
                self.logger(f"Failed to download logs: {e}")

    def download_outputs(self, impression=None):  # pylint: disable=too-many-locals
        """Download workflow results."""
        # self.logger("Downloading the files")
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        report = {"collected": [], "skipped": [], "failed": []}
        if impression:
            path = os.path.join(_yuki_dir(), "Storage",
                                self.project_uuid, impression, self.machine_id)
            try:
                if os.path.exists(os.path.join(path, "stageout.downloaded")):
                    report["skipped"].append(
                        {"file": "<stageout>", "reason": "already collected"})
                    return report
                files = client.list_files(
                    self.get_name(),
                    self.get_access_token(self.machine_id),
                    "imp"+impression[0:7]+"/stageout"
                )
                os.makedirs(os.path.join(path, "stageout"), exist_ok=True)
                # self.logger(f"Files: {files}")
                total_files = len(files)
                for i, file in enumerate(files):
                    name = file["name"]
                    prefix = f"imp{impression[0:7]}/stageout/"
                    rel = name[len(prefix):] if name.startswith(prefix) else name
                    self.logger(f'[{i+1}/{total_files}] Downloading stageout: {name}')
                    try:
                        output = client.download_file(
                            self.get_name(),
                            name,
                            self.get_access_token(self.machine_id),
                        )
                        filename = os.path.join(path, "stageout", rel)
                        os.makedirs(os.path.dirname(filename), exist_ok=True)
                        with open(filename, "wb") as f:
                            f.write(output[0])
                        report["collected"].append(rel)
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        report["failed"].append(
                            {"file": rel, "reason": str(exc)})
                # all done, make a finish file
                finish_file = os.path.join(path, "stageout.downloaded")
                with open(finish_file, "w", encoding='utf-8') as f:
                    pass
            except Exception as e:
                self.logger(f"Failed to download stageout: {e}")
                report["failed"].append(
                    {"file": "<stageout>", "reason": str(e)})
        return report

    def download_logs(self, impression=None,  # pylint: disable=too-many-locals
                      refresh=False):
        """Download workflow logs."""
        # self.logger("Downloading the files")
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        report = {"collected": [], "skipped": [], "failed": []}
        if impression:
            path = os.path.join(_yuki_dir(), "Storage",
                                self.project_uuid, impression, self.machine_id)
            try:
                if not refresh and os.path.exists(os.path.join(path, "logs.downloaded")):
                    report["skipped"].append(
                        {"file": "<logs>", "reason": "already collected"})
                    return report
                files = client.list_files(
                    self.get_name(),
                    self.get_access_token(self.machine_id),
                    "imp"+impression[0:7]+"/logs"
                )
                os.makedirs(os.path.join(path, "logs"), exist_ok=True)
                total_logs = len(files)
                for i, file in enumerate(files):
                    name = file["name"]
                    prefix = f"imp{impression[0:7]}/logs/"
                    rel = name[len(prefix):] if name.startswith(prefix) else name
                    self.logger(f'[{i+1}/{total_logs}] Downloading log: {name}')
                    try:
                        output = client.download_file(
                            self.get_name(),
                            name,
                            self.get_access_token(self.machine_id),
                        )
                        filename = os.path.join(path, "logs", rel)
                        os.makedirs(os.path.dirname(filename), exist_ok=True)
                        with open(filename, "wb") as f:
                            f.write(output[0])
                        report["collected"].append(rel)
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        report["failed"].append(
                            {"file": rel, "reason": str(exc)})
                # all done, make a finish file
                with open(os.path.join(path, "logs.downloaded"), "w", encoding='utf-8') as f:
                    pass
            except Exception as e:
                self.logger(f"Failed to download logs: {e}")
                report["failed"].append(
                    {"file": "<logs>", "reason": str(e)})
        return report

    @staticmethod
    def _size_bytes(size):
        """Normalize a REANA file size to int bytes.

        REANA's list_files reports size as {"raw": <int>, "human_readable":
        <str>}; the native runner and older REANA report a bare int. Flatten
        to int so the file_status JSON contract (size: int) always holds and
        the client's _human_size() never receives a dict.
        """
        if isinstance(size, dict):
            return size.get("raw", 0) or 0
        return size or 0

    def _list_files(self, impression, kind, attempts=3):
        """Call REANA list_files with bounded retry for transient TLS/connection
        failures (e.g. SSL UNEXPECTED_EOF_WHILE_READING, seen intermittently
        against reana.cern.ch). Returns the file list, or re-raises the last
        error if every attempt fails."""
        target = "imp" + impression[0:7] + "/" + kind
        last = None
        for i in range(attempts):
            try:
                return client.list_files(
                    self.get_name(), self.get_access_token(self.machine_id), target)
            except Exception as e:  # transient TLS/connection -> retry
                last = e
                self.logger(
                    f"list_files {target} attempt {i + 1}/{attempts} failed "
                    f"[{type(e).__name__}]: {e!r}")
                if i + 1 < attempts:
                    time.sleep(0.5 * (i + 1))
        raise last

    def list_runner_files(self, impression, kind="stageout"):
        """List files in the runner workspace under imp<short>/<kind> without
        downloading. Returns [{"name": <relative-to-kind>, "size": int}]."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        prefix = "imp" + impression[0:7] + "/" + kind + "/"
        try:
            files = self._list_files(impression, kind)
        except Exception as e:
            self.logger(
                f"Giving up listing imp{impression[0:7]}/{kind} after retries "
                f"[{type(e).__name__}]: {e!r}")
            raise
        result = []
        for f in files:
            name = f["name"]
            rel = name[len(prefix):] if name.startswith(prefix) else os.path.basename(name)
            if rel:
                result.append({"name": rel, "size": self._size_bytes(f.get("size", 0))})
        return result

    # pylint: disable=too-many-locals
    def download_selected(self, impression, predicate, kind="stageout"):
        """Download only remote files whose basename satisfies predicate and
        that are not already in Storage. Does not write the dir marker."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        report = {"collected": [], "skipped": [], "failed": []}
        path = os.path.join(_yuki_dir(), "Storage",
                            self.project_uuid, impression, self.machine_id)
        prefix = "imp" + impression[0:7] + "/" + kind + "/"
        try:
            files = self._list_files(impression, kind)
        except Exception as e:
            self.logger(
                f"Giving up listing imp{impression[0:7]}/{kind} after retries "
                f"[{type(e).__name__}]: {e!r}")
            report["skipped"].append(
                {"file": f"<{kind}>", "reason": f"failed to list files: {e}"})
            return report
        os.makedirs(os.path.join(path, kind), exist_ok=True)
        for f in files:
            name = f["name"]
            rel = name[len(prefix):] if name.startswith(prefix) else os.path.basename(name)
            if not rel:
                continue
            if not predicate(rel):
                report["skipped"].append(
                    {"file": rel, "reason": "does not match selector"})
                continue
            dest = os.path.join(path, kind, rel)
            if os.path.exists(dest):
                report["skipped"].append(
                    {"file": rel, "reason": "already in Yuki"})
                continue
            try:
                output = client.download_file(
                    self.get_name(), name, self.get_access_token(self.machine_id))
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with open(dest, "wb") as fh:
                    fh.write(output[0])
                report["collected"].append(rel)
                self.logger(f"Downloaded selected {kind}: {rel}")
            except Exception as exc:  # pylint: disable=broad-exception-caught
                report["failed"].append({"file": rel, "reason": str(exc)})
                self.logger(f"Failed to download {rel}: {exc}")
        return report

    def ping(self):
        """Ping the REANA server."""
        # Ping the server
        # We must import the client here because we need to set the environment variable first
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        return client.ping(self.access_token)

    def get_workflow_logs(self):
        """Fetch engine logs from REANA and store them locally."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        logpath = os.path.join(self.path, "engine_logs.json")
        if os.path.exists(logpath):
            return
        worrkflow_logs = client.get_workflow_logs(
                self.get_name(),
                self.access_token)
        # Save the logs to a file
        log_file = metadata.ConfigFile(logpath)
        log_file.write_variable("logs", worrkflow_logs)

    def homekeep(self):
        """Outdated: use collect plus delete_workspace (GET /delete-workflow).
        Perform homekeeping tasks for the workflow.
        Download all the results for the jobs in the workflow.
        """
        # if os.path.exists(os.path.join(self.path, "homekeep.done")):
        #     self.logger("Homekeeping already done, skipping")
        #     return
        if self.status() != "finished":
            self.logger("Workflow not finished, skipping homekeeping")
            return
        self.logger("Starting homekeeping")
        print(self.jobs)
        for job in self.jobs:
            print("Homekeeping job:", job)
            if job.is_input:
                continue
            self.logger(f"Homekeeping job: {job}")
            job.update_status_from_workflow(
                self.path,
                self.logger
                )
            print("Downloading", job.uuid)
            self.download(job.uuid)
        # Remove the online workflow
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        self.logger("Deleting the online workflow")
        try:
            print("Deleting workflow", self.get_name())
            client.delete_workflow(
                self.get_name(),
                True, True,
                self.get_access_token(self.machine_id)
            )
        except Exception as e:
            self.logger(f"Failed to delete the online workflow: {e}")
        # Write the workflow homekeep done file
        homekeep_done_path = os.path.join(self.path, "homekeep.done")
        with open(homekeep_done_path, "w", encoding='utf-8') as f:
            f.write("done")

    def delete_workspace(self):
        """Delete the online workflow on the REANA server."""
        if not REANA_AVAILABLE:
            raise ImportError("reana_client is not available")
        self.set_environment(self.machine_id)
        client.delete_workflow(
            self.get_name(),
            True, True,
            self.get_access_token(self.machine_id)
        )
