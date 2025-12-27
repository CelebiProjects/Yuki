"""
REANA workflow implementation.

This module provides the ReanaWorkflow class which implements workflow execution
through the REANA workflow management system.
"""
import os
import time
import json
from .vworkflow import VWorkflow
from CelebiChrono.utils import metadata

class ReanaWorkflow(VWorkflow):
    """REANA implementation of VWorkflow."""

    def __init__(self, project_uuid, jobs, uuid=None):
        """Initialize REANA workflow."""
        super().__init__(project_uuid, jobs, uuid)
        self.set_enviroment(self.machine_id)
        self.access_token = self.get_access_token(self.machine_id)

    def _execute_backend(self):
        """Execute workflow using REANA backend."""
        try:
            print("Creating the workflow")
            self.create_workflow()
        except:
            print("Failed to create the workflow")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

        try:
            print("Upload file")
            self.upload_file()
        except:
            print("Failed to upload the files")
            self.set_workflow_status("failed")
            for job in self.jobs:
                if job.is_input:
                    continue
                if job.job_type() == "algorithm":
                    continue
                job.set_status("failed")
            raise

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
        job.update_status_from_workflow(self.path)

    def create_workflow(self):
        """Create a workflow using REANA client."""
        from reana_client.api import client
        self.set_enviroment(self.machine_id)

        reana_json = {"workflow": {}}
        reana_json["workflow"]["specification"] = {
                "job_dependencies": self.dependencies,
                "steps": self.steps,
                }
        reana_json["workflow"]["type"] = "snakemake"
        reana_json["workflow"]["file"] = "Snakefile"
        client.create_workflow(
                reana_json,
                self.get_name(),
                self.get_access_token(self.machine_id)
                )


    def set_enviroment(self, machine_id):
        """Set the environment variable for REANA server URL."""
        # Set the environment variable
        path = os.path.join(os.environ["HOME"], ".Yuki", "config.json")
        config_file = metadata.ConfigFile(path)
        urls = config_file.read_variable("urls", {})
        url = urls.get(machine_id, "")
        from reana_client.api import client
        from reana_commons.api_client import BaseAPIClient
        os.environ["REANA_SERVER_URL"] = url
        BaseAPIClient("reana-server")

    def get_access_token(self, machine_id):
        """Get access token for the specified machine."""
        path = os.path.join(os.environ["HOME"], ".Yuki", "config.json")
        config_file = metadata.ConfigFile(path)
        tokens = config_file.read_variable("tokens", {})
        token = tokens.get(machine_id, "")
        return token

    def create_reana_workflow(self):
        """Create REANA workflow (deprecated - use create_workflow)."""
        from reana_client.api import client
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
        from reana_client.api import client
        self.set_enviroment(self.machine_id)
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
        from reana_client.api import client
        client.stop_workflow(
            self.get_name(),
            False,
            self.get_access_token(self.machine_id)
        )


    def writeline(self, line):
        """Write a line to the YAML file."""
        self.yaml_file.writeline(line)

    def upload_file(self):
        """Upload files to REANA workflow."""
        from reana_client.api import client
        self.set_enviroment(self.machine_id)
        for job in self.jobs:
            for name in job.files():
                print(f"upload file: {name}")
                with open(os.path.join(job.path, "contents", name[8:]), "rb") as f:
                    client.upload_file(
                        self.get_name(),
                        f,
                        "imp" + name,
                        self.get_access_token(self.machine_id)
                    )
            if job.environment() == "rawdata":
                filelist = os.listdir(os.path.join(job.path, "rawdata"))
                for filename in filelist:
                    with open(os.path.join(job.path, "rawdata", filename), "rb") as f:
                        client.upload_file(
                            self.get_name(),
                            f,
                            "imp" + job.short_uuid() + "/stageout/" + filename,
                            self.get_access_token(self.machine_id)
                        )
            elif job.is_input:
                if job.use_eos() and job.machine_id == self.machine_id:
                    continue
                impression = job.path.split("/")[-1]
                # print(f"Downloading the files from impression {impression}")
                path = os.path.join(os.environ["HOME"], ".Yuki", "Storage", self.project_uuid, impression, job.machine_id)
                if not os.path.exists(os.path.join(path, "stageout")):
                    workflow = ReanaWorkflow(self.project_uuid, [], job.workflow_id())
                    workflow.download_outputs(impression)

                # Reset the id
                self.set_enviroment(self.machine_id)
                filelist = os.listdir(os.path.join(path, "stageout"))
                for filename in filelist:
                    with open(os.path.join(path, "stageout", filename), "rb") as f:
                        client.upload_file(
                            self.get_name(),
                            f,
                            "imp"+job.short_uuid() + "/stageout/" + filename,
                            self.get_access_token(self.machine_id)
                        )

        with open(self.snakefile_path, "rb") as f:
            client.upload_file(
                self.get_name(),
                f,
                "Snakefile",
                self.get_access_token(self.machine_id)
            )
        yaml_file = metadata.YamlFile(os.path.join(self.path, "reana.yaml"))
        yaml_file.write_variable("workflow", {
            "type": "snakemake",
            "file": "Snakefile",
            })
        with open(os.path.join(self.path, "reana.yaml"), "rb") as f:
            client.upload_file(
                self.get_name(),
                f,
                "reana.yaml",
                self.get_access_token(self.machine_id)
            )


    def update_workflow_status(self):
        """Update workflow status from REANA."""
        try:
            from reana_client.api import client
            self.set_enviroment(self.machine_id)
            results = client.get_workflow_status(
                self.get_name(),
                self.get_access_token(self.machine_id))
            path = os.path.join(self.path, "results.json")
            results_file = metadata.ConfigFile(path)
            results_file.write_variable("results", results)
            logpath = os.path.join(self.path, "log.json")
            log_file = metadata.ConfigFile(logpath)
            logstring = results.get("logs", "{}")
            # decode the logstring with json
            log = json.loads(logstring)
            log_file.write_variable("logs", log)
        except Exception as e:
            print("Failed to update the workflow status")
            print(e)

    def download(self, impression=None):
        """Download workflow results."""
        # print("Downloading the files")
        from reana_client.api import client
        self.set_enviroment(self.machine_id)
        if impression:
            path = os.path.join(os.environ["HOME"], ".Yuki", "Storage", self.project_uuid, impression, self.machine_id)
            try: # try to download the files
                if not os.path.exists(os.path.join(path, "stageout.downloaded")):
                    files = client.list_files(
                        self.get_name(),
                        self.get_access_token(self.machine_id),
                        "imp"+impression[0:7]+"/stageout"
                    )
                    os.makedirs(os.path.join(path, "stageout"), exist_ok=True)
                    # print(f"Files: {files}")
                    for file in files:
                        # print(f'Downloading {file["name"]}')
                        output = client.download_file(
                            self.get_name(),
                            file["name"],
                            self.get_access_token(self.machine_id),
                        )
                        print(f'Downloading {file["name"]}')
                        filename = os.path.join(path, file["name"][11:])
                        with open(filename, "wb") as f:
                            f.write(output[0])
                    # all done, make a finish file
                    open(os.path.join(path, "stageout.downloaded"), "w").close()
            except Exception as e:
                print("Failed to download stageout:", e)

            try:
                if not os.path.exists(os.path.join(path, "logs.downloaded")):
                    files = client.list_files(
                        self.get_name(),
                        self.get_access_token(self.machine_id),
                        "imp"+impression[0:7]+"/logs"
                    )
                    os.makedirs(os.path.join(path, "logs"), exist_ok=True)
                    for file in files:
                        output = client.download_file(
                            self.get_name(),
                            file["name"],
                            self.get_access_token(self.machine_id),
                        )
                        print(f'Downloading {file["name"]}')
                        filename = os.path.join(path, file["name"][11:])
                        with open(filename, "wb") as f:
                            f.write(output[0])
                    # all done, make a finish file
                    open(os.path.join(path, "logs.downloaded"), "w").close()
            except Exception as e:
                print("Failed to download logs:", e)

    def download_outputs(self, impression=None):
        """Download workflow results."""
        # print("Downloading the files")
        from reana_client.api import client
        self.set_enviroment(self.machine_id)
        if impression:
            path = os.path.join(os.environ["HOME"], ".Yuki", "Storage", self.project_uuid, impression, self.machine_id)
            try:
                if not os.path.exists(os.path.join(path, "stageout.downloaded")):
                    files = client.list_files(
                        self.get_name(),
                        self.get_access_token(self.machine_id),
                        "imp"+impression[0:7]+"/stageout"
                    )
                    os.makedirs(os.path.join(path, "stageout"), exist_ok=True)
                    # print(f"Files: {files}")
                    for file in files:
                        # print(f'Downloading {file["name"]}')
                        output = client.download_file(
                            self.get_name(),
                            file["name"],
                            self.get_access_token(self.machine_id),
                        )
                        print(f'Downloading {file["name"]}')
                        filename = os.path.join(path, file["name"][11:])
                        with open(filename, "wb") as f:
                            f.write(output[0])
                    # all done, make a finish file
                    open(os.path.join(path, "stageout.downloaded"), "w").close()
            except Exception as e:
                print("Failed to download stageout:", e)

    def download_logs(self, impression=None):
        """Download workflow logs."""
        # print("Downloading the files")
        from reana_client.api import client
        self.set_enviroment(self.machine_id)
        if impression:
            path = os.path.join(os.environ["HOME"], ".Yuki", "Storage", self.project_uuid, impression, self.machine_id)
            try:
                if not os.path.exists(os.path.join(path, "logs.downloaded")):
                    files = client.list_files(
                        self.get_name(),
                        self.get_access_token(self.machine_id),
                        "imp"+impression[0:7]+"/logs"
                    )
                    os.makedirs(os.path.join(path, "logs"), exist_ok=True)
                    for file in files:
                        output = client.download_file(
                            self.get_name(),
                            file["name"],
                            self.get_access_token(self.machine_id),
                        )
                        print(f'Downloading {file["name"]}')
                        filename = os.path.join(path, file["name"][11:])
                        with open(filename, "wb") as f:
                            f.write(output[0])
                    # all done, make a finish file
                    open(os.path.join(path, "logs.downloaded"), "w").close()
            except Exception as e:
                print("Failed to download logs:", e)

    def ping(self):
        """Ping the REANA server."""
        # Ping the server
        # We must import the client here because we need to set the environment variable first
        from reana_client.api import client
        self.set_enviroment(self.machine_id)
        return client.ping(self.access_token)
