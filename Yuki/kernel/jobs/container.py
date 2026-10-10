
"""
Virtual Container module for Yuki kernel.

This module contains the ContainerJob class which represents a container-based job
that extends VJob functionality with container-specific operations like
environment management, command execution, and input/output handling.
"""
# pylint: disable=cyclic-import
import logging
import os
import shlex
import time
from CelebiChrono.utils import metadata
from .base import VJob
from .image import ImageJob
from ..storage.cache import (
    CACHE_COMPLETE_MARKER, CACHE_IN_PROGRESS_MARKER,
    WORKFLOW_FAILED_MARKER,
)
from ..storage.staging import walk_files

_debug = logging.getLogger("Yuki.kernel")


class ContainerJob(VJob):
    """
    Virtual Container class that extends VJob for container-based operations.

    This class handles container lifecycle management, environment setup,
    input/output processing, and command execution within containerized environments.
    """

    def __init__(self, path, machine_id):
        """
        Initialize a ContainerJob instance.

        Args:
            path (str): Path to the container job
            machine_id (str): Identifier for the target machine
        """
        self._image = None
        super().__init__(path, machine_id)

    def inputs(self):
        """
        Get input data aliases and their corresponding impressions.

        Returns:
            tuple: A tuple containing (alias_keys, alias_to_impression_map)
        """
        alias_to_imp = self.config_file.read_variable("alias_to_impression", {})
        return (alias_to_imp.keys(), alias_to_imp)

    def image(self):
        """
        Get the ImageJob instance from predecessor algorithm jobs.

        Returns:
            ImageJob or None: The image associated with predecessor algorithm jobs
        """
        if self._image:
            return self._image
        start_time = time.time()
        predecessors = self.predecessors()
        # print("Predecessors, ", self.predecessors())
        for pred_job in predecessors:
            if pred_job.job_type() == "algorithm":
                _debug.debug(f"    >>>> >>>> Image retrieval time after finding predecessor: "
                             f"{time.time() - start_time}")
                self._image = ImageJob(pred_job.path, self.machine_id)
                return self._image
        return None

    def step(self, request_machine_id, backend_type="reana"):
        """
        Generate a step configuration for REANA workflow execution.

        Returns:
            dict: A dictionary containing step configuration with commands,
                  environment, memory limits, and other execution parameters
        """
        start_time = time.time()
        commands = []
        commands.extend(self._create_directory_commands())
        # print(f"    >>>> Step creation time after directory commands: {time.time() - start_time}")
        commands.extend(self._create_symlink_commands())
        # print(f"    >>>> Step creation time after symlink commands: {time.time() - start_time}")
        commands.extend(self._process_user_commands_for_reana())
        # print(f"    >>>> Step creation time after user commands: {time.time() - start_time}")
        # print("-------------")
        # print("self.is_input", self.is_input)
        # print("self.use_eos()", self.use_eos())

        # Run datalist generator if this is a datalist task (and not an input)
        if self.environment() == "datalist" and not self.is_input:
            commands.append(f"python3 ../imp{self.short_uuid()}/generate_datalist.py")
        # Run LHCb AP datalist generator if this is an LHCb AP datalist task (and not an input)
        if self.environment() == "lhcb_ap_datalist" and not self.is_input:
            short = self.short_uuid()
            commands.append("export APD_USER_TOKEN_FILE=$(pwd)/lbapi_key.json")
            commands.append("apd-login")
            commands.append(f"python3 ../imp{short}/generate_lhcb_ap_datalist.py")

        commands.extend(self._cache_commands(request_machine_id, backend_type))
        commands.append("cd ..")
        commands.append(f"touch {self.short_uuid()}.done")

        step = self._create_reana_step_metadata()
        _debug.debug(f"    >>>> Step creation time after metadata creation: {time.time() - start_time}")
        # step["commands"] = " && ".join(commands)
        step["commands"] = commands

        return step

    def _process_user_commands_for_reana(self):
        """
        Process and prepare user-defined commands for REANA execution. The
        task's own celebi.yaml commands win over the linked algorithm's.

        Returns:
            list: List of processed commands ready for REANA execution
        """
        start_time = time.time()
        if self.is_input or self.compute_backend() == "htcondorcern":
            return []

        _debug.debug(f"    >>>> >>>> User command processing start time: {time.time() - start_time}")

        raw_commands = self.yaml_file.read_variable("commands", [])
        if not raw_commands:
            img = self.image()
            if img is not None:
                raw_commands = img.yaml_file.read_variable("commands", [])
        processed_commands = []

        _debug.debug(raw_commands)
        for i, command in enumerate(raw_commands):
            _debug.debug(f"    >>>> >>>> Processing command {i} start time: {time.time() - start_time}")
            command = self._substitute_parameters(command)
            _debug.debug(f"    >>>> >>>> After parameter substitution time: {time.time() - start_time}")
            command = self._substitute_inputs(command)
            _debug.debug(f"    >>>> >>>> After input substitution time: {time.time() - start_time}")
            command = self._substitute_paths(command)
            _debug.debug(f"    >>>> >>>> After path substitution time: {time.time() - start_time}")
            command = "{ " + command + " ; } >> " + f"logs/celebi_user_step{i}.log 2>&1"
            processed_commands.append(command.replace("\"", "\\\""))

        return processed_commands

    def lhcb_ap_environment(self):
        """
        Get the container environment for LHCb AP data list jobs.

        Returns:
            str: Docker image specification with the LHCb AP tool installed
        """
        return "docker://celebichrono/lhcb-apd:0.9.0"

    def _create_reana_step_metadata(self):
        """
        Create step metadata for REANA execution.

        Returns:
            dict: Dictionary containing REANA-specific step metadata
        """
        if self.is_input or self.environment() == "datalist":
            environment = self.default_environment()
        elif self.environment() == "lhcb_ap_datalist":
            environment = self.lhcb_ap_environment()
        else:
            environment = self.environment()
        compute_backend = self.compute_backend()

        step = {
            "environment": environment,
            "name": f"step{self.short_uuid()}"
        }

        if self.use_kerberos("reana"):
            step["kerberos"] = True

        cvmfs_repos = self.cvmfs()
        if cvmfs_repos:
            step["cvmfs"] = cvmfs_repos
            step["resources"] = {"cvmfs": cvmfs_repos}

        if compute_backend != "unsigned":
            step["compute_backend"] = compute_backend
            step["htcondor_max_runtime"] = "espresso"
            step["kerberos"] = True
        else:
            step["compute_backend"] = None
            step["kubernetes_memory_limit"] = self.memory()
            step["kubernetes_uid"] = None

        return step

    def default_environment(self):
        """
        Get the default container environment for input jobs.

        Returns:
            str: Default Docker environment specification
        """
        return "docker.io/reanahub/reana-env-root6:6.18.04"

    def snakemake_rule(self, request_machine_id, backend_type="reana"):
        """
        Generate a Snakemake rule configuration for workflow execution.

        Returns:
            dict: A dictionary containing rule configuration including commands,
                  environment, memory, inputs, and outputs for Snakemake workflow
        """
        commands = []
        commands.extend(self._create_directory_commands())
        commands.extend(self._create_symlink_commands())
        commands.extend(self._process_user_commands())

        # Run datalist generator if this is a datalist task (and not an input)
        if self.environment() == "datalist" and not self.is_input:
            commands.append(f"python3 ../imp{self.short_uuid()}/generate_datalist.py")
        # Run LHCb AP datalist generator if this is an LHCb AP datalist task (and not an input)
        if self.environment() == "lhcb_ap_datalist" and not self.is_input:
            short = self.short_uuid()
            commands.append("export APD_USER_TOKEN_FILE=$(pwd)/lbapi_key.json")
            commands.append("apd-login")
            commands.append(f"python3 ../imp{short}/generate_lhcb_ap_datalist.py")

        commands.extend(self._cache_commands(request_machine_id, backend_type))
        commands.append("cd ..")
        commands.append(f"touch {self.short_uuid()}.done")

        step = self._create_step_metadata()
        step["commands"] = commands
        step["inputs"] = self._get_step_inputs()
        cvmfs_repos = self.cvmfs()
        if cvmfs_repos:
            step["cvmfs"] = cvmfs_repos

        return step

    def _create_directory_commands(self):
        """
        Create commands for setting up required directories.

        Returns:
            list: List of directory setup commands
        """
        return [
            f"mkdir -p imp{self.short_uuid()}/stageout",
            f"mkdir -p imp{self.short_uuid()}/logs",
            f"cd imp{self.short_uuid()}"
        ]

    def _create_symlink_commands(self):
        """
        Create symbolic link commands for code and inputs.

        Returns:
            list: List of symlink commands
        """
        commands = []

        # Link to code directory if image exists
        image = self.image()
        if image:
            commands.append(f"ln -s ../imp{image.short_uuid()} code")

        # Link to input impressions
        start_time = time.time()
        alias_list, alias_map = self.inputs()
        _debug.debug(f"    >>>> >>>> Symlink creation time after inputs retrieval: "
                     f"{time.time() - start_time}")
        _debug.debug("The alias_list is: %s", alias_list)
        for alias in alias_list:
            impression = alias_map[alias]
            commands.append(f"ln -s ../imp{impression[:7]} {alias}")

        return commands

    def _process_user_commands(self):
        """
        Process and prepare user-defined commands with parameter substitution.

        Returns:
            list: List of processed commands ready for execution
        """
        if self.is_input or self.compute_backend() == "htcondorcern":
            return []

        raw_commands = self.yaml_file.read_variable("commands", [])
        if not raw_commands:
            img = self.image()
            if img is not None:
                raw_commands = img.yaml_file.read_variable("commands", [])
        processed_commands = []

        for i, command in enumerate(raw_commands):
            command = self._substitute_parameters(command)
            command = self._substitute_inputs(command)
            command = self._substitute_paths(command)
            command = "{{ " + command + " ; }} >> " + f"logs/celebi_user_step{i}.log 2>&1"
            processed_commands.append(command.replace("\"", "\\\""))

        return processed_commands

    def _substitute_parameters(self, command):
        """
        Replace parameter placeholders in command with actual values.

        Args:
            command (str): Command string with parameter placeholders

        Returns:
            str: Command with parameters substituted
        """
        parameters, values = self.parameters()
        for parameter in parameters:
            value = values[parameter]
            placeholder = "${" + parameter + "}"
            command = command.replace(placeholder, value)
        return command

    def _substitute_inputs(self, command):
        """
        Replace input alias placeholders in command with impression paths.

        Args:
            command (str): Command string with input placeholders

        Returns:
            str: Command with inputs substituted
        """
        alias_list, alias_map = self.inputs()
        for alias in alias_list:
            impression = alias_map[alias]
            placeholder = "${" + alias + "}"
            command = command.replace(placeholder, f"../imp{impression[:7]}")
        return command

    def _substitute_paths(self, command):
        """
        Replace workspace and code path placeholders in command.

        Args:
            command (str): Command string with path placeholders

        Returns:
            str: Command with paths substituted
        """
        command = command.replace("${workspace}", "..")
        command = command.replace("${output}", f"imp{self.short_uuid()}")

        image = self.image()
        if image:
            command = command.replace("${code}", f"../imp{image.short_uuid()}")

        return command

    def _create_step_metadata(self):
        """
        Create step metadata including environment, memory, and compute backend.

        Returns:
            dict: Dictionary containing step metadata
        """
        if self.is_input or self.environment() == "datalist":
            environment = self.default_environment()
        elif self.environment() == "lhcb_ap_datalist":
            environment = self.lhcb_ap_environment()
        else:
            environment = self.environment()
        compute_backend = self.compute_backend()

        step = {
            "environment": environment,
            "memory": self.memory(),
            "compute_backend": compute_backend if compute_backend != "unsigned" else None,
            "name": f"step{self.short_uuid()}",
            "output": f"{self.short_uuid()}.done"
        }

        return step

    def _get_step_inputs(self):
        """
        Get list of input dependencies for this step.

        Returns:
            list: List of input file dependencies
        """
        if self.is_input:
            return ["setup.done"]

        inputs = ["setup.done"]

        # Add input impression dependencies
        alias_list, alias_map = self.inputs()
        for alias in alias_list:
            impression = alias_map[alias]
            inputs.append(f"{impression[:7]}.done")

        # Add image dependency
        image = self.image()
        if image:
            inputs.append(f"{image.short_uuid()}.done")

        return inputs

    def _cache_source(self, backend_type, workflow_machine_id=None):
        """The runner-side cache location for this job, or None.

        reana -> the EOS mount; ssh -> the runner's managed impressions
        dir; native/dry -> None (no runner-side cache).
        """
        if backend_type in ("ssh", "ihep"):
            from Yuki.kernel.runners import config as runner_config
            # The cache lives on the runner the WORKFLOW runs on, not on
            # the runner that produced the input.
            machine_id = workflow_machine_id or self.machine_id
            settings = runner_config.get_ssh_settings(
                runner_config.open_config(), machine_id)
            base = settings.get("remote_workdir", "/tmp/yuki-workflows")
            return f"{base}/impressions/{self.project_uuid}/{self.impression()}/"
        if backend_type == "reana":
            config_path = os.path.join(os.environ["HOME"], ".Yuki", "config.json")
            eos_mount_points = metadata.ConfigFile(config_path).read_variable("eos_mount_point", {})
            return (eos_mount_points.get(self.machine_id, "/eos/user/unknown")
                    + f"/{self.project_uuid}/{self.impression()}/")
        return None

    def _cache_commands(self, _request_machine_id, backend_type):
        """Cache stageout outputs on the runner (EOS for reana, the
        runner's impressions dir for ssh). Empty for native/dry."""
        if self.is_input or not self.cache_on_runner():
            return []
        cache_path = self._cache_source(backend_type)
        if not cache_path:
            return []
        if backend_type not in ("ssh", "ihep"):
            return [f"mkdir -p {cache_path}",
                    f"cp -r stageout/* {cache_path}"]

        # A workflow may still be executing on the runner after Yuki has
        # fenced it as failed.  Such a zombie must never publish its output
        # into the shared impression cache.  Rebuild the exact cache path,
        # write the completion marker last, and clean up every failed copy.
        cache_path = cache_path.rstrip("/")
        cache = shlex.quote(cache_path)
        complete = shlex.quote(os.path.join(cache_path,
                                             CACHE_COMPLETE_MARKER))
        in_progress = shlex.quote(os.path.join(
            cache_path, CACHE_IN_PROGRESS_MARKER))
        failed = shlex.quote(f"../{WORKFLOW_FAILED_MARKER}")
        cleanup = (f"chmod -R u+w {cache} 2>/dev/null || true; "
                   f"rm -rf -- {cache}")
        publish = (
            f"if [ -e {failed} ]; then "
            f"echo 'Skipping cache publication: workflow is failed'; "
            f"else "
            f"if [ -e {cache} ]; then {cleanup}; fi; "
            f"mkdir -p {cache}; : > {in_progress}; "
            f"if cp -r stageout/. {cache}/; then "
            f"if [ -e {failed} ]; then {cleanup}; "
            f"echo 'Discarded cache publication: workflow became failed'; "
            f"else rm -f {in_progress}; : > {complete}; "
            f"chmod -R a-w {cache}; fi; "
            f"else {cleanup}; exit 1; fi; "
            f"fi"
        )
        return [publish]

    def setup_commands(self, backend_type="reana", workflow_machine_id=None):
        """Generate commands to set up the container environment from the
        runner-side cache (EOS on reana, the impressions dir on ssh)."""
        cache_path = self._cache_source(backend_type, workflow_machine_id)
        commands = [f"mkdir -p imp{self.short_uuid()}/stageout"]
        if cache_path:
            if backend_type in ("ssh", "ihep"):
                # The cache lives on the same runner as the workflow, so
                # link each entry into stageout instead of copying. The
                # cached data was made read-only when written.
                commands.append(f"ln -s {cache_path}* "
                                f"imp{self.short_uuid()}/stageout/")
            else:
                commands.append(f"cp -r {cache_path}* "
                                f"imp{self.short_uuid()}/stageout/")
        return commands

    def finalize_commands(self, backend_type="reana"):
        """Generate commands to clean up the container environment."""
        del backend_type  # finalize is backend-independent
        commands = []
        commands.append(f"rm -rf imp{self.short_uuid()}/stageout")
        return commands

    def environment(self):
        """
        Get the container environment configuration.

        Returns:
            str: Environment specification from YAML configuration
        """
        return self.yaml_file.read_variable("environment", "")

    def memory(self):
        """
        Get the memory limit for the container.

        Returns:
            str: Kubernetes memory limit specification
        """
        memory_limit = self.yaml_file.read_variable("memory_limit", "")
        if memory_limit:
            return memory_limit
        return self.yaml_file.read_variable("kubernetes_memory_limit", "4096Mi")

    def compute_backend(self):
        """
        Get the compute backend for the container.

        Returns:
            str: Compute backend specification from YAML configuration
        """
        return self.yaml_file.read_variable("compute_backend", "unsigned")

    def cvmfs(self):
        """
        Get the list of CVMFS repositories required by this job.

        Returns:
            list: List of CVMFS repository names
        """
        return self.yaml_file.read_variable("cvmfs", [])

    def parameters(self):
        """
        Read the parameters from the YAML configuration file.

        Returns:
            tuple: A tuple containing (sorted_parameter_keys, parameters_dict)
        """
        start_time = time.time()
        parameters = self.yaml_file.read_variable("parameters", {})
        sorted_keys = sorted(parameters.keys())
        _debug.debug(f"    >>>> >>>> Parameters retrieval time: {time.time() - start_time}")
        return sorted_keys, parameters

    def outputs(self):
        """
        Get the list of output files for this container.

        Returns:
            list: List of output file paths relative to the output root.
        """
        if self.machine_id is None:
            path = os.path.join(self.path, "rawdata")
            if not os.path.exists(path):
                return []
            return [rel for rel, _ in walk_files(path)]
        path = os.path.join(self.path, self.machine_id, "stageout")
        if not os.path.exists(path):
            return []
        return [rel for rel, _ in walk_files(path)]

    def _create_datalist_generator(self):
        """
        Create the generate_datalist.py script with embedded datalist content.

        The datalist entries are embedded directly in the script since celebi.yaml
        is not uploaded to the workflow environment.

        Returns:
            str: Path to the created generator script
        """
        datalist = self.yaml_file.read_variable("datalist", [])
        # Escape the datalist for embedding in Python script
        datalist_repr = repr(datalist)

        script_content = f'''#!/usr/bin/env python3
"""Generate dataList.txt from embedded datalist entries."""

# Embedded datalist from celebi.yaml
DATALIST = {datalist_repr}

# Write dataList.txt
with open('stageout/dataList.txt', 'w') as f:
    for path in DATALIST:
        f.write(path + '\\n')

print(f"Generated dataList.txt with {{len(DATALIST)}} entries")
'''
        # Write script to contents directory
        script_path = os.path.join(self.path, "contents", "generate_datalist.py")
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(script_content)
        # Make executable
        os.chmod(script_path, 0o755)
        return script_path

    def _create_lhcb_ap_datalist_generator(self):
        """
        Create the generate_lhcb_ap_datalist.py script that queries LHCb AP tool.

        Reads the ap_config from celebi.yaml, reads the apd_token from the
        deposited config.local.json, creates lbapi_key.json in contents, and
        generates a Python script that:
        1. Sets up AP authentication via lbapi_key.json
        2. Queries datasets using apd.get_analysis_data()
        3. Filters using the returned datasets() callable
        4. Writes the resulting paths to stageout/dataList.txt

        Returns:
            str: Path to the created generator script
        """
        ap_config = self.yaml_file.read_variable("ap_config", {})
        gda = ap_config.get("get_analysis_data", {})
        ds = ap_config.get("datasets", {})

        # Build the positional and keyword arguments for the script
        gda_args = repr(gda.get("args", []))
        gda_kwargs = repr(gda.get("kwargs", {}))
        ds_kwargs = repr(ds.get("kwargs", {}))

        # Read the APD token from the deposited config.local.json and write
        # lbapi_key.json inside the job contents.
        token = ""
        local_config_path = os.path.join(self.path, "config.local.json")
        if os.path.exists(local_config_path):
            local_config = metadata.ConfigFile(local_config_path)
            token = local_config.read_variable("apd_token", "")
        key_path = os.path.join(self.path, "contents", "lbapi_key.json")
        if token:
            with open(key_path, "w", encoding="utf-8") as f:
                f.write(f'"{token}"')

        script_content = f'''#!/usr/bin/env python3
"""Generate dataList.txt by querying the LHCb Analysis Productions tool."""

import apd

# Query datasets from AP
datasets = apd.get_analysis_data(*{gda_args}, **{gda_kwargs}, metadata_cache=".", data_cache=".")
paths = datasets(**{ds_kwargs})

# Strip the EOS protocol prefix from paths if present
prefix = "root://eoslhcb.cern.ch/"
clean_paths = [p[len(prefix):] if p.startswith(prefix) else p for p in paths]

# Write dataList.txt
with open("stageout/dataList.txt", "w") as f:
    for p in clean_paths:
        f.write(str(p) + "\\n")
print(f"Generated dataList.txt with {{len(clean_paths)}} entries")
'''
        # Write script to contents directory
        script_path = os.path.join(self.path, "contents", "generate_lhcb_ap_datalist.py")
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(script_content)
        # Make executable
        os.chmod(script_path, 0o755)
        return script_path

    def files(self):
        """
        Get list of files in this job, including datalist generators if applicable.

        Returns:
            list: List of file paths
        """
        file_list = super().files()
        if self.environment() == "datalist":
            # Create the generator script
            self._create_datalist_generator()
            # Add it to the file list (format: short_uuid/filename)
            file_list.append(f"{self.short_uuid()}/generate_datalist.py")
        if self.environment() == "lhcb_ap_datalist":
            # Create the AP datalist generator script (also writes lbapi_key.json)
            self._create_lhcb_ap_datalist_generator()
            # Add them to the file list (format: short_uuid/filename)
            file_list.append(f"{self.short_uuid()}/generate_lhcb_ap_datalist.py")
            if os.path.exists(os.path.join(self.path, "contents", "lbapi_key.json")):
                file_list.append(f"{self.short_uuid()}/lbapi_key.json")
        return file_list
