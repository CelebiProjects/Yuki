"""
Virtual Container module for Yuki kernel.

This module contains the VContainer class which represents a container-based job
that extends VJob functionality with container-specific operations like
environment management, command execution, and input/output handling.
"""
import os
from CelebiChrono.utils import csys
from CelebiChrono.utils import metadata
from Yuki.kernel.VJob import VJob
from Yuki.kernel.VImage import VImage
import time

class VContainer(VJob):
    """
    Virtual Container class that extends VJob for container-based operations.

    This class handles container lifecycle management, environment setup,
    input/output processing, and command execution within containerized environments.
    """

    def __init__(self, path, machine_id):
        """
        Initialize a VContainer instance.

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
        Get the VImage instance from predecessor algorithm jobs.

        Returns:
            VImage or None: The image associated with predecessor algorithm jobs
        """
        if self._image:
            return self._image
        start_time = time.time()
        predecessors = self.predecessors()
        # print("Predecessors, ", self.predecessors())
        for pred_job in predecessors:
            if pred_job.job_type() == "algorithm":
                print(f"    >>>> >>>> Image retrieval time after finding predecessor: {time.time() - start_time}")
                self._image = VImage(pred_job.path, self.machine_id)
                return self._image
        return None

    def step(self, request_machine_id):
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
        if (not self.is_input) and self.use_eos():
            # print("Using EOS for stageout")
            config_path = os.path.join(os.environ["HOME"], ".Yuki", "config.json")
            eos_mount_points = metadata.ConfigFile(config_path).read_variable("eos_mount_point", {})
            eos_path = eos_mount_points.get(request_machine_id, "/eos/user/unknown")
            commands.append("mkdir -p " + eos_path + f"/{self.project_uuid}/{self.impression()}/")
            commands.append("cp -r stageout/* " + eos_path + f"/{self.project_uuid}/{self.impression()}/")
        commands.append("cd ..")
        commands.append(f"touch {self.short_uuid()}.done")

        step = self._create_reana_step_metadata()
        print(f"    >>>> Step creation time after metadata creation: {time.time() - start_time}")
        # step["commands"] = " && ".join(commands)
        step["commands"] = commands

        return step

    def _process_user_commands_for_reana(self):
        """
        Process and prepare user-defined commands for REANA execution.

        Returns:
            list: List of processed commands ready for REANA execution
        """
        start_time = time.time()
        if self.is_input or self.compute_backend() == "htcondorcern":
            return []

        print(f"    >>>> >>>> User command processing start time: {time.time() - start_time}")

        raw_commands = self.image().yaml_file.read_variable("commands", [])
        processed_commands = []

        print(raw_commands)
        for i, command in enumerate(raw_commands):
            print(f"    >>>> >>>> Processing command {i} start time: {time.time() - start_time}")
            command = self._substitute_parameters(command)
            print(f"    >>>> >>>> After parameter substitution time: {time.time() - start_time}")
            command = self._substitute_inputs(command)
            print(f"    >>>> >>>> After input substitution time: {time.time() - start_time}")
            command = self._substitute_paths(command)
            print(f"    >>>> >>>> After path substitution time: {time.time() - start_time}")
            command = f"{{ " + command + f" ; }} >> logs/celebi_user_step{i}.log 2>&1"
            processed_commands.append(command.replace("\"", "\\\""))

        return processed_commands

    def _create_reana_step_metadata(self):
        """
        Create step metadata for REANA execution.

        Returns:
            dict: Dictionary containing REANA-specific step metadata
        """
        environment = self.default_environment() if self.is_input else self.environment()
        compute_backend = self.compute_backend()

        step = {
            "environment": environment,
            "name": f"step{self.short_uuid()}"
        }

        if self.use_eos() and self.use_kerberos():
            step["kerberos"] = True

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

    def snakemake_rule(self, request_machine_id):
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
        if (not self.is_input) and self.use_eos():
            print("Using EOS for stageout")
            config_path = os.path.join(os.environ["HOME"], ".Yuki", "config.json")
            eos_mount_points = metadata.ConfigFile(config_path).read_variable("eos_mount_point", {})
            eos_path = eos_mount_points.get(request_machine_id, "/eos/user/unknown")
            commands.append("mkdir -p " + eos_path + f"/{self.project_uuid}/{self.impression()}/")
            commands.append("cp -r stageout/* " + eos_path + f"/{self.project_uuid}/{self.impression()}/")
        commands.append("cd ..")
        commands.append(f"touch {self.short_uuid()}.done")

        step = self._create_step_metadata()
        step["commands"] = commands
        step["inputs"] = self._get_step_inputs()

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
        print(f"    >>>> >>>> Symlink creation time after inputs retrieval: {time.time() - start_time}")
        print("The alias_list is:", alias_list)
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

        raw_commands = self.image().yaml_file.read_variable("commands", [])
        processed_commands = []

        for i, command in enumerate(raw_commands):
            command = self._substitute_parameters(command)
            command = self._substitute_inputs(command)
            command = self._substitute_paths(command)
            command = f"{{{{ " + command + f" ; }}}} >> logs/celebi_user_step{i}.log 2>&1"
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
        environment = self.default_environment() if self.is_input else self.environment()
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

    def setup_commands(self):
        config_path = os.path.join(os.environ["HOME"], ".Yuki", "config.json")
        eos_mount_points = metadata.ConfigFile(config_path).read_variable("eos_mount_point", {})
        eos_path = eos_mount_points.get(self.machine_id, "/eos/user/unknown")
        commands = []
        commands.append(f"mkdir -p imp{self.short_uuid()}/stageout")
        commands.append(f"cp -r {eos_path}/{self.project_uuid}/{self.impression()}/* imp{self.short_uuid()}/stageout/")
        return commands

    def finalize_commands(self):
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

    def parameters(self):
        """
        Read the parameters from the YAML configuration file.

        Returns:
            tuple: A tuple containing (sorted_parameter_keys, parameters_dict)
        """
        start_time = time.time()
        parameters = self.yaml_file.read_variable("parameters", {})
        sorted_keys = sorted(parameters.keys())
        print(f"    >>>> >>>> Parameters retrieval time: {time.time() - start_time}")
        return sorted_keys, parameters

    def outputs(self):
        """
        Get the list of output directories for this container.

        Returns:
            list: List of output directory names
        """
        if self.machine_id is None:
            path = os.path.join(self.path, "rawdata")
            return csys.list_dir(path)
        path = os.path.join(self.path, self.machine_id, "stageout")
        if not os.path.exists(path):
            return []
        dirs = csys.list_dir(path)
        return dirs