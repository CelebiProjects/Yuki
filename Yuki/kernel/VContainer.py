"""
Virtual Container module for Yuki kernel.

This module contains the VContainer class which represents a container-based job
that extends VJob functionality with container-specific operations like
environment management, command execution, and input/output handling.
"""
import os
from CelebiChrono.utils import csys
from Yuki.kernel.VJob import VJob
from Yuki.kernel.VImage import VImage

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
        super().__init__(path, machine_id)

    def inputs(self):
        """
        Get input data aliases and their corresponding impressions.

        Returns:
            tuple: A tuple containing (alias_keys, alias_to_impression_map)
        """
        alias_to_imp = self.config_file.read_variable("alias_to_impression", {})
        print(alias_to_imp)
        return (alias_to_imp.keys(), alias_to_imp)

    def image(self):
        """
        Get the VImage instance from predecessor algorithm jobs.

        Returns:
            VImage or None: The image associated with predecessor algorithm jobs
        """
        predecessors = self.predecessors()
        for pred_job in predecessors:
            if pred_job.job_type() == "algorithm":
                return VImage(pred_job.path, self.machine_id)
        return None

    def step(self):
        """
        Generate a step configuration for REANA workflow execution.

        Returns:
            dict: A dictionary containing step configuration with commands,
                  environment, memory limits, and other execution parameters
        """
        commands = [f"mkdir -p imp{self.short_uuid()}/outputs"]
        commands.append(f"mkdir -p imp{self.short_uuid()}/logs")
        commands.append(f"cd imp{self.short_uuid()}")

        # Add ln -s $REANA_WORKSPACE/{alias} {alias} to the commands
        image = self.image()
        if image:
            # commands.append(f"ln -s $REANA_WORKSPACE/imp{image.short_uuid()} code")
            commands.append(f"ln -s ../imp{image.short_uuid()} code")
        alias_list, alias_map = self.inputs()
        for alias in alias_list:
            impression = alias_map[alias]
            # command = f"ln -s $REANA_WORKSPACE/imp{impression[:7]} {alias}"
            command = f"ln -s ../imp{impression[:7]} {alias}"
            commands.append(command)

        if self.is_input:
            raw_commands = []
        else:
            raw_commands = self.image().yaml_file.read_variable("commands", [])

        if self.compute_backend() == "htcondorcern":
            raw_commands = []

        for i, command in enumerate(raw_commands):
            parameters, values = self.parameters()
            for parameter in parameters:
                value = values[parameter]
                name = "${"+ parameter +"}"
                command = command.replace(name, value)

            # Replace the commands (inputs):
            alias_list, alias_map = self.inputs()
            for alias in alias_list:
                impression = alias_map[alias]
                name = "${"+ alias +"}"
                # command = command.replace(name, f"$REANA_WORKSPACE/imp{impression[:7]}")
                command = command.replace(name, f"../imp{impression[:7]}")
            # command = command.replace("${workspace}", "$REANA_WORKSPACE")
            command = command.replace("${workspace}", "..")
            command = command.replace("${output}", f"imp{self.short_uuid()}")
            image = self.image()
            if image:
                # command = command.replace("${code}", f"$REANA_WORKSPACE/imp{image.short_uuid()}")
                command = command.replace("${code}", f"../imp{image.short_uuid()}")
            command = f"{{ " + command + f" ; }} >> logs/chern_user_step{i}.log 2>&1"
            commands.append(command.replace("\"", "\\\""))
        step = {}
        step["commands"] = commands
        # commands.append("cd $REANA_WORKSPACE")
        commands.append("cd ..")
        commands.append(f"touch {self.short_uuid()}.done")
        commands = " && ".join(commands)
        if self.is_input:
            step["environment"] = self.default_environment()
        else:
            step["environment"] = self.environment()
        step["name"] = f"step{self.short_uuid()}"
        compute_backend = self.compute_backend()
        if compute_backend != "unsigned":
            step["compute_backend"] = compute_backend
            step["htcondor_max_runtime"] = "expresso"
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

    def snakemake_rule(self):
        """
        Generate a Snakemake rule configuration for workflow execution.

        Returns:
            dict: A dictionary containing rule configuration including commands,
                  environment, memory, inputs, and outputs for Snakemake workflow
        """
        commands = [f"pwd && echo $REANA_WORKSPACE && mkdir -p imp{self.short_uuid()}/outputs"]
        commands.append(f"mkdir -p imp{self.short_uuid()}/logs")
        commands.append(f"cd imp{self.short_uuid()}")

        # Add ln -s $REANA_WORKSPACE/{alias} {alias} to the commands
        image = self.image()
        if image:
            # commands.append(f"ln -s $REANA_WORKSPACE/imp{image.short_uuid()} code")
            commands.append(f"ln -s ../imp{image.short_uuid()} code")
        print("self.inputs", self.inputs())
        alias_list, alias_map = self.inputs()
        for alias in alias_list:
            impression = alias_map[alias]
            # command = f"ln -s $REANA_WORKSPACE/imp{impression[:7]} {alias}"
            command = f"ln -s ../imp{impression[:7]} {alias}"
            commands.append(command)

        raw_commands = []
        if not self.is_input:
            raw_commands = self.image().yaml_file.read_variable("commands", [])


        if self.compute_backend() == "htcondorcern":
            raw_commands = []

        for i, command in enumerate(raw_commands):
            # Replace the commands (parameters):
            parameters, values = self.parameters()
            for parameter in parameters:
                value = values[parameter]
                name = "${"+ parameter +"}"
                command = command.replace(name, value)

            # Replace the commands (inputs):
            alias_list, alias_map = self.inputs()
            for alias in alias_list:
                impression = alias_map[alias]
                name = "${"+ alias +"}"
                # command = command.replace(name, f"$REANA_WORKSPACE/imp{impression[:7]}")
                command = command.replace(name, f"../imp{impression[:7]}")
            # command = command.replace("${workspace}", "$REANA_WORKSPACE")
            command = command.replace("${workspace}", "..")
            command = command.replace("${output}", f"imp{self.short_uuid()}")
            image = self.image()
            if image:
                # command = command.replace("${code}", f"$REANA_WORKSPACE/imp{image.short_uuid()}")
                command = command.replace("${code}", f"../imp{image.short_uuid()}")
            command = f"{{{{ " + command + f" ; }}}} >> logs/chern_user_step{i}.log 2>&1"
            commands.append(command.replace("\"", "\\\""))
        step = {}
        step["commands"] = commands
        # commands.append("cd $REANA_WORKSPACE")
        commands.append("cd ..")
        commands.append(f"touch {self.short_uuid()}.done")
        environment = self.default_environment() if self.is_input else self.environment()
        step["environment"] = environment
        step["memory"] = self.memory()
        compute_backend = self.compute_backend()
        if compute_backend != "unsigned":
            step["compute_backend"] = compute_backend
        else:
            step["compute_backend"] = None
        step["name"] = f"step{self.short_uuid()}"
        step["output"] = f"{self.short_uuid()}.done"

        step["inputs"] = []
        if not self.is_input:
            alias_list, alias_map = self.inputs()
            for alias in alias_list:
                impression = alias_map[alias]
                step["inputs"].append(f"{impression[:7]}.done")
            image = self.image()
            if image:
                step["inputs"].append(f"{image.short_uuid()}.done")

        return step


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
        parameters = self.yaml_file.read_variable("parameters", {})
        return sorted(parameters.keys()), parameters

    def outputs(self):
        """
        Get the list of output directories for this container.

        Returns:
            list: List of output directory names
        """
        if self.machine_id is None:
            path = os.path.join(self.path, "rawdata")
            return csys.list_dir(path)
        path = os.path.join(self.path, self.machine_id, "outputs")
        if not os.path.exists(path):
            return []
        dirs = csys.list_dir(path)
        return dirs

    # def collect(self, impression):
    #     workflow_id = self.workflow_id()
    #     workflow = VWorkflow(os.path.join(self.path, workflow_id))
    #     workflow.collect(impression)
