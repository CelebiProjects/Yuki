"""
Virtual Image module for Yuki kernel.

This module contains the VImage class which represents a container image job
that extends VJob functionality with image building, environment management,
and workflow execution capabilities. A image can be determined uniquely by its
build configuration and dependencies.
"""
import os

from CelebiChrono.utils import csys
from CelebiChrono.utils import metadata
from Yuki.kernel.VJob import VJob
# from Yuki.kernel.VWorkflow import VWorkflow
class VImage(VJob):
    """
    Virtual Image class that extends VJob for container image operations.

    This class handles container image building, environment configuration,
    dependency management, and workflow step generation for image-based jobs.
    """

    def __init__(self, path, machine_id):
        """
        Initialize a VImage instance.

        Args:
            path (str): Path to the image job
            machine_id (str): Identifier for the target machine
        """
        super().__init__(path, machine_id)

    def inputs(self):
        """
        Get input data aliases and their corresponding impressions.

        Returns:
            tuple: A tuple containing (alias_keys, alias_to_impression_map)
        """
        print("Check the inputs of the image")
        alias_to_imp = self.config_file.read_variable("alias_to_impression", {})
        print(alias_to_imp)
        return (alias_to_imp.keys(), alias_to_imp)

    def image_id(self):
        """
        Get the image ID for a built image from run directories.

        Returns:
            str: The image ID if found and built, empty string otherwise
        """
        dirs = csys.list_dir(self.path)
        for run in dirs:
            if run.startswith("run."):
                config_file = metadata.ConfigFile(os.path.join(self.path, run, "status.json"))
                status = config_file.read_variable("status", "submitted")
                if status == "built":
                    return config_file.read_variable("image_id")
        return ""

    def step(self):
        """
        Generate a step configuration for REANA workflow execution.

        Returns:
            dict: A dictionary containing step configuration with commands,
                  environment, memory limits, and other execution parameters
        """
        commands = [f"mkdir -p imp{self.short_uuid()}"]
        commands.append(f"cd imp{self.short_uuid()}")

        # Add ln -s $REANA_WORKSPACE/{alias} {alias} to the commands
        alias_list, alias_map = self.inputs()
        for alias in alias_list:
            impression = alias_map[alias]
            # command = f"ln -s $REANA_WORKSPACE/imp{impression[:7]} {alias}"
            command = f"ln -s ../imp{impression[:7]} {alias}"
            commands.append(command)

        compile_rules = self.yaml_file.read_variable("build", [])
        for rule in compile_rules:
            # Replace the ${code} with the code path
            # rule = rule.replace("${workspace}", "$REANA_WORKSPACE")
            rule = rule.replace("${workspace}", "..")
            # rule = rule.replace("${code}", f"$REANA_WORKSPACE/imp{self.short_uuid()}")
            rule = rule.replace("${code}", f"../imp{self.short_uuid()}")

            alias_list, alias_map = self.inputs()
            for alias in alias_list:
                impression = alias_map[alias]
                # rule = rule.replace("${"+ alias +"}", f"$REANA_WORKSPACE/imp{impression[:7]}")
                rule = rule.replace("${"+ alias +"}", f"../imp{impression[:7]}")
            commands.append(rule)

        # commands.append("cd $REANA_WORKSPACE")
        commands.append("cd ..")
        commands.append(f"touch {self.short_uuid()}.done")
        step = {}
        step["inputs"] = []
        step["commands"] = commands
        step["environment"] = self.environment()
        step["memory"] = self.memory()
        step["name"] = f"step{self.short_uuid()}"
        return step

    def snakemake_rule(self):
        """
        Generate a Snakemake rule configuration for workflow execution.

        Returns:
            dict: A dictionary containing rule configuration including commands,
                  environment, memory, inputs, and name for Snakemake workflow
        """
        commands = [f"mkdir -p imp{self.short_uuid()}"]
        commands.append(f"cd imp{self.short_uuid()}")

        # Add ln -s $REANA_WORKSPACE/{alias} {alias} to the commands
        alias_list, alias_map = self.inputs()
        for alias in alias_list:
            impression = alias_map[alias]
            # command = f"ln -s $REANA_WORKSPACE/imp{impression[:7]} {alias}"
            command = f"ln -s ../imp{impression[:7]} {alias}"
            commands.append(command)

        compile_rules = self.yaml_file.read_variable("build", [])
        for rule in compile_rules:
            # Replace the ${code} with the code path
            # rule = rule.replace("${workspace}", "$REANA_WORKSPACE")
            rule = rule.replace("${workspace}", "..")
            # rule = rule.replace("${code}", f"$REANA_WORKSPACE/imp{self.short_uuid()}")
            rule = rule.replace("${code}", f"../imp{self.short_uuid()}")

            alias_list, alias_map = self.inputs()
            for alias in alias_list:
                impression = alias_map[alias]
                rule = rule.replace("${"+ alias +"}", f"../imp{impression[:7]}")
                # rule = rule.replace("${"+ alias +"}", f"$REANA_WORKSPACE/imp{impression[:7]}")
            commands.append(rule)

        # commands.append("cd $REANA_WORKSPACE")
        commands.append("cd ..")
        commands.append(f"touch {self.short_uuid()}.done")
        step = {}
        step["inputs"] = []
        step["commands"] = commands
        step["environment"] = self.environment()
        step["memory"] = self.memory()
        step["compute_backend"] = None
        step["name"] = f"step{self.short_uuid()}"

        return step

    def default_environment(self):
        """
        Get the default container environment.

        Returns:
            str: Default Docker environment specification
        """
        return "docker.io/reanahub/reana-env-root6:6.18.04"

    def environment(self):
        """
        Get the container environment configuration.

        Returns:
            str: Environment specification from YAML configuration or default
        """
        environment = self.yaml_file.read_variable("environment", self.default_environment())
        if environment == "script":
            return self.default_environment()
        return environment

    def memory(self):
        """
        Get the memory limit for the container.

        Returns:
            str: Kubernetes memory limit specification
        """
        return self.yaml_file.read_variable("kubernetes_memory_limit", "256Mi")
