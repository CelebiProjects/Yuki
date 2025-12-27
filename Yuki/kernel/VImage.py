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

    def _setup_directory_commands(self):
        """
        Generate commands to create and navigate to the impression directory.

        Returns:
            list: Commands to create and change to the impression directory
        """
        return [
            f"mkdir -p imp{self.short_uuid()}",
            f"cd imp{self.short_uuid()}"
        ]

    def _create_symlink_commands(self):
        """
        Generate commands to create symbolic links for input aliases.

        Returns:
            list: Commands to create symbolic links to input impressions
        """
        commands = []
        alias_list, alias_map = self.inputs()
        for alias in alias_list:
            impression = alias_map[alias]
            command = f"ln -s ../imp{impression[:7]} {alias}"
            commands.append(command)
        return commands

    def _process_build_rules(self):
        """
        Process and substitute variables in build rules.

        Returns:
            list: Processed build commands with substituted variables
        """
        commands = []
        compile_rules = self.yaml_file.read_variable("build", [])
        alias_list, alias_map = self.inputs()

        for rule in compile_rules:
            rule = rule.replace("${workspace}", "..")
            rule = rule.replace("${code}", f"../imp{self.short_uuid()}")

            for alias in alias_list:
                impression = alias_map[alias]
                rule = rule.replace("${"+ alias +"}", f"../imp{impression[:7]}")
            commands.append(rule)

        return commands

    def _cleanup_commands(self):
        """
        Generate cleanup commands to return to parent directory and mark completion.

        Returns:
            list: Commands to navigate back and create completion marker
        """
        return [
            "cd ..",
            f"touch {self.short_uuid()}.done"
        ]

    def _generate_all_commands(self):
        """
        Generate all build commands by combining setup, symlinks, build rules, and cleanup.

        Returns:
            list: Complete list of shell commands to build the image
        """
        commands = []
        commands.extend(self._setup_directory_commands())
        commands.extend(self._create_symlink_commands())
        commands.extend(self._process_build_rules())
        commands.extend(self._cleanup_commands())
        return commands

    def step(self, request_machine):
        """
        Generate a step configuration for REANA workflow execution.

        Returns:
            dict: A dictionary containing step configuration with commands,
                  environment, memory limits, and other execution parameters
        """
        return {
            "inputs": ["setup.done"],
            "commands": self._generate_all_commands(),
            "environment": self.environment(),
            "memory": self.memory(),
            "name": f"step{self.short_uuid()}"
        }

    def snakemake_rule(self, request_machine):
        """
        Generate a Snakemake rule configuration for workflow execution.

        Returns:
            dict: A dictionary containing rule configuration including commands,
                  environment, memory, inputs, and name for Snakemake workflow
        """
        return {
            "inputs": ["setup.done"],
            "commands": self._generate_all_commands(),
            "environment": self.environment(),
            "memory": self.memory(),
            "compute_backend": None,
            "name": f"step{self.short_uuid()}"
        }

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
