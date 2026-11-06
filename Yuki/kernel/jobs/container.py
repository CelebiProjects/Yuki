
"""
Virtual Container module for Yuki kernel.

This module contains the ContainerJob class which represents a container-based job
that extends VJob functionality with container-specific operations like
environment management, command execution, and input/output handling.
"""
# pylint: disable=cyclic-import
import logging
import os
import re
import shlex
import time
from CelebiChrono.utils import metadata
from CelebiChrono.utils.lb_run import validated_lb_run_spec
from CelebiChrono.utils.subtasks import read_subtask_count
from .base import VJob
from .image import ImageJob
from ..storage.cache import (
    CACHE_COMPLETE_MARKER, CACHE_IN_PROGRESS_MARKER,
    WORKFLOW_FAILED_MARKER,
)
from ..storage.staging import walk_files

_debug = logging.getLogger("Yuki.kernel")

LB_RUN_EXECUTABLE = (
    "/cvmfs/lhcb.cern.ch/lib/var/lib/LbEnv/stable/linux-64/bin/lb-run"
)
LHCB_CVMFS_REPOSITORY = "lhcb.cern.ch"
LHCB_CVMFS_PATH = f"/cvmfs/{LHCB_CVMFS_REPOSITORY}"
LHCB_NIGHTLY_CVMFS_REPOSITORY = "lhcbdev.cern.ch"
LHCB_NIGHTLY_CVMFS_PATH = f"/cvmfs/{LHCB_NIGHTLY_CVMFS_REPOSITORY}"
LB_CONDA_EXECUTABLE = (
    "/cvmfs/lhcb.cern.ch/lib/var/lib/LbEnv/stable/linux-64/bin/lb-conda"
)
LB_CONDA_PREFIX = "lb-conda/"
LB_CONDA_REANA_ENVIRONMENT = "celebichrono/root-xrootd:6.32.02"


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

    def subtasks(self):
        """Read the repeat count from the frozen task configuration."""
        return read_subtask_count(self.yaml_file)

    def subtask_rule(self, backend_type="reana"):
        """Build one wildcard rule; the task itself keeps a single impression.

        Workspaces are private working directories, not a host security sandbox.
        The literal stageout/index link exposes only this execution's output
        through the supported output interface. Aliases retain the whole input.
        """
        count = self.subtasks()
        short = self.short_uuid()
        root = f"imp{short}"
        index = "{wildcards.index}"
        work = f"{root}/subtasks/{index}"
        self._subtask_index = index
        try:
            commands = [
                f"test {index} -ge 0 && test {index} -lt {count}",
                f"mkdir -p {work}/stageout {root}/stageout/{index} {root}/logs/{index}",
                f"cd {work}",
                f"ln -sfn ../../../stageout/{index} stageout/index",
                f"ln -sfn ../../logs/{index} logs",
            ]
            image = self.image()
            code = f"../../../imp{image.short_uuid()}" if image else "../.."
            commands.append(f"ln -sfn {code} code")
            for alias, impression in self.inputs()[1].items():
                commands.append(
                    f"ln -sfn ../../../imp{impression[:7]} {shlex.quote(alias)}")
            commands.extend([
                "echo running > logs/status",
                "subtask_status=$(pwd)/logs/status",
                "trap 'rc=$?; if [ $rc -eq 0 ]; then echo finished > "
                "\\\"$subtask_status\\\"; else echo failed > "
                "\\\"$subtask_status\\\"; fi' EXIT",
            ])
            commands.extend(self._process_user_commands())
            commands.extend(["cd ../../..", f"touch {root}/subtasks/{index}.done"])
        finally:
            del self._subtask_index
        rule = self._create_step_metadata(backend_type)
        rule.update(commands=commands, inputs=self._get_step_inputs(), cvmfs=self.cvmfs())
        return rule

    def subtask_completion_rule(self, request_machine_id, backend_type):
        """Publish the cache and the existing task marker after every repeat."""
        rule = self._create_step_metadata(backend_type)
        rule["inputs"] = []  # Written as expand(..., range(N)), not N strings.
        rule["commands"] = [
            f"cd imp{self.short_uuid()}",
            *self._cache_commands(request_machine_id, backend_type),
            "cd ..",
            f"touch {self.short_uuid()}.done",
        ]
        return rule

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

        raw_commands, command_yaml = self._user_commands_and_yaml()
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
            command = self._wrap_lb_conda_command(command, command_yaml)
            command = self._wrap_lb_run_command(command, command_yaml)
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
            environment = self._execution_environment()
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

        step = self._create_step_metadata(backend_type)
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

        raw_commands, command_yaml = self._user_commands_and_yaml()
        processed_commands = []

        for i, command in enumerate(raw_commands):
            command = self._substitute_parameters(command)
            command = self._substitute_inputs(command)
            command = self._substitute_paths(command)
            command = self._wrap_lb_conda_command(command, command_yaml)
            command = self._wrap_lb_run_command(command, command_yaml)
            command = "{{ " + command + " ; }} >> " + f"logs/celebi_user_step{i}.log 2>&1"
            processed_commands.append(command.replace("\"", "\\\""))

        return processed_commands

    def _user_commands_and_yaml(self):
        """Return user commands and the YAML file that declared them.

        A task that supplies inline commands replaces the linked algorithm's
        execution contract, including its optional ``lb_run`` declaration.
        Otherwise both commands and ``lb_run`` are inherited from the linked
        algorithm.
        """
        raw_commands = self.yaml_file.read_variable("commands", [])
        if raw_commands:
            return raw_commands, self.yaml_file
        image = self.image()
        if image is None:
            return [], None
        return image.yaml_file.read_variable("commands", []), image.yaml_file

    def _lb_run_spec(self, command_yaml):
        """Validate and resolve an ``lb_run`` declaration.

        Supported schema::

            lb_run:
              project: Moore
              version: v55r12
              platform: x86_64_v2-el9-gcc13-opt  # optional
              nightly: lhcb-head/1234             # optional
        """
        command_environment = ""
        raw = None
        if command_yaml is not None:
            command_environment = command_yaml.read_variable(
                "environment", "")
            raw = command_yaml.read_variable("lb_run", None)
        return validated_lb_run_spec(
            self.environment(), command_environment, raw,
            self._substitute_parameters)

    def _wrap_lb_run_command(self, command, command_yaml):
        """Validate CVMFS and run one command in the configured release."""
        spec = self._lb_run_spec(command_yaml)
        if spec is None:
            return command
        argv = [LB_RUN_EXECUTABLE]
        if spec["platform"]:
            argv.extend(("-c", spec["platform"]))
        if spec["nightly"]:
            argv.extend(("--nightly", spec["nightly"]))
        argv.extend((
            f'{spec["project"]}/{spec["version"]}',
            "bash", "--noprofile", "--norc", "-c", command,
        ))
        invocation = " ".join(shlex.quote(value) for value in argv)
        repository = shlex.quote(LHCB_CVMFS_PATH)
        executable = shlex.quote(LB_RUN_EXECUTABLE)
        checks = (
            f"if ! test -d {repository}; then "
            f"echo 'Celebi lb-run error: CVMFS repository "
            f"{LHCB_CVMFS_PATH} is not available' >&2; exit 127; fi; "
            f"if ! test -x {executable}; then "
            f"echo 'Celebi lb-run error: executable "
            f"{LB_RUN_EXECUTABLE} is not available' >&2; exit 127; fi; "
        )
        if spec["nightly"]:
            nightly_repository = shlex.quote(LHCB_NIGHTLY_CVMFS_PATH)
            checks += (
                f"if ! test -d {nightly_repository}; then "
                f"echo 'Celebi lb-run error: CVMFS repository "
                f"{LHCB_NIGHTLY_CVMFS_PATH} is not available' >&2; "
                "exit 127; fi; "
            )
        return checks + invocation

    def _lb_conda_spec(self):
        """Return ``package/date`` for an LbConda environment declaration.

        LbConda is an LbEnv command wrapper, not a Conda environment that
        Snakemake should resolve or create.
        """
        environment = self.environment()
        if not (environment == "lb-conda" or
                environment.startswith(LB_CONDA_PREFIX)):
            return None
        parts = environment.split("/")
        if len(parts) != 3 or not all(parts):
            raise ValueError(
                "lb-conda environment must use "
                "'lb-conda/[package]/[date]' format")
        _prefix, package, date = parts
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", package) is None:
            raise ValueError(f"invalid lb-conda package: {package}")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date) is None:
            raise ValueError(f"invalid lb-conda date: {date}")
        return f"{package}/{date}"

    def _execution_environment(self, backend_type="reana"):
        """Return the backend environment used to host this task."""
        if self._lb_conda_spec() is not None:
            if backend_type == "reana":
                return LB_CONDA_REANA_ENVIRONMENT
            return self.default_environment()
        if self.environment() == "lb-run":
            return self.default_environment()
        return self.environment()

    def _wrap_lb_conda_command(self, command, command_yaml):
        """Run one user command in a dated CVMFS LbConda environment."""
        environment = self._lb_conda_spec()
        if environment is None:
            return command
        if (command_yaml is not None
                and command_yaml.read_variable("lb_run", None) is not None):
            raise ValueError(
                "lb-conda environment cannot be combined with lb_run")
        argv = (
            LB_CONDA_EXECUTABLE,
            environment,
            "bash", "--noprofile", "--norc", "-c", command,
        )
        invocation = " ".join(shlex.quote(value) for value in argv)
        repository = shlex.quote(LHCB_CVMFS_PATH)
        executable = shlex.quote(LB_CONDA_EXECUTABLE)
        return (
            f"if ! test -d {repository}; then "
            f"echo 'Celebi lb-conda error: CVMFS repository "
            f"{LHCB_CVMFS_PATH} is not available' >&2; exit 127; fi; "
            f"if ! test -x {executable}; then "
            f"echo 'Celebi lb-conda error: executable "
            f"{LB_CONDA_EXECUTABLE} is not available' >&2; exit 127; fi; "
            f"{invocation}"
        )

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
            command = command.replace(placeholder, str(value))
        index = getattr(self, "_subtask_index", None)
        if index is not None:
            command = command.replace("${index}", str(index))
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
            target = alias if hasattr(self, "_subtask_index") else f"../imp{impression[:7]}"
            command = command.replace(placeholder, target)
        return command

    def _substitute_paths(self, command):
        """
        Replace workspace and code path placeholders in command.

        Args:
            command (str): Command string with path placeholders

        Returns:
            str: Command with paths substituted
        """
        if hasattr(self, "_subtask_index"):
            return (command.replace("${workspace}", ".")
                    .replace("${output}", ".").replace("${code}", "code"))
        command = command.replace("${workspace}", "..")
        command = command.replace("${output}", f"imp{self.short_uuid()}")

        image = self.image()
        if image:
            command = command.replace("${code}", f"../imp{image.short_uuid()}")

        return command

    def _create_step_metadata(self, backend_type="reana"):
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
            environment = self._execution_environment(backend_type)
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
                # link each verified entry into stageout instead of copying.
                # Validate again inside the setup rule so deletion after the
                # submission preflight cannot create a literal '*' symlink.
                cache_path = cache_path.rstrip("/")
                cache = shlex.quote(cache_path)
                complete = shlex.quote(os.path.join(
                    cache_path, CACHE_COMPLETE_MARKER))
                in_progress = shlex.quote(os.path.join(
                    cache_path, CACHE_IN_PROGRESS_MARKER))
                destination = shlex.quote(
                    f"imp{self.short_uuid()}/stageout/")
                payload_probe = (
                    f"find {cache} -mindepth 1 -maxdepth 1 "
                    "! -name '.yuki-cache-*' -print -quit")
                commands.append(
                    f"test -d {cache} && test ! -e {in_progress} && "
                    f"(test -f {complete} || "
                    f"{payload_probe} | grep -q .)")
                commands.append(
                    f"find {cache} -mindepth 1 -maxdepth 1 "
                    f"! -name '.yuki-cache-*' -exec ln -s {{}} "
                    f"{destination} \\;")
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
        repositories = self.yaml_file.read_variable("cvmfs", [])
        if repositories is None:
            repositories = []
        if not isinstance(repositories, list):
            raise ValueError("cvmfs must be a list")
        repositories = list(repositories)
        _commands, command_yaml = self._user_commands_and_yaml()
        lb_run_spec = self._lb_run_spec(command_yaml)
        lb_conda_spec = self._lb_conda_spec()
        if ((lb_run_spec is not None or lb_conda_spec is not None)
                and LHCB_CVMFS_REPOSITORY not in repositories):
            repositories.append(LHCB_CVMFS_REPOSITORY)
        if ((lb_conda_spec is not None
                or (lb_run_spec is not None and lb_run_spec["nightly"]))
                and LHCB_NIGHTLY_CVMFS_REPOSITORY not in repositories):
            repositories.append(LHCB_NIGHTLY_CVMFS_REPOSITORY)
        return repositories

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
"""Generate compatible file-list names from embedded datalist entries."""

# Embedded datalist from celebi.yaml
DATALIST = {datalist_repr}

# Keep dataList.txt for existing analyses and expose the new canonical name.
payload = "".join(path + "\\n" for path in DATALIST)
for filename in ("stageout/filelist.txt", "stageout/dataList.txt"):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(payload)

print(f"Generated filelist.txt with {{len(DATALIST)}} entries")
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
