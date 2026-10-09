"""REANA Repository Booking module.

Uses the official reana_client library for correct API formatting.
"""
import json
import os
import fnmatch
from logging import getLogger

import yaml

from reana_client.api import client as reana_client
from reana_commons.api_client import BaseAPIClient

from CelebiChrono.utils.message import Message
from CelebiChrono.utils import metadata

from Yuki.kernel.storage import file_types

logger = getLogger("YukiLogger")


DEFAULT_IGNORE_PATTERNS = [
    ".celebi/impressions/*",
    ".celebi/impressions_store/*",
    ".celebi/config.local.json",
    ".git/*",
    "__pycache__/*",
    "*.pyc",
    "*~",
    "*.swp",
    "*.swo",
    "*.~undo-tree~",
    ".DS_Store",
    "*.tmp",
    "*.temp",
]


class ReanaBooker:  # pylint: disable=too-few-public-methods
    """Handles booking (uploading) a project to REANA."""

    def __init__(self, server_url: str, access_token: str, verify_ssl: bool = True,
                 progress_callback=None):
        """Initialize with REANA server URL and access token.

        Args:
            server_url: REANA server URL (e.g., "https://reana.cern.ch")
            access_token: REANA access token for authentication
            verify_ssl: Whether to verify SSL certificates
            progress_callback: Optional callback(text, status) for streaming
                progress updates. If provided, all progress messages are
                emitted through this callback instead of being collected.
        """
        self.server_url = server_url.rstrip("/")
        self.access_token = access_token
        self.verify_ssl = verify_ssl
        self.timeout = 30
        self._progress_callback = progress_callback
        self._progress_messages: list = []

    def _notify(self, text: str, status: str = "normal") -> None:
        """Emit a progress message via callback and/or print.

        Messages are always collected internally. If a progress_callback
        was provided, it is invoked for live streaming. Otherwise the
        message is printed to stdout.

        Args:
            text: Message text to emit.
            status: Message status type ("normal", "success", "error", "warning", etc.).
        """
        self._progress_messages.append((text, status))
        if self._progress_callback is not None:
            self._progress_callback(text, status)
        else:
            print(text, end="", flush=True)

    def _setup_env(self):
        """Set REANA_SERVER_URL environment variable for the client."""
        import urllib3

        os.environ["REANA_SERVER_URL"] = self.server_url
        # Disable SSL verification if requested
        if not self.verify_ssl:
            os.environ["REANA_CLIENT_VERIFY_SSL"] = "false"
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        elif "REANA_CLIENT_VERIFY_SSL" in os.environ:
            del os.environ["REANA_CLIENT_VERIFY_SSL"]
        # Force re-initialization of the API client singleton
        try:
            BaseAPIClient("reana-server")
        except Exception:
            pass

    def book_project(self, project_path: str, project_name: str,  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
                     upload_mode: str = "plots+logs") -> Message:
        """Book a project to REANA.

        Args:
            project_path: Path to the extracted project directory.
            project_name: Name of the project.
            upload_mode: Selection spec for which outputs to upload from Yuki
                storage to [reana_workspace]/impression_data/[impression_id]/.
                One of plots+logs (default), plots, data, data+logs, logs, all,
                or a glob. The legacy "stageout-or-nothing" toggle is gone:
                the upload step always runs and upload_mode decides what.

        Returns:
            Message: Collected progress messages. If a progress_callback
            was provided, messages are also emitted live through it.
        """
        self._progress_messages = []

        if not os.path.isdir(project_path):
            self._notify(f"Invalid project path: {project_path}\n", "error")
            msg = Message()
            msg.messages = self._progress_messages[:]
            return msg

        workflow_name = f"celebi-{project_name}"

        self._setup_env()

        # Check if workflow exists
        workflow = self._get_workflow(workflow_name)
        if workflow is None:
            self._notify(f"Creating REANA workflow '{workflow_name}'...\n", "normal")
            workflow = self._create_workflow(workflow_name, project_path)
            self._notify(f"Workflow created: {workflow_name}\n", "success")
        else:
            self._notify(f"Using existing REANA workflow '{workflow_name}'\n", "success")

        workflow_id = workflow.get(
            "workflow_id", workflow.get("id", workflow.get("name", workflow_name))
        )

        # Download existing reana_repo.yaml for comparison
        old_metadata = None
        if workflow is not None:
            self._notify("Checking for existing reana_repo.yaml...\n", "normal")
            old_repo_yaml = self._download_workspace_file(workflow_id, "reana_repo.yaml")
            if old_repo_yaml:
                old_metadata = yaml.safe_load(old_repo_yaml)
                self._notify("Found existing reana_repo.yaml.\n", "normal")

        # Build new project metadata with impression UUIDs
        new_metadata = self._build_repo_metadata(project_path)
        object_paths = [obj.get("path", "") for obj in new_metadata.get("objects", [])]

        # Determine unchanged objects (same path + impression UUID)
        skip_prefixes = set()
        if old_metadata and "objects" in old_metadata:
            old_impressions = {
                obj.get("path", ""): obj.get("impression", "")
                for obj in old_metadata["objects"]
                if obj.get("path")
            }
            for obj in new_metadata.get("objects", []):
                path = obj.get("path", "")
                impression = obj.get("impression", "")
                if path and impression and old_impressions.get(path) == impression:
                    skip_prefixes.add(path)
            if skip_prefixes:
                self._notify(
                    f"{len(skip_prefixes)} object(s) unchanged, will skip re-upload.\n",
                    "normal"
                )

        # If reusing existing workflow, clear old folders first
        if workflow is not None:
            cleared = self._clear_old_folders(workflow_id, old_metadata, new_metadata)
            if cleared:
                self._notify("Cleared old folders from workspace.\n", "normal")

        # Upload files, skipping unchanged objects
        self._notify("Uploading project files...\n", "normal")
        try:
            self._upload_files(workflow_id, project_path,
                                skip_prefixes=skip_prefixes,
                                object_paths=object_paths)
            # Upload reana_repo.yaml so it can be fetched on next booking
            self._upload_repo_yaml(workflow_id, new_metadata)
            self._notify("Files uploaded successfully.\n", "success")
            self._notify(
                f"REANA workspace: {self.server_url}/api/workflows/{workflow_id}/workspace\n",
                "info",
            )
        except Exception as e:
            self._notify(f"Upload failed: {e}\n", "error")
            msg = Message()
            msg.messages = self._progress_messages[:]
            msg.data["workflow_name"] = workflow_name
            msg.data["workflow_id"] = workflow_id
            msg.data["server_url"] = self.server_url
            return msg

        # Upload selected outputs (plots/data/logs per upload_mode) from Yuki
        # storage. This always runs; upload_mode decides what gets uploaded.
        try:
            self._upload_stageout_files(workflow_id, project_path, new_metadata,
                                        upload_mode=upload_mode)
        except Exception as e:
            logger.warning("Stageout upload failed: %s", e)
            self._notify(f"Stageout upload warning: {e}\n", "warning")

        msg = Message()
        msg.messages = self._progress_messages[:]
        msg.data["workflow_name"] = workflow_name
        msg.data["workflow_id"] = workflow_id
        msg.data["server_url"] = self.server_url
        return msg

    def _get_workflow(self, name: str):
        """Get workflow by name, or None if not found.

        Uses get_workflow_status instead of get_workflows to avoid
        parameters (type, search, size) that older REANA servers reject.
        """
        try:
            status = reana_client.get_workflow_status(
                workflow=name,
                access_token=self.access_token,
            )
            # get_workflow_status returns a dict with workflow info
            return status
        except Exception:
            # Workflow not found or other error
            return None

    def _create_workflow(self, name: str, project_path: str = ""):
        """Create a new minimal workflow on REANA."""
        spec_path = os.path.join(
            os.path.dirname(__file__), "reana_booking_spec.yaml"
        )
        with open(spec_path, "r", encoding="utf-8") as f:
            reana_specification = yaml.safe_load(f)

        # Inject project structure metadata if project_path is provided
        if project_path and os.path.isdir(project_path):
            reana_specification["reana_repo"] = self._build_repo_metadata(project_path)

        result = reana_client.create_workflow(
            reana_specification=reana_specification,
            name=name,
            access_token=self.access_token,
        )

        if not result.get("workflow_id") and not result.get("workflow_name"):
            raise RuntimeError(f"Workflow creation failed: {result}")

        return result

    def _build_repo_metadata(self, project_path: str) -> dict:  # pylint: disable=too-many-locals
        """Build project structure metadata for the reana_repo field.

        Walks the project directory and records Celebi objects
        (tasks, algorithms, directories) with their metadata.

        Args:
            project_path: Path to the Celebi project directory.

        Returns:
            dict: Project structure metadata.
        """
        repo_metadata = {
            "project_name": os.path.basename(os.path.normpath(project_path)),
            "description": "Celebi project structure catalog",
            "objects": [],
        }

        for root, dirs, _files in os.walk(project_path):  # pylint: disable=too-many-nested-blocks
            # Skip ignored directories
            dirs[:] = [
                d for d in dirs
                if not self._should_ignore(
                    os.path.relpath(os.path.join(root, d), project_path)
                )
            ]

            for d in dirs:
                dir_path = os.path.join(root, d)
                config_path = os.path.join(dir_path, ".celebi", "config.json")
                if not os.path.exists(config_path):
                    continue

                # Use TwoTierConfigFile to read both config.json and
                # config.local.json (local takes precedence)
                config_file = metadata.TwoTierConfigFile(config_path)
                obj_type = config_file.read_variable("object_type", "")
                if not obj_type:
                    continue

                rel_path = os.path.relpath(dir_path, project_path)
                obj_entry = {
                    "path": rel_path,
                    "type": obj_type,
                    "impression": config_file.read_variable("impression", ""),
                }

                # Read celebi.yaml for tasks and algorithms
                if obj_type in ("task", "algorithm"):
                    celebi_yaml_path = os.path.join(dir_path, "celebi.yaml")
                    if os.path.exists(celebi_yaml_path):
                        try:
                            with open(celebi_yaml_path, "r", encoding="utf-8") as f:
                                celebi_meta = yaml.safe_load(f) or {}
                            if "descriptor" in celebi_meta:
                                obj_entry["descriptor"] = celebi_meta["descriptor"]
                            if "environment" in celebi_meta:
                                obj_entry["environment"] = celebi_meta["environment"]
                            if "memory_limit" in celebi_meta:
                                obj_entry["memory_limit"] = celebi_meta["memory_limit"]
                        except (yaml.YAMLError, OSError):
                            pass

                repo_metadata["objects"].append(obj_entry)

        # Sort by path for deterministic output
        repo_metadata["objects"].sort(key=lambda x: x["path"])

        return repo_metadata

    def _get_object_prefix_for_file(self, relative_path: str, object_paths: list) -> str:
        """Find which object path a file belongs to.

        Args:
            relative_path: File path relative to project root.
            object_paths: List of object directory paths.

        Returns:
            The matching object path, or empty string for top-level files.
        """
        for obj_path in sorted(object_paths, key=len, reverse=True):
            if relative_path == obj_path or relative_path.startswith(obj_path + "/"):
                return obj_path
        return ""

    def _sanitize_upload_path(self, relative_path: str) -> str:
        """Convert hidden directory names to readable names for REANA upload.

        REANA's REST API may have issues with hidden file paths (e.g., .celebi).
        This method prefixes hidden directory names with "dot_" so they remain
        identifiable while being REST-API friendly.

        Args:
            relative_path: Original relative path from project root.

        Returns:
            str: Sanitized path with hidden directory names prefixed by dot_.

        Examples:
            .celebi/config.json -> dot_celebi/config.json
            tasks/.hidden/file.txt -> tasks/dot_hidden/file.txt
            src/main.py -> src/main.py
        """
        parts = relative_path.replace(os.sep, "/").split("/")
        sanitized = []
        for part in parts[:-1]:  # All but the file name
            if part.startswith(".") and len(part) > 1:
                sanitized.append(f"dot_{part[1:]}")
            else:
                sanitized.append(part)
        sanitized.append(parts[-1])  # Keep file name unchanged
        return "/".join(sanitized)

    def _upload_files(self, workflow_id: str, project_path: str,  # pylint: disable=too-many-locals,too-many-branches
                      skip_prefixes: set = None, object_paths: list = None):
        """Upload project files to REANA workflow workspace.

        Args:
            workflow_id: REANA workflow ID.
            project_path: Local project path.
            skip_prefixes: Set of object paths whose files should NOT be
                re-uploaded (impression UUID unchanged).
            object_paths: List of all object paths for prefix matching.
        """
        skip_prefixes = skip_prefixes or set()
        object_paths = object_paths or []

        # First pass: count total files to upload (excluding skipped objects)
        total_files = 0
        skipped_files = 0
        for root, dirs, files in os.walk(project_path):
            dirs[:] = [
                d for d in dirs
                if not self._should_ignore(
                    os.path.relpath(os.path.join(root, d), project_path)
                )
            ]
            for filename in files:
                relative_path = os.path.relpath(os.path.join(root, filename), project_path)
                if self._should_ignore(relative_path):
                    continue
                obj_prefix = self._get_object_prefix_for_file(relative_path, object_paths)
                if obj_prefix and obj_prefix in skip_prefixes:
                    skipped_files += 1
                    continue
                total_files += 1

        if total_files == 0:
            if skipped_files:
                self._notify(
                    f"No files to upload ({skipped_files} files unchanged, skipped).\n",
                    "normal"
                )
            else:
                self._notify("No files to upload.\n", "normal")
            return

        self._notify(
            f"Uploading {total_files} files ({skipped_files} unchanged, skipped)...\n",
            "normal"
        )
        uploaded_count = 0
        failed_count = 0

        for root, dirs, files in os.walk(project_path):
            dirs[:] = [
                d for d in dirs
                if not self._should_ignore(
                    os.path.relpath(os.path.join(root, d), project_path)
                )
            ]

            for filename in files:
                file_path = os.path.join(root, filename)
                relative_path = os.path.relpath(file_path, project_path)

                if self._should_ignore(relative_path):
                    continue

                obj_prefix = self._get_object_prefix_for_file(relative_path, object_paths)
                if obj_prefix and obj_prefix in skip_prefixes:
                    continue

                try:
                    with open(file_path, "rb") as f:
                        file_content = f.read()
                except OSError as e:
                    logger.warning("Skipping unreadable file %s: %s", file_path, e)
                    failed_count += 1
                    continue

                upload_name = self._sanitize_upload_path(relative_path)
                try:
                    reana_client.upload_file(
                        workflow=workflow_id,
                        file_=file_content,
                        file_name=upload_name,
                        access_token=self.access_token,
                    )
                    uploaded_count += 1
                    if uploaded_count % 10 == 0 or uploaded_count == total_files:
                        self._notify(
                            f"  Progress: {uploaded_count}/{total_files} files uploaded...\n",
                            "normal"
                        )
                except Exception as e:
                    logger.warning("Failed to upload %s: %s", relative_path, e)
                    failed_count += 1

        self._notify(
            f"Upload complete: {uploaded_count} succeeded, {failed_count} failed.\n",
            "normal"
        )

    def _upload_repo_yaml(self, workflow_id: str, repo_metadata: dict):
        """Upload reana_repo.yaml to workspace for future cleanup.

        Args:
            workflow_id: REANA workflow ID.
            repo_metadata: Project metadata dict (from _build_repo_metadata).
        """
        yaml_content = yaml.safe_dump(
            repo_metadata,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        ).encode("utf-8")
        self._notify("Uploading reana_repo.yaml...\n", "normal")
        try:
            reana_client.upload_file(
                workflow=workflow_id,
                file_=yaml_content,
                file_name="reana_repo.yaml",
                access_token=self.access_token,
            )
            self._notify("reana_repo.yaml uploaded successfully.\n", "success")
        except Exception as e:
            logger.warning("Failed to upload reana_repo.yaml: %s", e)

    def _upload_stageout_files(self, workflow_id: str, project_path: str,  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
                                repo_metadata: dict, upload_mode: str = "plots+logs"):
        """Upload stageout files from Yuki storage to REANA workspace.

        Looks up stageout files in ~/.Yuki/Storage/{project_uuid}/
        {impression_uuid}/stageout/ and uploads them to the REANA workspace
        at impression_data/{impression_uuid}/stageout/.

        Args:
            workflow_id: REANA workflow ID.
            project_path: Path to the extracted project directory.
            repo_metadata: Project metadata dict with impression UUIDs.
            upload_mode: Selection spec for what to upload (plots/data/all/logs).
        """
        # Read project UUID from project's config
        project_config_path = os.path.join(project_path, ".celebi", "config.json")
        if not os.path.exists(project_config_path):
            self._notify("No project config found. Skipping stageout upload.\n", "normal")
            return

        try:
            with open(project_config_path, "r", encoding="utf-8") as f:
                project_config = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            self._notify(
                f"Failed to read project config: {e}. Skipping stageout upload.\n",
                "normal"
            )
            return

        project_uuid = project_config.get("project_uuid", "")
        if not project_uuid:
            self._notify("No project_uuid in config. Skipping stageout upload.\n", "normal")
            return

        # Yuki storage base path
        yuki_home = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
        storage_base = os.path.join(yuki_home, "Storage", project_uuid)

        if not os.path.isdir(storage_base):
            self._notify(
                f"No storage directory found for project {project_uuid}. "
                "Skipping stageout upload.\n",
                "normal"
            )
            return

        # Collect impression UUIDs from metadata
        impressions = set()
        for obj in repo_metadata.get("objects", []):
            impression = obj.get("impression", "")
            if impression:
                impressions.add(impression)

        if not impressions:
            self._notify("No impression UUIDs found. Skipping stageout upload.\n", "normal")
            return

        self._notify(
            f"Uploading stageout files for {len(impressions)} impression(s)...\n",
            "normal"
        )
        total_uploaded = 0

        tokens = set(upload_mode.split("+"))
        include_logs = "logs" in tokens or "all" in tokens
        if "all" in tokens:
            stageout_spec = "all"
        elif "data" in tokens and "plots" in tokens:
            stageout_spec = "all"
        elif "data" in tokens:
            stageout_spec = "data"
        elif "plots" in tokens:
            stageout_spec = "plots"
        else:
            stageout_spec = None     # logs-only mode uploads no stageout
        stage_pred = (
            file_types.make_predicate(stageout_spec)
            if stageout_spec else (lambda n: False)
        )

        for impression_id in impressions:  # pylint: disable=too-many-nested-blocks
            impression_dir = os.path.join(storage_base, impression_id)
            if not os.path.isdir(impression_dir):
                continue

            # Each impression may have multiple runner subdirectories:
            # Storage/{project_uuid}/{impression_uuid}/{runner_uuid}/stageout/
            for runner_id in os.listdir(impression_dir):
                runner_dir = os.path.join(impression_dir, runner_id)

                # Upload the runner file manifest (filelist) produced by status/
                # collect, independent of upload_mode, so the booked record lists
                # every output even when the large data was not uploaded.
                filelist_path = os.path.join(runner_dir, "stageout.filelist.json")
                if os.path.isfile(filelist_path):
                    manifest_name = (
                        f"impression_data/{impression_id}/stageout.filelist.json")
                    try:
                        with open(filelist_path, "rb") as f:
                            reana_client.upload_file(
                                workflow=workflow_id, file_=f.read(),
                                file_name=manifest_name,
                                access_token=self.access_token)
                        total_uploaded += 1
                    except Exception as e:
                        logger.warning("Failed to upload filelist %s: %s",
                                       manifest_name, e)

                stageout_dir = os.path.join(runner_dir, "stageout")
                if not os.path.isdir(stageout_dir):
                    continue

                # Walk and upload all files in this stageout directory
                for root, _dirs, files in os.walk(stageout_dir):
                    for filename in files:
                        file_path = os.path.join(root, filename)
                        rel_path = os.path.relpath(file_path, stageout_dir)
                        if not stage_pred(rel_path):
                            continue
                        upload_name = (
                            f"impression_data/{impression_id}/"
                            f"stageout/{rel_path}"
                        )

                        try:
                            with open(file_path, "rb") as f:
                                file_content = f.read()
                            reana_client.upload_file(
                                workflow=workflow_id,
                                file_=file_content,
                                file_name=upload_name,
                                access_token=self.access_token,
                            )
                            total_uploaded += 1
                        except Exception as e:
                            logger.warning(
                                "Failed to upload stageout file %s: %s",
                                upload_name, e
                            )

                if include_logs:
                    logs_dir = os.path.join(impression_dir, runner_id, "logs")
                    if os.path.isdir(logs_dir):
                        for root, _dirs, files in os.walk(logs_dir):
                            for filename in files:
                                file_path = os.path.join(root, filename)
                                rel_path = os.path.relpath(file_path, logs_dir)
                                upload_name = (
                                    f"impression_data/{impression_id}/logs/{rel_path}")
                                try:
                                    with open(file_path, "rb") as f:
                                        content = f.read()
                                    reana_client.upload_file(
                                        workflow=workflow_id, file_=content,
                                        file_name=upload_name, access_token=self.access_token)
                                    total_uploaded += 1
                                except Exception as e:
                                    logger.warning("Failed to upload log %s: %s", upload_name, e)

        if total_uploaded:
            self._notify(
                f"Stageout upload complete: {total_uploaded} file(s) uploaded.\n",
                "normal"
            )
        else:
            self._notify("No stageout files found to upload.\n", "normal")

    def _clear_old_folders(self, workflow_id: str, old_metadata: dict = None,  # pylint: disable=too-many-locals,too-many-branches
                           new_metadata: dict = None) -> bool:
        """Clear old folders from REANA workspace before re-uploading.

        Only deletes files for objects that have been removed or whose
        impression UUID has changed. Objects with matching (path, impression)
        are preserved to avoid unnecessary re-upload.

        Args:
            workflow_id: REANA workflow ID.
            old_metadata: Previous reana_repo.yaml content, or None.
            new_metadata: Current project metadata, or None.

        Returns:
            bool: True if old folders were cleared (or nothing to clear).
        """
        if old_metadata is None:
            self._notify("No previous reana_repo.yaml. Skipping cleanup.\n", "normal")
            return True

        if not old_metadata or "objects" not in old_metadata:
            self._notify("No old folders recorded. Skipping cleanup.\n", "normal")
            return True

        # Build map of old object path -> impression UUID
        old_impressions = {}
        for obj in old_metadata["objects"]:
            path = obj.get("path", "")
            if path:
                old_impressions[path] = obj.get("impression", "")

        # Build map of new object path -> impression UUID
        new_impressions = {}
        if new_metadata and "objects" in new_metadata:
            for obj in new_metadata["objects"]:
                path = obj.get("path", "")
                if path:
                    new_impressions[path] = obj.get("impression", "")

        # Determine which old prefixes to delete:
        # - Objects no longer in new project (deleted)
        # - Objects whose impression UUID changed
        delete_prefixes = set()
        for old_path, old_imp in old_impressions.items():
            if old_path not in new_impressions:
                # Object deleted
                delete_prefixes.add(self._sanitize_upload_path(old_path))
            elif new_impressions.get(old_path) != old_imp:
                # Impression changed
                delete_prefixes.add(self._sanitize_upload_path(old_path))

        if not delete_prefixes:
            self._notify("All objects unchanged. No cleanup needed.\n", "normal")
            return True

        # Get list of current files in workspace
        self._notify("Listing current workspace files...\n", "normal")
        workspace_files = self._list_workspace_files(workflow_id)
        self._notify(f"Found {len(workspace_files)} files in workspace.\n", "normal")

        # Delete files that belong to changed/deleted folders
        files_to_delete = []
        for file_info in workspace_files:
            file_name = file_info.get("name", "")
            if any(
                file_name == prefix or file_name.startswith(prefix + "/")
                for prefix in delete_prefixes
            ):
                files_to_delete.append(file_name)

        if not files_to_delete:
            self._notify("No old files to delete.\n", "normal")
            return True

        self._notify(
            f"Deleting {len(files_to_delete)} old files from changed objects...\n",
            "normal"
        )
        deleted_count = 0
        for idx, file_name in enumerate(files_to_delete, 1):
            try:
                self._notify(
                    f"  [{idx}/{len(files_to_delete)}] Deleting {file_name}...\n",
                    "normal"
                )
                self._delete_workspace_file(workflow_id, file_name)
                deleted_count += 1
            except Exception as e:
                logger.warning("Failed to delete %s: %s", file_name, e)

        self._notify(
            f"Deleted {deleted_count}/{len(files_to_delete)} old files.\n",
            "normal"
        )
        return True

    def _download_workspace_file(self, workflow_id: str, file_name: str):
        """Download a file from REANA workflow workspace.

        Args:
            workflow_id: REANA workflow ID.
            file_name: Name/path of the file to download.

        Returns:
            str or None: File content as string, or None if not found.
        """
        try:
            content, _filename, _is_zip = reana_client.download_file(
                workflow=workflow_id,
                file_name=file_name,
                access_token=self.access_token,
            )
            if isinstance(content, bytes):
                return content.decode("utf-8")
            return content
        except Exception:
            return None

    def _list_workspace_files(self, workflow_id: str) -> list:
        """List files in REANA workflow workspace.

        Args:
            workflow_id: REANA workflow ID.

        Returns:
            list: List of file info dicts with 'name' keys.
        """
        try:
            return reana_client.list_files(
                workflow=workflow_id,
                access_token=self.access_token,
            ) or []
        except Exception:
            return []

    def _delete_workspace_file(self, workflow_id: str, file_name: str):
        """Delete a file from REANA workflow workspace.

        Args:
            workflow_id: REANA workflow ID.
            file_name: Name/path of the file to delete.

        Raises:
            Exception: On deletion failure.
        """
        reana_client.delete_file(
            workflow=workflow_id,
            file_name=file_name,
            access_token=self.access_token,
        )

    def _should_ignore(self, relative_path: str) -> bool:
        """Check if a relative path should be ignored during upload."""
        normalized = relative_path.replace(os.sep, "/")
        for pattern in DEFAULT_IGNORE_PATTERNS:
            if fnmatch.fnmatch(normalized, pattern):
                return True
        return False
