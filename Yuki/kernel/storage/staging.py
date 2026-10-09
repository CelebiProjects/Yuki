"""
File staging utility for local workflow execution.

Handles copying files from Storage to LocalWorkflows (stage in) and
copying results back (stage out) using CoW clones when possible.
"""
import os
import sys
import json
import shutil


def walk_files(top_dir):
    """Yield (rel_path, abs_path) for every regular file under top_dir.

    rel_path is relative to top_dir and uses the platform separator.
    """
    if not os.path.isdir(top_dir):
        return
    for root, _dirs, filenames in os.walk(top_dir):
        for filename in filenames:
            abs_path = os.path.join(root, filename)
            rel_path = os.path.relpath(abs_path, top_dir)
            yield rel_path, abs_path


class FileStager:
    """Handle file staging with CoW-clone optimization."""

    def __init__(self, workflow_path, local_exec_path, project_uuid, logger=None):
        """
        Initialize file stager.

        Args:
            workflow_path: Path to ~/.Yuki/Workflows/<project>/<uuid>/
            local_exec_path: Path to ~/.Yuki/LocalWorkflows/<uuid>/
            project_uuid: Project UUID for accessing Storage
            logger: Optional logger function
        """
        self.workflow_path = workflow_path
        self.local_exec_path = local_exec_path
        self.project_uuid = project_uuid
        self.logger = logger
        self.yuki_home = os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))
        self.storage_dir = os.path.join(self.yuki_home, "Storage", project_uuid)
        self._staged_in = set()  # paths in LocalWorkflows staged by manifest

    def _log(self, message):
        """Log a message (always prints to stdout, also goes to logger if set)."""
        print(message)
        if self.logger:
            self.logger(message)

    @staticmethod
    def _cow_clone(src, dst):
        """Try Copy-on-Write clone via platform-specific APIs.

        Returns True on success, False on failure (caller should clean up dst).
        """
        # macOS: clonefile(2) on APFS
        if sys.platform == "darwin":
            import ctypes
            try:
                libc = ctypes.CDLL("libc.dylib")
                result = libc.clonefile(src.encode(), dst.encode(), 0)
                if result == 0:
                    return True
            except (OSError, AttributeError):
                pass
            return False

        # Linux: FICLONE ioctl (btrfs / XFS reflink)
        if sys.platform == "linux":
            import fcntl
            FICLONE = 0x40049409  # pylint: disable=invalid-name
            try:
                with open(src, 'rb') as s:
                    with open(dst, 'wb') as d:
                        fcntl.ioctl(d.fileno(), FICLONE, s.fileno())
                return True
            except (OSError, IOError):
                try:
                    os.remove(dst)
                except OSError:
                    pass
                return False

        return False

    @staticmethod
    def _link_or_copy(src, dst):
        """Try symlink, then CoW clone, then copy. Always sets dst read-only.

        Input files are expected not to change, so the destination is always
        made read-only regardless of the method.
        """
        # If src and dst are already the same file (e.g. already staged by
        # stage_in), skip silently.
        try:
            if os.path.samefile(src, dst):
                return "Already-staged"
        except OSError:
            pass

        # 1. Try symlink (read-only reference to source)
        try:
            os.symlink(src, dst)
            try:
                os.chmod(dst, 0o444, follow_symlinks=False)
            except (NotImplementedError, OSError):
                pass
            return "Symlinked"
        except OSError:
            pass

        # 2. Try Copy-on-Write clone
        if FileStager._cow_clone(src, dst):
            try:
                os.chmod(dst, 0o444)
            except OSError:
                pass
            return "CoW-cloned"

        # 3. Fall back to regular copy, then make read-only
        shutil.copy2(src, dst)
        try:
            os.chmod(dst, 0o444)
        except OSError:
            pass
        return "Copied"

    def _stage_file(self, src, dst):
        """Stage a single file: CoW clone, fall back to copy.

        Returns:
            Method string ("CoW-cloned", "Copied") or None on failure.
        """
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)

            if os.path.exists(dst):
                os.remove(dst)

            if FileStager._cow_clone(src, dst):
                return "CoW-cloned"

            shutil.copy2(src, dst)
            return "Copied"

        except Exception as e:
            self._log(f"[FILE_STAGING] Error {src} -> {dst}: {e}")
            return None

    def _stage_directory(self, src_dir, dst_dir, skip_src_paths=None):
        """Stage a directory tree (CoW clone or copy).

        Args:
            skip_src_paths: Optional set of source file paths to skip.

        Returns:
            Number of files staged.
        """
        count = 0
        try:
            for root, _dirs, files in os.walk(src_dir):
                rel_root = os.path.relpath(root, src_dir)
                dst_root = os.path.join(dst_dir, rel_root) if rel_root != '.' else dst_dir
                os.makedirs(dst_root, exist_ok=True)

                for file in files:
                    src_file = os.path.join(root, file)
                    if skip_src_paths and src_file in skip_src_paths:
                        continue
                    dst_file = os.path.join(dst_root, file)
                    method = self._stage_file(src_file, dst_file)
                    if method:
                        self._log(f"[FILE_STAGING]   {method}: {file}")
                        count += 1

            return count
        except Exception as e:
            self._log(f"[FILE_STAGING] Error staging directory: {e}")
            return count

    def stage_in(self):  # pylint: disable=too-many-locals,too-many-nested-blocks
        """
        Stage input files from Storage to LocalWorkflows.

        Reads workflow config to get actual job UUIDs, then copies all
        necessary files using CoW clones when possible.

        Returns:
            True if successful, False otherwise
        """
        self._log("[FILE_STAGING] Starting stage-in...")

        try:  # pylint: disable=too-many-nested-blocks
            # Read workflow config to get actual job UUIDs
            config_path = os.path.join(self.workflow_path, "config.json")
            if not os.path.exists(config_path):
                self._log("[FILE_STAGING] No workflow config.json found")
                return False

            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            jobs_info = config.get("jobs_info", {})
            if not jobs_info:
                self._log("[FILE_STAGING] No jobs_info in workflow config")
                return False

            total_jobs = len(jobs_info)
            self._log(f"[FILE_STAGING] Found {total_jobs} jobs in workflow")

            for job_idx, (job_uuid, _job_data) in enumerate(jobs_info.items()):
                self._log(f"[FILE_STAGING] [{job_idx + 1}/{total_jobs}] Processing job: {job_uuid}")

                # Copy job files from Storage
                impression_dir = os.path.join(self.storage_dir, job_uuid)
                if os.path.isdir(impression_dir):
                    # Copy job definition files (contents)
                    contents_dir = os.path.join(impression_dir, "contents")
                    if os.path.isdir(contents_dir):
                        staging_dir = os.path.join(self.local_exec_path, f"imp{job_uuid[:7]}")
                        count = self._stage_directory(contents_dir, staging_dir)
                        self._log(f"[FILE_STAGING] Copied {count} job definition files")

                    # If a stage manifest exists, rawdata and stageout files
                    # are deferred to _process_stage_manifest() for host-side
                    # symlink/CoW-clone resolution. Skip them here to avoid
                    # double-staging.
                    manifest_path = os.path.join(self.local_exec_path, "stage_manifest.json")
                    if not os.path.exists(manifest_path):
                        # Copy rawdata if it exists
                        rawdata_dir = os.path.join(impression_dir, "rawdata")
                        if os.path.isdir(rawdata_dir):
                            staging_stageout = os.path.join(
                                self.local_exec_path, f"imp{job_uuid[:7]}", "stageout"
                            )
                            count = self._stage_directory(rawdata_dir, staging_stageout)
                            self._log(f"[FILE_STAGING] Copied {count} rawdata files")

                        # Copy input files from previous job stageout
                        # Check all machines for this impression
                        machines_dir = os.path.join(impression_dir)
                        for machine_id in os.listdir(machines_dir):
                            machine_path = os.path.join(machines_dir, machine_id)
                            if not os.path.isdir(machine_path):
                                continue

                            stageout_dir = os.path.join(machine_path, "stageout")
                            if os.path.isdir(stageout_dir):
                                staging_stageout = os.path.join(
                                    self.local_exec_path, f"imp{job_uuid[:7]}", "stageout"
                                )
                                count = self._stage_directory(stageout_dir, staging_stageout)
                                if count > 0:
                                    self._log(
                                        f"[FILE_STAGING] Copied {count} input files "
                                        f"from {machine_id}"
                                    )

            self._log("[FILE_STAGING] Stage-in completed")

            # Process stage manifest from native workflow (files deferred to host-side staging)
            self._process_stage_manifest()

            return True

        except Exception as e:
            import traceback
            self._log(f"[FILE_STAGING] Stage-in failed: {e}")
            self._log(f"[FILE_STAGING] Traceback: {traceback.format_exc()}")
            return False

    def _process_stage_manifest(self):
        """
        Process stage_manifest.json created by NativeWorkflow.copy_files_local().

        The manifest records files that should be staged on the host (not from
        inside Docker) so that symlink targets resolve correctly.
        Falls back to reconstructing host-side Storage paths when the Docker-side
        source path doesn't exist.
        """
        manifest_path = os.path.join(self.local_exec_path, "stage_manifest.json")
        if not os.path.exists(manifest_path):
            return

        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)

            entries = manifest.get("entries", [])
            if not entries:
                os.remove(manifest_path)
                return

            self._log(f"[FILE_STAGING] Processing stage manifest: {len(entries)} entries")

            for idx, entry in enumerate(entries):
                dst_path = os.path.join(self.local_exec_path, entry["dst_rel"])
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)

                # Try the Docker-side source path first (works when HOME matches)
                src_path = entry["src_path"]
                resolved_from = "manifest"
                if not os.path.exists(src_path):
                    # Reconstruct from host-side Storage path using the full
                    # relative path captured in dst_rel (imp<short>/stageout/<rel>).
                    short_uuid = entry["job_uuid"][:7]
                    rel_path = os.path.relpath(
                        entry["dst_rel"],
                        os.path.join(f"imp{short_uuid}", "stageout")
                    )
                    if entry["type"] == "rawdata":
                        src_path = os.path.join(
                            self.storage_dir, entry["job_uuid"], "rawdata", rel_path
                        )
                    elif entry["type"] == "input":
                        src_path = os.path.join(
                            self.storage_dir, entry["job_uuid"],
                            entry.get("machine_id", ""), "stageout", rel_path
                        )
                    resolved_from = "storage"

                if os.path.exists(src_path):
                    method = self._link_or_copy(src_path, dst_path)
                    self._staged_in.add(dst_path)
                    self._log(f"[FILE_STAGING] [{idx+1}/{len(entries)}] {method}: "
                              f"{entry['type']}/{os.path.basename(dst_path)} "
                              f"(resolved via {resolved_from})")
                else:
                    self._log(f"[FILE_STAGING] [{idx+1}/{len(entries)}] SKIP source not found: "
                              f"{entry.get('type', '?')}/{os.path.basename(dst_path)}")

            os.remove(manifest_path)
            self._log("[FILE_STAGING] Stage manifest processed and removed")

        except Exception as e:
            import traceback
            self._log(f"[FILE_STAGING] Failed to process stage manifest: {e}")
            self._log(f"[FILE_STAGING] Traceback: {traceback.format_exc()}")

    def stage_out(self):  # pylint: disable=too-many-locals
        """
        Copy execution results from LocalWorkflows back to Storage.

        Reads workflow config to get actual job UUIDs, then copies results
        and logs back to Storage.

        Returns:
            True if successful, False otherwise
        """
        self._log("[FILE_STAGING] Starting stage-out...")

        try:
            # Read workflow config to get actual job UUIDs
            config_path = os.path.join(self.workflow_path, "config.json")
            if not os.path.exists(config_path):
                self._log("[FILE_STAGING] No workflow config.json found")
                return False

            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            jobs_info = config.get("jobs_info", {})
            if not jobs_info:
                self._log("[FILE_STAGING] No jobs_info in workflow config")
                return False

            machine_id = config.get("machine_id", "default")
            self._log(f"[FILE_STAGING] Using machine_id: {machine_id}")

            total_jobs = len(jobs_info)

            for job_idx, (job_uuid, _job_data) in enumerate(jobs_info.items()):
                self._log(
                    f"[FILE_STAGING] [{job_idx + 1}/{total_jobs}] "
                    f"Collecting results for: {job_uuid}"
                )

                # Get the execution directory for this job
                exec_job_dir = os.path.join(self.local_exec_path, f"imp{job_uuid[:7]}")
                impression_dir = os.path.join(self.storage_dir, job_uuid)

                self._log(f"[FILE_STAGING] Impression dir: {impression_dir}")
                self._log(f"[FILE_STAGING] Execution dir: {exec_job_dir}")

                # Copy stageout files back to Storage
                src_stageout = os.path.join(exec_job_dir, "stageout")
                if os.path.isdir(src_stageout):
                    dst_stageout = os.path.join(impression_dir, machine_id, "stageout")
                    self._log(f"[FILE_STAGING] Copying stageout from {src_stageout}")
                    self._log(f"[FILE_STAGING] Copying stageout to {dst_stageout}")
                    count = self._stage_directory(src_stageout, dst_stageout,
                                                  skip_src_paths=self._staged_in)
                    self._log(f"[FILE_STAGING] Copied {count} output files")

                    # Create downloaded marker
                    downloaded_marker = os.path.join(
                        impression_dir, machine_id, "stageout.downloaded"
                    )
                    os.makedirs(os.path.dirname(downloaded_marker), exist_ok=True)
                    with open(downloaded_marker, 'w', encoding='utf-8') as f:
                        pass
                    self._log(f"[FILE_STAGING] Created marker: {downloaded_marker}")
                else:
                    self._log(f"[FILE_STAGING] No stageout directory found: {src_stageout}")

                # Copy logs back to Storage
                src_logs = os.path.join(exec_job_dir, "logs")
                if os.path.isdir(src_logs):
                    dst_logs = os.path.join(impression_dir, machine_id, "logs")
                    self._log(f"[FILE_STAGING] Copying logs to {dst_logs}")
                    count = self._stage_directory(src_logs, dst_logs)
                    self._log(f"[FILE_STAGING] Copied {count} log files")

                    # Create logs downloaded marker
                    logs_marker = os.path.join(impression_dir, machine_id, "logs.downloaded")
                    os.makedirs(os.path.dirname(logs_marker), exist_ok=True)
                    with open(logs_marker, 'w', encoding='utf-8') as f:
                        pass
                    self._log(f"[FILE_STAGING] Created marker: {logs_marker}")

            self._log("[FILE_STAGING] Stage-out completed")
            return True

        except Exception as e:
            import traceback
            self._log(f"[FILE_STAGING] Stage-out failed: {e}")
            self._log(f"[FILE_STAGING] Traceback: {traceback.format_exc()}")
            return False
