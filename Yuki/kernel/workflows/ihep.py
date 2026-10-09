"""IHEP batch workflow backend.

Files and results use the SSH workflow transport, but execution is handed to
IHEP's HepJob scheduler with ``hep_sub`` instead of running on the login node.
"""
import os
import re
import shlex
import time

from ..runners import config as runner_config
from .ssh import SshWorkflow


HEP_JOB_ID_PATTERN = re.compile(
    r"(?:cluster|job(?:\s+id)?)\D+(\d+(?:\.\d+)?)", re.IGNORECASE)
HEP_SUBMISSION_GRACE_SECONDS = 120


class IhepWorkflow(SshWorkflow):
    """Run a remote Snakemake wrapper as one IHEP HepJob batch job."""

    def _load_ssh_config(self):
        """Load SSH transport plus settings specific to HepJob."""
        if not self.machine_id:
            return {}
        return runner_config.get_ihep_settings(
            runner_config.open_config(), self.machine_id)

    def _hep_command(self, setting, default):
        """Return a configured HepJob executable, safely shell quoted."""
        return shlex.quote(str(self.ssh_config.get(setting) or default))

    @staticmethod
    def _parse_hep_job_id(output):
        """Extract the scheduler id from normal ``hep_sub`` output."""
        match = HEP_JOB_ID_PATTERN.search(output or "")
        if not match:
            raise RuntimeError(
                f"hep_sub succeeded but returned no recognizable job id: "
                f"{(output or '').strip()!r}")
        return match.group(1)

    def _build_hep_submit_command(self, remote_wrapper):
        """Build the remote command that submits the executable wrapper."""
        parts = [self._hep_command("hep_sub_path", "hep_sub")]
        group = self.ssh_config.get("hep_group", "")
        if group:
            parts.extend(("-g", shlex.quote(str(group))))
        # HepJob executes submitted scripts via ``./``; submit from inside the
        # workflow directory rather than passing an absolute script path.
        parts.append(shlex.quote(f"./{remote_wrapper.rsplit('/', 1)[-1]}"))
        return (f"cd {shlex.quote(self.remote_exec_path)} || exit 1; "
                + " ".join(parts))

    def _build_hep_step_submitter(self):
        """Build the adapter used by Snakemake to submit every rule."""
        hep_sub = self._hep_command("hep_sub_path", "hep_sub")
        group = self.ssh_config.get("hep_group", "")
        group_arg = f" -g {shlex.quote(str(group))}" if group else ""
        try:
            memory_mb = int(self.ssh_config.get("hep_step_memory_mb", 1024))
        except (TypeError, ValueError):
            memory_mb = 1024
        if memory_mb <= 0:
            memory_mb = 1024
        return rf'''#!/bin/bash
set -Eeuo pipefail
if [ "$#" -ne 1 ]; then
    echo "usage: $0 JOBSCRIPT" >&2
    exit 64
fi
workflow_dir="$(cd "$(dirname "$0")" && pwd)"
job_dir="$workflow_dir/hep_jobs"
mkdir -p "$job_dir"
fallback_memory_mb={memory_mb}
memory_limit="$(sed -nE \
    's/^# properties = .*"mem_mb"[[:space:]]*:[[:space:]]*([0-9]+).*$/\1/p' \
    "$1" | head -n 1)"
memory_mb="$fallback_memory_mb"
if [[ "$memory_limit" =~ ^[1-9][0-9]*$ ]]; then
    memory_mb="$memory_limit"
fi
# The cluster-generic plugin owns its temporary jobscript and may remove it
# as soon as a job finishes.  Submit a durable copy so HepJob can start it
# asynchronously and so its default log files remain available afterwards.
jobscript="$(mktemp "$job_dir/job.XXXXXX.sh")"
cp "$1" "$jobscript"
chmod +x "$jobscript"
output="$({hep_sub}{group_arg} -m "$memory_mb" \
    -o "$job_dir" -e "$job_dir" "$jobscript" 2>&1)" || {{
    rc=$?
    printf '%s\n' "$output" >&2
    exit "$rc"
}}
job_id="$(printf '%s\n' "$output" | sed -nE \
    's/.*(cluster|job([[:space:]]+id)?)[^0-9]*([0-9]+(\.[0-9]+)?).*/\3/p' \
    | head -n 1)"
if [[ ! "$job_id" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    printf 'hep_sub returned no recognizable job id: %s\n' "$output" >&2
    exit 65
fi
printf '%s\n' "$job_id" >> "$workflow_dir/yuki.hep_children"
# Snakemake treats stdout as the external scheduler id, so emit only the id.
printf '%s\n' "$job_id"
'''

    def _build_hep_step_canceller(self):
        """Build the cancellation adapter for Snakemake child jobs."""
        hep_rm = self._hep_command("hep_rm_path", "hep_rm")
        return rf'''#!/bin/bash
set -Eeuo pipefail
for job_id in "$@"; do
    [[ "$job_id" =~ ^[0-9]+(\.[0-9]+)?$ ]] || exit 64
    {hep_rm} "$job_id"
done
'''

    def _build_remote_wrapper(self):
        """Build the batch coordinator that submits every rule via hep_sub."""
        snakemake_bin = self.ssh_config.get("snakemake_path") or "snakemake"
        cores = self.ssh_config.get("cores", "all")
        conda_path = self.ssh_config.get("conda_path") or ""
        try:
            max_jobs = int(self.ssh_config.get("hep_max_jobs", 100))
        except (TypeError, ValueError):
            max_jobs = 100
        if max_jobs <= 0:
            max_jobs = 100
        if conda_path:
            conda_setup = f"CONDA_BIN={shlex.quote(os.path.dirname(conda_path))}\n"
        else:
            conda_setup = (
                'CONDA_BIN="$(conda info --base 2>/dev/null || true)"\n'
                'CONDA_BIN="${CONDA_BIN:+$CONDA_BIN/bin}"\n')
        return f'''#!/bin/bash
set -Eeuo pipefail
{conda_setup}unset PYTHONPATH LD_LIBRARY_PATH
export PATH="${{CONDA_BIN:+$CONDA_BIN:}}/usr/local/bin:/usr/bin:/bin"
cd "$(dirname "$0")"
export XDG_CACHE_HOME="$PWD/.cache"
mkdir -p "$XDG_CACHE_HOME"

atomic_write() {{
    local value="$1"
    local destination="$2"
    local temporary="${{destination}}.tmp.$$"
    printf '%s\n' "$value" > "$temporary"
    mv -f "$temporary" "$destination"
}}

record_exit() {{
    local rc=$?
    trap - EXIT
    atomic_write "$rc" yuki.exit
    exit "$rc"
}}
trap record_exit EXIT
trap 'exit 143' TERM INT

SNAKEMAKE_BIN={shlex.quote(str(snakemake_bin))}
executor_args=()
snakemake_help="$("$SNAKEMAKE_BIN" --help 2>&1 || true)"
if [[ "$snakemake_help" == *"--cluster-generic-submit-cmd"* ]]; then
    executor_args=(
        --executor cluster-generic
        --cluster-generic-submit-cmd "./yuki_hep_submit.sh"
        --cluster-generic-cancel-cmd "./yuki_hep_cancel.sh"
    )
elif [[ "$snakemake_help" == *"--cluster CMD"* ]]; then
    executor_args=(
        --cluster "./yuki_hep_submit.sh"
        --cluster-cancel "./yuki_hep_cancel.sh"
    )
else
    echo "Snakemake has no cluster-generic executor or legacy --cluster support" >&2
    exit 64
fi

atomic_write "$$" yuki.pid
atomic_write "$$ $$" yuki.started
"$SNAKEMAKE_BIN" --use-conda --cores {shlex.quote(str(cores))} \
    --jobs {max_jobs} --latency-wait 60 --snakefile Snakefile \
    --apptainer-prefix "$PWD/.snakemake/apptainer" \
    "${{executor_args[@]}}" > snakemake.log 2>&1
'''

    def _start_remote_snakemake(self):
        """Upload the coordinator/adapters and submit through ``hep_sub``."""
        remote_wrapper = f"{self.remote_exec_path}/yuki_run.sh"
        remote_submitter = f"{self.remote_exec_path}/yuki_hep_submit.sh"
        remote_canceller = f"{self.remote_exec_path}/yuki_hep_cancel.sh"
        with self._ssh() as ssh:
            ssh.put_text(self._build_remote_wrapper(), remote_wrapper)
            ssh.put_text(self._build_hep_step_submitter(), remote_submitter)
            ssh.put_text(self._build_hep_step_canceller(), remote_canceller)
            out, err, code = ssh.exec(
                "chmod +x " + " ".join(shlex.quote(path) for path in (
                    remote_wrapper, remote_submitter, remote_canceller)))
            if code != 0:
                raise RuntimeError(
                    f"Failed to make yuki_run.sh executable: "
                    f"{err or out} (exit {code})")
            for name in ("yuki.started", "yuki.pid", "yuki.exit",
                         "yuki.hep_job_id", "yuki.hep_children"):
                ssh.remove(f"{self.remote_exec_path}/{name}")

            out, err, code = ssh.exec(
                self._build_hep_submit_command(remote_wrapper), timeout=30)
            if code != 0:
                raise RuntimeError(
                    f"hep_sub failed: {err or out} (exit {code})")
            job_id = self._parse_hep_job_id("\n".join((out, err)))
            ssh.put_text(job_id + "\n",
                         f"{self.remote_exec_path}/yuki.hep_job_id")

        self.config_file.write_variable("hep_job_id", job_id)
        self.config_file.write_variable("hep_submitted_at", time.time())
        self.logger(f"[IHEP] Submitted batch job {job_id} with hep_sub")

    def _read_hep_job_id(self, ssh):
        """Read the persisted HepJob id, preferring the local mirror."""
        job_id = str(self.config_file.read_variable("hep_job_id", "") or "")
        if job_id:
            return job_id
        path = f"{self.remote_exec_path}/yuki.hep_job_id"
        if not ssh.exists(path):
            return ""
        out, _err, code = ssh.exec(f"cat {shlex.quote(path)}")
        return out.strip() if code == 0 else ""

    def _hep_job_is_queued(self, ssh, job_id):
        """Return whether ``hep_q`` still reports this exact job id."""
        if not re.fullmatch(r"\d+(?:\.\d+)?", job_id):
            raise RuntimeError(f"Invalid persisted IHEP job id: {job_id!r}")
        command = (f"{self._hep_command('hep_q_path', 'hep_q')} -i "
                   f"{shlex.quote(job_id)}")
        out, err, code = ssh.exec(command)
        text = "\n".join((out, err))
        lower_text = text.lower()
        if any(marker in lower_text
               for marker in ("not found", "no job", "0 jobs")):
            return False
        if code != 0:
            raise RuntimeError(
                f"hep_q failed for job {job_id}: {err or out} (exit {code})")
        suffix = "" if "." in job_id else r"(?:\.\d+)?"
        return bool(re.search(
            rf"(?<![\d.]){re.escape(job_id)}{suffix}(?![\d.])", text))

    def _remote_execution_state(self, ssh, jobs):  # pylint: disable=too-many-return-statements
        """Resolve state from workflow markers plus the IHEP batch queue."""
        completed = [
            job_uuid for job_uuid in jobs
            if ssh.exists(f"{self.remote_exec_path}/{job_uuid}.done")]
        missing = [job_uuid for job_uuid in jobs if job_uuid not in completed]
        exit_code = self._read_remote_exit(ssh)

        if exit_code not in (None, 0):
            detail = (self._read_remote_snakemake_tail(ssh)
                      or self._read_remote_wrapper_tail(ssh))
            return "failed", detail, exit_code, completed
        if exit_code == 0:
            if not missing:
                return "finished", "", exit_code, completed
            return ("failed",
                    "IHEP batch job exited successfully but did not create "
                    f"completion markers for: {', '.join(missing)}",
                    exit_code, completed)
        if not missing:
            return "finished", "", exit_code, completed

        job_id = self._read_hep_job_id(ssh)
        if not job_id:
            return "failed", "IHEP submission has no recorded HepJob id", None, completed
        if self._hep_job_is_queued(ssh, job_id):
            return "running", "", None, completed

        # hep_q can briefly lag a successful submission. Do not turn that
        # visibility delay into a terminal workflow failure.
        submitted_at = float(
            self.config_file.read_variable("hep_submitted_at", 0) or 0)
        if submitted_at and time.time() - submitted_at < HEP_SUBMISSION_GRACE_SECONDS:
            return "running", "Waiting for IHEP job to become visible", None, completed

        # Close the queue-exit/marker-write race before reporting a failure.
        exit_code = self._read_remote_exit(ssh)
        if exit_code is not None:
            return self._remote_execution_state(ssh, jobs)
        detail = (self._read_remote_snakemake_tail(ssh)
                  or self._read_remote_wrapper_tail(ssh)
                  or f"IHEP job {job_id} left the queue without an exit marker")
        return "failed", detail, None, completed

    def _cancel_hep_job(self):  # pylint: disable=too-many-locals
        """Cancel the recorded batch job and make the local stop terminal."""
        if self._workflow_is_terminal():
            return False
        with self._ssh() as ssh:
            job_id = self._read_hep_job_id(ssh)
            if not re.fullmatch(r"\d+(?:\.\d+)?", job_id):
                self.logger("[IHEP] No valid batch job id; not changing state")
                return False
            command = (f"{self._hep_command('hep_rm_path', 'hep_rm')} "
                       f"{shlex.quote(job_id)}")
            out, err, code = ssh.exec(command)
            if code != 0:
                raise RuntimeError(
                    f"hep_rm failed for job {job_id}: "
                    f"{err or out} (exit {code})")
            children_path = f"{self.remote_exec_path}/yuki.hep_children"
            child_ids = []
            if ssh.exists(children_path):
                child_out, _child_err, child_code = ssh.exec(
                    f"cat {shlex.quote(children_path)}")
                if child_code == 0:
                    child_ids = sorted(set(child_out.split()))
            for child_id in child_ids:
                if not re.fullmatch(r"\d+(?:\.\d+)?", child_id):
                    continue
                _out, child_err, child_code = ssh.exec(
                    f"{self._hep_command('hep_rm_path', 'hep_rm')} "
                    f"{shlex.quote(child_id)}")
                if child_code != 0:
                    self.logger(
                        f"[IHEP] Could not cancel child job {child_id}: "
                        f"{child_err or _out}")
            exit_path = f"{self.remote_exec_path}/yuki.exit"
            ssh.put_text("137\n", exit_path)
        self.logger(f"[IHEP] Cancelled batch job {job_id} with hep_rm")
        return self._finalize_stop("IHEP workflow stopped by user")

    def kill(self):
        return self._cancel_hep_job()

    def force_kill(self, strict=False):  # pylint: disable=unused-argument
        return self._cancel_hep_job()
