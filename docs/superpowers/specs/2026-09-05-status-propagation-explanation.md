# Status Propagation Explanation

This document explains how job and workflow status moves through Yuki, how the
different backend paths write state, and how the API reads it back.

## What "status propagation" means

In Yuki, a workflow has two related layers of status:

1. Workflow status
   - Stored in `~/.Yuki/Workflows/<project_uuid>/<workflow_uuid>/results.json`
   - Describes the overall run: `running`, `finished`, or `failed`

2. Job status
   - Stored in each impression's `status.json`
   - Describes the per-job state seen by the UI and by clients
   - Also carries `detailed_status`, which explains why the job is in that
     state

Propagation is the process of reconciling the per-job `status.json` files with
the real execution outcome of the workflow.

## Core status model

The status names live in [`Yuki/kernel/status_constants.py`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/status_constants.py).

Important groups:

- Pre-execution states: `silence`, `prelude`, `composing`, `orchestrating`,
  `tuning`
- Execution state: `in movement`
- Terminal states: `coda`, `final note`, `failed`, `dissonance`, `stopped`,
  `deleted`

The helper [`is_terminal_status()`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/status_constants.py#L186)
is the main guard used by propagation code to avoid rewriting settled jobs.

## How a job status is written

The write path goes through [`VJob.set_status()`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/vjob.py#L189).

When a caller sets a status:

1. The status is validated.
2. The status is translated to the musical naming scheme if needed.
3. `status.json` is updated with:
   - `status`
   - `status_legacy`
   - `detailed_status`
4. If no detailed message is provided, Yuki generates a default one from the
   status constant table.

This means propagation is not just a cosmetic label change. It rewrites the
canonical status file for the impression.

## Native workflow propagation

The native/local backend is implemented in
[`Yuki/kernel/native_workflow.py`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/native_workflow.py).

### Where the propagation happens

`NativeWorkflow.update_workflow_status()` reads the workflow result, writes the
workflow-level `results.json`, then calls
[`propagate_job_statuses()`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/native_workflow.py#L239).

Relevant flow:

1. Check the local workflow directory for `*.done` marker files.
2. Decide workflow status:
   - all markers present -> `finished`
   - otherwise -> `running`
3. Write `results.json`
4. Propagate job statuses

### Native propagation rules

[`propagate_job_statuses()`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/native_workflow.py#L239)
applies these rules:

- Skip input jobs
- Skip algorithm jobs
- Skip jobs already in a terminal status
- If `<short_uuid>.done` exists, mark the job `finished`
- If the workflow is not terminal yet, leave missing jobs unchanged
- If the workflow is terminal and the job has logs, mark the job `failed` and
  include the tail of the latest `celebi_user_step*.log`
- If the workflow is terminal and the job has no logs, mark the job `failed`
  with a skip message saying the job never ran because an upstream dependency
  failed

### Why `.done` is authoritative

The native backend uses `.done` files as the success signal because they are
written only after the job's commands complete successfully. That makes them a
stable marker for propagation.

### How failure detail is chosen

[`_read_job_log_tail()`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/native_workflow.py#L276)
searches the job's `imp<short_uuid>/logs/` directory for the highest-indexed
`celebi_user_step*.log` file and returns the tail of that file.

This is important because:

- the highest step index is usually the most recent user command
- the failure is most likely visible in that log
- only a small tail is stored, which keeps `detailed_status` compact

## SSH workflow propagation

The SSH backend follows the same model in
[`Yuki/kernel/ssh_workflow.py`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/ssh_workflow.py).

Its propagation method mirrors the native backend:

- `.done` means the job is finished
- terminal workflow plus missing `.done` means failure or skip
- logs determine whether the job truly ran before failing

The SSH version also records distribution metadata when a job reaches a final
state, so the rest of the system can see where the job's data landed.

SSH launch and workflow status use a supervised marker protocol:

- `yuki.started` atomically records the wrapper and Snakemake PIDs
- `yuki.pid` atomically records the Snakemake PID for compatibility
- `yuki.exit` atomically records the wrapper's final exit code
- `<short_uuid>.done` records successful completion of each executable job

The SSH exec channel is not considered proof that execution started. After the
detached command is submitted, Yuki waits for either a live wrapper described
by `yuki.started` or an early `yuki.exit`. If neither appears within the bounded
startup interval, submission fails instead of leaving the workflow running.

During later polling, a live wrapper with no exit marker means `running`. A
nonzero exit means `failed`; exit zero requires every expected `.done` marker,
otherwise the run is failed as inconsistent. A dead wrapper without
`yuki.exit` is also failed. Workflows created before `yuki.started` was
introduced retain legacy `.done`/`yuki.exit` monitoring.

## Snakemake CLI propagation

The command-line local runner uses
[`Yuki/kernel/snakemake_monitor.py`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/snakemake_monitor.py).

The monitor does two things:

1. Writes workflow-level `results.json` while snakemake is running
2. Calls `workflow.propagate_job_statuses(workflow_terminal=True)` when the run
   ends successfully or with failure

It also passes `--keep-going` to snakemake. That matters because it allows
independent jobs to finish even if one branch fails, which makes the
missing-`.done` classification meaningful.

## How the API surfaces status

The status route in
[`Yuki/server/routes/status.py`](/Users/wave/workdir/Celebi/Yuki/Yuki/server/routes/status.py)
is read-only, but it triggers refreshes when needed.

The route:

1. Loads the impression and current runner context
2. Builds a `VJob`
3. Loads the workflow by UUID
4. Calls `job.update_status_from_workflow(workflow_path)`
5. Returns the current job status and `detailed_status`

If the workflow is not terminal, the route may enqueue
`task_update_workflow_status` so the backend can refresh the workflow state in
the background.

This means the UI does not invent job state. It reads the status files written
by the backend propagation logic.

## Storage-root consistency

Every stage of propagation must resolve the same Yuki data root. Workflow
creation, workflow reload, runner configuration, REANA credentials, dependency
jobs, downloaded outputs, and status files now use this precedence:

1. A non-empty `YUKIDIR` value
2. `~/.Yuki`, expanded using the current `HOME`

This is required for deployments and tests that relocate Yuki's state. If one
component uses `YUKIDIR` while `VWorkflow` uses a hardcoded `$HOME/.Yuki`, the
workflow can be found by the transfer service but reloaded from a different
directory. The observable symptoms include missing workflow configuration,
the wrong backend being selected, status updates written outside the active
data root, and permission failures while opening `workflow.log`.

An empty `YUKIDIR` is treated as unset. Treating it as a real path would make
paths such as `Workflows/<project>/<workflow>` relative to the process working
directory.

## `update_status_from_workflow()`

[`VJob.update_status_from_workflow()`](/Users/wave/workdir/Celebi/Yuki/Yuki/kernel/vjob.py#L367)
is the compatibility bridge between workflow state and impression state.

It:

1. Loads the current impression `status.json`
2. Normalizes `coda` back to `finished` for storage compatibility
3. Skips terminal jobs
4. Reads workflow `results.json`
5. Reads the workflow log to find a matching step, if available
6. Updates the impression status based on the workflow step and workflow
   outcome

This path is mostly about reloading and reconciling state for existing
workflows, while the newer `propagate_job_statuses()` methods are about writing
the per-job end state directly from execution markers.

## Practical example

Suppose a native workflow has three jobs:

- Job A completes successfully
- Job B fails while running
- Job C never starts because it depends on Job B

After the workflow ends:

- Job A gets `finished`
- Job B gets `failed` with a log tail in `detailed_status`
- Job C gets `failed` with the skip message

If the workflow is still running:

- Job A may already be `finished`
- Job B and Job C stay unchanged until the workflow becomes terminal

## Why this design is useful

This propagation model gives three benefits:

- The UI sees the real state of each impression
- Operators get a useful `detailed_status` without opening log files
- Native and SSH workflows behave consistently enough that the same client
  code can consume both

## Test coverage

The behavior is covered by focused tests in
[`UnitTest/test_native_workflow.py`](/Users/wave/workdir/Celebi/Yuki/UnitTest/test_native_workflow.py)
and [`UnitTest/test_ssh_workflow.py`](/Users/wave/workdir/Celebi/Yuki/UnitTest/test_ssh_workflow.py).

Those tests verify:

- `.done` promotes a job to completion
- missing `.done` stays unchanged while the workflow is running
- terminal workflows mark unfinished jobs failed
- failure detail comes from logs when available
- SSH launch requires an atomic startup or exit marker
- a dead SSH supervisor cannot remain indefinitely `running`
- exit zero with missing job completion markers is treated as failure
- workflow and REANA paths honor `YUKIDIR` consistently
- input and algorithm jobs are skipped
- already terminal jobs are not rewritten

## Summary

Status propagation in Yuki is a reconciliation step that turns execution
markers into persisted job state. The workflow backend owns the truth, writes
the workflow result, and then propagates that result into each impression's
`status.json`. The API reads those files back and exposes them to clients.
