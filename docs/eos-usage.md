# EOS usage by project

Available with matching CelebiChrono and Yuki `1.1.2b1`.

`celebi-cli eos-usage cern --all-projects` submits a small, read-only REANA
serial workflow. It measures the runner's configured
`eos_mount_point/<project UUID>/<impression UUID>/` cache directories and
returns `eos-usage.json`. The Yuki host never mounts or directly accesses EOS.
This accounts for managed EOS caches, not arbitrary EOS datasets, REANA
workspaces, local CAS, or collected Yuki copies.

```sh
# Current project; first synchronize its current pointers and dependency closure.
celebi-cli eos-usage cern
# All projects under the runner's configured EOS cache root.
celebi-cli eos-usage cern --all-projects
# Submit and return without waiting for execution.
celebi-cli eos-usage cern --all-projects --no-wait
# Resume an existing query, including after a client timeout; do not submit again.
celebi-cli eos-usage --scan-id SCAN_ID --json
```

Each request prints its scan ID before submission. The Yuki state and REANA
workflow name are persisted before contacting REANA. An uncertain submission
is never automatically repeated. Querying an ID can recover a successful
submission after a lost response. The workflow is retained in REANA for diagnosis;
this command never deletes workflows or EOS files.

The current project's live set is synced before submitting a new scan. For an
all-project scan, run `celebi-cli sync-live` in other projects whose classification
must be refreshed. The report shows each live-set timestamp. History entries
alone do not make an impression live.

- **live**: in a valid synced live set or referenced by a nonterminal workflow
  mirror (inputs and outputs are protected).
- **non_live**: explicitly superseded in a valid live set, without known active
  references or unreadable workflow mirrors. This is not deletion authorization.
- **unknown**: no valid live set, a set older than 24 hours, an unrecognized
  impression, incomplete measurement, or uncertain workflow references.

Sizes are logical regular-file bytes. They do not account for EOS replication,
sparse allocation or hard-link deduplication. Symlinks are not followed. Permission
errors, missing directories and unrecognized entries mark the report incomplete;
measured totals are then lower bounds, not a claim of zero missing usage.
The scan is not an atomic filesystem snapshot. Files can change while it runs.

`--json` includes per-impression counts, classification reasons and errors.
Re-querying a completed scan reclassifies its saved measurements against the
latest live sets, but does not remeasure EOS. Both scan and classification times
are included. Start a new scan when new measurements are required.

## Deployment

Update both Celebi and Yuki, then reload the Yuki web service so it registers:

- `POST /eos-usage`: JSON `runner`, `scan_id` (32 lowercase hex), optional `project`.
- `GET /eos-usage/<scan_id>`: poll REANA, collect a finished report, or reclassify
  an already downloaded report.

The registered runner must have REANA `urls`, `tokens` and an absolute
`eos_mount_point` under `/eos/`. The job requests Kerberos and uses
`python:3.11-slim` by default. An administrator can override its container with
`eos_usage_image[runner_id]` in Yuki config; it must provide `python3` and work
with the site's EOS mount. REANA must supply access to that mount and credentials.
SDK operations run in a separate Python process to isolate the global REANA URL
from concurrent web requests. No access tokens are included in scan reports.

Scan records live under `$YUKIDIR/eos-usage` (default `~/.Yuki/eos-usage`). No
Celery worker upgrade is required for this endpoint; the POST only creates,
uploads and starts a workflow, while subsequent GETs check progress.

## Legacy EOS directories

Older versions cached files directly at `<root>/<impression UUID>/`. These are
measured as well. Yuki attributes a legacy directory to a project only when its
Storage records or synced sets identify exactly one owner. Ambiguous or missing
ownership is reported under `unassigned_usage`, separate from project totals.
Files with an unknown layout inside a known project count toward that project's
unknown bytes. Nothing is silently treated as an empty project merely because
its directory layout is old.

## EOS cleanup commands

```sh
# Preview the current project's non-live caches (submits only a read-only scan).
celebi-cli eos-cleanup cern
# Preview non-live + unknown caches for all projects; live caches stay protected.
celebi-cli eos-cleanup cern --all-projects --include-unknown
# Additionally clear one selected project's entire cache, including live.
celebi-cli eos-cleanup cern --all-projects --include-unknown --clear-project PROJECT_UUID
# Execute a saved, reviewed manifest after confirmation.
celebi-cli eos-cleanup --plan-id PLAN_ID --execute
# Inspect progress, including the separate verification job.
celebi-cli eos-cleanup --plan-id PLAN_ID
```

`--execute` can also accompany a new preview. It displays the preview before
asking for confirmation. `--yes` skips that prompt only with `--execute`.
`--json` prints the exact directory manifest, fingerprints and reports. When
executing with JSON output, use `--yes` after reviewing a prior preview.
`--no-wait` returns after cleanup submission; the scan needed to build the preview
still has to finish first. A timeout never means deletion was cancelled.

`celebi-cli tidyup` now offers EOS cleanup when a REANA runner is selected for
cache inspection. It asks before submitting the read-only scan and separately
confirms the concrete deletion manifest. SSH cache behavior is unchanged.

Before planning, the CLI refreshes live sets for matching locally registered
projects (and the current checkout). Projects lacking a local checkout retain
their existing registry status, which is shown/reported as unknown when missing
or stale. Explicitly cleared projects do not need a live-set refresh. Unknown
caches are selected only with `--include-unknown`; directories without a unique
project owner are skipped, even with that flag. Unknown layout entries within a
project are included only when that entire project is explicitly cleared.

A cleanup preview needs a complete scan less than 24 hours old with metadata
fingerprints. Older scan reports created before cleanup support must be replaced
with a new `eos-usage` scan. Fingerprints cover sorted relative paths, file sizes,
modification timestamps and modes; file contents are not read. The server checks
live/ownership changes again at execution and refuses a changed preview. Each
execution is bound to the manifest's SHA256 and persisted plan ID.

REANA refuses submission if a selected project has a running/queued REANA
workflow. The remote worker rejects symlinks, path escapes, overlapping scopes,
protected live caches and content/metadata changes. It checks all targets before
the first deletion, and each target again before removing it. This is not an
atomic filesystem transaction: avoid starting new analysis workflows until
cleanup and verification have completed.

After deletion, a separate read-only REANA workflow checks the deleted paths
and protected live directories using a fresh mount/client. Until that check
passes, state is `verifying`, not `done`. Residual paths, missing protected paths,
or lookup/listing disagreements produce `partial`; the command never blindly
retries a partial deletion. Reports retain the workflow names and exact paths.
Even independent checks are point-in-time observations; another client may still
have an outdated EOS view, and later workflows can recreate caches.

Plans/reports live under `$YUKIDIR/eos-cleanup`. New routes are
`POST /eos-cleanup/plan`, `POST /eos-cleanup/<plan_id>/execute` (requires the preview
`manifest_sha256`) and `GET /eos-cleanup/<plan_id>`. Reload the Yuki web service
after updating; no analysis workflow or EOS data is changed by installing this code.
