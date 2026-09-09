# Kill unreferenced running workflows

In a Celebi project, use either the interactive shell or `celebi-cli`:

```bash
celebi-cli kill-running-workflows pkufarm212 --dry-run
celebi-cli kill-running-workflows pkufarm212
```

The command previews workflows recorded as `running` / `in_movement` on
the named runner that are no longer referenced by the current project's live
impressions. It requires a successful live-set sync. It does not select
`orchestrating` workflows or workflows in other projects.

Without `--dry-run`, confirmation defaults to no. `--yes` skips the prompt
but still previews. Execution is restricted to the preview's IDs and rechecks
their runner, saved status, and live references. Newly live workflows are
skipped. Failures are reported per workflow.

The operation uses the backend's force-stop method. SSH batch cancellation
checks saved PID ownership and reports connection or cancellation failures
instead of clearing the local status on an SSH error. Native force-stop is
limited to clearing recorded state because that backend does not track its
foreground process. REANA stop rejection is reported as a failure.

Workspaces are retained. After reviewing the result, use
`purge-stale-workflows pkufarm212` to preview and remove eligible workspaces.
