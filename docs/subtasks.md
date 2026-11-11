# Repeated Task execution

A Task impression may declare `subtasks: N` in its frozen `celebi.yaml`.
N must be a positive integer. This remains one Task impression and one Yuki
job: workflow construction emits a wildcard rule plus an aggregate completion
rule, rather than creating N impressions or registering N jobs.

```yaml
environment: analysis
subtasks: 100
commands:
  - python code/simulate.py --index ${index} --output stageout/index/a.root
```

`index` ranges from 0 to N-1 and is reserved in repeated tasks. The literal
directory `stageout/index` in each execution's working directory links to
`imp<short>/stageout/<index>`. Runner caches, collection, and downstream aliases
retain the numeric layout. Inputs always expose the entire upstream Task:
use `input/stageout/${index}/a.root` to pair instances, or a glob to reduce them.
Every downstream Task waits for the upstream aggregate completion marker.

Logs and execution status files live under `imp<short>/logs/<index>/`.
`celebi-cli log 0 --subtask 7` selects a command log from one execution.
Live and terminal status details report the number of completed, running, and
failed executions and retain failed indices after the workflow terminates. A
short execution that starts and fails between status polls is classified as a
runtime failure rather than a workflow-construction failure.
The Task cache and `.done` marker are published only after every instance
has succeeded. Snakemake can resume individual incomplete executions in the
same workspace; fresh workflows do not reuse individual old instance results.

REANA may return nested file listings or archive downloads for repeated outputs.
Yuki expands those entries while collecting stageout and logs, preserves their
numeric directory layout, and writes a downloaded marker only after every
selected file has been stored successfully.

Separate working directories prevent accidental relative-path output collisions,
but do not add a filesystem security sandbox. Native/SSH commands retain the
runner account's filesystem permissions. Shared code and inputs are read-only
by convention, as in existing runner execution.

Ordinary Tasks without `subtasks` keep their existing behavior. Explicit null,
boolean, non-integer, and non-positive counts are rejected, as are data-only
environments and the special `htcondorcern` compute backend. Deploy the matching
Celebi client update providing `CelebiChrono.utils.subtasks` with this Yuki update.
