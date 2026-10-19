# Running LHCb releases inside Celebi tasks

An algorithm can request that its user commands run through the `lb-run`
installation published on CVMFS:

```yaml
environment: <compatible-base-container>
memory_limit: 4Gi
parameters:
  project: Moore
  version: v55r12
  platform: x86_64_v2-el9-gcc13-opt
lb_run:
  project: ${project}
  version: ${version}
  platform: ${platform}
commands:
  - python code/options.py
```

`project` and `version` are required strings. `platform` is optional. Task
parameters are substituted before execution, so a task can select a fixed
release without copying the algorithm.

Yuki wraps each user command as an invocation of:

```text
/cvmfs/lhcb.cern.ch/lib/var/lib/LbEnv/stable/linux-64/bin/lb-run \
  -c PLATFORM PROJECT/VERSION bash --noprofile --norc -c COMMAND
```

Only user commands run inside the LHCb environment. Workspace setup, input
linking, output caching, and finalization remain under Yuki's control. An
`lb_run` declaration automatically adds `lhcb.cern.ch` to the task's CVMFS
requirements.

Immediately before invoking `lb-run`, Yuki checks that
`/cvmfs/lhcb.cern.ch` is mounted and that the configured `lb-run` executable
is executable. A missing repository or executable fails the user step with a
specific `Celebi lb-run error` message instead of a generic command-not-found
failure.

The normal `environment` field remains required: it selects the base container
on REANA or the mapped execution environment on native/SSH runners. That base
system must be ABI-compatible with the requested LHCb platform. REANA can mount
the declared CVMFS repository; native, SSH, and IHEP runners must already expose
it at `/cvmfs/lhcb.cern.ch`. Use `celebi-cli test-runner RUNNER` to inspect a
native or SSH runner's OS and machine platform before choosing an explicit
LHCb binary tag.

If a task defines its own `commands`, those commands and any task-level
`lb_run` declaration replace the linked algorithm's command contract together.
Outputs still have to be written below `stageout/`.

## LbConda environments

`lb-run` is separate from LbConda. To run user commands in one of LHCb's
dated Conda environments, set the task environment instead:

```yaml
environment: lb-conda/default/2020-05-14
```

The supported form is `lb-conda/[package]/[date]`. Yuki mounts
`lhcb.cern.ch`, keeps its normal backend base environment, and invokes each
user command with `lb-conda package/date`. An `lb-conda/...` environment and
an `lb_run` declaration cannot be combined on the same task.
