# Running LHCb releases inside Celebi tasks

An algorithm can request that its user commands run through the `lb-run`
installation published on CVMFS:

```yaml
environment: lb-run
memory_limit: 4Gi
parameters:
  project: Moore
  version: v55r12
  platform: x86_64_v2-el9-gcc13-opt
  nightly: lhcb-head/1234
lb_run:
  project: ${project}
  version: ${version}
  platform: ${platform}
  nightly: ${nightly}
commands:
  - python code/options.py
```

`project` and `version` are required strings. `platform` and `nightly` are
optional. Task parameters are substituted before execution, so a task can
select a fixed release or nightly slot without copying the algorithm.

Yuki wraps each user command as an invocation of:

```text
/cvmfs/lhcb.cern.ch/lib/var/lib/LbEnv/stable/linux-64/bin/lb-run \
  -c PLATFORM PROJECT/VERSION bash --noprofile --norc -c COMMAND
```

When `nightly` is set, the invocation additionally contains
`--nightly NIGHTLY` before `PROJECT/VERSION`.

Only user commands run inside the LHCb environment. Workspace setup, input
linking, output caching, and finalization remain under Yuki's control. An
`lb_run` declaration automatically adds `lhcb.cern.ch` to the task's CVMFS
requirements. A nightly declaration also adds `lhcbdev.cern.ch`.

Immediately before invoking `lb-run`, Yuki checks that
`/cvmfs/lhcb.cern.ch` is mounted and that the configured `lb-run` executable
is executable. Nightly execution additionally checks `/cvmfs/lhcbdev.cern.ch`.
A missing repository or executable fails the user step with a specific
`Celebi lb-run error` message instead of a generic command-not-found failure.

`lb-run` is a reserved execution environment. Yuki maps it to the backend's
default base container/environment and then invokes the CVMFS release wrapper.
That base system must be ABI-compatible with the requested LHCb platform. Both
the task and the object supplying the effective commands must declare
`environment: lb-run`. With inherited commands this means both task and linked
algorithm; an inline task is its own command source and needs no algorithm.
REANA can mount
the declared CVMFS repository; native, SSH, and IHEP runners must already expose
it at `/cvmfs/lhcb.cern.ch`. Use `celebi-cli test-runner RUNNER` to inspect a
native or SSH runner's OS and machine platform before choosing an explicit
LHCb binary tag. Hosts running nightlies must expose both LHCb repositories.

If a task defines its own `commands`, those commands and its task-level
`lb_run` declaration replace the linked algorithm's command contract together.
Outputs still have to be written below `stageout/`.

## LbConda environments

`lb-run` is separate from LbConda. To run user commands in one of LHCb's
dated Conda environments, set the task environment instead:

```yaml
environment: lb-conda/default/2020-05-14
```

The supported form is `lb-conda/[package]/[date]`. On REANA, Yuki uses
`celebichrono/root-xrootd:6.32.02` as the base environment and automatically
mounts both `lhcb.cern.ch` and `lhcbdev.cern.ch`. It then invokes each user
command with `lb-conda package/date`; tasks do not need to declare those CVMFS
repositories themselves. Other backends retain their existing base-environment
selection. An `lb-conda/...` environment and an `lb_run` declaration cannot be
combined on the same task.
