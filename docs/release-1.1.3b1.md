# Yuki 1.1.3b1

This beta adds credential management, EOS result downloads, SSH APD execution
through CVMFS and per-workflow core budgets. See the
[full release notes](../releaseNote/release_notes_v1.1.3b1).

Upgrade both CelebiChrono and Yuki to `1.1.3b1`, then restart Yuki's HTTP service
and Celery workers. Rebuild the Docker image if using the new dependency split
or bundled Kerberos/XRootD clients. Existing EOS usage and cleanup remain available.

## Credentials and EOS downloads

Import a keytab with `celebi-cli add-secret NAME --file KEYTAB --type kerberos
--principal USER@CERN.CH`, then bind it separately with
`celebi-cli update-runner RUNNER --secret NAME`. REANA and SSH bindings are
supported; native and IHEP workflow credential injection are not supported.
See [SSH credential lifecycle](ssh-connections.md#kerberos-credentials-for-ssh-workflows).

Secrets use filesystem permissions (0700 directory, 0600 files), not encryption
at rest. This feature adds no user authentication; restrict service access to
trusted clients. The CLI requires HTTPS or loopback HTTP and rejects redirects.
REANA secrets are account-wide and existing names are not overwritten. If binding
times out, inspect remote secrets before retrying. SSH copies persist on runners.

Configure `eos_xrootd_url` by REANA runner ID in Yuki's configuration before using
`celebi-cli collect outputs --from eos:RUNNER`. Downloads can use the source
runner's bound keytab or existing environment credentials. Direct EOS-to-SSH
transfers need Python 3, `xrdfs`, `xrdcp` and credentials on the destination.
Imported SSH caches remain incomplete for automatic reuse until full stageout
can be verified. See [deployment and EOS setup](../README.md).

## Submission and local services

`celebi-cli submit dirA dirB --runner RUNNER` submits selected objects together
with their dependencies. `--cores N` sets a positive workflow scheduling budget
for native, SSH or IHEP; REANA rejects it. This is not a per-command thread count,
does not resize active workflows and does not force completed workflows to rerun.

SSH APD tasks use CVMFS `lb-conda default`; optionally pin `ap_config.lb_conda`.
The runner still needs LHCb CVMFS and the standard LbEnv executable.

The HTTP service supports `YUKI_HTTP_HOST`, `YUKI_HTTP_PORT` and
`YUKI_HTTP_PORT_FILE`. Celebi reads the local service's `run/yuki.port` under the
shared `YUKIDIR`, falling back to the project endpoint when no valid local port
exists. Service wrappers that select automatic ports must publish this file.

This source release does not publish a PyPI package or restart deployed services.
