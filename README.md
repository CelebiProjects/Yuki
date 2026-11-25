# Yuki

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/hepChern/Yuki)

Yuki is the Data Integration Thought Entity for the Chern Project — a data
analysis management toolkit for high energy physics. A Flask web server with a
Celery task queue manages jobs, workflows (REANA and native), runners, and
impressions, storing data under `~/.Yuki/Storage/`.

Contributor-facing package boundaries and canonical import paths are described
in [the package layout guide](docs/package-layout.md).

IHEP clusters are supported through the `ihep` runner backend, which stages via
SSH and submits work through `hep_sub`; see [the IHEP runner guide](docs/ihep-runner.md).

Celebi algorithms can run their user commands in centrally published LHCb
releases through CVMFS; see [the lb-run guide](docs/lb-run.md).

## 1.1.3b1 beta

### Local keytab import

```sh
celebi-cli add-secret cern-keytab --file ~/.cern.keytab --type kerberos --principal USER@CERN.CH
celebi-cli update-runner cern --secret cern-keytab
```

Update both source checkouts and reload Yuki to enable these commands. Credentials
are stored beside the server configuration under `Secrets/` (0700 directory,
0600 files), outside projects and impressions. Storage is not encrypted at rest.
The import endpoint never returns key contents and rejects duplicate names.
Use deployment authentication/access restrictions for trusted clients; this feature
does not add user authentication. The CLI requires HTTPS or localhost HTTP via a
local connection/SSH tunnel and does not follow redirects for secret operations.

Binding distributes the credential to REANA's account-wide secrets API
(`yuki-NAME.keytab`, `CERN_USER`, `CERN_KEYTAB`) or an SSH runner's private directory,
and enables Kerberos only after successful upload. Bind separately from connection
setting changes. REANA requires a CERN user principal and existing remote secrets
are not overwritten. After a timeout, inspect the remote state before retrying.
REANA bindings also supply credentials for EOS downloads performed by the Yuki
worker. IHEP batch and native workflow injection are not yet supported.
REANA API reference: [add_secrets](https://reana-client.readthedocs.io/en/latest/#reana_client.api.client.add_secrets).

### Beta release notes

See the [upgrade notes](docs/release-1.1.3b1.md) and
[EOS usage and cleanup guide](docs/eos-usage.md). The previous beta added REANA-based
EOS accounting/cleanup and direct native-runner output previews.

## Celebi bookkeeping uploads

The updated Celebi client runs `celebi-cli bookkeep --commit "Publish" --tag "v1"`
to publish code through GitLab or GitHub and collected results through Yuki to `celebi_server`.
Yuki receives only the project UUID, the server's needed impression UUIDs, and
a short-lived upload grant. The website's analysis token stays on the client.

`POST /book-celebi-server/pack` selects images and UTF-8 text from collected
runner stageout directories (maximum 5 MiB per file), returns archive hashes,
sizes, missing UUIDs, and a `pack_id`. `POST /book-celebi-server/upload` resolves
those archives from the stored batch and uploads them in verified chunks.
Interrupted uploads resume even when a retry creates a new pack batch.
Archives exclude symlinks, ROOT/data files, and oversized files; their file
list records skipped paths. Impressions with no eligible collected files stay
missing so later runs can retry. Result collection from runners remains a
separate step.

Install the updated Celebi, Yuki, and celebi_server checkouts for this workflow.

## Install

```bash
# From source (development)
pip install -e .

# Or build and install the package
python -m build
pip install dist/yuki-*.whl
```

The standard installation includes Snakemake for native execution. Install
Yuki in a Conda environment so the host also provides the ``conda`` command
used to create project environments.

## Run the server

Requires a RabbitMQ broker at `amqp://localhost` (or run everything in Docker — see below).

Create a dedicated Conda environment containing `rabbitmq-server` and start a
local broker:

```bash
scripts/setup-rabbitmq.sh
```

The script is idempotent and waits for port 5672 before returning. Its default
environment is `~/.Yuki/Conda/rabbitmq`; pass `--prefix PATH` to select another
location. To reduce peak solver memory, it explicitly uses libmamba and
conda-forge's `current_repodata.json`. Use `--dry-run` to preview installation
and startup commands. The readiness timeout can be changed with
`RABBITMQ_WAIT_TIMEOUT`.

RabbitMQ 4.3 disables transient non-exclusive queues by default, while the
current Celery/Kombu control mailbox still declares them. The script enables
that deprecated compatibility feature in its private `rabbitmq.conf`. If the
broker was already running, restart it once to apply the setting:

```bash
scripts/setup-rabbitmq.sh --restart
```

```bash
yuki server start    # Flask on port 3315 + Celery worker
yuki server status
yuki server stop     # or Ctrl-C
```

On first server start, Yuki registers a native runner named ``local`` unless a
runner with that name already exists. Existing runner settings are preserved.

## CLI overview

```bash
yuki server start|stop|status      # manage the web server
yuki docker run|restart            # run Yuki in Docker (see below)
yuki run-workflow <uuid>           # execute a workflow
yuki native-runner start            # execute queued native workflows on the host
yuki impression-export <uuids...> --project-uuid <uuid> -o out.tar.gz
yuki impression-import <tar_file> --project-uuid <uuid>
yuki env-map add|list|remove       # manage environment mappings
```

Celebi can also share the current task, all tasks in the current directory, or
all tasks in the project when run from its root, through a managed store on a
registered SSH runner:

```bash
celebi-cli share-ssh-impressions --export RUNNER
celebi-cli share-ssh-impressions --import RUNNER
```

## APD execution tokens

Celebi can deposit a git-excluded APD token with an impression. Yuki stages the
token as a mode-`0600` `lbapi_key.json` file only in the execution workspace and
sets `APD_USER_TOKEN_FILE` without placing the token value in generated commands
or logs. This works for ordinary execution Tasks, including `lb-conda` Tasks, as
well as `lhcb_ap_datalist`. A later deposit refreshes the local secret while
preserving the immutable shared impression configuration.

## Run with Docker

The container is all-in-one: RabbitMQ (the Celery broker) starts inside it,
then `yuki server start` runs on port 3315 as a non-root user.

The image uses Python 3.11. REANA's Python dependencies include Snakemake 7;
the native Snakemake 8/9 CLI lives in a separate environment at
`/opt/yuki-native` and is exposed as `/usr/local/bin/snakemake`. This avoids
mixing their incompatible dependencies. Yuki itself is installed with
`--no-deps` after the Docker dependency installer provisions both environments.

The image includes Kerberos tools (`kinit`, `klist`) and XRootD clients
(`xrdcp`, `xrdfs`) with the Kerberos authentication plugin. To access EOS,
provide a keytab as a runtime secret readable by the `yuki` user and obtain
a ticket with `kinit -kt /path/to/cern.keytab USER@CERN.CH`. Set
`KRB5CCNAME=FILE:/path/to/writable/ticket-cache` for both authentication and
download processes. Credentials are not included in the image.
Use the EOS XRootD namespace (for example,
`root://eoshome-m.cern.ch//eos/user/m/USER/`), which may differ from a REANA
filesystem mount alias such as `/eos/home-m/USER/`.

To enable EOS result collection, an administrator adds an
`eos_xrootd_url` map to Yuki's `config.json` keyed by REANA runner ID
(the value in `runners_id`). Each value is the XRootD URL for that
runner's cache root, before the project and impression UUIDs. Add
`docker-compose.eos.yml` after `docker-compose.yml` when starting the
development container and set `YUKI_EOS_KEYTAB_FILE` and
`YUKI_EOS_PRINCIPAL`. The worker acquires a fresh ticket for each download
job. Without these settings it uses the worker's existing credentials.
Limit HTTP access to trusted clients before enabling credential-backed
collection. Clients with access to Yuki can request files from the
configured cache roots for impressions registered in that instance.

From a Celebi task, `celebi-cli collect outputs --from eos:cern` downloads
stageout into Yuki. Replace `outputs` with a filename or glob to select
files. `celebi-cli transfer eos:cern runner:farm --pattern '*.root'` runs
the download on the destination SSH runner, which needs Python 3,
`xrdfs`, `xrdcp`, and credentials accessible to its SSH session.
SSH imports remain incomplete for automatic workflow reuse until there
is a way to verify that the EOS cache contains the full stageout.

### Development (hot-reload)

```bash
# Builds the dev image and mounts this repo into the container —
# edits take effect without rebuilding
docker compose up

# Optional: develop against a local CelebiChrono checkout instead of PyPI
CELEBI_DIR=../CelebiChrono docker compose up
```

Storage persists in your host `~/.Yuki` (override with `YUKIDIR=... docker compose up`),
shared with native runs and `yuki docker run`. The compose setup targets macOS;
on Linux, prefer the CLI below.

### Automatic native execution on the host

Install Yuki on the host and start the agent there, using the same storage
directory that is mounted into Docker:

```bash
yuki native-runner start
yuki native-runner status
yuki native-runner logs
yuki native-runner stop
```

`start` runs in the background; use `start --foreground` when running under a
service manager. Yuki prepares native workflows in the shared directory, and
the agent claims them and runs Snakemake and Conda on the host. No API address
or published Docker port is needed. Set `YUKIDIR` or `--yuki-dir` if the host
storage directory is not `~/.Yuki`. A custom native runner workdir must be a
path available with the same absolute name inside Docker and on the host;
the default `LocalWorkflows` directory needs no extra setup. Native execution
state is project-scoped below
`<workdir>/workflows/<project_uuid>/<workflow_uuid>`, while managed Conda
environments live below `<workdir>/environments/<project_uuid>/<name>`.
Existing flat `<workdir>/<workflow_uuid>` workspaces remain readable during
migration.

Install a project's `.celebi-env/NAME.yaml` with
`celebi-cli install-environment <native-runner> NAME`; workflow construction
resolves the logical name to that project prefix and fails before queueing when
it is missing. Celebi's `test-runner` also verifies that the host agent owns its
lock, rather than reporting a stopped native runner healthy. For native and
SSH runners it reports the operating-system release, kernel, and machine
architecture as `platform`, which helps select compatible binary environments.

Native execution shares Yuki's storage domain: successful stage-out is copied
directly into Yuki Storage. Native therefore has no separate managed runner
cache and is not a transfer source/destination; use `yuki` as the location.
`cache-on-runner` has no additional effect for native workflows.
The standalone `yuki-native-runner` command remains available as a backwards-
compatible alias.
If the agent is interrupted during execution, a restart waits for the host
process to exit and then marks the unfinished workflow failed for inspection;
it does not launch that workflow a second time automatically.

### Building images

```bash
docker/scripts/build.sh dev            # yuki:dev
docker/scripts/build.sh prod           # yuki:<version> + yuki:latest (version from pyproject.toml)
docker/scripts/build.sh prod --tar     # also exports yuki-<version>.tar (for machines without registry access)
docker/scripts/build.sh prod --nightly # yuki-nightly:0.0.<date>-1 naming
```

### Running the production image

```bash
docker run -d -p 3315:3315 yuki:latest
```

or via the CLI (mounts host `~/.Yuki` for storage; auto-detects rootless Docker
and runs as root there so the mount stays writable):

```bash
yuki docker run                 # yuki:latest, port 3315, ~/.Yuki
yuki docker run --port 3316     # custom host port
```

Nightly images are also published to `ghcr.io` by CI.

## Testing

```bash
python -m pytest UnitTest/ -v
```

### SSH submission timeouts

`submit --runner my_runner --timeout 3000` in the Celebi shell sends a
`timeout` field (positive integer seconds) to Yuki's `/execute` endpoint.
Yuki passes it to the background worker and saves it as `submission_timeout`
in the workflow configuration. SSH commands, connection handshakes, and
startup-marker confirmation use at least this limit, including after the
workflow is reloaded. Existing longer operation limits remain in effect.
Omitting the option preserves the existing server defaults.

The detached SSH session still gets a quick 2-second check before Yuki
switches to remote startup-marker verification. This is not a workflow
runtime limit. Both the Celebi client and Yuki server/worker need the update.

### APD tasks on SSH runners

`environment: lhcb_ap_datalist` automatically runs its APD login and list
query through CVMFS `lb-conda default` on SSH. It no longer requires a
`celebichrono_lhcb-apd_0.9.0` Conda environment on the runner.
Optionally pin `ap_config.lb_conda: default/2026-09-17`; existing queries
without this field continue to work. The runner must provide LHCb CVMFS
and the standard LbEnv `lb-conda` executable. APD token-file injection and
`stageout/dataList.txt` are preserved; cached inputs do not query again.
REANA and other backend environment selection are unchanged.


### Per-submission cores

`celebi-cli submit --runner pkufarm212 --cores 16` overrides the Snakemake
core budget for the new workflow only. `POST /execute` accepts an optional
positive-integer `cores` form field; the worker persists it in the workflow's
`config.json`. Native, SSH and IHEP executors prefer this value over their runner
configuration. Without it, the existing runner default applies. REANA rejects
this option. Existing active/completed workflows retain normal deduplication;
this option neither resizes running workflows nor forces recomputation.
