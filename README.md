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

## Install

```bash
# From source (development)
pip install -e .

# Or build and install the package
python -m build
pip install dist/yuki-*.whl
```

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

## CLI overview

```bash
yuki server start|stop|status      # manage the web server
yuki docker run|restart            # run Yuki in Docker (see below)
yuki run-workflow <uuid>           # execute a workflow
yuki-native-runner start            # execute queued native workflows on the host
yuki impression-export <uuids...> --project-uuid <uuid> -o out.tar.gz
yuki impression-import <tar_file> --project-uuid <uuid>
yuki env-map add|list|remove       # manage environment mappings
```

## Run with Docker

The container is all-in-one: RabbitMQ (the Celery broker) starts inside it,
then `yuki server start` runs on port 3315 as a non-root user.

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
yuki-native-runner start
yuki-native-runner status
yuki-native-runner logs
yuki-native-runner stop
```

`start` runs in the background; use `start --foreground` when running under a
service manager. Yuki prepares native workflows in the shared directory, and
the agent claims them and runs Snakemake and Conda on the host. No API address
or published Docker port is needed. Set `YUKIDIR` or `--yuki-dir` if the host
storage directory is not `~/.Yuki`. A custom native runner workdir must be a
path available with the same absolute name inside Docker and on the host;
the default `LocalWorkflows` directory needs no extra setup.
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
