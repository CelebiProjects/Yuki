# Yuki

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/hepChern/Yuki)

Yuki is the Data Integration Thought Entity for the Chern Project — a data
analysis management toolkit for high energy physics. A Flask web server with a
Celery task queue manages jobs, workflows (REANA and native), runners, and
impressions, storing data under `~/.Yuki/Storage/`.

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
