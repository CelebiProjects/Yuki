#!/bin/sh
set -eu

RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
RABBITMQ_WAIT_TIMEOUT="${RABBITMQ_WAIT_TIMEOUT:-60}"
RABBITMQ_DATA_DIR="${RABBITMQ_DATA_DIR:-${YUKIDIR:-$HOME/.Yuki}/RabbitMQ}"
CONDA_ENV_PREFIX="${RABBITMQ_CONDA_PREFIX:-${YUKIDIR:-$HOME/.Yuki}/Conda/rabbitmq}"
DRY_RUN=0
RESTART=0

usage() {
    cat <<'EOF'
Usage: scripts/setup-rabbitmq.sh [--prefix CONDA_PREFIX] [--restart] [--dry-run]

Create a small dedicated Conda environment containing rabbitmq-server, start it
in detached mode, and wait until the AMQP port is ready. The solver uses only
conda-forge's smaller current repodata to reduce peak memory usage.

Options:
  --prefix PATH  Create or reuse the RabbitMQ Conda environment at this path.
  --restart      Restart a running broker so configuration changes take effect.
  --dry-run      Print installation and startup commands without running them.
  -h, --help     Show this help message.

Environment variables:
  RABBITMQ_PORT          AMQP port to probe (default: 5672)
  RABBITMQ_WAIT_TIMEOUT  Readiness timeout in seconds (default: 60)
  RABBITMQ_DATA_DIR      State/log directory (default: ~/.Yuki/RabbitMQ)
  RABBITMQ_CONDA_PREFIX  Conda environment (default: ~/.Yuki/Conda/rabbitmq)
  YUKIDIR                Changes the default state directory parent
EOF
}

log() {
    printf '%s\n' "$*"
}

run() {
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '+ '
        printf '%s ' "$@"
        printf '\n'
        return 0
    fi
    "$@"
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --prefix)
            if [ "$#" -lt 2 ]; then
                log "Error: --prefix requires a path."
                exit 2
            fi
            CONDA_ENV_PREFIX="$2"
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            ;;
        --restart)
            RESTART=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            log "Error: unknown option: $1"
            usage
            exit 2
            ;;
    esac
    shift
done

if ! command -v conda >/dev/null 2>&1; then
    log "Error: conda is required but was not found in PATH."
    exit 1
fi

CONDA_ENV_PREFIX="${CONDA_ENV_PREFIX%/}"

find_rabbitmq_server() {
    for candidate in \
        "$CONDA_ENV_PREFIX/lib/rabbitmq/sbin/rabbitmq-server" \
        "$CONDA_ENV_PREFIX/sbin/rabbitmq-server" \
        "$CONDA_ENV_PREFIX/bin/rabbitmq-server"
    do
        if [ -x "$candidate" ]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    return 1
}

find_rabbitmqctl() {
    for candidate in \
        "$CONDA_ENV_PREFIX/lib/rabbitmq/sbin/rabbitmqctl" \
        "$CONDA_ENV_PREFIX/sbin/rabbitmqctl" \
        "$CONDA_ENV_PREFIX/bin/rabbitmqctl"
    do
        if [ -x "$candidate" ]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    return 1
}

if RABBITMQ_SERVER="$(find_rabbitmq_server)"; then
    log "RabbitMQ is already installed in $CONDA_ENV_PREFIX."
else
    if [ -d "$CONDA_ENV_PREFIX/conda-meta" ]; then
        conda_action="install"
        log "Installing rabbitmq-server into $CONDA_ENV_PREFIX..."
    else
        conda_action="create"
        log "Creating a dedicated RabbitMQ environment at $CONDA_ENV_PREFIX..."
    fi
    run conda "$conda_action" --yes --prefix "$CONDA_ENV_PREFIX" \
        --solver libmamba --override-channels --channel conda-forge \
        --repodata-fn current_repodata.json rabbitmq-server
    if [ "$DRY_RUN" -eq 1 ]; then
        RABBITMQ_SERVER="$CONDA_ENV_PREFIX/lib/rabbitmq/sbin/rabbitmq-server"
    elif ! RABBITMQ_SERVER="$(find_rabbitmq_server)"; then
        log "Error: Conda installation completed, but rabbitmq-server was not found."
        exit 1
    fi
fi

CONDA_BASE="$(conda info --base)"
PYTHON_BIN="$CONDA_BASE/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
    log "Error: Python was not found in the Conda base environment: $PYTHON_BIN"
    exit 1
fi

RABBITMQ_CONFIG_FILE="${RABBITMQ_CONFIG_FILE:-$RABBITMQ_DATA_DIR/rabbitmq.conf}"
export RABBITMQ_ALLOW_INPUT_NON_SENSITIVE_DATA=1
export RABBITMQ_CONFIG_FILE
export RABBITMQ_MNESIA_BASE="$RABBITMQ_DATA_DIR/mnesia"
export RABBITMQ_LOG_BASE="$RABBITMQ_DATA_DIR/log"
export RABBITMQ_PID_FILE="$RABBITMQ_DATA_DIR/rabbitmq.pid"
export PATH="$CONDA_ENV_PREFIX/bin:$CONDA_ENV_PREFIX/lib/rabbitmq/sbin:$PATH"

run mkdir -p "$RABBITMQ_MNESIA_BASE" "$RABBITMQ_LOG_BASE" \
    "$(dirname "$RABBITMQ_CONFIG_FILE")"

if [ "$DRY_RUN" -eq 1 ]; then
    log "+ write compatibility setting to $RABBITMQ_CONFIG_FILE"
else
    "$PYTHON_BIN" - "$RABBITMQ_CONFIG_FILE" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
key = "deprecated_features.permit.transient_nonexcl_queues"
setting = f"{key} = true"
lines = path.read_text().splitlines() if path.exists() else []
lines = [line for line in lines if not line.strip().startswith(f"{key} =")]
lines.append(setting)
path.write_text("\n".join(lines) + "\n")
PY
fi

port_is_open() {
    "$PYTHON_BIN" - "$RABBITMQ_PORT" <<'PY'
import socket
import sys

try:
    with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.5):
        pass
except OSError:
    raise SystemExit(1)
PY
}

if [ "$DRY_RUN" -eq 0 ] && port_is_open; then
    if [ "$RESTART" -eq 0 ]; then
        log "RabbitMQ is already reachable at amqp://localhost:${RABBITMQ_PORT}."
        log "Run again with --restart to apply the compatibility configuration."
        exit 0
    fi
    if ! RABBITMQCTL="$(find_rabbitmqctl)"; then
        log "Error: rabbitmqctl was not found in $CONDA_ENV_PREFIX."
        exit 1
    fi
    log "Stopping the running RabbitMQ node..."
    run "$RABBITMQCTL" shutdown
fi

log "Starting RabbitMQ from Conda in detached mode..."
run "$RABBITMQ_SERVER" -detached

if [ "$DRY_RUN" -eq 1 ]; then
    log "Dry run complete; readiness probe skipped."
    exit 0
fi

log "Waiting for RabbitMQ on 127.0.0.1:${RABBITMQ_PORT}..."
"$PYTHON_BIN" - "$RABBITMQ_PORT" "$RABBITMQ_WAIT_TIMEOUT" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
timeout = float(sys.argv[2])
deadline = time.monotonic() + timeout

while True:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            break
    except OSError as exc:
        if time.monotonic() >= deadline:
            raise SystemExit(
                f"RabbitMQ did not become ready on 127.0.0.1:{port} "
                f"within {timeout:g} seconds: {exc}"
            ) from exc
        time.sleep(1)
PY

log "RabbitMQ is ready at amqp://localhost:${RABBITMQ_PORT}."
