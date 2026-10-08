#!/bin/sh
set -eu

RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
RABBITMQ_WAIT_TIMEOUT="${RABBITMQ_WAIT_TIMEOUT:-60}"
RABBITMQ_DATA_DIR="${RABBITMQ_DATA_DIR:-${YUKIDIR:-$HOME/.Yuki}/RabbitMQ}"
CONDA_ENV_PREFIX="${CONDA_PREFIX:-}"
DRY_RUN=0

usage() {
    cat <<'EOF'
Usage: scripts/setup-rabbitmq.sh [--prefix CONDA_PREFIX] [--dry-run]

Install rabbitmq-server from conda-forge into a Conda environment, start it in
detached mode, and wait until the AMQP port is ready. The active environment is
used by default; when no environment is active, the Conda base environment is
used.

Options:
  --prefix PATH  Install into and run from this existing Conda environment.
  --dry-run      Print installation and startup commands without running them.
  -h, --help     Show this help message.

Environment variables:
  RABBITMQ_PORT          AMQP port to probe (default: 5672)
  RABBITMQ_WAIT_TIMEOUT  Readiness timeout in seconds (default: 60)
  RABBITMQ_DATA_DIR      State/log directory (default: ~/.Yuki/RabbitMQ)
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

if [ -z "$CONDA_ENV_PREFIX" ]; then
    CONDA_ENV_PREFIX="$(conda info --base)"
fi
CONDA_ENV_PREFIX="${CONDA_ENV_PREFIX%/}"

if [ ! -d "$CONDA_ENV_PREFIX/conda-meta" ]; then
    log "Error: not an existing Conda environment: $CONDA_ENV_PREFIX"
    exit 1
fi

find_rabbitmq_server() {
    for candidate in \
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

if RABBITMQ_SERVER="$(find_rabbitmq_server)"; then
    log "RabbitMQ is already installed in $CONDA_ENV_PREFIX."
else
    log "Installing rabbitmq-server from conda-forge into $CONDA_ENV_PREFIX..."
    run conda install --yes --prefix "$CONDA_ENV_PREFIX" \
        --channel conda-forge rabbitmq-server
    if [ "$DRY_RUN" -eq 1 ]; then
        RABBITMQ_SERVER="$CONDA_ENV_PREFIX/sbin/rabbitmq-server"
    elif ! RABBITMQ_SERVER="$(find_rabbitmq_server)"; then
        log "Error: Conda installation completed, but rabbitmq-server was not found."
        exit 1
    fi
fi

PYTHON_BIN="$CONDA_ENV_PREFIX/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
    log "Error: Python was not found in the Conda environment: $PYTHON_BIN"
    exit 1
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
    log "RabbitMQ is already reachable at amqp://localhost:${RABBITMQ_PORT}."
    exit 0
fi

export RABBITMQ_ALLOW_INPUT_NON_SENSITIVE_DATA=1
export RABBITMQ_MNESIA_BASE="$RABBITMQ_DATA_DIR/mnesia"
export RABBITMQ_LOG_BASE="$RABBITMQ_DATA_DIR/log"
export RABBITMQ_PID_FILE="$RABBITMQ_DATA_DIR/rabbitmq.pid"

run mkdir -p "$RABBITMQ_MNESIA_BASE" "$RABBITMQ_LOG_BASE"
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
