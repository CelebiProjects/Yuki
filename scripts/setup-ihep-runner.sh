#!/bin/sh
set -eu

IHEP_ROOT="${YUKI_IHEP_ROOT:-/cefs/higgs/zhaomr/yuki-runner}"
MINICONDA_URL="${YUKI_IHEP_MINICONDA_URL:-https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh}"
SKIP_AUTH=0
DRY_RUN=0

usage() {
    cat <<'EOF'
Usage: scripts/setup-ihep-runner.sh [options]

Install a self-contained Conda and Snakemake environment for Yuki's IHEP
runner. Run this script interactively on lxlogin.ihep.ac.cn so kinit can ask
for your password. Runtime files live on CEFS and do not require an AFS token.

Options:
  --root PATH   Installation and workflow root
                (default: /cefs/higgs/zhaomr/yuki-runner)
  --skip-auth   Skip kinit/aklog (only when a valid AFS token already exists)
  --dry-run     Print the installation actions without changing files
  -h, --help    Show this help message

Environment variables:
  YUKI_IHEP_ROOT            Alternative default for --root
  YUKI_IHEP_MINICONDA_URL   Alternative Miniconda installer URL
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
        --root)
            if [ "$#" -lt 2 ]; then
                log "Error: --root requires a path."
                exit 2
            fi
            IHEP_ROOT="$2"
            shift
            ;;
        --skip-auth)
            SKIP_AUTH=1
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

case "$IHEP_ROOT" in
    /*) ;;
    *)
        log "Error: --root must be an absolute shared-filesystem path."
        exit 2
        ;;
esac

IHEP_ROOT="${IHEP_ROOT%/}"
CONDA_ROOT="$IHEP_ROOT/miniconda3"
CONDA_REAL="$CONDA_ROOT/bin/conda"
RUNNER_ENV="$IHEP_ROOT/envs/snakemake"
SCRIPT_ENV="$IHEP_ROOT/conda-envs/script"
WORKFLOW_ROOT="$IHEP_ROOT/workflows"
RUNNER_BIN="$IHEP_ROOT/bin"
RUNNER_CONDA="$RUNNER_BIN/conda"
CONDARC="$IHEP_ROOT/condarc.yml"
CACHE_ROOT="$IHEP_ROOT/cache"
CONFIG_ROOT="$IHEP_ROOT/config"
PKGS_ROOT="$IHEP_ROOT/conda-pkgs"
ENVS_ROOT="$IHEP_ROOT/conda-envs"
INSTALLER="$IHEP_ROOT/downloads/Miniconda3-latest-Linux-x86_64.sh"
SETTINGS="$IHEP_ROOT/runner-settings.txt"

HEP_ROOT=/afs/ihep.ac.cn/soft/common/sysgroup/hep_job/bin
HEP_SUB="$HEP_ROOT/hep_sub"
HEP_Q="$HEP_ROOT/hep_q"
HEP_RM="$HEP_ROOT/hep_rm"

for command_name in kinit aklog tokens curl; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        log "Error: required command is unavailable: $command_name"
        exit 1
    fi
done

for hep_command in "$HEP_SUB" "$HEP_Q" "$HEP_RM"; do
    if [ ! -x "$hep_command" ]; then
        log "Error: IHEP batch command is unavailable: $hep_command"
        exit 1
    fi
done

if [ "$SKIP_AUTH" -eq 0 ]; then
    if ! klist >/dev/null 2>&1; then
        if [ "$DRY_RUN" -eq 1 ]; then
            log "+ kinit"
        elif [ ! -t 0 ]; then
            log "Error: kinit needs an interactive terminal."
            log "SSH to lxlogin first, then run this script there."
            exit 1
        else
            log "No Kerberos ticket found; starting interactive kinit..."
            kinit
        fi
    else
        log "Using the existing Kerberos ticket."
    fi
    run aklog -d
fi

if [ "$DRY_RUN" -eq 0 ] && [ "$SKIP_AUTH" -eq 0 ]; then
    if ! tokens 2>&1 | grep -q 'ihep.ac.cn'; then
        log "Error: no IHEP AFS token is active; run kinit and aklog -d."
        exit 1
    fi
    if [ ! -w "${HOME}" ]; then
        log "Error: AFS home is still not writable after aklog: ${HOME}"
        exit 1
    fi
fi

run mkdir -p "$IHEP_ROOT/downloads" "$RUNNER_BIN" "$WORKFLOW_ROOT" \
    "$CACHE_ROOT" "$CONFIG_ROOT" "$PKGS_ROOT" "$ENVS_ROOT"

if [ ! -x "$CONDA_REAL" ]; then
    if [ -e "$CONDA_ROOT" ]; then
        log "Error: incomplete installation already exists: $CONDA_ROOT"
        log "Move it aside and rerun this script."
        exit 1
    fi
    log "Downloading Miniconda from $MINICONDA_URL ..."
    run curl --fail --location --retry 3 --connect-timeout 20 \
        --max-time 300 --output "$INSTALLER" "$MINICONDA_URL"
    log "Installing Miniconda at $CONDA_ROOT ..."
    run sh "$INSTALLER" -b -p "$CONDA_ROOT"
else
    log "Reusing Miniconda at $CONDA_ROOT."
fi

if [ "$DRY_RUN" -eq 1 ]; then
    log "+ write $CONDARC"
    log "+ write $RUNNER_CONDA"
else
    cat > "$CONDARC" <<'EOF'
register_envs: false
channels:
  - conda-forge
channel_priority: strict
EOF
    cat > "$RUNNER_CONDA" <<EOF
#!/bin/sh
export CONDARC='$CONDARC'
export XDG_CACHE_HOME='$CACHE_ROOT'
export XDG_CONFIG_HOME='$CONFIG_ROOT'
export CONDA_PKGS_DIRS='$PKGS_ROOT'
export CONDA_ENVS_PATH='$ENVS_ROOT'
exec '$CONDA_REAL' "\$@"
EOF
    chmod +x "$RUNNER_CONDA"
fi

if [ -x "$RUNNER_ENV/bin/snakemake" ]; then
    log "Updating the existing Snakemake runner environment..."
    conda_action=install
else
    log "Creating the Snakemake runner environment..."
    conda_action=create
fi
run "$RUNNER_CONDA" "$conda_action" --yes --prefix "$RUNNER_ENV" \
    --solver libmamba --override-channels --channel conda-forge \
    --repodata-fn current_repodata.json \
    'python=3.12' pip
run env "PIP_CACHE_DIR=$CACHE_ROOT/pip" "XDG_CACHE_HOME=$CACHE_ROOT" \
    "$RUNNER_ENV/bin/python" -m pip install --upgrade \
    'snakemake>=8.6,<10' snakemake-executor-plugin-cluster-generic

if [ -x "$SCRIPT_ENV/bin/python" ]; then
    log "Updating the script analysis environment..."
    script_action=install
else
    log "Creating the script analysis environment..."
    script_action=create
fi
run "$RUNNER_CONDA" "$script_action" --yes --prefix "$SCRIPT_ENV" \
    --solver libmamba --override-channels --channel conda-forge \
    --repodata-fn current_repodata.json \
    'python=3.12'

if [ "$DRY_RUN" -eq 1 ]; then
    log "+ verify conda, snakemake, cluster-generic, and shared workdir"
    exit 0
fi

SNAKEMAKE="$RUNNER_ENV/bin/snakemake"
if [ ! -x "$SNAKEMAKE" ]; then
    log "Error: Snakemake was not installed at $SNAKEMAKE"
    exit 1
fi

"$RUNNER_CONDA" --version
"$SNAKEMAKE" --version
"$RUNNER_CONDA" run --name script python --version
if ! "$SNAKEMAKE" --help 2>&1 | grep -q -- '--cluster-generic-submit-cmd'; then
    log "Error: Snakemake cannot see the cluster-generic executor plugin."
    exit 1
fi
if ! "$SNAKEMAKE" --help 2>&1 | grep -q -- '--use-conda'; then
    log "Error: this Snakemake version does not expose --use-conda."
    exit 1
fi

IHEP_GROUP="$(id -gn)"
cat > "$SETTINGS" <<EOF
backend_type=ihep
ssh_host=lxlogin.ihep.ac.cn
ssh_user=$(id -un)
remote_workdir=$WORKFLOW_ROOT
snakemake_path=$SNAKEMAKE
conda_path=$RUNNER_CONDA
hep_group=$IHEP_GROUP
hep_sub_path=$HEP_SUB
hep_q_path=$HEP_Q
hep_rm_path=$HEP_RM
hep_max_jobs=10
hep_step_memory_mb=1024
EOF

log ""
log "IHEP runner environment is ready."
log "Yuki settings were written to: $SETTINGS"
log ""
cat "$SETTINGS"
log ""
log "Keep this shell open until validation is complete so its AFS token can"
log "be used for any manual home-directory checks. Batch execution itself uses CEFS."
