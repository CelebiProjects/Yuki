"""Tests for the IHEP runner environment setup script."""
import subprocess
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "setup-ihep-runner.sh"


def test_setup_ihep_runner_script_has_valid_shell_syntax():
    result = subprocess.run(
        ["sh", "-n", str(SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_setup_ihep_runner_script_help_is_side_effect_free():
    result = subprocess.run(
        ["sh", str(SCRIPT), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "Usage: scripts/setup-ihep-runner.sh" in result.stdout
    assert "kinit" in result.stdout
    assert "CEFS" in result.stdout
    assert "--skip-auth" in result.stdout
    assert "--dry-run" in result.stdout


def test_setup_ihep_runner_is_independent_of_afs_at_runtime():
    source = SCRIPT.read_text()

    assert "register_envs: false" in source
    assert "exec '$CONDA_REAL'" in source
    assert "XDG_CACHE_HOME" in source
    assert "XDG_CONFIG_HOME" in source
    assert "CONDA_PKGS_DIRS" in source
    assert "CONDA_ENVS_PATH" in source
    assert "snakemake-executor-plugin-cluster-generic" in source
    assert '"$RUNNER_ENV/bin/python" -m pip install' in source
    assert "remote_workdir=$WORKFLOW_ROOT" in source
    assert "hep_sub_path=$HEP_SUB" in source


def test_setup_ihep_runner_checks_kerberos_and_afs():
    source = SCRIPT.read_text()

    assert "kinit" in source
    assert "aklog -d" in source
    assert "tokens" in source
    assert 'if [ ! -w "${HOME}" ]' in source
