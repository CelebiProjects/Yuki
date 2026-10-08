"""Tests for the host RabbitMQ setup script."""
import subprocess
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "setup-rabbitmq.sh"


def test_setup_rabbitmq_script_has_valid_shell_syntax():
    result = subprocess.run(
        ["sh", "-n", str(SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_setup_rabbitmq_script_help_is_side_effect_free():
    result = subprocess.run(
        ["sh", str(SCRIPT), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "Usage: scripts/setup-rabbitmq.sh" in result.stdout
    assert "conda-forge" in result.stdout
    assert "--prefix" in result.stdout
    assert "--dry-run" in result.stdout
