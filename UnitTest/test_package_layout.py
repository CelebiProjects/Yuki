"""Checks for the reorganized Yuki package boundaries."""
import importlib
from pathlib import Path

import pytest


CANONICAL_MODULES = (
    "Yuki.kernel.jobs.base",
    "Yuki.kernel.jobs.container",
    "Yuki.kernel.jobs.image",
    "Yuki.kernel.workflows.base",
    "Yuki.kernel.workflows.native",
    "Yuki.kernel.workflows.ssh",
    "Yuki.kernel.workflows.ihep",
    "Yuki.kernel.workflows.reana",
    "Yuki.kernel.runners.config",
    "Yuki.kernel.runners.environments",
    "Yuki.kernel.runners.ssh_pool",
    "Yuki.kernel.execution.lease",
    "Yuki.kernel.execution.submissions",
    "Yuki.kernel.execution.progress",
    "Yuki.kernel.execution.status",
    "Yuki.kernel.execution.detached_copy",
    "Yuki.kernel.execution.monitor",
    "Yuki.kernel.execution.local",
    "Yuki.kernel.storage.liveness",
    "Yuki.kernel.storage.cache",
    "Yuki.kernel.storage.file_types",
    "Yuki.kernel.storage.staging",
    "Yuki.kernel.storage.remote",
    "Yuki.kernel.storage.rawdata",
    "Yuki.kernel.storage.impressions",
    "Yuki.kernel.workflows.snakefile",
    "Yuki.services.impression_transfer",
    "Yuki.services.result_transfer",
    "Yuki.services.runner_inventory",
    "Yuki.services.workflow_kill",
    "Yuki.services.workflow_purge",
    "Yuki.server.services.reana_booking",
    "Yuki.utils.locked_metadata",
    "Yuki.utils.resource_units",
)

@pytest.mark.parametrize("module", CANONICAL_MODULES)
def test_canonical_module_imports(module):
    """Every canonical package path is importable."""
    assert importlib.import_module(module) is not None


def test_flat_kernel_implementations_are_removed():
    """The kernel root is a namespace, not a home for implementations."""
    kernel = Path(__file__).parents[1] / "Yuki" / "kernel"
    assert sorted(path.name for path in kernel.glob("*.py")) == ["__init__.py"]


def test_misplaced_utils_are_removed():
    """Runner and workflow helpers no longer masquerade as generic utilities."""
    utils = Path(__file__).parents[1] / "Yuki" / "utils"
    assert not (utils / "env_interpreter.py").exists()
    assert not (utils / "snakefile.py").exists()
