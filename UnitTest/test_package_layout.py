"""Import compatibility checks for the reorganized Yuki package."""
import importlib

import pytest


@pytest.mark.parametrize(("legacy", "canonical"), [
    ("Yuki.kernel.vjob", "Yuki.kernel.jobs.base"),
    ("Yuki.kernel.container_job", "Yuki.kernel.jobs.container"),
    ("Yuki.kernel.image_job", "Yuki.kernel.jobs.image"),
    ("Yuki.kernel.vworkflow", "Yuki.kernel.workflows.base"),
    ("Yuki.kernel.native_workflow", "Yuki.kernel.workflows.native"),
    ("Yuki.kernel.ssh_workflow", "Yuki.kernel.workflows.ssh"),
    ("Yuki.kernel.ihep_workflow", "Yuki.kernel.workflows.ihep"),
    ("Yuki.kernel.reana_workflow", "Yuki.kernel.workflows.reana"),
    ("Yuki.kernel.runner_config", "Yuki.kernel.runners.config"),
    ("Yuki.kernel.ssh_pool", "Yuki.kernel.runners.ssh_pool"),
    ("Yuki.kernel.execution_lease", "Yuki.kernel.execution.lease"),
    ("Yuki.kernel.submission_store", "Yuki.kernel.execution.submissions"),
    ("Yuki.kernel.registration_progress", "Yuki.kernel.execution.progress"),
    ("Yuki.kernel.liveness", "Yuki.kernel.storage.liveness"),
    ("Yuki.kernel.file_types", "Yuki.kernel.storage.file_types"),
    ("Yuki.kernel.impression_transfer", "Yuki.services.impression_transfer"),
    ("Yuki.kernel.runner_inventory", "Yuki.services.runner_inventory"),
    ("Yuki.kernel.workflow_kill", "Yuki.services.workflow_kill"),
    ("Yuki.kernel.workflow_purge", "Yuki.services.workflow_purge"),
    ("Yuki.kernel.reana_booker", "Yuki.server.services.reana_booking"),
    ("Yuki.kernel.locked_metadata", "Yuki.utils.locked_metadata"),
    ("Yuki.kernel.resource_units", "Yuki.utils.resource_units"),
    ("Yuki.utils.env_interpreter", "Yuki.kernel.runners.environments"),
    ("Yuki.utils.snakefile", "Yuki.kernel.workflows.snakefile"),
])
def test_legacy_module_is_canonical_module(legacy, canonical):
    """Legacy imports remain the same module object, preserving patch targets."""
    assert importlib.import_module(legacy) is importlib.import_module(canonical)
