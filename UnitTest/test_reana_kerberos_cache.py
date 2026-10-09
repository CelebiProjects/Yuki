"""Kerberos auto-enable for REANA jobs that cache outputs on the runner.

The reana cache lives on EOS, so any job with cache_on_runner=True needs
Kerberos there. SSH/native/dry caches live on the runner's own filesystem
and do not imply Kerberos.
"""
# pylint: disable=protected-access
import os
from unittest import mock

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.workflows import base as vworkflow
from Yuki.kernel.jobs.container import ContainerJob
from Yuki.kernel.workflows.reana import ReanaWorkflow
from Yuki.kernel.workflows.ssh import SshWorkflow
from Yuki.kernel.jobs.base import VJob


def _write_runner_flag(tmp_path, machine_id, value):
    """Write the runner-level use_kerberos flag into a temp HOME config."""
    yuki_dir = tmp_path / ".Yuki"
    os.makedirs(yuki_dir, exist_ok=True)
    cfg = ConfigFile(str(yuki_dir / "config.json"))
    cfg.write_variable("use_kerberos", {machine_id: value})


def _vjob(machine_id="m1", cache=True, predecessors=None):
    """Build a VJob stub whose use_kerberos() runs the real logic."""
    job = object.__new__(VJob)
    job.machine_id = machine_id
    job._use_kerberos = None
    job.path = "/store/proj/imp123456"
    job.cache_on_runner = mock.MagicMock(return_value=cache)
    job.predecessors = mock.MagicMock(return_value=predecessors or [])
    return job


def test_use_kerberos_reana_cache_on_runner_enables(monkeypatch, tmp_path):
    """cache_on_runner implies Kerberos only when the backend is reana."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    job = _vjob(cache=True)
    assert job.use_kerberos("reana") is True


def test_use_kerberos_no_backend_ignores_cache(monkeypatch, tmp_path):
    """Without backend context, cache_on_runner does not imply Kerberos."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    job = _vjob(cache=True)
    assert job.use_kerberos() is False


def test_use_kerberos_ssh_backend_ignores_cache(monkeypatch, tmp_path):
    """SSH caches on the runner's own filesystem, no Kerberos needed."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    job = _vjob(cache=True)
    assert job.use_kerberos("ssh") is False


def test_use_kerberos_native_backend_ignores_cache(monkeypatch, tmp_path):
    """Native backends have no runner-side cache, so no Kerberos."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    job = _vjob(cache=True)
    assert job.use_kerberos("native") is False


def test_use_kerberos_runner_flag_still_works(monkeypatch, tmp_path):
    """The runner-level flag still enables Kerberos on any backend."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", True)
    job = _vjob(cache=False)
    assert job.use_kerberos() is True
    assert job.use_kerberos("reana") is True
    assert job.use_kerberos("ssh") is True


def test_use_kerberos_lhcb_ap_datalist_still_works(monkeypatch, tmp_path):
    """An LHCb AP datalist predecessor still auto-enables Kerberos."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    pred = mock.MagicMock()
    pred.environment.return_value = "lhcb_ap_datalist"
    job = _vjob(cache=False, predecessors=[pred])
    assert job.use_kerberos() is True


def _container_job(cache=True, compute_backend="unsigned"):
    """Build a ContainerJob stub for _create_reana_step_metadata()."""
    job = object.__new__(ContainerJob)
    job.machine_id = "m1"
    job._use_kerberos = None
    job.is_input = False
    job.path = "/store/proj/imp123456"
    job.cache_on_runner = mock.MagicMock(return_value=cache)
    job.predecessors = mock.MagicMock(return_value=[])
    job.environment = mock.MagicMock(return_value="root")
    job.compute_backend = mock.MagicMock(return_value=compute_backend)
    job.memory = mock.MagicMock(return_value="4096Mi")
    job.short_uuid = mock.MagicMock(return_value="1234567")
    job.cvmfs = mock.MagicMock(return_value=[])
    return job


def test_reana_step_kerberos_from_cache_on_runner(monkeypatch, tmp_path):
    """A cached-on-runner job gets kerberos=True in its REANA step."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    job = _container_job(cache=True)
    step = job._create_reana_step_metadata()
    assert step["kerberos"] is True


def test_reana_step_no_kerberos_without_cache_or_flag(monkeypatch, tmp_path):
    """No cache and no runner flag means no kerberos key in the step."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    job = _container_job(cache=False)
    step = job._create_reana_step_metadata()
    assert "kerberos" not in step


def test_reana_step_kerberos_htcondor_forces(monkeypatch, tmp_path):
    """htcondorcern steps always get kerberos=True regardless of cache."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    job = _container_job(cache=False, compute_backend="htcondorcern")
    step = job._create_reana_step_metadata()
    assert step["kerberos"] is True


class _FakeContainer:
    """ContainerJob stand-in for construct_snake_file tests."""

    def __init__(self, path, machine_id):
        del path, machine_id

    def setup_commands(self, backend_type, workflow_machine_id=None):
        """Fake: copy the cached input into place in the setup rule."""
        del backend_type, workflow_machine_id
        return ["mkdir -p imp1234567/stageout"]

    def finalize_commands(self, backend_type):
        """Fake: clean up the cache in the finalize rule."""
        del backend_type
        return ["rm -rf imp1234567/stageout"]

    def snakemake_rule(self, request_machine_id, backend_type="reana"):
        """Fake: a minimal unsigned-container rule dict."""
        del request_machine_id, backend_type
        return {"inputs": [],
                "environment": "reanahub/reana-env-root6:6.18.04",
                "compute_backend": "unsigned",
                "memory": "4096Mi",
                "commands": ["mkdir -p imp1234567/stageout"]}

    def step(self, request_machine_id, backend_type="reana"):
        """Fake: an empty REANA step dict."""
        del request_machine_id, backend_type
        return {}


def _workflow_job(machine_id="m1", is_input=False, cache=True):
    """A real-VJob task stub for construct_snake_file()."""
    job = _vjob(machine_id=machine_id, cache=cache)
    job.is_input = is_input
    job.object_type = mock.MagicMock(return_value="task")
    job.job_type = mock.MagicMock(return_value="task")
    job.short_uuid = mock.MagicMock(return_value="1234567")
    job.dependencies = mock.MagicMock(return_value=[])
    return job


def _build_snakefile(tmp_path, jobs, machine_id="m1", backend="reana"):
    """Run construct_snake_file on a stub workflow, return the file text."""
    wf_cls = SshWorkflow if backend == "ssh" else ReanaWorkflow
    wf = object.__new__(wf_cls)
    wf.project_uuid = "proj"
    wf.machine_id = machine_id
    wf.path = str(tmp_path / "wdir")
    os.makedirs(wf.path, exist_ok=True)
    wf.snakefile_path = os.path.join(wf.path, "Snakefile")
    wf.jobs = jobs
    wf.dependencies = {}
    wf.steps = []
    wf.logger = lambda *a, **k: None
    wf.backend_type = mock.MagicMock(return_value=backend)
    with mock.patch.object(vworkflow, "ContainerJob", _FakeContainer):
        wf.construct_snake_file()
    with open(wf.snakefile_path, encoding="utf-8") as f:
        return f.read()


def test_snakefile_reana_job_kerberos_from_cache_on_runner(monkeypatch, tmp_path):
    """A REANA task with cache_on_runner gets kerberos=True resources."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    text = _build_snakefile(tmp_path, [_workflow_job(cache=True)])
    assert "kerberos=True" in text


def test_snakefile_reana_setup_kerberos_from_cached_input(monkeypatch, tmp_path):
    """Fetching a cached input from EOS in setup enables Kerberos there."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    text = _build_snakefile(tmp_path, [_workflow_job(is_input=True, cache=True)])
    setup_block = text.split("rule setup:", 1)[1].split("\nrule ", 1)[0]
    assert "kerberos=True" in setup_block


def test_snakefile_ssh_no_kerberos_from_cache_on_runner(monkeypatch, tmp_path):
    """SSH cache_on_runner does not add kerberos=True anywhere."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    text = _build_snakefile(
        tmp_path, [_workflow_job(cache=True)], backend="ssh")
    assert "kerberos" not in text


def test_snakefile_reana_no_kerberos_without_cache(monkeypatch, tmp_path):
    """REANA tasks without cache and without the flag get no Kerberos."""
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_runner_flag(tmp_path, "m1", False)
    text = _build_snakefile(tmp_path, [_workflow_job(cache=False)])
    assert "kerberos" not in text
