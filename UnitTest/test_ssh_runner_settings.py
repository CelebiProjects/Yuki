"""Tests for ssh runner settings consumption."""
# pylint: disable=protected-access
import json
from unittest import mock

import pytest

from Yuki.kernel.ssh_workflow import SshWorkflow


def _workflow(tmp_path, monkeypatch, config_data):
    yuki_dir = tmp_path / ".Yuki"
    yuki_dir.mkdir(parents=True, exist_ok=True)
    (yuki_dir / "config.json").write_text(json.dumps(config_data))
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))
    monkeypatch.setenv("HOME", str(tmp_path))
    with mock.patch.object(SshWorkflow, "__init__", lambda self, *a, **k: None):
        wf = SshWorkflow.__new__(SshWorkflow)
    wf.machine_id = "m1"
    return wf


def test_load_ssh_config_merges_new_and_legacy(tmp_path, monkeypatch):
    """_load_ssh_config merges runner_settings over legacy ssh_* maps."""
    wf = _workflow(tmp_path, monkeypatch, {
        "ssh_hosts": {"m1": "legacy-host"},
        "runner_settings": {"m1": {"ssh_user": "new-user", "cores": 16}},
    })
    cfg = wf._load_ssh_config()
    assert cfg["host"] == "legacy-host"   # legacy fallback
    assert cfg["user"] == "new-user"      # new map
    assert cfg["cores"] == 16


def test_wrapper_uses_cores_and_paths(tmp_path, monkeypatch):
    """The remote snakemake wrapper embeds cores and conda paths."""
    wf = _workflow(tmp_path, monkeypatch, {
        "runner_settings": {"m1": {
            "ssh_host": "h", "ssh_user": "u", "cores": 8,
            "snakemake_path": "/opt/bin/snakemake",
            "conda_path": "/opt/conda/bin/conda",
            "remote_workdir": "/remote",
        }},
    })
    wf.ssh_config = wf._load_ssh_config()
    wf.remote_exec_path = "/remote/wf-uuid"
    wf.logger = lambda msg: None

    written = {}

    class FakeSsh:
        """Ssh shim recording wrapper files written remotely."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def put_text(self, text, path):
            """Record the written wrapper file."""
            written[path] = text

        def remove(self, _path):
            """Ignore stale runtime-marker cleanup."""

        def exec(self, _cmd, timeout=None):  # pylint: disable=unused-argument
            """Report a successful remote command."""
            return "", "", 0

        def exec_start_detached(self, command, timeout=30, grace=2):  # pylint: disable=unused-argument
            """Report a successful detached start."""
            return "", "", 0

    with mock.patch.object(SshWorkflow, "_ssh", return_value=FakeSsh()), \
            mock.patch.object(SshWorkflow, "_confirm_remote_start"):
        wf._start_remote_snakemake()

    wrapper = written["/remote/wf-uuid/yuki_run.sh"]
    assert "/opt/bin/snakemake --use-conda --cores 8" in wrapper
    assert "/opt/conda/bin" in wrapper  # PATH injection for conda


def _workflow_for_layout(tmp_path, monkeypatch):
    wf = _workflow(tmp_path, monkeypatch, {
        "runner_settings": {"m1": {"ssh_host": "h", "ssh_user": "u",
                                   "remote_workdir": "/remote"}},
    })
    wf.project_uuid = "proj-123"
    wf.uuid = "wf-456"
    return wf


def test_remote_exec_path_has_workflows_and_project(tmp_path, monkeypatch):
    """New layout: <remote_workdir>/workflows/<project_uuid>/<uuid>."""
    yuki_dir = tmp_path / ".Yuki"
    yuki_dir.mkdir(parents=True)
    (yuki_dir / "config.json").write_text(json.dumps({
        "runner_settings": {"m1": {"ssh_host": "h", "ssh_user": "u",
                                   "remote_workdir": "/remote"}},
    }))
    monkeypatch.setenv("YUKIDIR", str(yuki_dir))
    monkeypatch.setenv("HOME", str(tmp_path))

    from Yuki.kernel.vworkflow import VWorkflow

    def fake_init(self, project_uuid, _jobs, uuid=None):
        self.project_uuid = project_uuid
        self.uuid = uuid
        self.machine_id = "m1"

    with mock.patch.object(VWorkflow, "__init__", fake_init):
        wf = SshWorkflow("proj-123", [], uuid="wf-456")
    assert wf.remote_exec_path == "/remote/workflows/proj-123/wf-456"


def test_create_remote_structure_creates_impressions_dir(tmp_path, monkeypatch):
    """The reserved impressions/<project_uuid> dir is created alongside."""
    wf = _workflow_for_layout(tmp_path, monkeypatch)
    wf.ssh_config = wf._load_ssh_config()
    wf.remote_exec_path = "/remote/workflows/proj-123/wf-456"
    wf.remote_impressions_path = "/remote/impressions/proj-123"
    wf.logger = lambda msg: None
    wf.dependencies = {}
    wf.steps = []

    made_dirs = []

    class FakeSsh:
        """Ssh shim recording the directories created remotely."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def mkdir_p(self, path):
            """Record the created directory."""
            made_dirs.append(path)

        def put_text(self, text, path):
            """No-op text upload."""

    with mock.patch.object(SshWorkflow, "_ssh", return_value=FakeSsh()), \
            mock.patch.object(SshWorkflow, "get_name", return_value="wf"):
        wf._create_remote_structure()

    assert "/remote/workflows/proj-123/wf-456" in made_dirs
    assert "/remote/impressions/proj-123" in made_dirs


def test_stage_remote_hosted_input_copies_locally(tmp_path, monkeypatch):
    """Remote-hosted data on the same runner: one remote cp, no SFTP."""
    yuki_dir = tmp_path / ".Yuki"
    marker_dir = yuki_dir / "Storage" / "proj-123" / "imp-abc"
    marker_dir.mkdir(parents=True)
    with open(marker_dir / "remote.json", "w", encoding="utf-8") as f:
        json.dump({"host_runner_id": "m1", "source_path": "/src",
                   "remote_path": "/remote/impressions/proj-123/imp-abc"}, f)
    monkeypatch.setenv("HOME", str(tmp_path))

    wf = _workflow(tmp_path, monkeypatch, {
        "runner_settings": {"m1": {"ssh_host": "h", "ssh_user": "u"}},
    })
    wf.ssh_config = wf._load_ssh_config()
    wf.remote_exec_path = "/remote/workflows/proj-123/wf-456"
    wf.project_uuid = "proj-123"
    wf.uuid = "wf-456"
    wf.machine_id = "m1"
    wf.snakefile_path = "/local/Snakefile"
    wf.logger = lambda msg: None

    fake_job = mock.MagicMock()
    fake_job.files.return_value = []
    fake_job.environment.return_value = "rawdata"
    fake_job.is_input = True
    fake_job.short_uuid.return_value = "abc1234"
    fake_job.path = f"{marker_dir.parent}/imp-abc"  # parent = Storage/proj-123
    wf.jobs = [fake_job]

    commands = []

    class FakeSsh:
        """Ssh shim recording every remote operation."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def mkdir_p(self, path):
            """Record the created directory."""
            commands.append(("mkdir", path))

        def put(self, local_path, remote_path):
            """Record the sftp upload."""
            commands.append(("put", f"{local_path} -> {remote_path}"))

        def exec(self, command, timeout=None):  # pylint: disable=unused-argument
            """Record the remote command and report success."""
            commands.append(("exec", command))
            return "", "", 0

    with mock.patch.object(SshWorkflow, "_ssh", return_value=FakeSsh()):
        wf._upload_files_remote()

    execs = [c for kind, c in commands if kind == "exec"]
    copy_cmd = [c for c in execs if "cp -a --reflink=auto" in c]
    assert not copy_cmd, (
        f"registered data is already in the cache; the setup rule copies "
        f"it, staging must not: {execs}")
    puts = [c for kind, c in commands if kind == "put"]
    data_puts = [c for c in puts if "impabc1234/stageout" in c]
    assert not data_puts, \
        f"expected no SFTP put of remote-hosted data, got: {data_puts}"


def test_stage_remote_hosted_input_wrong_runner_raises(tmp_path, monkeypatch):
    """Remote-hosted data on another runner raises with a clear error."""
    yuki_dir = tmp_path / ".Yuki"
    marker_dir = yuki_dir / "Storage" / "proj-123" / "imp-abc"
    marker_dir.mkdir(parents=True)
    with open(marker_dir / "remote.json", "w", encoding="utf-8") as f:
        json.dump({"host_runner_id": "OTHER-RUNNER", "source_path": "/src",
                   "remote_path": "/remote/impressions/proj-123/imp-abc"}, f)
    monkeypatch.setenv("HOME", str(tmp_path))

    wf = _workflow(tmp_path, monkeypatch, {
        "runner_settings": {"m1": {"ssh_host": "h", "ssh_user": "u"}},
    })
    wf.ssh_config = wf._load_ssh_config()
    wf.remote_exec_path = "/remote/workflows/proj-123/wf-456"
    wf.project_uuid = "proj-123"
    wf.machine_id = "m1"
    wf.snakefile_path = "/local/Snakefile"
    wf.logger = lambda msg: None
    fake_job = mock.MagicMock()
    fake_job.files.return_value = []
    fake_job.environment.return_value = "rawdata"
    fake_job.is_input = True
    fake_job.short_uuid.return_value = "abc1234"
    fake_job.path = f"{marker_dir.parent}/imp-abc"
    wf.jobs = [fake_job]

    with mock.patch.object(SshWorkflow, "_ssh", return_value=mock.MagicMock()):
        with pytest.raises(RuntimeError) as exc:
            wf._upload_files_remote()
    assert "another runner" in str(exc.value)


def _input_job_stub(marker_dir, env="rawdata"):
    fake_job = mock.MagicMock()
    fake_job.files.return_value = []
    fake_job.environment.return_value = env
    fake_job.is_input = True
    fake_job.machine_id = "m1"
    fake_job.short_uuid.return_value = "abc1234"
    fake_job.path = str(marker_dir.parent / "imp-abc")
    return fake_job


def _wf_with_input(tmp_path, monkeypatch, fake_job):
    wf = _workflow(tmp_path, monkeypatch, {
        "runner_settings": {"m1": {"ssh_host": "h", "ssh_user": "u",
                                   "remote_workdir": "/remote/work"}},
    })
    wf.ssh_config = wf._load_ssh_config()
    wf.remote_exec_path = "/remote/work/workflows/proj-123/wf-456"
    wf.project_uuid = "proj-123"
    wf.machine_id = "m1"
    wf.snakefile_path = "/local/Snakefile"
    wf.jobs = [fake_job]
    wf.logger = lambda msg: None
    return wf


def test_input_cache_hit_skips_sftp(tmp_path, monkeypatch):
    """A non-empty impressions cache stages via remote cp, no SFTP upload."""
    yuki_dir = tmp_path / ".Yuki"
    (yuki_dir / "Storage" / "proj-123").mkdir(parents=True)
    monkeypatch.setenv("HOME", str(tmp_path))
    fake_job = _input_job_stub(yuki_dir / "Storage" / "proj-123" / "imp-abc")
    wf = _wf_with_input(tmp_path, monkeypatch, fake_job)

    commands = []
    puts = []

    class FakeSsh:
        """Ssh shim answering the cache-hit probe positively."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def mkdir_p(self, path):
            """Record the created directory."""
            commands.append(("mkdir", path))

        def exec(self, command, timeout=None):  # pylint: disable=unused-argument
            """Record the command; probe answers as a cache hit."""
            commands.append(("exec", command))
            if command.startswith("test -d"):
                return "", "", 0  # cache hit
            return "", "", 0

        def put(self, src, dst):
            """Record the sftp upload."""
            puts.append((src, dst))

        def put_text(self, text, path):
            """No-op text upload."""

    with mock.patch.object(SshWorkflow, "_ssh", return_value=FakeSsh()):
        wf._upload_files_remote()

    execs = [c for kind, c in commands if kind == "exec"]
    cp_cmd = [c for c in execs if "cp -a --reflink=auto" in c]
    assert not cp_cmd, f"the setup rule copies on cache hit: {execs}"
    # no SFTP put of data files (only the Snakefile put is allowed)
    data_puts = [p for p in puts if "impressions" in p[1] or "stageout" in p[1]]
    assert data_puts == []


def test_input_cache_miss_writes_through(tmp_path, monkeypatch):
    """On a miss, data is SFTP-uploaded AND cached into impressions."""
    yuki_dir = tmp_path / ".Yuki"
    src_stageout = yuki_dir / "Storage" / "proj-123" / "imp-abc" / "m1" / "stageout"
    src_stageout.mkdir(parents=True)
    with open(src_stageout / "data.txt", "w", encoding="utf-8") as f:
        f.write("payload")
    monkeypatch.setenv("HOME", str(tmp_path))
    fake_job = _input_job_stub(yuki_dir / "Storage" / "proj-123" / "imp-abc",
                               env="analysis")
    wf = _wf_with_input(tmp_path, monkeypatch, fake_job)

    commands = []

    class FakeSsh:
        """Ssh shim answering the cache-hit probe negatively."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def mkdir_p(self, path):
            """Record the created directory."""
            commands.append(("mkdir", path))

        def exec(self, command, timeout=None):  # pylint: disable=unused-argument
            """Record the command; probe answers as a cache miss."""
            commands.append(("exec", command))
            if command.startswith("test -d"):
                return "", "", 1  # cache miss
            return "", "", 0

        def put(self, src, dst):
            """Record the sftp upload."""
            commands.append(("put", f"{src}->{dst}"))

        def put_text(self, text, path):
            """No-op text upload."""

    with mock.patch.object(SshWorkflow, "_ssh", return_value=FakeSsh()):
        wf._upload_files_remote()

    execs = [c for kind, c in commands if kind == "exec"]
    puts = [c for kind, c in commands if kind == "put"]
    data_puts = [p for p in puts if "data.txt" in p]
    assert data_puts, puts
    assert data_puts[0].endswith(
        "stageout/data.txt->"
        "/remote/work/impressions/proj-123/imp-abc/data.txt"), data_puts[0]
    copy_cmd = [c for c in execs if "cp -a --reflink=auto" in c]
    assert not copy_cmd, f"staging must not copy into imp dirs: {execs}"


def test_remote_hosted_staging_does_not_shadow_outer_connection(tmp_path, monkeypatch):
    """The snakefile put after the job loop must use the OPEN outer connection.

    Regression: the remote.json branch used to open a nested `with
    self._ssh() as ssh:` which shadowed the outer connection; after the
    inner connection closed, the trailing snakefile put crashed with
    AttributeError: 'NoneType' object has no attribute 'stat'.
    """
    yuki_dir = tmp_path / ".Yuki"
    marker_dir = yuki_dir / "Storage" / "proj-123" / "imp-abc"
    marker_dir.mkdir(parents=True)
    with open(marker_dir / "remote.json", "w", encoding="utf-8") as f:
        json.dump({"host_runner_id": "m1", "source_path": "/src",
                   "remote_path": "/remote/impressions/proj-123/imp-abc"}, f)
    monkeypatch.setenv("HOME", str(tmp_path))

    wf = _workflow(tmp_path, monkeypatch, {
        "runner_settings": {"m1": {"ssh_host": "h", "ssh_user": "u"}},
    })
    wf.ssh_config = wf._load_ssh_config()
    wf.remote_exec_path = "/remote/workflows/proj-123/wf-456"
    wf.project_uuid = "proj-123"
    wf.machine_id = "m1"
    wf.snakefile_path = "/local/Snakefile"
    wf.logger = lambda msg: None

    data_job = mock.MagicMock()
    data_job.files.return_value = []
    data_job.environment.return_value = "analysis"
    data_job.is_input = True
    data_job.machine_id = "m1"
    data_job.short_uuid.return_value = "abc1234"
    data_job.path = str(marker_dir.parent / "imp-abc")

    analysis_job = mock.MagicMock()
    analysis_job.files.return_value = []
    analysis_job.environment.return_value = "analysis"
    analysis_job.is_input = False
    analysis_job.machine_id = "m1"
    analysis_job.short_uuid.return_value = "def5678"
    analysis_job.path = str(marker_dir.parent / "imp-def")

    wf.jobs = [data_job, analysis_job]

    instances = []

    class FakeSsh:
        def __init__(self):
            self.closed = False
            instances.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self.closed = True
            return False

        def mkdir_p(self, path):
            assert not self.closed, "mkdir_p on a closed connection"

        def exec(self, command, timeout=None):
            assert not self.closed, "exec on a closed connection"
            if command.startswith("test -d"):
                return "", "", 1
            return "", "", 0

        def put(self, src, dst):
            assert not self.closed, "put on a closed connection"
            self._last_put = (src, dst)

        def put_text(self, text, path):
            assert not self.closed, "put_text on a closed connection"

    with mock.patch.object(SshWorkflow, "_ssh", side_effect=FakeSsh):
        wf._upload_files_remote()

    # exactly one connection was opened for the whole staging
    assert len(instances) == 1, f"expected 1 connection, got {len(instances)}"
    assert instances[0]._last_put == (
        "/local/Snakefile", "/remote/workflows/proj-123/wf-456/Snakefile")


def test_wrapper_sanitizes_environment_without_conda_path(tmp_path, monkeypatch):
    """The wrapper resets PYTHONPATH/LD_LIBRARY_PATH and curates PATH."""
    wf = _workflow(tmp_path, monkeypatch, {
        "runner_settings": {"m1": {
            "ssh_host": "h", "ssh_user": "u",
            "snakemake_path": "/opt/bin/snakemake",
            "remote_workdir": "/remote",
        }},
    })
    wf.ssh_config = wf._load_ssh_config()
    wf.remote_exec_path = "/remote/wf-uuid"
    wf.logger = lambda msg: None

    written = {}

    class FakeSsh:
        """Ssh shim recording wrapper files written remotely."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def put_text(self, text, path):
            """Record the written wrapper file."""
            written[path] = text

        def remove(self, _path):
            """Ignore stale runtime-marker cleanup."""

        def exec(self, _cmd, timeout=None):  # pylint: disable=unused-argument
            """Report a successful remote command."""
            return "", "", 0

        def exec_start_detached(self, command, timeout=30, grace=2):  # pylint: disable=unused-argument
            """Report a successful detached start."""
            return "", "", 0

    with mock.patch.object(SshWorkflow, "_ssh", return_value=FakeSsh()), \
            mock.patch.object(SshWorkflow, "_confirm_remote_start"):
        wf._start_remote_snakemake()

    wrapper = written["/remote/wf-uuid/yuki_run.sh"]
    assert "unset PYTHONPATH LD_LIBRARY_PATH" in wrapper
    assert "conda info --base" in wrapper
    assert "$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin" in wrapper
    assert ":$PATH" not in wrapper  # no ambient PATH leakage


def test_wrapper_conda_path_curates_path(tmp_path, monkeypatch):
    """With conda_path configured, the curated PATH uses it and no probe."""
    wf = _workflow(tmp_path, monkeypatch, {
        "runner_settings": {"m1": {
            "ssh_host": "h", "ssh_user": "u", "cores": 8,
            "snakemake_path": "/opt/bin/snakemake",
            "conda_path": "/opt/conda/bin/conda",
            "remote_workdir": "/remote",
        }},
    })
    wf.ssh_config = wf._load_ssh_config()
    wf.remote_exec_path = "/remote/wf-uuid"
    wf.logger = lambda msg: None

    written = {}

    class FakeSsh:
        """Ssh shim recording wrapper files written remotely."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def put_text(self, text, path):
            """Record the written wrapper file."""
            written[path] = text

        def remove(self, _path):
            """Ignore stale runtime-marker cleanup."""

        def exec(self, _cmd, timeout=None):  # pylint: disable=unused-argument
            """Report a successful remote command."""
            return "", "", 0

        def exec_start_detached(self, command, timeout=30, grace=2):  # pylint: disable=unused-argument
            """Report a successful detached start."""
            return "", "", 0

    with mock.patch.object(SshWorkflow, "_ssh", return_value=FakeSsh()), \
            mock.patch.object(SshWorkflow, "_confirm_remote_start"):
        wf._start_remote_snakemake()

    wrapper = written["/remote/wf-uuid/yuki_run.sh"]
    assert "/opt/conda/bin" in wrapper
    assert "conda info --base" not in wrapper
    assert "$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin" in wrapper
    assert ":$PATH" not in wrapper
