"""Exercise generated SSH credential lifecycle without a KDC or keytab secrets."""
import os
from pathlib import Path
import signal
import subprocess
import time

import pytest

from CelebiChrono.utils.metadata import ConfigFile
from Yuki.kernel.runners.kerberos import workflow_kerberos
from Yuki.kernel.workflows.ssh import SshWorkflow


def _run(tmp_path, mode='success'):
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    keytab = tmp_path / 'keytab with spaces'
    keytab.touch()
    # Fake KDC records cache paths and refuses credentials in failure mode.
    (bindir / 'kinit').write_text('''#!/bin/bash
printf '%s\\n' "$KRB5CCNAME" >> "$PROBE_DIR/tickets"
touch "${KRB5CCNAME#FILE:}"
chmod 600 "${KRB5CCNAME#FILE:}"
[ "$PROBE_MODE" != fail ]
''')
    (bindir / 'timeout').write_text('#!/bin/bash\nshift\nexec "$@"\n')
    (bindir / 'snakemake').write_text('''#!/bin/bash
printf '%s\\n' "$KRB5CCNAME" > "$PROBE_DIR/child-cache"
if [ "$PROBE_MODE" = cancel ]; then sleep 30; else sleep 2.3; fi
''')
    for file in bindir.iterdir():
        file.chmod(0o755)
    wf = object.__new__(SshWorkflow)
    wf.config_file = ConfigFile(str(tmp_path / 'workflow-config.json'))
    wf.ssh_config = dict(use_kerberos=True, kerberos_keytab=str(keytab),
                         kerberos_principal='test@EXAMPLE.ORG', kerberos_renew_interval=1,
                         conda_path=str(bindir / 'conda'), snakemake_path=str(bindir / 'snakemake'))
    wrapper = tmp_path / 'yuki_run.sh'
    wrapper.write_text(wf._build_remote_wrapper())
    proc = subprocess.Popen(['bash', str(wrapper)], start_new_session=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            env=dict(os.environ, PROBE_DIR=str(tmp_path), PROBE_MODE=mode))
    return proc


def test_ticket_refresh_and_cleanup(tmp_path):
    proc = _run(tmp_path)
    _, err = proc.communicate(timeout=10)
    assert proc.returncode == 0, err
    caches = (tmp_path / 'tickets').read_text().splitlines()
    assert len(caches) >= 2
    assert len(set(caches)) == 1
    assert (tmp_path / 'child-cache').read_text().strip() == caches[0]
    assert not Path(caches[0].removeprefix('FILE:')).parent.exists()
    assert (tmp_path / 'yuki.exit').read_text().strip() == '0'


def test_initial_auth_failure_never_starts_workflow(tmp_path):
    proc = _run(tmp_path, 'fail')
    proc.communicate(timeout=10)
    assert proc.returncode != 0
    assert not (tmp_path / 'child-cache').exists()
    assert not (tmp_path / 'yuki.started').exists()
    cache = (tmp_path / 'tickets').read_text().strip().removeprefix('FILE:')
    assert not Path(cache).parent.exists()


def test_cancellation_cleans_private_cache(tmp_path):
    proc = _run(tmp_path, 'cancel')
    try:
        deadline = time.monotonic() + 5
        while not (tmp_path / 'child-cache').exists():
            assert time.monotonic() < deadline
            time.sleep(.05)
        cache = (tmp_path / 'child-cache').read_text().strip().removeprefix('FILE:')
        assert Path(cache).parent.stat().st_mode & 0o777 == 0o700
        os.killpg(proc.pid, signal.SIGTERM)
        proc.communicate(timeout=5)
        assert proc.returncode == 143
        assert not Path(cache).parent.exists()
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()


def test_disabled_and_invalid_settings():
    assert workflow_kerberos({}) == ('', '', '')
    with pytest.raises(ValueError):
        workflow_kerberos({'use_kerberos': True})
    with pytest.raises(ValueError):
        workflow_kerberos({'use_kerberos': True, 'kerberos_principal': 'x',
                           'kerberos_renew_interval': 0})


def test_refresh_failure_stops_workflow(tmp_path):
    proc = _run(tmp_path, 'cancel')
    try:
        deadline = time.monotonic() + 5
        while not (tmp_path / 'child-cache').exists():
            assert time.monotonic() < deadline
            time.sleep(.05)
        # Make the next refresh fail. Accelerate retry sleeps for this test only.
        (tmp_path / 'bin' / 'kinit').write_text('#!/bin/bash\nexit 1\n')
        (tmp_path / 'bin' / 'sleep').write_text('#!/bin/bash\nexec /bin/sleep .1\n')
        (tmp_path / 'bin' / 'sleep').chmod(0o755)
        _, err = proc.communicate(timeout=10)
        assert proc.returncode == 143, err
        assert b'Kerberos refresh failed (3/3)' in err
        cache = (tmp_path / 'child-cache').read_text().strip().removeprefix('FILE:')
        assert not Path(cache).parent.exists()
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
