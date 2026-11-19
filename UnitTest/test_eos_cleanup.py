"""Cleanup manifests, idempotence, preflight and independent verification."""
import datetime
import json
from unittest.mock import Mock

import pytest
from Yuki.services import eos_cleanup as cleanup, eos_usage, eos_scan, eos_delete
from Yuki.kernel.storage import liveness

P, LIVE, OLD, UNKNOWN, SCAN = (c * 32 for c in 'abcde')


@pytest.fixture
def scan_state(tmp_path):
    for imp in (LIVE, OLD, UNKNOWN):
        path = tmp_path / 'eos' / P / imp
        path.mkdir(parents=True)
        (path / 'file').write_text('data')
    inventory = eos_scan.scan(str(tmp_path / 'eos'))
    inventory['root'] = '/eos/test/cache'
    state = {'id': SCAN, 'status': 'done', 'inventory': inventory,
             'root': inventory['root'], 'runner': 'cern', 'runner_id': 'r1',
             'image': 'python:3.11-slim'}
    eos_usage.directory(tmp_path).mkdir()
    eos_usage.save(eos_usage.directory(tmp_path) / f'{SCAN}.json', state)
    (tmp_path / 'config.json').write_text(json.dumps({
        'runners_id': {'cern': 'r1'}, 'eos_mount_point': {'r1': inventory['root']}}))
    liveness.save_live_set(P, [LIVE], [OLD], str(tmp_path))
    return state


def test_preview_default_and_explicit_scopes(tmp_path, scan_state, monkeypatch):
    call = Mock()
    monkeypatch.setattr(cleanup, 'call_reana', call)
    state = cleanup.plan(SCAN, [P], yuki_dir=str(tmp_path))
    assert state['status'] == 'planned'
    assert [e['relative'] for e in state['manifest']['entries']] == [P + '/' + OLD]
    assert state['manifest']['protected'] == [P + '/' + LIVE]
    with_unknown = cleanup.plan(SCAN, [P], True, yuki_dir=str(tmp_path))
    assert len(with_unknown['manifest']['entries']) == 2
    cleared = cleanup.plan(SCAN, [P], clear_projects=[P], yuki_dir=str(tmp_path))
    assert cleared['manifest']['entries'][0]['relative'] == P
    assert cleared['manifest']['entries'][0]['mode'] == 'whole_project'
    assert not cleared['manifest']['protected']
    call.assert_not_called()


def test_invalid_scope_expired_or_old_scan(tmp_path, scan_state):
    with pytest.raises(ValueError):
        cleanup.plan(SCAN, [P], clear_projects=[UNKNOWN], yuki_dir=str(tmp_path))
    with pytest.raises(ValueError):
        cleanup.plan(SCAN, [], yuki_dir=str(tmp_path))
    scan_state['inventory']['projects'][0]['impressions'][1].pop('metadata_sha256')
    eos_usage.save(eos_usage.directory(tmp_path) / f'{SCAN}.json', scan_state)
    with pytest.raises(ValueError, match='fingerprints'):
        cleanup.plan(SCAN, [P], yuki_dir=str(tmp_path))
    scan_state['inventory']['finished_at'] = (eos_usage.now() - datetime.timedelta(days=2)).isoformat()
    eos_usage.save(eos_usage.directory(tmp_path) / f'{SCAN}.json', scan_state)
    with pytest.raises(ValueError, match='24 hours'):
        cleanup.plan(SCAN, [P], yuki_dir=str(tmp_path))


def test_execute_rechecks_live_root_and_confirmation(tmp_path, scan_state, monkeypatch):
    state = cleanup.plan(SCAN, [P], yuki_dir=str(tmp_path))
    call = Mock()
    monkeypatch.setattr(cleanup, 'call_reana', call)
    with pytest.raises(ValueError, match='Confirmation'):
        cleanup.execute(state['id'], 'wrong', str(tmp_path))
    liveness.save_live_set(P, [LIVE, OLD], [], str(tmp_path))
    with pytest.raises(ValueError, match='Liveness'):
        cleanup.execute(state['id'], state['manifest_sha256'], str(tmp_path))
    call.assert_not_called()


def test_delete_then_separate_verification_and_idempotent_execution(tmp_path, scan_state, monkeypatch):
    state = cleanup.plan(SCAN, [P], yuki_dir=str(tmp_path))
    sha = state['manifest_sha256']
    call = Mock(side_effect=[{'status': 'submitted'},
        {'remote_status': 'finished', 'cleanup_report': {'status': 'deleted_unverified', 'manifest_sha256': sha}},
        {'status': 'submitted'},
        {'remote_status': 'finished', 'verification': {'status': 'verified', 'manifest_sha256': sha}}])
    monkeypatch.setattr(cleanup, 'call_reana', call)
    result = cleanup.execute(state['id'], sha, str(tmp_path))
    assert result['status'] == 'submitted'
    cleanup.execute(state['id'], sha, str(tmp_path))
    assert call.call_count == 1
    result = cleanup.status(state['id'], str(tmp_path))
    assert result['status'] == 'verifying'
    result = cleanup.status(state['id'], str(tmp_path))
    assert result['status'] == 'done'
    cleanup.status(state['id'], str(tmp_path))
    assert [c.args[1] for c in call.call_args_list] == [
        'cleanup-submit', 'cleanup-poll', 'cleanup-verify-submit', 'cleanup-verify-poll']


def test_uncertain_submission_is_not_repeated(tmp_path, scan_state, monkeypatch):
    state = cleanup.plan(SCAN, [P], yuki_dir=str(tmp_path))
    call = Mock(side_effect=TimeoutError())
    monkeypatch.setattr(cleanup, 'call_reana', call)
    assert cleanup.execute(state['id'], state['manifest_sha256'], str(tmp_path))['status'] == 'submission_unknown'
    cleanup.execute(state['id'], state['manifest_sha256'], str(tmp_path))
    call.assert_called_once()


def test_verification_residual_is_partial_not_retried(tmp_path, scan_state, monkeypatch):
    state = cleanup.plan(SCAN, [P], yuki_dir=str(tmp_path))
    state['status'] = 'verifying'
    with cleanup.locked(state['id'], str(tmp_path)) as path:
        eos_usage.save(path, state)
    call = Mock(return_value={'remote_status': 'finished', 'verification': {
        'status': 'partial', 'manifest_sha256': state['manifest_sha256'], 'remaining': [P + '/' + OLD]}})
    monkeypatch.setattr(cleanup, 'call_reana', call)
    assert cleanup.status(state['id'], str(tmp_path))['status'] == 'partial'
    cleanup.execute(state['id'], state['manifest_sha256'], str(tmp_path))
    call.assert_called_once()


def test_active_workflow_blocks_before_submission(tmp_path, scan_state, monkeypatch):
    state = cleanup.plan(SCAN, [P], yuki_dir=str(tmp_path))
    monkeypatch.setattr(cleanup, 'call_reana', Mock(return_value={
        'not_submitted': True, 'reason': 'Selected project has an active REANA workflow'}))
    assert cleanup.execute(state['id'], state['manifest_sha256'], str(tmp_path))['status'] == 'preflight_failed'


def local_manifest(tmp_path, monkeypatch):
    root = tmp_path / 'eos'
    old, live = root / P / OLD, root / P / LIVE
    for path in (old, live):
        path.mkdir(parents=True)
        (path / 'file').write_text('data')
    manifest = {'root': str(root), 'schema': 1, 'clear_projects': [], 'protected': [P + '/' + LIVE],
                'entries': [{'relative': P + '/' + OLD, 'project': P, 'mode': 'impression',
                             **eos_scan.measure(str(old))}]}
    # The mount prefix is enforced separately; exercise actual deletion in a temp directory.
    monkeypatch.setattr(eos_delete, 'validate', lambda m: (root, [old], [live]))
    return manifest, old, live


def test_worker_exact_delete_preserves_live_and_fresh_verify(tmp_path, monkeypatch):
    manifest, old, live = local_manifest(tmp_path, monkeypatch)
    assert eos_delete.execute(manifest)['status'] == 'deleted_unverified'
    assert not old.exists() and (live / 'file').read_text() == 'data'
    assert eos_delete.verify(manifest)['status'] == 'verified'
    old.mkdir()  # A different client later sees a residual/recreated directory.
    assert eos_delete.verify(manifest)['status'] == 'partial'


def test_same_size_content_change_aborts_all_deletion(tmp_path, monkeypatch):
    manifest, old, live = local_manifest(tmp_path, monkeypatch)
    (old / 'file').write_text('EDIT')
    assert eos_delete.execute(manifest)['status'] == 'preflight_failed'
    assert old.exists() and live.exists()


def test_path_scope_and_symlinks(tmp_path):
    with pytest.raises(ValueError):
        eos_delete.resolve(tmp_path, '../escape')
    (tmp_path / OLD).symlink_to(tmp_path / 'elsewhere')
    with pytest.raises(ValueError, match='Symlink'):
        eos_delete.resolve(tmp_path, OLD)
    usage = {'metadata_sha256': '0' * 64, 'bytes': 0, 'files': 0}
    manifest = {'schema': 1, 'root': '/eos/test', 'clear_projects': [], 'protected': [],
                'entries': [{'relative': P, 'project': P, 'mode': 'whole_project', **usage}]}
    with pytest.raises(ValueError, match='authorized'):
        eos_delete.validate(manifest)
    manifest['clear_projects'] = [P]
    manifest['protected'] = [P + '/' + LIVE]
    with pytest.raises(ValueError, match='protected'):
        eos_delete.validate(manifest)


def test_stale_parent_listing_is_not_verified(tmp_path, monkeypatch):
    target = tmp_path / OLD
    monkeypatch.setattr(eos_delete.os, 'listdir', lambda path: [OLD])
    assert not eos_delete.check_absent(target)


def test_routes_require_preview_hash(tmp_path, monkeypatch):
    from flask import Flask
    from Yuki.server.routes.eos_usage import bp
    app = Flask(__name__); app.register_blueprint(bp)
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    client = app.test_client()
    assert client.post('/eos-cleanup/plan', json=[]).status_code == 400
    assert client.post('/eos-cleanup/' + SCAN + '/execute', json={}).status_code == 400
    assert client.get('/eos-cleanup/' + SCAN).status_code == 404
