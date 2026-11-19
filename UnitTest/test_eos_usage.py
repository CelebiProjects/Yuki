"""EOS inventory, classification and resumable submission contract."""
import datetime
import json
from unittest.mock import Mock

import pytest

from Yuki.services import eos_scan, eos_usage
from Yuki.kernel.storage import liveness

P, LIVE, OLD, OTHER = (c * 32 for c in 'abcd')
SCAN = 'e' * 32


def make_config(root):
    (root / 'config.json').write_text(json.dumps({
        'runners_id': {'cern': 'r1'}, 'backend_types': {'r1': 'reana'},
        'eos_mount_point': {'r1': '/eos/user/u/test/celebi'},
    }))


def inventory(root):
    for imp, size in [(LIVE, 3), (OLD, 5), (OTHER, 7)]:
        folder = root / P / imp
        folder.mkdir(parents=True)
        (folder / 'result').write_bytes(b'x' * size)
    return eos_scan.scan(str(root))


def test_scan_nested_files_does_not_follow_symlinks(tmp_path):
    folder = tmp_path / P / LIVE / 'nested'
    folder.mkdir(parents=True)
    (folder / 'file').write_bytes(b'1234')
    (folder / 'outside').symlink_to('/etc')
    result = eos_scan.scan(str(tmp_path))
    item = result['projects'][0]['impressions'][0]
    assert item['bytes'] == 4 and item['files'] == 1
    assert not item['complete'] and item['errors']
    assert not result['complete']


def test_missing_root_and_project_are_not_zero_success(tmp_path):
    for root, project in [(tmp_path / 'missing', None), (tmp_path, P)]:
        result = eos_scan.scan(str(root), project)
        assert not result['complete']
        assert result['errors']


def test_unattributed_entries_are_reported(tmp_path):
    (tmp_path / 'unowned').mkdir()
    result = eos_scan.scan(str(tmp_path))
    assert result['unassigned'][0]['path'] == 'unowned'


def test_categories_require_explicit_recent_sets_and_protect_active_inputs(tmp_path):
    data = inventory(tmp_path / 'eos')
    liveness.save_live_set(P, [LIVE], [OLD], str(tmp_path))
    result = eos_usage.classify(data, str(tmp_path))['projects'][0]
    assert [result['usage'][key]['bytes'] for key in ('live', 'non_live', 'unknown')] == [3, 5, 7]
    workflow = tmp_path / 'Workflows' / P / 'wf'
    workflow.mkdir(parents=True)
    (workflow / 'config.json').write_text(json.dumps({'jobs_info': {OLD: {'is_input': True}}}))
    (workflow / 'results.json').write_text(json.dumps({'results': {'status': 'running'}}))
    result = eos_usage.classify(data, str(tmp_path))['projects'][0]
    assert result['usage']['live']['bytes'] == 8
    assert result['usage']['non_live']['bytes'] == 0
    (workflow / 'results.json').write_text('invalid')
    assert eos_usage.classify(data, str(tmp_path))['projects'][0]['usage']['non_live']['bytes'] == 0


def test_stale_missing_and_corrupt_live_sets_are_unknown(tmp_path):
    data = inventory(tmp_path / 'eos')
    assert eos_usage.classify(data, str(tmp_path))['projects'][0]['usage']['unknown']['bytes'] == 15
    liveness.save_live_set(P, [LIVE], [OLD], str(tmp_path))
    later = eos_usage.now() + datetime.timedelta(days=2)
    assert eos_usage.classify(data, str(tmp_path), later)['projects'][0]['usage']['unknown']['bytes'] == 15
    (tmp_path / 'Live' / f'{P}.json').write_text('{}')
    assert eos_usage.classify(data, str(tmp_path))['projects'][0]['usage']['unknown']['bytes'] == 15


def test_partial_inventory_is_unknown_even_for_live(tmp_path):
    data = inventory(tmp_path / 'eos')
    liveness.save_live_set(P, [LIVE], [OLD], str(tmp_path))
    data['projects'][0]['impressions'][0]['complete'] = False
    assert eos_usage.classify(data, str(tmp_path))['projects'][0]['usage']['unknown']['bytes'] == 10


def test_submission_is_idempotent_and_poll_downloads_once(monkeypatch, tmp_path):
    make_config(tmp_path)
    data = inventory(tmp_path / 'eos')
    call = Mock(side_effect=[{'status': 'submitted'},
                            {'remote_status': 'finished', 'inventory': data}])
    monkeypatch.setattr(eos_usage, 'reana_call', call)
    state = eos_usage.start('cern', SCAN, yuki_dir=str(tmp_path))
    assert state['status'] == 'submitted'
    assert eos_usage.start('cern', SCAN, yuki_dir=str(tmp_path)) == state
    result = eos_usage.poll(SCAN, str(tmp_path))
    assert result['status'] == 'done'
    assert result['report']['projects'][0]['usage']['unknown']['bytes'] == 15
    # Later sync reclassifies cached EOS measurements, without a second scan/download.
    liveness.save_live_set(P, [LIVE], [OLD], str(tmp_path))
    result = eos_usage.poll(SCAN, str(tmp_path))
    assert result['report']['projects'][0]['usage']['non_live']['bytes'] == 5
    assert call.call_count == 2
    with pytest.raises(ValueError):
        eos_usage.start('other', SCAN, yuki_dir=str(tmp_path))


def test_submission_uncertainty_preserves_identity(monkeypatch, tmp_path):
    make_config(tmp_path)
    call = Mock(side_effect=TimeoutError('secret'))
    monkeypatch.setattr(eos_usage, 'reana_call', call)
    state = eos_usage.start('cern', SCAN, P, str(tmp_path))
    assert state['status'] == 'submission_unknown'
    assert 'secret' not in state['error']
    eos_usage.start('cern', SCAN, P, str(tmp_path))
    assert call.call_count == 1
    call.side_effect = None
    call.return_value = {'remote_status': 'running'}
    assert eos_usage.poll(SCAN, str(tmp_path))['status'] == 'running'


def test_validation_and_failed_job(monkeypatch, tmp_path):
    make_config(tmp_path)
    call = Mock(return_value={'status': 'submitted'})
    monkeypatch.setattr(eos_usage, 'reana_call', call)
    for scan_id, project in [('../x', None), (SCAN, '../x')]:
        with pytest.raises(ValueError):
            eos_usage.start('cern', scan_id, project, str(tmp_path))
    assert not call.called
    eos_usage.start('cern', SCAN, yuki_dir=str(tmp_path))
    call.return_value = {'remote_status': 'failed'}
    state = eos_usage.poll(SCAN, str(tmp_path))
    assert state['status'] == 'failed' and 'report' not in state


def test_routes(monkeypatch, tmp_path):
    from flask import Flask
    from Yuki.server.routes.eos_usage import bp
    app = Flask(__name__)
    app.register_blueprint(bp)
    monkeypatch.setenv('YUKIDIR', str(tmp_path))
    make_config(tmp_path)
    monkeypatch.setattr(eos_usage, 'reana_call', Mock(return_value={'status': 'submitted'}))
    client = app.test_client()
    assert client.post('/eos-usage', json=[]).status_code == 400
    assert client.get('/eos-usage/bad').status_code == 400
    assert client.get('/eos-usage/' + SCAN).status_code == 404
    assert client.post('/eos-usage', json={'runner': 'cern', 'scan_id': SCAN}).status_code == 202


def test_reana_gateway_uploads_read_only_program_and_parses_report(monkeypatch, tmp_path):
    from Yuki.services import eos_reana
    from reana_client.api import client
    import reana_commons.api_client
    config = {'urls': {'r1': 'https://example.invalid'}, 'tokens': {'r1': 'secret'}}
    (tmp_path / 'config.json').write_text(json.dumps(config))
    monkeypatch.setattr(reana_commons.api_client, 'BaseAPIClient', Mock())
    create = Mock()
    uploads = []
    monkeypatch.setattr(client, 'create_workflow', create)
    monkeypatch.setattr(client, 'upload_file',
                        lambda name, file, filename, token: uploads.append((filename, file.read())))
    start = Mock()
    monkeypatch.setattr(client, 'start_workflow', start)
    request = {'runner_id': 'r1', 'yuki_dir': str(tmp_path), 'workflow': 'scan-test',
               'operation': 'submit', 'root': '/eos/test', 'project': P,
               'image': 'python:3.11-slim'}
    assert eos_reana.execute(request)['status'] == 'submitted'
    spec = create.call_args.args[0]
    assert spec['workflow']['type'] == 'serial'
    assert spec['workflow']['specification']['steps'][0]['kerberos'] is True
    assert [name for name, _ in uploads] == ['eos_scan.py', 'scan-request.json']
    assert json.loads(uploads[1][1])['project'] == P
    assert b'secret' not in uploads[0][1] + uploads[1][1]
    start.assert_called_once_with('scan-test', 'secret', {})
    request['operation'] = 'poll'
    monkeypatch.setattr(client, 'get_workflow_status', Mock(return_value={'status': 'finished'}))
    monkeypatch.setattr(client, 'download_file', Mock(return_value=(b'{"schema": 1}', 'eos-usage.json', False)))
    assert eos_reana.execute(request)['inventory'] == {'schema': 1}


def test_legacy_flat_cache_is_attributed_only_with_unique_owner(tmp_path):
    root = tmp_path / 'eos'
    folder = root / LIVE
    folder.mkdir(parents=True)
    (folder / 'old.root').write_bytes(b'123456')
    data = eos_scan.scan(str(root))
    assert data['projects'] == []
    assert data['unassigned'][0]['bytes'] == 6
    report = eos_usage.classify(data, str(tmp_path))
    assert report['unassigned_usage']['bytes'] == 6
    liveness.save_live_set(P, [LIVE], [], str(tmp_path))
    report = eos_usage.classify(data, str(tmp_path))
    assert report['projects'][0]['project'] == P
    assert report['projects'][0]['usage']['live']['bytes'] == 6
    assert report['unassigned_usage']['bytes'] == 0
    liveness.save_live_set(OTHER, [LIVE], [], str(tmp_path))
    report = eos_usage.classify(data, str(tmp_path))
    assert report['unassigned_usage']['bytes'] == 6
    assert not report['projects']


def test_project_scope_includes_legacy_and_counts_unclassified_project_files(tmp_path):
    root = tmp_path / 'eos'
    legacy = root / LIVE
    legacy.mkdir(parents=True)
    (legacy / 'result').write_bytes(b'12')
    project = root / P
    project.mkdir()
    (project / 'unclassified').write_bytes(b'123')
    data = eos_scan.scan(str(root), P, [P], [LIVE])
    liveness.save_live_set(P, [LIVE], [], str(tmp_path))
    report = eos_usage.classify(data, str(tmp_path))
    assert report['complete']
    assert report['projects'][0]['usage']['unknown']['bytes'] == 3
    assert report['projects'][0]['usage']['live']['bytes'] == 2


def test_unknown_workflow_status_does_not_falsely_assert_live(tmp_path):
    data = inventory(tmp_path / 'eos')
    liveness.save_live_set(P, [LIVE], [OLD], str(tmp_path))
    workflow = tmp_path / 'Workflows' / P / 'wf'
    workflow.mkdir(parents=True)
    (workflow / 'config.json').write_text(json.dumps({'jobs_info': {OLD: {}}}))
    (workflow / 'results.json').write_text(json.dumps({'results': {'status': 'unknown'}}))
    usage = eos_usage.classify(data, str(tmp_path))['projects'][0]['usage']
    assert usage['live']['bytes'] == 3
    assert usage['non_live']['bytes'] == 0
    assert usage['unknown']['bytes'] == 12
