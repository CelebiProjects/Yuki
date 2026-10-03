"""Tests for the imp-view page listing."""
import os
from unittest import mock

from Yuki.server.routes import status as status_routes


def _stub_config(root):
    """A config shim returning the fixture directory as the job path."""
    stub = mock.MagicMock()
    stub.get_job_path = lambda p, i: str(root)
    return stub


def _app():
    from flask import Flask
    from Yuki.server.routes import upload
    template_folder = os.path.join(
        os.path.dirname(status_routes.__file__), "..", "..", "templates")
    app = Flask(__name__, template_folder=template_folder)
    app.register_blueprint(status_routes.bp)
    app.register_blueprint(upload.bp)
    return app


def _job_on(runner_id):
    """A minimal job object carrying the selected runner."""
    return mock.MagicMock(machine_id=runner_id)


def test_impview_lists_nested_files_recursively(tmp_path):
    """Files in subdirectories are listed by their relative paths."""
    stageout = tmp_path / "default_runner" / "stageout"
    (stageout / "plots").mkdir(parents=True)
    (stageout / "weights").mkdir(parents=True)
    (stageout / "plots" / "overtrain_BDT.png").write_bytes(b"png-bytes")
    (stageout / "tmva.root").write_bytes(b"root-bytes")
    (stageout / "weights" / "tmva_BDT.weights.xml").write_bytes(b"xml-bytes")

    with mock.patch.object(status_routes, "config", _stub_config(tmp_path)), \
            mock.patch.object(status_routes, "VJob",
                              return_value=_job_on("default_runner")):
        r = _app().test_client().get("/imp-view/proj/imp-1")
    body = r.get_data(as_text=True)
    assert r.status_code == 200
    assert "plots/overtrain_BDT.png" in body
    assert "tmva.root" in body
    assert "weights/tmva_BDT.weights.xml" in body
    # subdirectories themselves must not appear as bare entries
    assert ">plots<" not in body
    assert ">weights<" not in body


def test_impview_renders_nested_image_inline(tmp_path):
    """A nested plot renders as an inline image."""
    stageout = tmp_path / "default_runner" / "stageout" / "plots"
    stageout.mkdir(parents=True)
    (stageout / "overtrain_BDT.png").write_bytes(b"png-bytes")

    with mock.patch.object(status_routes, "config", _stub_config(tmp_path)), \
            mock.patch.object(status_routes, "VJob",
                              return_value=_job_on("default_runner")):
        r = _app().test_client().get("/imp-view/proj/imp-1")
    body = r.get_data(as_text=True)
    assert 'src="/file-view/proj/imp-1/default_runner/plots/overtrain_BDT.png"' \
        in body


def test_impview_without_runner_shows_not_run_empty_state(tmp_path):
    """An impression that has not been submitted renders a useful empty state."""
    with mock.patch.object(status_routes, "config", _stub_config(tmp_path)), \
            mock.patch.object(status_routes, "VJob",
                              return_value=_job_on(None)):
        r = _app().test_client().get("/imp-view/proj/imp-1")

    assert r.status_code == 200
    assert "尚未运行/暂无输出" in r.get_data(as_text=True)
