"""APD local settings retain private permissions when deposited to Yuki."""

import io
import json
import stat
import tarfile
from unittest import mock

from flask import Flask

from Yuki.server.routes import upload as upload_routes


def _archive():
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        info = tarfile.TarInfo("contents/celebi.yaml")
        content = b"environment: script\n"
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))
    payload.seek(0)
    return payload


def test_upload_stores_local_config_owner_only(tmp_path):
    """A deposited APD token is not readable by other local users."""
    app = Flask(__name__)
    app.register_blueprint(upload_routes.bp)
    project_uuid = "a" * 32
    impression_uuid = "b" * 32
    tarname = f"{impression_uuid}.tar.gz"
    local_payload = json.dumps({"apd_token": "lbapi:user:secret"}).encode()

    with mock.patch.object(upload_routes.config, "storage_path", str(tmp_path)):
        response = app.test_client().post("/upload", data={
            "project_uuid": project_uuid,
            "tarname": tarname,
            "config": "config.json",
            tarname: (_archive(), tarname),
            "config.json": (io.BytesIO(b"{}"), "config.json"),
            "config.local.json": (
                io.BytesIO(local_payload), "config.local.json"),
        })

    assert response.status_code == 200
    local_path = tmp_path / project_uuid / impression_uuid / "config.local.json"
    assert stat.S_IMODE(local_path.stat().st_mode) == 0o600
    assert json.loads(local_path.read_text(encoding="utf-8"))["apd_token"]
