"""Isolated REANA secret upload. Never print SDK errors or credential values."""
import base64
import json
import logging
import os
from pathlib import Path
import sys

from CelebiChrono.utils.metadata import ConfigFile
from .secrets import read


def upload(request):
    """Install the CERN Kerberos secrets without overwriting account credentials."""
    config = ConfigFile(request["config_path"])
    runner = request["runner_id"]
    url = config.read_variable("urls", {}).get(runner, "")
    token = config.read_variable("tokens", {}).get(runner, "")
    if not url or not token:
        raise ValueError("Missing REANA URL/token")
    record = read(Path(request["config_path"]).parent, request["name"])
    user, realm = record["principal"].rsplit("@", 1)
    if realm != "CERN.CH" or "/" in user:
        raise ValueError("REANA CERN Kerberos requires a user principal at CERN.CH")
    os.environ["REANA_SERVER_URL"] = url
    # The SDK reads the selected endpoint at import time.
    from reana_client.api import client  # pylint: disable=import-outside-toplevel
    from reana_commons.api_client import BaseAPIClient  # pylint: disable=import-outside-toplevel
    BaseAPIClient("reana-server")
    filename = "yuki-" + record["name"] + ".keytab"
    def encode(value):
        return base64.b64encode(value.encode()).decode("ascii")
    payload = {filename: {"type": "file", "value": record["data"]},
               "CERN_USER": {"type": "env", "value": encode(user)},
               "CERN_KEYTAB": {"type": "env", "value": encode(filename)}}
    client.add_secrets(payload, overwrite=False, access_token=token)


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        upload(json.load(sys.stdin))
    except Exception:  # pylint: disable=broad-exception-caught
        sys.exit(1)
    print("YUKI_SECRET_BOUND")
