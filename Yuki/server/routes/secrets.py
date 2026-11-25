"""Import credentials; no endpoint returns their contents."""
from pathlib import Path
from flask import Blueprint, jsonify, request
from ...services import secrets
from ..config import config

bp = Blueprint("secrets", __name__)


@bp.route("/secrets", methods=["POST"])
def add_secret():
    """Accept a bounded keytab upload and return metadata only."""
    upload = request.files.get("file")
    if upload is None:
        return jsonify(error="A keytab file is required"), 400
    try:
        result = secrets.add(
            Path(config.config_path).parent, request.form.get("name"),
            request.form.get("type"), request.form.get("principal"),
            upload.stream.read(secrets.MAX_KEYTAB_BYTES + 1))
    except FileExistsError:
        return jsonify(error="Secret already exists; choose a new name"), 409
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    except OSError:
        return jsonify(error="Unable to store secret"), 500
    return jsonify(result), 201
