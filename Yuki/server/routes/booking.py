"""
REANA booking routes.

Provides an endpoint for Celebi to send project files,
which Yuki then books (uploads) to a REANA server.
"""
import io
import os
import shutil
import tarfile
import tempfile
from logging import getLogger

from flask import Blueprint, request, jsonify

from CelebiChrono.utils.metadata import ConfigFile
from ..services.reana_booking import ReanaBooker

bp = Blueprint('booking', __name__)
logger = getLogger("YukiLogger")


def _get_yuki_config():
    """Get Yuki config file instance."""
    config_path = os.path.join(os.environ["HOME"], ".Yuki", "config.json")
    return ConfigFile(config_path)


def _get_booking_credentials(request_form=None):
    """Get REANA booking credentials from request or stored config.

    Priority:
        1. Credentials provided in the request
        2. Credentials stored in Yuki config (booking_server_url, booking_access_token)

    Returns:
        (server_url, access_token) tuple, or (None, None) if not found.
    """
    if request_form:
        server_url = request_form.get("server_url", "").strip()
        access_token = request_form.get("access_token", "").strip()
        if server_url and access_token:
            return server_url, access_token

    # Fall back to stored config
    try:
        config_file = _get_yuki_config()
        server_url = config_file.read_variable("booking_server_url", "")
        access_token = config_file.read_variable("booking_access_token", "")
        if server_url and access_token:
            return server_url, access_token
    except Exception as e:
        logger.warning("Failed to read stored booking credentials: %s", e)

    return None, None


@bp.route('/register-booking-server', methods=['POST'])
def register_booking_server():
    """Register REANA server URL and access token in Yuki config.

    Request (JSON):
        - server_url: REANA server URL
        - access_token: REANA access token

    Returns:
        JSON with registration result.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Missing JSON body"}), 400

        server_url = data.get("server_url", "").strip()
        access_token = data.get("access_token", "").strip()

        if not server_url:
            return jsonify({"error": "Missing server_url"}), 400
        if not access_token:
            return jsonify({"error": "Missing access_token"}), 400

        config_file = _get_yuki_config()
        config_file.write_variable("booking_server_url", server_url)
        config_file.write_variable("booking_access_token", access_token)

        logger.info("Registered booking server: %s", server_url)
        return jsonify({
            "success": True,
            "message": f"Booking server registered: {server_url}",
        })

    except Exception as e:
        logger.error("Failed to register booking server: %s", e)
        return jsonify({"error": str(e)}), 500


@bp.route('/booking-server', methods=['GET'])
def get_booking_server():
    """Get the registered booking server URL and status.

    Returns:
        JSON with server_url, token_status, and ping_status.
    """
    try:
        config_file = _get_yuki_config()
        server_url = config_file.read_variable("booking_server_url", "")
        access_token = config_file.read_variable("booking_access_token", "")

        if not server_url:
            return jsonify({
                "registered": False,
                "message": "No booking server registered.",
            })

        # Try to ping the REANA server
        ping_status = "unknown"
        try:
            from reana_client.api import client as reana_client
            os.environ["REANA_SERVER_URL"] = server_url
            reana_client.ping(access_token)
            ping_status = "ok"
        except Exception as e:
            ping_status = f"failed: {e}"

        # Mask the token for display
        token_status = "set" if access_token else "missing"
        masked_token = ""
        if access_token:
            if len(access_token) > 8:
                masked_token = access_token[:4] + "..." + access_token[-4:]
            else:
                masked_token = "***"

        return jsonify({
            "registered": True,
            "server_url": server_url,
            "token_status": token_status,
            "masked_token": masked_token,
            "ping_status": ping_status,
        })

    except Exception as e:
        logger.error("Failed to get booking server info: %s", e)
        return jsonify({"error": str(e)}), 500


@bp.route('/book-reana', methods=['POST'])
def book_reana():
    """Receive a project archive from Celebi and book it to REANA.

    Request (multipart/form-data):
        - project_tar: tar.gz archive of the project files
        - project_name: name of the project
        - server_url: REANA server URL (optional if registered)
        - access_token: REANA access token (optional if registered)
        - verify_ssl: "true" or "false" (default "true")
        - stageout: "true" or "false" (default "false")
        - upload: "plots+logs" / "data+logs" / "all" / "logs" (default "plots+logs")

    Returns:
        JSON with booking result.
    """
    try:
        # Get form fields
        project_name = request.form.get("project_name", "")
        verify_ssl = request.form.get("verify_ssl", "true").lower() != "false"
        upload_mode = request.form.get("upload", "plots+logs")
        # Legacy: a bare stageout=true means "upload everything".
        if request.form.get("stageout", "false").lower() == "true":
            upload_mode = "all"

        if not project_name:
            return jsonify({"error": "Missing project_name"}), 400

        # Resolve credentials (request overrides stored config)
        server_url, access_token = _get_booking_credentials(request.form)
        if not server_url or not access_token:
            return jsonify({
                "error": (
                    "REANA credentials not available. "
                    "Either provide server_url + access_token in the request, "
                    "or register them first via POST /register-booking-server"
                )
            }), 400

        # Check for uploaded tar file
        if "project_tar" not in request.files:
            return jsonify({"error": "Missing project_tar file"}), 400

        project_tar = request.files["project_tar"]

        # Extract to a temporary directory
        temp_dir = tempfile.mkdtemp(prefix="yuki_booking_")
        try:
            project_path = os.path.join(temp_dir, project_name)
            os.makedirs(project_path, exist_ok=True)

            with tarfile.open(fileobj=project_tar.stream, mode="r:gz") as tar:
                tar.extractall(project_path)

            logger.info(
                "Booking project '%s' to REANA server %s",
                project_name, server_url
            )

            # Book to REANA
            booker = ReanaBooker(server_url, access_token, verify_ssl=verify_ssl)
            result = booker.book_project(project_path, project_name, upload_mode=upload_mode)

            response = {
                "success": result.success,
                "messages": [
                    {"text": text, "status": msg_type}
                    for text, msg_type in result.messages
                ],
            }
            response.update(result.data)

            status_code = 200 if response["success"] else 500
            return jsonify(response), status_code

        finally:
            # Clean up temp directory
            shutil.rmtree(temp_dir, ignore_errors=True)

    except Exception as e:
        logger.error("Booking failed: %s", e)
        return jsonify({"error": str(e)}), 500


@bp.route('/book-reana-stream', methods=['POST'])
def book_reana_stream():
    """Stream booking progress to Celebi as NDJSON.

    Same request format as /book-reana, but returns a chunked
    NDJSON stream. Each line is a JSON object with 'text' and
    'status' keys. The final line has 'done': true and includes
    the booking result data.

    Request (multipart/form-data):
        - project_tar: tar.gz archive of the project files
        - project_name: name of the project
        - server_url: REANA server URL (optional if registered)
        - access_token: REANA access token (optional if registered)
        - verify_ssl: "true" or "false" (default "true")
        - stageout: "true" or "false" (default "false")
        - upload: "plots+logs" / "data+logs" / "all" / "logs" (default "plots+logs")

    Returns:
        Chunked NDJSON stream of progress messages.
    """
    import json
    import queue
    import threading
    from flask import Response, stream_with_context

    # Validate request up front (before starting the stream)
    project_name = request.form.get("project_name", "")
    verify_ssl = request.form.get("verify_ssl", "true").lower() != "false"
    upload_mode = request.form.get("upload", "plots+logs")
    # Legacy: a bare stageout=true means "upload everything".
    if request.form.get("stageout", "false").lower() == "true":
        upload_mode = "all"

    if not project_name:
        return jsonify({"error": "Missing project_name"}), 400

    server_url, access_token = _get_booking_credentials(request.form)
    if not server_url or not access_token:
        return jsonify({
            "error": (
                "REANA credentials not available. "
                "Either provide server_url + access_token in the request, "
                "or register them first via POST /register-booking-server"
            )
        }), 400

    if "project_tar" not in request.files:
        return jsonify({"error": "Missing project_tar file"}), 400

    project_tar = request.files["project_tar"]
    # Read tar data into memory in the main thread — Flask's FileStorage
    # stream may not be safe to read from a background thread.
    tar_data = project_tar.read()

    def generate():
        msg_queue = queue.Queue()

        def run_booking():
            """Run booking in background thread, pushing messages to queue."""
            temp_dir = tempfile.mkdtemp(prefix="yuki_booking_")
            try:
                project_path = os.path.join(temp_dir, project_name)
                os.makedirs(project_path, exist_ok=True)

                with tarfile.open(fileobj=io.BytesIO(tar_data), mode="r:gz") as tar:
                    tar.extractall(project_path)

                logger.info(
                    "Streaming booking for project '%s' to REANA server %s",
                    project_name, server_url
                )

                def progress_callback(text, status="normal"):
                    msg_queue.put({"text": text, "status": status})

                booker = ReanaBooker(
                    server_url, access_token,
                    verify_ssl=verify_ssl,
                    progress_callback=progress_callback
                )
                result = booker.book_project(project_path, project_name, upload_mode=upload_mode)

                msg_queue.put({
                    "done": True,
                    "success": result.success,
                    "data": result.data,
                })
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                logger.error("Streaming booking failed: %s\n%s", e, tb)
                error_msg = str(e) if str(e) else repr(e)
                msg_queue.put({
                    "done": True,
                    "success": False,
                    "error": error_msg,
                    "traceback": tb,
                })
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

        # Start booking in background thread
        thread = threading.Thread(target=run_booking)
        thread.start()

        # Consume queue and yield NDJSON lines as they arrive
        while True:
            msg = msg_queue.get()
            yield json.dumps(msg) + "\n"
            if msg.get("done"):
                break

        thread.join(timeout=5)

    return Response(stream_with_context(generate()), mimetype='application/x-ndjson')
