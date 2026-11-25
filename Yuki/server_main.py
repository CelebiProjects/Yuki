"""
Main entry point and daemon management for Yuki server.
"""
import os
import subprocess
from multiprocessing import Process
from pathlib import Path
from tempfile import NamedTemporaryFile

from CelebiChrono.utils.pretty import colorize
from werkzeug.serving import make_server
from .server.app import app, celeryapp
from .server.config import config
from .kernel.runners import config as runner_config

HTTP_PORT_FILE_ENV = "YUKI_HTTP_PORT_FILE"


def start_flask_app():
    """Start the Flask application."""
    host = os.environ.get("YUKI_HTTP_HOST", "0.0.0.0")
    port = int(os.environ.get("YUKI_HTTP_PORT", "3315"))
    port_file = os.environ.get(HTTP_PORT_FILE_ENV)
    if not port_file:
        app.run(host=host, port=port, debug=False)
        return

    # Publish the port from the bound socket. No probe/rebind race is needed.
    target = Path(port_file)
    with make_server(host, port, app, threaded=True) as server:
        temporary = None
        try:
            with NamedTemporaryFile(mode="w", dir=target.parent, delete=False,
                                    encoding="ascii") as stream:
                temporary = Path(stream.name)
                stream.write(f"{server.server_port}\n")
            temporary.replace(target)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        server.serve_forever()


def start_celery_worker():
    """Start the Celery worker."""
    argv = ["-A", "Yuki.server.tasks.celeryapp", "worker", "--loglevel=info"]
    celeryapp.worker_main(argv)


def server_start():
    """Start the Yuki server with both Flask and Celery processes."""
    runner_config.ensure_default_local_runner(config.get_config_file())
    print(colorize("[*[*[* Starting the Data Integration Thought Entity *]*]*]", "title0"))

    flask_process = Process(target=start_flask_app)
    celery_process = Process(target=start_celery_worker)

    flask_process.start()
    celery_process.start()

    flask_process.join()
    celery_process.join()


def stop():
    """Stop the Yuki server."""
    if status() == "stop":
        return
    with open(config.daemon_path + "/server.pid", 'r', encoding='utf-8') as f:
        subprocess.call(f"kill {f.read()}", shell=True)
    with open(config.daemon_path + "/runner.pid", 'r', encoding='utf-8') as f:
        subprocess.call(f"kill {f.read()}", shell=True)


def status():
    """Check server status."""
    # Implementation would check if server is running
    return "running"  # Placeholder


if __name__ == "__main__":
    server_start()
