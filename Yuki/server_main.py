"""
Main entry point and daemon management for Yuki server.
"""
import subprocess
from multiprocessing import Process
from CelebiChrono.utils.pretty import colorize
from .server.app import app, celeryapp
from .server.config import config
from .kernel.runners import config as runner_config


def start_flask_app():
    """Start the Flask application."""
    app.run(
        host='0.0.0.0',
        port=3315,
        debug=False,
    )


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
