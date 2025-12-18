"""
File upload and download routes.
"""
import os
import tarfile
from logging import getLogger

from flask import Blueprint, request, send_from_directory

from ..config import config

bp = Blueprint('upload', __name__)
logger = getLogger("YukiLogger")


@bp.route('/upload', methods=['GET', 'POST'])
def upload_file():
    """Handle file uploads."""
    if request.method == 'POST':
        print("Trying to upload files:", request.form)
        project_uuid = request.form["project_uuid"]
        tarname = request.form["tarname"]
        request.files[tarname].save(os.path.join("/tmp", tarname))

        with tarfile.open(os.path.join("/tmp", tarname), "r") as tar:
            for ti in tar:
                tar.extract(ti, os.path.join(config.storage_path, project_uuid, tarname[:-7]))

        config_file = request.form['config']
        logger.info(config_file)
        request.files[config_file].save(
            os.path.join(config.storage_path, project_uuid, tarname[:-7], config_file)
        )
    return "Successful"


@bp.route("/download/<filename>", methods=['GET'])
def download_file(filename):
    """Download a file."""
    directory = os.path.join(os.getcwd(), "data")
    return send_from_directory(directory, filename, as_attachment=True)


@bp.route("/export/<project_uuid>/<impression>/<filename>", methods=['GET'])
def export(project_uuid, impression, filename):
    """Export a file from an impression."""
    job_path = config.get_job_path(project_uuid, impression)
    config_file = config.get_config_file()

    print("EXPORTING", job_path, filename)
    if os.path.exists(os.path.join(job_path, "rawdata")):
        full_path = os.path.join(job_path, "rawdata", filename)
        if os.path.exists(full_path):
            return send_from_directory(os.path.join(job_path, "rawdata"), filename, as_attachment=True)

    runners = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    # Search for the first machine that has the file
    for runner in runners:
        runner_id = runners_id[runner]
        path = os.path.join(job_path, runner_id, "stageout")
        full_path = os.path.join(path, filename)
        print("path", full_path)
        if os.path.exists(full_path):
            return send_from_directory(path, filename, as_attachment=True)
    return "NOTFOUND"


@bp.route("/get-file/<project_uuid>/<impression>/<filename>", methods=['GET'])
def get_file(project_uuid, impression, filename):
    """Get the path to a specific file in an impression."""
    job_path = config.get_job_path(project_uuid, impression)
    config_file = config.get_config_file()
    runners = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})

    for machine in runners:
        machine_id = runners_id[machine]
        path = os.path.join(job_path, machine_id, "stageout", filename)
        if os.path.exists(path):
            return path
    return "NOTFOUND"


@bp.route("/file-view/<project_uuid>/<impression>/<runner_id>/<filename>", methods=['GET'])
def fileview(project_uuid, impression, runner_id, filename):
    """View a specific file."""
    job_path = config.get_job_path(project_uuid, impression)
    path = os.path.join(job_path, runner_id, "stageout")
    print(path)
    return send_from_directory(path, filename)

@bp.route("/watermark-view/<impression>/<runner_id>/<filename>", methods=['GET'])
def watermarkview(project_uuid, impression, runner_id, filename):
    """View a specific file."""
    job_path = config.get_job_path(project_uuid, impression)
    path = os.path.join(job_path, runner_id, "watermarks")
    return send_from_directory(path, filename)
