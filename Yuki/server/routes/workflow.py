from flask import Blueprint
from Yuki.kernel.impression_storage import ImpressionStorage

bp = Blueprint('workflow', __name__)

@bp.route("/kill/<project_uuid>/<impression>", methods=['GET'])
def kill(project_uuid, impression):
    storage = ImpressionStorage(project_uuid, impression)
    storage.kill()
    return "ok"

@bp.route("/collect/<project_uuid>/<impression>", methods=['GET'])
def collect(project_uuid, impression):
    storage = ImpressionStorage(project_uuid, impression)
    storage.collect()
    return "ok"

@bp.route("/watermark/<project_uuid>/<impression>", methods=['GET'])
def watermark(project_uuid, impression):
    storage = ImpressionStorage(project_uuid, impression)
    storage.watermark()
    return "ok"

@bp.route('/workflow/<project_uuid>/<impression>', methods=['GET'])
def workflow(project_uuid, impression):
    storage = ImpressionStorage(project_uuid, impression)
    return storage.get_info()
