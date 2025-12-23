"""
Runner management routes.
"""
from flask import Blueprint, request
from CelebiChrono.utils import csys
from ..config import config
from ..utils import ping

bp = Blueprint('runner', __name__)


@bp.route("/runners", methods=['GET'])
def runners():
    """Get list of available runners."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    return " ".join(runners_list)


@bp.route("/runners-url", methods=['GET'])
def runnersurl():
    """Get URLs of all runners."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})
    runners_url = config_file.read_variable("urls", {})
    return " ".join([runners_url[runners_id[runner]] for runner in runners_list])


@bp.route("/runner-connection/<runner>", methods=['GET'])
def runnerconnection(runner):
    """Test connection to a specific runner."""
    config_file = config.get_config_file()
    runners_id = config_file.read_variable("runners_id", {})
    runner_id = runners_id.get(runner, "")
    tokens = config_file.read_variable("tokens", {})
    token = tokens.get(runner_id, "")
    urls = config_file.read_variable("urls", {})
    url = urls.get(runner_id, "")
    backend_types = config_file.read_variable("backend_types", {})
    backend_type = backend_types.get(runner_id, "reana")
    if backend_type != "reana":
        return {'status': 'Connected'}
    return ping(url, token)


@bp.route("/register-runner", methods=['POST'])
def registerrunner():
    """Register a new runner."""
    if request.method == 'POST':
        print(request.form)
        runner = request.form["runner"]
        runner_url = request.form["url"]
        runner_token = request.form["token"]
        backend_type = request.form.get("backend_type", "dry")
        runner_id = csys.generate_uuid()

        config_file = config.get_config_file()
        runners_list = config_file.read_variable("runners", [])
        runners_id = config_file.read_variable("runners_id", {})
        runners_url = config_file.read_variable("urls", {})
        tokens = config_file.read_variable("tokens", {})
        backend_types = config_file.read_variable("backend_types", {})

        runners_list.append(runner)
        runners_id[runner] = runner_id
        runners_url[runner_id] = runner_url
        tokens[runner_id] = runner_token
        backend_types[runner_id] = backend_type

        config_file.write_variable("runners", runners_list)
        config_file.write_variable("runners_id", runners_id)
        config_file.write_variable("urls", runners_url)
        config_file.write_variable("tokens", tokens)
        config_file.write_variable("backend_types", backend_types)
    return "successful"


@bp.route("/remove-runner/<runner>", methods=['GET'])
def removerunner(runner):
    """Remove a runner."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})
    urls = config_file.read_variable("urls", {})
    tokens = config_file.read_variable("tokens", {})
    backend_types = config_file.read_variable("backend_types", {})


    if runner not in runners_list:
        return "runner not found"

    runner_id = runners_id[runner]
    print("runner_id", runner_id)
    runners_list.remove(runner)
    del runners_id[runner]

    # Safe deletion of URL
    if runner_id in urls:
        del urls[runner_id]

    # Safe deletion of token
    if runner_id in tokens:
        del tokens[runner_id]

    # Safe deletion of backend type
    if runner_id in backend_types:
        del backend_types[runner_id]

    config_file.write_variable("runners", runners_list)
    config_file.write_variable("runners_id", runners_id)
    config_file.write_variable("urls", urls)
    config_file.write_variable("tokens", tokens)
    config_file.write_variable("backend_types", backend_types)
    return "successful"


@bp.route("/register-machine/<machine>/<machine_uuid>", methods=['GET'])
def register_machine(machine, machine_uuid):
    """Register a machine."""
    config_file = config.get_config_file()
    runners_list = config_file.read_variable("runners", [])
    runners_id = config_file.read_variable("runners_id", {})
    runners_list.append(machine)
    runners_id[machine] = machine_uuid
    config_file.write_variable("runners", runners_list)
    config_file.write_variable("runners_id", runners_id)
    return "successful"


@bp.route("/machine-id/<machine>", methods=["GET"])
def machine_id(machine):
    """Get machine ID for a specific machine."""
    config_file = config.get_config_file()
    runner_id = config_file.read_variable("runners_id", {})
    return runner_id[machine]
