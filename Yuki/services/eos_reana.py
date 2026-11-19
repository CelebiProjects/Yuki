"""Isolated REANA client process for EOS usage jobs.

REANA's client config is process-global. Keeping it out of the Flask process
prevents concurrent runners from exchanging URLs or authentication context.
"""
import io
import json
import os
from pathlib import Path
import sys


def execute(request):
    from CelebiChrono.utils.metadata import ConfigFile
    config = ConfigFile(os.path.join(request["yuki_dir"], "config.json"))
    runner = request["runner_id"]
    url = config.read_variable("urls", {}).get(runner)
    token = config.read_variable("tokens", {}).get(runner)
    if not url or not token:
        raise ValueError("REANA URL or access token is missing")
    os.environ["REANA_SERVER_URL"] = url
    from reana_client.api import client
    from reana_commons.api_client import BaseAPIClient
    BaseAPIClient("reana-server")
    name = request["workflow"]
    operation = request["operation"]
    if operation.startswith("cleanup-"):
        verifying = operation in ("cleanup-verify-submit", "cleanup-verify-poll")
        submitting = operation in ("cleanup-submit", "cleanup-verify-submit")
        if not submitting and operation not in ("cleanup-poll", "cleanup-verify-poll"):
            raise ValueError("Unknown cleanup operation")
        output_name = "verification.json" if verifying else "cleanup-report.json"
        if submitting:
            if not verifying:
                manifest = json.loads(request["manifest"])
                prefixes = tuple("w-" + project[:8] + "-" for project in manifest["projects"])
                page = 1
                while True:
                    rows = client.get_workflows(token, "batch", page=page, size=100,
                                                status=["running", "queued"])
                    if any(str(row.get("name", "")).startswith(prefixes) for row in rows):
                        return {"not_submitted": True,
                                "reason": "Selected project has an active REANA workflow"}
                    if len(rows) < 100:
                        break
                    page += 1
            files = {filename: Path(__file__).with_name(filename).read_bytes()
                     for filename in ("eos_scan.py", "eos_delete.py")}
            files["delete-manifest.json"] = request["manifest"].encode()
            command = "python3 eos_delete.py" + (" --verify-only" if verifying else "")
            spec = {"version": "0.9.0", "inputs": {"files": list(files)},
                    "workflow": {"type": "serial", "specification": {"steps": [{
                        "name": "verify-eos-cleanup" if verifying else "cleanup-eos",
                        "environment": request["image"], "commands": [command],
                        "kerberos": True, "kubernetes_memory_limit": "512Mi"}]}},
                    "outputs": {"files": [output_name]}}
            client.create_workflow(spec, name, token)
            for filename, content in files.items():
                client.upload_file(name, io.BytesIO(content), filename, token)
            client.start_workflow(name, token, {})
            return {"status": "submitted"}
        remote_status = client.get_workflow_status(name, token).get("status", "unknown")
        result = {"remote_status": remote_status}
        if remote_status in ("finished", "success"):
            output = client.download_file(name, output_name, token)
            if len(output) > 2 and output[2]:
                import zipfile
                with zipfile.ZipFile(io.BytesIO(output[0])) as archive:
                    payload = archive.read(output_name)
            else:
                payload = output[0]
            result["verification" if verifying else "cleanup_report"] = json.loads(payload)
        return result
    if request["operation"] == "submit":
        specification = {
            "version": "0.9.0",
            "inputs": {"files": ["eos_scan.py", "scan-request.json"]},
            "workflow": {"type": "serial", "specification": {"steps": [{
                "name": "scan-eos", "environment": request["image"],
                "commands": ["python3 eos_scan.py"], "kerberos": True,
                "kubernetes_memory_limit": "512Mi"}]}},
            "outputs": {"files": ["eos-usage.json"]},
        }
        client.create_workflow(specification, name, token)
        source = Path(__file__).with_name("eos_scan.py").read_bytes()
        options = json.dumps({"root": request["root"],
                              "project": request.get("project"),
                              "known_projects": request.get("known_projects", []),
                              "legacy_impressions": request.get("legacy_impressions", [])}).encode()
        for filename, content in [("eos_scan.py", source), ("scan-request.json", options)]:
            client.upload_file(name, io.BytesIO(content), filename, token)
        client.start_workflow(name, token, {})
        return {"status": "submitted"}
    status = client.get_workflow_status(name, token).get("status", "unknown")
    result = {"remote_status": status}
    if status in ("finished", "success"):
        output = client.download_file(name, "eos-usage.json", token)
        if len(output) > 2 and output[2]:
            import zipfile
            with zipfile.ZipFile(io.BytesIO(output[0])) as archive:
                payload = archive.read("eos-usage.json")
        else:
            payload = output[0]
        result["inventory"] = json.loads(payload)
    return result


if __name__ == "__main__":
    try:
        value = execute(json.load(sys.stdin))
    except Exception as exc:  # Keep SDK diagnostics (which may contain tokens) out of state.
        value = {"error": f"REANA operation failed ({type(exc).__name__}); "
                          "check runner authentication and workflow in REANA."}
    print("EOS_USAGE_RESULT=" + json.dumps(value))
