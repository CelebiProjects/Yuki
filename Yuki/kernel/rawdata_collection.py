"""Collection of data impressions that do not have an execution workflow."""
import os
import posixpath
import tempfile

from CelebiChrono.utils.metadata import ConfigFile

from . import file_types, remote_data_ops
from .file_staging import walk_files


def local_rawdata_files(job_path):
    """Expose uploaded data as already available in Yuki."""
    return [{"name": rel, "size": os.path.getsize(path),
             "type": file_types.classify(rel),
             "in_runner": False, "in_yuki": True}
            for rel, path in walk_files(os.path.join(job_path, "rawdata"))]


def collect_rawdata(job_path, runners_id, predicate):  # pylint: disable=too-many-locals
    """Report local data and collect selected remote data without a workflow."""
    reports = {}
    local = local_rawdata_files(job_path)
    if local:
        reports["yuki"] = {
            "collected": [], "failed": [],
            "skipped": [{"file": row["name"],
                         "reason": ("already in Yuki" if predicate(row["name"])
                                    else "does not match selector")}
                        for row in local],
        }
    marker_path = os.path.join(job_path, "remote.json")
    if not os.path.isfile(marker_path):
        return reports
    marker = ConfigFile(marker_path)
    runner_id = marker.read_variable("host_runner_id", "")
    source = marker.read_variable("remote_path", "")
    if not runner_id or not source:
        reports["remote-data"] = {"collected": [], "skipped": [], "failed": [
            {"file": "<stageout>", "reason": "incomplete remote data registration"}]}
        return reports
    name = next((name for name, rid in runners_id.items() if rid == runner_id),
                runner_id)
    report = {"collected": [], "skipped": [], "failed": []}
    reports[name] = report
    destination = os.path.realpath(os.path.join(job_path, runner_id, "stageout"))
    try:
        with remote_data_ops._ssh_connection(runner_id) as ssh:  # pylint: disable=protected-access
            for rel, _remote_path, size in ssh.walk_files(source):
                temporary = None
                try:
                    target = os.path.realpath(os.path.join(destination, rel))
                    if (os.path.isabs(rel) or ".." in rel.split("/") or
                            os.path.commonpath([destination, target]) != destination):
                        raise ValueError("unsafe relative data path")
                    if not predicate(rel):
                        report["skipped"].append(
                            {"file": rel, "reason": "does not match selector"})
                        continue
                    if os.path.isfile(target) and os.path.getsize(target) == size:
                        report["skipped"].append(
                            {"file": rel, "reason": "already in Yuki"})
                        continue
                    os.makedirs(os.path.dirname(target), exist_ok=True)
                    with tempfile.NamedTemporaryFile(
                            dir=os.path.dirname(target), delete=False) as handle:
                        temporary = handle.name
                    ssh.get(posixpath.join(source, rel), temporary)
                    if os.path.getsize(temporary) != size:
                        raise OSError("incomplete remote data download")
                    os.replace(temporary, target)
                    report["collected"].append(rel)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    report["failed"].append({"file": rel, "reason": str(exc)})
                finally:
                    if temporary and os.path.exists(temporary):
                        os.unlink(temporary)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        report["failed"].append({"file": "<stageout>", "reason": str(exc)})
    return reports
