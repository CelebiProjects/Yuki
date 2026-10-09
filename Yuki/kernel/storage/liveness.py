"""Live-set registry: which impressions are the current versions of
tasks/algorithms, and which workflows their runs produced."""
import datetime
import json
import os
import re

UUID_RE = re.compile(r"^[0-9a-f]{32}$")


def _yuki_dir():
    """Yuki data root ($YUKIDIR or ~/.Yuki)."""
    return os.path.expanduser(os.environ.get("YUKIDIR", "~/.Yuki"))


def live_path(yuki_dir, project_uuid):
    """Path of a project's live-set file."""
    return os.path.join(yuki_dir, "Live", f"{project_uuid}.json")


def validate_sets(live, superseded):
    """Raise ValueError naming any entry that is not a 32-hex uuid or
    appears in both lists."""
    problems = []
    seen_live, seen_sup = set(), set()
    for uuid in live:
        if not isinstance(uuid, str) or not UUID_RE.match(uuid):
            problems.append(f"invalid live entry: {uuid!r}")
            continue
        seen_live.add(uuid)
    for uuid in superseded:
        if not isinstance(uuid, str) or not UUID_RE.match(uuid):
            problems.append(f"invalid superseded entry: {uuid!r}")
            continue
        seen_sup.add(uuid)
    for uuid in seen_live & seen_sup:
        problems.append(f"uuid in both lists: {uuid}")
    if problems:
        raise ValueError("; ".join(problems))


def derive_live_workflows(project_uuid, live_impressions, yuki_dir=None):
    """Workflow uuids from the per-machine run configs of live impressions."""
    yuki_dir = yuki_dir or _yuki_dir()
    from CelebiChrono.utils.metadata import ConfigFile
    storage = os.path.join(yuki_dir, "Storage", project_uuid)
    workflows = set()
    if not os.path.isdir(storage):
        return sorted(workflows)
    for impression in live_impressions:
        imp_dir = os.path.join(storage, impression)
        if not os.path.isdir(imp_dir):
            continue
        for machine in os.listdir(imp_dir):
            run_cfg = os.path.join(imp_dir, machine, "config.json")
            if not os.path.isfile(run_cfg):
                continue
            workflow = ConfigFile(run_cfg).read_variable("workflow", "")
            if workflow:
                workflows.add(workflow)
    return sorted(workflows)


def save_live_set(project_uuid, live, superseded, yuki_dir=None):
    """Validate, derive live workflows, and atomically replace the set.

    Returns {"stored": True, "live": n, "superseded": n,
    "live_workflows": n}. Raises ValueError on invalid input; anything
    else propagates (nothing is stored on failure).
    """
    yuki_dir = yuki_dir or _yuki_dir()
    validate_sets(live, superseded)
    live = sorted(set(live))
    superseded = sorted(set(superseded))
    live_workflows = derive_live_workflows(project_uuid, live, yuki_dir)
    payload = {
        "live": live,
        "superseded": superseded,
        "live_workflows": live_workflows,
        "updated": datetime.datetime.now(
            datetime.timezone.utc).isoformat(),
    }
    path = live_path(yuki_dir, project_uuid)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    os.replace(tmp_path, path)
    return {"stored": True, "live": len(live),
            "superseded": len(superseded),
            "live_workflows": len(live_workflows)}


def load_live_set(project_uuid, yuki_dir=None):
    """The stored set, or None."""
    yuki_dir = yuki_dir or _yuki_dir()
    path = live_path(yuki_dir, project_uuid)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def impression_live(project_uuid, impression, yuki_dir=None):
    """True (live) / False (explicitly superseded) / None (no set)."""
    data = load_live_set(project_uuid, yuki_dir)
    if data is None:
        return None
    if impression in data.get("superseded", []):
        return False
    return True


def workflow_live(project_uuid, workflow_uuid, yuki_dir=None):
    """True (derived live workflow) / False / None (no set)."""
    data = load_live_set(project_uuid, yuki_dir)
    if data is None:
        return None
    return workflow_uuid in data.get("live_workflows", [])
