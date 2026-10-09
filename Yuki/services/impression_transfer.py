"""Impression import/export use cases shared by CLI and API."""

import os
import re
import tarfile


UUID_RE = re.compile(r'^[0-9a-f]{32}$')


def _resolve_yuki_dir(yuki_dir):
    """Resolve yuki_dir, expanding ~ and env vars."""
    return os.path.expanduser(os.path.expandvars(yuki_dir or "~/.Yuki"))


def export_impression(project_uuid, impression_uuid, output_path,
                      yuki_dir="~/.Yuki"):
    """Export a single impression to a tar.gz file."""
    yuki_dir = _resolve_yuki_dir(yuki_dir)
    storage = os.path.join(yuki_dir, "Storage")
    src_dir = os.path.join(storage, project_uuid, impression_uuid)

    if not os.path.isdir(src_dir):
        raise FileNotFoundError(f"Impression not found: {src_dir}")

    with tarfile.open(output_path, "w:gz") as tar:
        _add_impression_to_tar(tar, src_dir, impression_uuid)

    return output_path


def export_impressions(project_uuid, impression_uuids, output_path,
                       yuki_dir="~/.Yuki"):
    """Export multiple impressions to a single tar.gz file."""
    yuki_dir = _resolve_yuki_dir(yuki_dir)
    storage = os.path.join(yuki_dir, "Storage")

    with tarfile.open(output_path, "w:gz") as tar:
        for impression_uuid in impression_uuids:
            src_dir = os.path.join(storage, project_uuid, impression_uuid)
            if not os.path.isdir(src_dir):
                continue
            _add_impression_to_tar(tar, src_dir, impression_uuid)

    return output_path


def export_impression_to_buffer(project_uuid, impression_uuids,
                                yuki_dir="~/.Yuki"):
    """Export impressions to an in-memory BytesIO buffer (for API use)."""
    import io

    yuki_dir = _resolve_yuki_dir(yuki_dir)
    storage = os.path.join(yuki_dir, "Storage")
    buf = io.BytesIO()

    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for impression_uuid in impression_uuids:
            src_dir = os.path.join(storage, project_uuid, impression_uuid)
            if not os.path.isdir(src_dir):
                continue
            _add_impression_to_tar(tar, src_dir, impression_uuid)

    buf.seek(0)
    return buf


def _add_impression_to_tar(tar, src_dir, arcname_base):
    """Add an impression directory tree to a tarfile."""
    for root, dirs, files in os.walk(src_dir):
        for fname in files:
            full_path = os.path.join(root, fname)
            rel = os.path.relpath(full_path, src_dir)
            arcname = os.path.join(arcname_base, rel)
            tar.add(full_path, arcname=arcname)
        # Filter out EOS mount directories that may be empty/inaccessible
        dirs[:] = [d for d in dirs if not d.startswith('.')]


def import_impression(project_uuid, tar_path, yuki_dir="~/.Yuki"):
    """Import impressions from a tar.gz file.

    Returns dict with 'imported' (list of UUIDs), 'skipped' (list of
    {name, reason} dicts), and 'count'.
    """
    yuki_dir = _resolve_yuki_dir(yuki_dir)
    storage = os.path.join(yuki_dir, "Storage")
    target_dir = os.path.join(storage, project_uuid)

    imported = set()
    skipped = []

    with tarfile.open(tar_path, "r:*") as tar:
        _extract_impressions(tar, target_dir, imported, skipped)

    return {
        "imported": sorted(imported),
        "skipped": skipped,
        "count": len(imported),
    }


def import_impression_from_stream(project_uuid, fileobj, yuki_dir="~/.Yuki"):
    """Import impressions from a file-like object (for API use).

    Returns same dict as import_impression.
    """
    yuki_dir = _resolve_yuki_dir(yuki_dir)
    storage = os.path.join(yuki_dir, "Storage")
    target_dir = os.path.join(storage, project_uuid)

    imported = set()
    skipped = []

    with tarfile.open(fileobj=fileobj, mode="r:*") as tar:
        _extract_impressions(tar, target_dir, imported, skipped)

    return {
        "imported": sorted(imported),
        "skipped": skipped,
        "count": len(imported),
    }


def _extract_impressions(tar, target_dir, imported, skipped):
    """Extract and validate impression entries from a tarfile."""
    impression_dirs = {}  # impression_uuid -> set of member paths

    for member in tar.getmembers():
        parts = member.name.split("/")
        if len(parts) < 2:
            if not member.isdir():
                skipped.append({
                    "name": member.name,
                    "reason": "Not inside an impression directory",
                })
            continue

        impression = parts[0]

        # Reject path traversal or absolute paths
        if ".." in parts or any(p.startswith("/") for p in parts):
            skipped.append({
                "name": member.name,
                "reason": "Path traversal or absolute path rejected",
            })
            continue

        # Validate impression UUID
        if not UUID_RE.match(impression):
            skipped.append({
                "name": member.name,
                "reason": f"'{impression}' is not a valid impression UUID",
            })
            continue

        # Track what we see for each impression
        if impression not in impression_dirs:
            impression_dirs[impression] = set()
        impression_dirs[impression].add(member.name)

        if member.isdir():
            continue

        # Extract the file
        target_path = os.path.join(target_dir, member.name)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        source = tar.extractfile(member)
        if source is None:
            continue
        with open(target_path, "wb") as f:
            f.write(source.read())

    # Validate each impression has config.json and mark as imported
    for impression, paths in impression_dirs.items():
        config_path = os.path.join(impression, "config.json")
        if config_path not in paths:
            skipped.append({
                "name": impression,
                "reason": "Missing config.json — not a valid impression",
            })
            continue
        imported.add(impression)
        _mark_imported(target_dir, impression)


def _mark_imported(target_dir, impression_uuid):
    """Write origin=imported marker into the impression's config.json."""
    from CelebiChrono.utils.metadata import ConfigFile

    config_path = os.path.join(target_dir, impression_uuid, "config.json")
    try:
        config = ConfigFile(config_path)
        config.write_variable("origin", "imported")
    except (OSError, ValueError) as exc:
        import logging
        logging.getLogger("YukiLogger").warning(
            "Failed to mark impression %s as imported: %s", impression_uuid, exc
        )
