"""Store REANA downloads without confusing directories and ZIP responses."""
import io
import os
from pathlib import Path, PurePosixPath
import stat
import zipfile


def leaf_entries(entries):
    """Skip directory rows when their descendants are present in the listing."""
    names = {entry["name"].rstrip("/") for entry in entries}
    parents = set()
    for name in names:
        parents.update(str(parent) for parent in PurePosixPath(name).parents)
    return [entry for entry in entries
            if entry["name"].rstrip("/") not in parents
            and not entry["name"].endswith("/")
            and entry.get("type") not in ("directory", "dir")]


def _directory(path):
    """Preserve an old misdownloaded directory ZIP before restoring its folder."""
    path = Path(path)
    if path.is_dir():
        return
    if path.exists():
        if path.is_symlink() or not zipfile.is_zipfile(path):
            raise FileExistsError(f"Download directory conflicts with file: {path}")
        backup = path.with_name(path.name + ".reana-directory.zip")
        suffix = 1
        while backup.exists():
            backup = path.with_name(path.name + f".reana-directory.{suffix}.zip")
            suffix += 1
        path.rename(backup)
    _directory(path.parent)
    path.mkdir(exist_ok=True)


def store_download(output, destination, root, remote_prefix):
    """Handle the REANA (bytes, filename, is_zip) API response.

    ZIP members use workspace-relative names. Only regular files under the
    requested stageout/log root are accepted; validate every member first.
    Older mocked clients can return only the payload element.
    """
    root = Path(root)
    if len(output) > 2 and output[2] is True:
        with zipfile.ZipFile(io.BytesIO(output[0])) as archive:
            members = []
            for item in archive.infolist():
                if item.is_dir():
                    continue
                name = PurePosixPath(item.filename)
                mode = item.external_attr >> 16
                if (name.is_absolute() or ".." in name.parts
                        or stat.S_ISLNK(mode)
                        or not item.filename.startswith(remote_prefix)):
                    raise ValueError(f"Unexpected REANA archive member: {item.filename}")
                relative = item.filename[len(remote_prefix):]
                if not relative:
                    raise ValueError("Empty REANA archive member path")
                target = root / relative
                if not target.resolve().is_relative_to(root.resolve()):
                    raise ValueError(f"Archive member escapes download root: {relative}")
                members.append((item, target))
            for item, target in members:
                _write(target, archive.read(item))
            return [str(target.relative_to(root)) for _, target in members]
    destination = Path(destination)
    if not destination.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"File escapes download root: {destination}")
    _write(destination, output[0])
    return [os.path.relpath(destination, root)]


def _write(path, payload):
    _directory(path.parent)
    if path.is_symlink():
        raise ValueError(f"Refusing to overwrite download symlink: {path}")
    path.write_bytes(payload)
