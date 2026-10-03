"""Read ConfigFile JSON safely alongside its flock-protected writers."""
import fcntl
import json


def read_variable(path, name, default=None):
    """Read one JSON variable while respecting ConfigFile.write_variable's lock."""
    try:
        with open(path, encoding="utf-8") as source:
            fcntl.flock(source, fcntl.LOCK_SH)
            try:
                contents = source.read()
                if not contents.strip():
                    return default
                try:
                    return json.loads(contents).get(name, default)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid JSON in {path}: {exc}") from exc
            finally:
                fcntl.flock(source, fcntl.LOCK_UN)
    except FileNotFoundError:
        return default
