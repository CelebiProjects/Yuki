"""Read EOS cache files using the credentials of the downloading host."""
import contextlib
import fnmatch
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

from Yuki.kernel.storage.cache import CACHE_IN_PROGRESS_MARKER, is_cache_marker


# Runs on either Yuki or a bare SSH runner, without importing Yuki remotely.
COPY_FILE = """
import os, pathlib, subprocess, sys, tempfile

url, root, relative, size, force = sys.argv[1:]
root = pathlib.Path(root)
target = root / relative
if root.absolute() != root.resolve() or target.absolute() != target.resolve():
    raise ValueError("symlink in destination path")
if target.exists():
    if not target.is_file():
        raise ValueError("destination is not a regular file")
    if force == "False":
        print("skipped")
        sys.exit(0)
target.parent.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(prefix=".yuki-eos-", dir=target.parent) as scratch:
    partial = pathlib.Path(scratch) / "download"
    result = subprocess.run(
        ["xrdcp", "--nopbar", "--cksum", "adler32", url, str(partial)],
        capture_output=True, text=True, timeout=3400,
    )
    if result.returncode:
        sys.exit(result.stderr.strip() or result.stdout.strip() or "xrdcp failed")
    if partial.stat().st_size != int(size):
        sys.exit("downloaded file size differs from EOS listing")
    if force == "True":
        os.replace(partial, target)
    else:
        try:
            os.link(partial, target)
        except FileExistsError:
            print("skipped")
            sys.exit(0)
print("transferred")
"""


def cache_url(config, runner_id, project, impression):
    """Resolve a managed cache root, with identifiers restricted to UUIDs."""
    if any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value)
           for value in (project, impression)):
        raise ValueError("EOS transfer requires project and impression UUIDs")
    base = config.read_variable("eos_xrootd_url", {}).get(runner_id, "")
    if not isinstance(base, str) or not base:
        raise ValueError("Set eos_xrootd_url for the REANA runner in Yuki configuration")
    validate_base_url(base)
    return f"{base.rstrip('/')}/{project}/{impression}"


def validate_base_url(base):
    """Check the configured EOS cache URL before adding managed paths."""
    if not isinstance(base, str):
        raise ValueError("EOS cache URL must be a string")
    parsed = urlsplit(base)
    if (parsed.scheme != "root" or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment
            or not parsed.path.startswith("//eos/")
            or ".." in parsed.path.split("/")):
        raise ValueError("Invalid EOS cache URL (expected root://host//eos/...)")


@contextlib.contextmanager
def worker_environment(secret_name=None, yuki_dir=None):
    """Optionally create an isolated ticket from the worker's mounted secret."""
    if secret_name:
        import base64
        from Yuki.services import secrets
        record = secrets.read(yuki_dir, secret_name)
        with tempfile.TemporaryDirectory(prefix="yuki-eos-keytab-") as scratch:
            path = Path(scratch) / "credential.keytab"
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(base64.b64decode(record["data"], validate=True))
            with _ticket_environment(str(path), record["principal"]) as env:
                yield env
        return
    keytab = os.environ.get("YUKI_EOS_KEYTAB")
    principal = os.environ.get("YUKI_EOS_PRINCIPAL")
    with _ticket_environment(keytab, principal) as env:
        yield env


@contextlib.contextmanager
def _ticket_environment(keytab, principal):
    """Acquire tickets without changing process-global credential settings."""
    env = os.environ.copy()
    if not (keytab or principal):
        yield env
        return
    if not (keytab and principal):
        raise ValueError("Set both YUKI_EOS_KEYTAB and YUKI_EOS_PRINCIPAL")
    with tempfile.TemporaryDirectory(prefix="yuki-eos-ticket-") as scratch:
        env["KRB5CCNAME"] = f"FILE:{scratch}/ccache"
        result = subprocess.run(["kinit", "-kt", keytab, principal], env=env,
                                capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise RuntimeError(f"EOS Kerberos authentication failed: {result.stderr.strip()}")
        yield env


def _run(args, env, ssh, timeout):
    if ssh is None:
        result = subprocess.run(args, env=env, capture_output=True, text=True,
                                timeout=timeout)
        out, err, code = result.stdout, result.stderr, result.returncode
    else:
        out, err, code = ssh.exec(shlex.join(args), timeout=timeout)
    if code:
        raise RuntimeError(err.strip() or out.strip() or f"{args[0]} failed")
    return out


def list_files(url, pattern, env, ssh):
    """Enumerate regular files on the same host that will download them."""
    parsed = urlsplit(url)
    endpoint = f"{parsed.scheme}://{parsed.netloc}"
    root = parsed.path.lstrip("/")
    args = ["xrdfs", endpoint, "ls", "-l", "-R", "/" + root]
    if ssh is None:
        listing = _run(args, env, None, 300)
    else:
        # SFTP the listing so a large stdout cannot fill the SSH exec window.
        scratch = _run(["mktemp", "-d"], None, ssh, 30).strip()
        target = f"{scratch}/listing"
        try:
            _run(["sh", "-c", f"{shlex.join(args)} > {shlex.quote(target)}"],
                 None, ssh, 300)
            listing = b"".join(ssh.stream(target)).decode()
        finally:
            ssh.remove(target)
            _run(["rmdir", "--", scratch], None, ssh, 30)
    prefix = f"/{root}/"
    for line in listing.splitlines():
        fields = line.split(maxsplit=6)
        if not fields or not fields[0].startswith("-"):
            continue
        if len(fields) != 7 or not fields[6].startswith(prefix):
            raise ValueError("Unexpected file path in EOS listing")
        relative = fields[6][len(prefix):]
        if (not relative or any(part in ("", ".", "..") for part in relative.split("/"))
                or any(char in relative for char in "?#%")):
            raise ValueError("Invalid relative file path in EOS listing")
        if is_cache_marker(relative) or (pattern and not fnmatch.fnmatch(relative, pattern)):
            continue
        yield {"name": relative, "size": int(fields[3])}


def copy_file(url, destination, entry, force, env, ssh):
    """Publish a verified file atomically and preserve existing files by default."""
    python = sys.executable if ssh is None else "python3"
    return _run([python, "-c", COPY_FILE, f"{url}/{entry['name']}",
                 str(Path(destination)), entry["name"], str(entry["size"]),
                 str(force)], env, ssh, 3500).strip()


def prepare_ssh_cache(root, ssh, files, force):
    """Fence EOS imports from workflow reuse until completeness is known."""
    if not force and all(ssh.isfile(f"{root}/{entry['name']}") for entry in files):
        return
    script = r"""
import pathlib, sys
root = pathlib.Path(sys.argv[1])
if root.absolute() != root.resolve():
    sys.exit("symlink in SSH cache path")
marker = root / sys.argv[2]
if root.exists() and any(root.iterdir()):
    if (marker.is_symlink() or not marker.is_file()
            or marker.read_text() != "eos-transfer\n"):
        sys.exit("Existing SSH cache is not an incomplete EOS import")
else:
    root.mkdir(parents=True, exist_ok=True)
    with marker.open("x") as stream:
        stream.write("eos-transfer\n")
"""
    _run(["python3", "-c", script, root, CACHE_IN_PROGRESS_MARKER],
         None, ssh, 30)
