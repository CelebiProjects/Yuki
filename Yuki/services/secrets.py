"""Private, immutable named credentials, separate from analysis storage."""
import base64
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile

MAX_KEYTAB_BYTES = 1024 * 1024


def validate_name(name):
    """Restrict names to a single portable path component."""
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name):
        raise ValueError("Secret name must use 1-64 letters, digits, dots, underscores or hyphens")


def secret_path(root, name):
    """Locate a credential under the service's private directory."""
    validate_name(name)
    return Path(root) / "Secrets" / (name + ".json")


def add(root, name, kind, principal, content):
    """Publish once, atomically; never replace an existing credential."""
    path = secret_path(root, name)
    if kind != "kerberos":
        raise ValueError("Only kerberos keytabs are supported")
    if not isinstance(principal, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._/-]*@[A-Za-z0-9][A-Za-z0-9.-]*", principal):
        raise ValueError("Principal must have the form USER@REALM")
    if not 2 < len(content) <= MAX_KEYTAB_BYTES or content[:2] not in (b"\x05\x01", b"\x05\x02"):
        raise ValueError("Expected a non-empty Kerberos keytab (at most 1 MiB)")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.is_symlink():
        raise ValueError("Secret directory must not be a symlink")
    os.chmod(path.parent, 0o700)
    record = {"name": name, "type": kind, "principal": principal,
              "data": base64.b64encode(content).decode("ascii")}
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(record, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        os.unlink(temporary)
    return {key: record[key] for key in ("name", "type", "principal")}


def read(root, name):
    """Read credentials for internal use only."""
    path = secret_path(root, name)
    if path.is_symlink() or path.parent.is_symlink():
        raise ValueError("Secret paths must not be symlinks")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ValueError("Secret not found") from None


def bind_reana(config_path, runner_id, name):
    """Keep REANA's global SDK configuration and diagnostics out of Flask."""
    read(Path(config_path).parent, name)
    result = subprocess.run(
        [sys.executable, "-m", "Yuki.services.secrets_reana"],
        input=json.dumps({"config_path": str(config_path), "runner_id": runner_id, "name": name}),
        capture_output=True, text=True, timeout=90, check=False,
    )
    if result.returncode or "YUKI_SECRET_BOUND" not in result.stdout.splitlines():
        raise RuntimeError("REANA secret binding failed; check connectivity, token and existing "
                           "CERN_USER/CERN_KEYTAB secrets. No local binding was saved.")


def bind_ssh(config_file, name, settings):
    """Copy a keytab into a private runner directory outside workflow storage."""
    # Import lazily to avoid a workflow/service dependency cycle.
    from Yuki.kernel.workflows.ssh import _SshConnection  # pylint: disable=import-outside-toplevel
    record = read(Path(config_file.file_path).parent, name)
    with _SshConnection(settings["host"], settings["user"],
                        settings.get("key_path"), settings.get("port", 22)) as ssh:
        out, _, code = ssh.exec('umask 077; mktemp -d "$HOME/.yuki-secret.XXXXXXXXXX"', timeout=30)
        directory = out.strip()
        if code or not directory.startswith("/") or "\n" in directory:
            raise RuntimeError("Unable to create private runner credential directory")
        remote_path = directory + "/credential.keytab"
        try:
            with tempfile.TemporaryDirectory(prefix="yuki-secret-") as scratch:
                local_path = Path(scratch) / "keytab"
                fd = os.open(local_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(base64.b64decode(record["data"], validate=True))
                ssh.put(str(local_path), remote_path)
            _, _, code = ssh.exec("chmod 600 " + shlex.quote(remote_path), timeout=30)
            if code:
                raise RuntimeError("Unable to restrict runner keytab permissions")
        except Exception:
            ssh.exec("rm -f -- " + shlex.quote(remote_path) + "; rmdir -- " +
                     shlex.quote(directory), timeout=30)
            raise RuntimeError("SSH credential distribution failed") from None
    return {"secret": name, "kerberos_keytab": remote_path,
            "kerberos_principal": record["principal"]}
