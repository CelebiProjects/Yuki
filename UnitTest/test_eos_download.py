"""EOS transfer behavior at the SSH execution boundary."""
import os
from pathlib import Path
import subprocess

import pytest

from Yuki.kernel.storage.cache import CACHE_COMPLETE_MARKER, CACHE_IN_PROGRESS_MARKER
from Yuki.services import eos_download


class LocalSSH:
    """Execute the destination-side commands locally for this test."""

    def exec(self, command, timeout):
        result = subprocess.run(command, shell=True, text=True, capture_output=True,
                                timeout=timeout, check=False)
        return result.stdout, result.stderr, result.returncode

    def isfile(self, path):
        return Path(path).is_file()

    def stream(self, path):
        yield Path(path).read_bytes()

    def remove(self, path):
        Path(path).unlink(missing_ok=True)


def test_ssh_eos_transfer_fences_partial_cache_and_publishes_verified_files(tmp_path, monkeypatch):
    root = tmp_path / "cache"
    tools = tmp_path / "bin"
    tools.mkdir()
    monkeypatch.setenv("PATH", f"{tools}:{os.environ['PATH']}")
    listing = tools / "xrdfs"
    listing.write_text(
        "#!/bin/sh\nprintf '%s\\n' "
        "'-rw-r--r-- user group 4 2026-11-19 00:00:00 /eos/test/0/file.root' "
        "'-rw-r--r-- user group 4 2026-11-19 00:00:00 /eos/test/1/file.txt'\n"
    )
    copy = tools / "xrdcp"
    copy.write_text('#!/bin/sh\ntest "$2 $3" = "--cksum adler32" || exit 1\nprintf data > "$5"\n')
    for tool in (listing, copy):
        tool.chmod(0o755)

    ssh = LocalSSH()
    url = "root://example.test//eos/test"
    files = list(eos_download.list_files(url, "*.root", None, ssh))
    assert files == [{"name": "0/file.root", "size": 4}]
    eos_download.prepare_ssh_cache(str(root), ssh, files, False)
    assert (root / CACHE_IN_PROGRESS_MARKER).is_file()
    assert not (root / CACHE_COMPLETE_MARKER).exists()
    assert eos_download.copy_file(url, root, files[0], False, None, ssh) == "transferred"
    assert (root / "0/file.root").read_bytes() == b"data"
    assert eos_download.copy_file(url, root, files[0], False, None, ssh) == "skipped"

    with pytest.raises(RuntimeError, match="file size differs"):
        eos_download.copy_file(url, root, {"name": "1/file.root", "size": 5},
                               False, None, ssh)
    assert not (root / "1/file.root").exists()
    assert not list(root.rglob(".yuki-eos-*"))
