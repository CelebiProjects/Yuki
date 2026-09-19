"""Tests for remote data operation helpers."""
import json
import os
import subprocess
from unittest import mock

import pytest

from CelebiChrono.utils.file_utils import dir_md5
from Yuki.kernel.remote_data_ops import (
    REMOTE_MD5_SCRIPT, remote_md5_command, build_remote_fast_copy_command,
    read_remote_progress, _yuki_dir,
)


def _fixture_tree(root):
    """Nested tree with hidden files, matching dir_md5's exclude rules."""
    os.makedirs(os.path.join(root, "sub", "deep"))
    os.makedirs(os.path.join(root, ".hidden_dir"))
    with open(os.path.join(root, "a.txt"), "w", encoding="utf-8") as f:
        f.write("alpha")
    with open(os.path.join(root, "sub", "b.txt"), "w", encoding="utf-8") as f:
        f.write("beta")
    with open(os.path.join(root, "sub", "deep", "c.txt"), "w", encoding="utf-8") as f:
        f.write("gamma")
    with open(os.path.join(root, ".secret"), "w", encoding="utf-8") as f:
        f.write("hidden file content")
    with open(os.path.join(root, ".hidden_dir", "d.txt"), "w", encoding="utf-8") as f:
        f.write("hidden dir content")


def test_remote_md5_matches_dir_md5_semantics(tmp_path):
    """The remote md5 script agrees with dir_md5 on the same tree."""
    fixture = tmp_path / "data"
    _fixture_tree(str(fixture))
    expected = dir_md5(str(fixture))

    result = subprocess.run(
        ["python3", "-c", REMOTE_MD5_SCRIPT, str(fixture)],
        capture_output=True, text=True, timeout=60, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


@pytest.mark.parametrize("source_kind", ["missing", "file", "broken_symlink"])
@pytest.mark.parametrize("with_progress", [False, True])
def test_remote_md5_rejects_invalid_source(tmp_path, source_kind, with_progress):
    """Invalid sources fail without emitting an empty-tree hash or progress."""
    source = tmp_path / "source"
    if source_kind == "file":
        source.write_text("not a directory", encoding="utf-8")
    elif source_kind == "broken_symlink":
        source.symlink_to(tmp_path / "missing-target", target_is_directory=True)
    progress = tmp_path / "progress.json"
    args = ["python3", "-c", REMOTE_MD5_SCRIPT, str(source)]
    if with_progress:
        args.append(str(progress))
    result = subprocess.run(args, capture_output=True, text=True,
                            timeout=60, check=False)
    assert result.returncode != 0
    assert result.stdout == ""
    assert str(source) in result.stderr
    assert ("NotADirectoryError" if source_kind == "file" else
            "FileNotFoundError") in result.stderr
    assert not progress.exists()


def test_remote_md5_does_not_ignore_directory_scan_errors(tmp_path):
    """A failed subtree scan must not produce a hash of partial contents."""
    source = tmp_path / "data"
    _fixture_tree(str(source))
    # Inject a scan failure so the test also works as root and on all platforms.
    prefix = (
        "import os\n"
        "original_scandir = os.scandir\n"
        "def failing_scandir(path):\n"
        f"    if path == {str(source / 'sub')!r}:\n"
        "        raise PermissionError('Cannot scan source subtree')\n"
        "    return original_scandir(path)\n"
        "os.scandir = failing_scandir\n"
    )
    progress = tmp_path / "progress.json"
    result = subprocess.run(
        ["python3", "-c", prefix + REMOTE_MD5_SCRIPT, str(source), str(progress)],
        capture_output=True, text=True, timeout=60, check=False)
    assert result.returncode != 0
    assert result.stdout == ""
    assert "Cannot scan source subtree" in result.stderr
    assert not progress.exists()


def test_remote_md5_command_quotes_args():
    """remote_md5_command quotes paths containing spaces."""
    cmd = remote_md5_command("/data/my dir/with spaces")
    assert cmd.startswith("python3 -c ")
    assert "'/data/my dir/with spaces'" in cmd


def test_remote_md5_with_progress_writes_progress_file(tmp_path):
    """With a progress path, the script writes cumulative byte progress."""
    fixture = tmp_path / "data"
    _fixture_tree(str(fixture))
    expected = dir_md5(str(fixture))
    progress = tmp_path / "progress.json"

    result = subprocess.run(
        ["python3", "-c", REMOTE_MD5_SCRIPT, str(fixture), str(progress)],
        capture_output=True, text=True, timeout=60, check=False)
    assert result.returncode == 0, result.stderr
    # The md5 must not change when progress reporting is enabled.
    assert result.stdout.strip() == expected

    with open(progress, encoding="utf-8") as f:
        data = json.load(f)
    assert data["stage"] == "hashing"
    # Only non-hidden files count: alpha(5) + beta(4) + gamma(5).
    assert data["bytes_total"] == 14
    assert data["bytes_done"] == 14


def test_remote_md5_command_with_progress_quotes_both_paths():
    """remote_md5_command quotes the progress path when given one."""
    cmd = remote_md5_command("/data/my dir", "/tmp/prog file.json")
    assert "'/data/my dir'" in cmd
    assert "'/tmp/prog file.json'" in cmd


def test_remote_md5_progress_creates_progress_dir(tmp_path):
    """The script creates the progress directory when it does not exist."""
    fixture = tmp_path / "data"
    _fixture_tree(str(fixture))
    progress = tmp_path / "new" / "nested" / "progress.json"

    result = subprocess.run(
        ["python3", "-c", REMOTE_MD5_SCRIPT, str(fixture), str(progress)],
        capture_output=True, text=True, timeout=60, check=False)
    assert result.returncode == 0, result.stderr
    assert progress.exists()


def test_remote_md5_progress_writes_zero_entry_for_empty_tree(tmp_path):
    """An empty tree still gets an initial progress entry (total known).

    Without the initial write, no per-file write would ever run, so the
    progress file would not exist.
    """
    fixture = tmp_path / "empty"
    fixture.mkdir()
    progress = tmp_path / "progress.json"

    result = subprocess.run(
        ["python3", "-c", REMOTE_MD5_SCRIPT, str(fixture), str(progress)],
        capture_output=True, text=True, timeout=60, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == dir_md5(str(fixture))
    data = json.loads(progress.read_text(encoding="utf-8"))
    assert data == {"stage": "hashing", "bytes_done": 0, "bytes_total": 0}


def test_fast_copy_command_chain():
    """The fast-copy command falls back through reflink/hardlink/rsync/cp
    and makes the copied data read-only."""
    cmd = build_remote_fast_copy_command("/src dir", "/dst dir")
    assert "mkdir -p '/dst dir'" in cmd
    assert "cp -a --reflink=auto '/src dir'/." in cmd
    assert "cp -al '/src dir'/." in cmd
    assert "rsync -a '/src dir'/" in cmd
    assert "cp -r '/src dir'/." in cmd
    assert ("find '/dst dir' -mindepth 1 -maxdepth 1 "
            "-exec chmod -R a-w -- {} +") in cmd


def test_fast_copy_command_with_progress_watches_dst_bytes():
    """With a progress path, the copy runs backgrounded under a du watcher."""
    cmd = build_remote_fast_copy_command("/src dir", "/dst dir",
                                         "/tmp/prog file.json")
    assert "mkdir -p '/dst dir'" in cmd
    # The fallback chain is preserved, now backgrounded.
    assert "cp -a --reflink=auto '/src dir'/." in cmd
    assert "rsync -a '/src dir'/" in cmd
    # The read-only step runs with the copy, before the watcher finishes.
    assert "chmod -R a-w -- {} +" in cmd
    # Watcher polls dst bytes into the progress file as stage copying.
    assert "du -sb '/dst dir'" in cmd
    assert '"stage": "copying"' in cmd
    assert "'/tmp/prog file.json'" in cmd
    # The chain's exit code survives and the progress file is removed.
    assert "wait $_pid" in cmd
    assert "exit $_code" in cmd
    assert "rm -f '/tmp/prog file.json'" in cmd


def test_fast_copy_command_progress_reads_total_from_progress_file():
    """bytes_total comes from the progress file left by the md5 stage."""
    cmd = build_remote_fast_copy_command("/s", "/d", "/prog file.json")
    assert 'bytes_total' in cmd
    assert "'/prog file.json'" in cmd


def test_fast_copy_command_progress_runs_end_to_end(tmp_path):
    """The full copy command parses, copies, and removes the progress file."""
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    (src / "sub").mkdir(parents=True)
    (src / "a.txt").write_text("alpha" * 1000, encoding="utf-8")
    (src / "sub" / "b.txt").write_text("beta" * 2000, encoding="utf-8")
    progress = tmp_path / "prog.json"
    progress.write_text(json.dumps(
        {"stage": "hashing", "bytes_done": 13000, "bytes_total": 13000}),
        encoding="utf-8")

    cmd = build_remote_fast_copy_command(str(src), str(dst), str(progress))
    result = subprocess.run(["bash", "-c", cmd], capture_output=True,
                            text=True, timeout=60, check=False)
    assert result.returncode == 0, result.stderr
    assert (dst / "a.txt").read_text(encoding="utf-8") == "alpha" * 1000
    assert (dst / "sub" / "b.txt").read_text(encoding="utf-8") == "beta" * 2000
    assert not progress.exists()
    if os.geteuid() != 0:  # root bypasses write permissions
        assert not os.access(dst / "a.txt", os.W_OK)
        assert not os.access(dst / "sub" / "b.txt", os.W_OK)


def test_fast_copy_command_empty_src_succeeds(tmp_path):
    """An empty source dir still completes (the ro step tolerates it)."""
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    progress = tmp_path / "prog.json"
    progress.write_text(json.dumps(
        {"stage": "hashing", "bytes_done": 0, "bytes_total": 0}),
        encoding="utf-8")

    cmd = build_remote_fast_copy_command(str(src), str(dst), str(progress))
    result = subprocess.run(["bash", "-c", cmd], capture_output=True,
                            text=True, timeout=60, check=False)
    assert result.returncode == 0, result.stderr
    assert dst.is_dir()
    assert not progress.exists()


def test_remote_md5_command_executes_end_to_end(tmp_path):
    """The md5 command string parses, hashes, and writes progress."""
    fixture = tmp_path / "data"
    _fixture_tree(str(fixture))
    progress = tmp_path / "prog.json"

    cmd = remote_md5_command(str(fixture), str(progress))
    result = subprocess.run(["bash", "-c", cmd], capture_output=True,
                            text=True, timeout=60, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == dir_md5(str(fixture))
    data = json.loads(progress.read_text(encoding="utf-8"))
    assert data["bytes_done"] == data["bytes_total"] == 14


def test_yuki_dir_env(monkeypatch, tmp_path):
    """_yuki_dir honors YUKIDIR and falls back to HOME/.Yuki."""
    monkeypatch.setenv("YUKIDIR", str(tmp_path / "custom"))
    assert _yuki_dir() == str(tmp_path / "custom")
    monkeypatch.delenv("YUKIDIR")
    with mock.patch.dict(os.environ, {"HOME": str(tmp_path)}):
        assert _yuki_dir() == os.path.join(str(tmp_path), ".Yuki")


class _FakeSsh:
    """Ssh shim answering cat-style progress reads."""

    def __init__(self, out="", err="", code=0):
        self.out, self.err, self.code = out, err, code

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def exec(self, command, timeout=None):  # pylint: disable=unused-argument
        """Answer with the canned output."""
        return self.out, self.err, self.code


def test_read_remote_progress_parses_file():
    """read_remote_progress returns the parsed progress dict."""
    fake = _FakeSsh(out='{"stage": "copying", "bytes_done": 3, "bytes_total": 7}')
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    return_value=fake), \
            mock.patch("Yuki.kernel.remote_data_ops.progress_file_path",
                       return_value="/w/prog.json"):
        assert read_remote_progress("r1", "job-1") == {
            "stage": "copying", "bytes_done": 3, "bytes_total": 7}


def test_read_remote_progress_none_on_any_failure():
    """Missing file, bad json, and ssh errors all yield None."""
    for fake in (_FakeSsh(out="", err="No such file", code=1),
                 _FakeSsh(out="not json", code=0),
                 _FakeSsh(out="42", code=0)):
        with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                        return_value=fake), \
                mock.patch("Yuki.kernel.remote_data_ops.progress_file_path",
                           return_value="/w/prog.json"):
            assert read_remote_progress("r1", "job-1") is None
    with mock.patch("Yuki.kernel.remote_data_ops._ssh_connection",
                    side_effect=ConnectionError("banner")):
        assert read_remote_progress("r1", "job-1") is None
