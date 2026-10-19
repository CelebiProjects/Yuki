"""Small-result selection and repeatable archives."""
import tarfile
import zipfile
import pytest
from Yuki.services.celebi_server_booker import pack_booking_archives

def test_pack_filters_sizes_types_and_symlinks_deterministically(tmp_path):
    project, impression = "1" * 32, "a" * 32
    stageout = tmp_path / "storage" / project / impression / "runner" / "stageout"
    stageout.mkdir(parents=True)
    (stageout / "plot.png").write_bytes(b"PNG")
    (stageout / "summary.txt").write_text("fit succeeded")
    (stageout / "events.root").write_bytes(b"DATA")
    (stageout / "large.txt").write_bytes(b"x" * 200)
    (tmp_path / "secret.txt").write_text("secret")
    (stageout / "escape.txt").symlink_to(tmp_path / "secret.txt")
    one = pack_booking_archives(tmp_path / "storage", project, [impression],
                                tmp_path / "one", max_file_size=100)
    two = pack_booking_archives(tmp_path / "storage", project, [impression],
                                tmp_path / "two", max_file_size=100)
    assert one == two
    assert one["packed"][0]["file_count"] == 2
    with tarfile.open(tmp_path / "one" / (impression + ".tar.gz")) as archive:
        names = archive.getnames()
        assert "contents/runner/stageout/plot.png" in names
        assert "contents/runner/stageout/summary.txt" in names
        assert not any("escape" in n or "large" in n or "root" in n for n in names)
        listing = archive.extractfile("stageout.filelist.json").read().decode()
        assert "too-large" in listing and "unsupported-type" in listing

def test_no_results_stays_missing_for_later_retry(tmp_path):
    source = tmp_path / "storage" / ("1" * 32) / ("a" * 32)
    source.mkdir(parents=True)
    result = pack_booking_archives(tmp_path / "storage", "1" * 32, ["a" * 32], tmp_path / "packed")
    assert result == {"packed": [], "missing": ["a" * 32]}


def test_pack_reads_small_results_from_extensionless_directory_zip(tmp_path):
    project, impression = "1" * 32, "a" * 32
    stageout = tmp_path / "storage" / project / impression / "runner" / "stageout"
    stageout.mkdir(parents=True)
    bundle = stageout / "sweight"
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("imp/stageout/sweight/fit.png", b"PNG")
        archive.writestr("imp/stageout/sweight/summary.txt", "fit succeeded")
        archive.writestr("imp/stageout/sweight/events.root", b"DATA")
        archive.writestr("../escape.txt", "secret")

    result = pack_booking_archives(tmp_path / "storage", project, [impression],
                                   tmp_path / "packed")

    assert result["missing"] == []
    assert result["packed"][0]["file_count"] == 2
    with tarfile.open(tmp_path / "packed" / (impression + ".tar.gz")) as archive:
        names = archive.getnames()
        assert "contents/runner/stageout/sweight/fit.png" in names
        assert "contents/runner/stageout/sweight/summary.txt" in names
        assert not any("escape" in name or name.endswith(".root") for name in names)

def test_rejects_traversal(tmp_path):
    with pytest.raises(ValueError):
        pack_booking_archives(tmp_path, "../secret", ["a" * 32], tmp_path / "packed")


def test_new_pack_batch_resumes_interrupted_upload(tmp_path, monkeypatch):
    import hashlib
    import requests
    from Yuki.services.celebi_server_booker import upload_booking_archives

    uid, project = "a" * 32, "1" * 32
    data = b"abcdefghij"
    entry = {"uuid": uid, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}
    first, second = tmp_path / "first", tmp_path / "second"
    for path in (first, second):
        path.mkdir()
        (path / (uid + ".tar.gz")).write_bytes(data)
    received, calls = set(), []
    interrupted = False

    class Response:
        status_code = 200
        def __init__(self, data):
            self.data = data
        def json(self):
            return self.data
        def raise_for_status(self):
            return None

    class Session:
        def __init__(self):
            self.headers = {}
        def post(self, url, **kwargs):
            calls.append(url)
            return Response({"upload_id": "upload-1", "chunk_size": 4})
        def get(self, url, **kwargs):
            return Response({"received": sorted(received), "chunk_size": 4})
        def put(self, url, **kwargs):
            nonlocal interrupted
            index = int(url.rsplit("/", 1)[1])
            calls.append("chunk-" + str(index))
            if index == 1 and not interrupted:
                interrupted = True
                raise requests.ConnectionError("interrupted")
            received.add(index)
            return Response({})
        def close(self):
            pass

    monkeypatch.setattr(requests, "Session", Session)
    failed = upload_booking_archives("http://server", project, [entry], "grant", first)
    assert failed["failed"] and received == {0}
    result = upload_booking_archives("http://server", project, [entry], "grant", second)
    assert result["uploaded"] == [uid] and not result["failed"]
    assert calls.count("chunk-0") == 1
    assert sum(url.endswith("/upload/init") for url in calls) == 1
