import os
import stat
import tempfile
from pathlib import Path
from typing import BinaryIO

import pytest

from databricks_dbt_factory import file_io


def test_atomic_write_bytes_creates_parent_and_applies_content_and_mode(tmp_path: Path):
    target = tmp_path / "nested" / "artifact.bin"

    file_io.atomic_write_bytes(target, b"new content\x00", 0o640)

    assert target.read_bytes() == b"new content\x00"
    if os.name != "nt":
        assert stat.S_IMODE(target.stat().st_mode) == 0o640
    assert list(target.parent.iterdir()) == [target]


def test_atomic_write_bytes_supports_a_near_name_max_target(tmp_path: Path):
    try:
        name_max = os.pathconf(tmp_path, "PC_NAME_MAX")
    except (AttributeError, OSError, ValueError) as error:
        pytest.skip(f"filesystem NAME_MAX is unavailable: {error}")
    if name_max < 250:
        pytest.skip(f"filesystem NAME_MAX {name_max} is too small for this regression")

    target = tmp_path / f"{'a' * 246}.bin"
    assert len(os.fsencode(target.name)) == 250

    file_io.atomic_write_bytes(target, b"near name max", 0o600)

    assert target.read_bytes() == b"near name max"
    assert list(tmp_path.iterdir()) == [target]


def test_atomic_write_bytes_closes_hidden_same_directory_temp_before_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    target = tmp_path / "artifact.bin"
    events: list[str] = []
    temporary_files: list[RecordingTemporaryFile] = []
    real_named_temporary_file = tempfile.NamedTemporaryFile
    real_fsync = os.fsync
    real_chmod = os.chmod
    real_replace = os.replace

    def recording_named_temporary_file(*args, **kwargs):
        wrapped = real_named_temporary_file(*args, **kwargs)  # pylint: disable=consider-using-with
        temporary_file = RecordingTemporaryFile(wrapped, events)
        temporary_files.append(temporary_file)
        return temporary_file

    def recording_fsync(file_descriptor: int):
        events.append("fsync")
        real_fsync(file_descriptor)

    def recording_chmod(path: os.PathLike[str] | str, mode: int):
        events.append("chmod")
        real_chmod(path, mode)

    def recording_replace(source: os.PathLike[str] | str, destination: os.PathLike[str] | str):
        source_path = Path(source)
        assert source_path.parent == target.parent
        assert source_path.name.startswith(".dbf-")
        assert temporary_files[0].closed
        events.append("replace")
        real_replace(source, destination)

    monkeypatch.setattr(file_io.tempfile, "NamedTemporaryFile", recording_named_temporary_file)
    monkeypatch.setattr(file_io.os, "fsync", recording_fsync)
    monkeypatch.setattr(file_io.os, "chmod", recording_chmod)
    monkeypatch.setattr(file_io.os, "replace", recording_replace)

    file_io.atomic_write_bytes(target, b"content", 0o600)

    assert events == ["write", "flush", "fsync", "chmod", "close", "replace"]


def test_atomic_write_bytes_does_not_unlink_a_reused_temp_path_after_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    target = tmp_path / "artifact.bin"
    replacement_path: Path | None = None
    real_replace = os.replace

    def replace_and_reuse_path(source: os.PathLike[str] | str, destination: os.PathLike[str] | str):
        nonlocal replacement_path
        replacement_path = Path(source)
        real_replace(source, destination)
        replacement_path.write_bytes(b"new owner")

    monkeypatch.setattr(file_io.os, "replace", replace_and_reuse_path)

    file_io.atomic_write_bytes(target, b"content", 0o600)

    assert target.read_bytes() == b"content"
    assert replacement_path is not None
    assert replacement_path.read_bytes() == b"new owner"


@pytest.mark.parametrize("failure_point", ["write", "fsync", "chmod", "replace"])
def test_atomic_write_bytes_cleans_temp_and_preserves_target_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_point: str
):
    target = tmp_path / "artifact.bin"
    target.write_bytes(b"original")
    real_named_temporary_file = tempfile.NamedTemporaryFile

    if failure_point == "write":

        def failing_named_temporary_file(*args, **kwargs):
            wrapped = real_named_temporary_file(*args, **kwargs)  # pylint: disable=consider-using-with
            return WriteFailingTemporaryFile(wrapped)

        monkeypatch.setattr(file_io.tempfile, "NamedTemporaryFile", failing_named_temporary_file)
    else:

        def fail(*args, **kwargs):
            raise OSError(f"{failure_point} failed")

        monkeypatch.setattr(file_io.os, failure_point, fail)

    with pytest.raises(OSError, match=rf"{failure_point} failed"):
        file_io.atomic_write_bytes(target, b"replacement", 0o640)

    assert target.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [target]


class RecordingTemporaryFile:
    def __init__(self, wrapped: BinaryIO, events: list[str]):
        self._wrapped = wrapped
        self._events = events

    def __enter__(self):
        self._wrapped.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        result = self._wrapped.__exit__(exc_type, exc_value, traceback)
        self._events.append("close")
        return result

    def write(self, content: bytes):
        self._events.append("write")
        return self._wrapped.write(content)

    def flush(self):
        self._events.append("flush")
        return self._wrapped.flush()

    def fileno(self):
        return self._wrapped.fileno()

    @property
    def name(self):
        return self._wrapped.name

    @property
    def closed(self):
        return self._wrapped.closed


class WriteFailingTemporaryFile:
    def __init__(self, wrapped: BinaryIO):
        self._wrapped = wrapped

    def __enter__(self):
        self._wrapped.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self._wrapped.__exit__(exc_type, exc_value, traceback)

    def write(self, content: bytes):
        raise OSError("write failed")

    @property
    def name(self):
        return self._wrapped.name
