import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Protocol


class _TemporaryBinaryFile(Protocol):
    """Operations required while preparing an atomic-write temporary file."""

    def write(self, content: bytes) -> int: ...

    def flush(self) -> None: ...

    def fileno(self) -> int: ...


def _write_temporary_file(
    temporary_file: _TemporaryBinaryFile, temporary_path: Path, content: bytes, mode: int
) -> None:
    """Writes and synchronizes a temporary artifact before publication."""
    temporary_file.write(content)
    temporary_file.flush()
    os.fsync(temporary_file.fileno())
    os.chmod(temporary_path, mode)


@contextmanager
def _prepared_temporary_path(target: Path, content: bytes, mode: int) -> Iterator[Path]:
    """Yields a closed temporary file and removes it unless publication succeeds."""
    temporary_path: Path | None = None
    published = False
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=target.parent,
            prefix=".dbf-",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            _write_temporary_file(temporary_file, temporary_path, content, mode)

        yield temporary_path
        published = True
    finally:
        if temporary_path is not None and not published:
            temporary_path.unlink(missing_ok=True)


def atomic_write_bytes(target: Path, content: bytes, mode: int) -> None:
    """Atomically replaces ``target`` with the requested bytes and file mode."""
    target.parent.mkdir(parents=True, exist_ok=True)
    with _prepared_temporary_path(target, content, mode) as temporary_path:
        os.replace(temporary_path, target)
