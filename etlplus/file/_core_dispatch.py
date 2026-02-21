"""
:mod:`etlplus.file._core_dispatch` module.

Shared helpers that route typed payloads through :mod:`etlplus.file.core`.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..types import JSONData
from .enums import FileFormat

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _core_file(
    path: Path,
    fmt: FileFormat,
) -> Any:
    """
    Build one ``File`` instance lazily to preserve monkeypatch-friendly tests.

    Parameters
    ----------
    path : Path
        Path to the file on disk.
    fmt : FileFormat
        File format to read/write.

    Returns
    -------
    Any
        Core file handler instance for *fmt*.
    """
    from .core import File

    return File(path, fmt)


def _safe_tmp_path(
    *,
    tmpdir: str,
    filename: str,
    keep_dirs: bool,
) -> Path:
    """
    Resolve *filename* under *tmpdir* and block directory traversal.

    Parameters
    ----------
    tmpdir : str
        Path to the temporary directory.
    filename : str
        Name of the file.
    keep_dirs : bool
        Whether to keep directory structure.

    Returns
    -------
    Path
        Resolved path within the temporary directory.

    Raises
    ------
    ValueError
        If *filename* is absolute or escapes the temporary directory.
    """
    root = Path(tmpdir).resolve()
    raw = Path(filename)
    if raw.is_absolute():
        raise ValueError('filename must be a relative path')
    relative = raw if keep_dirs else Path(raw.name)
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as err:
        raise ValueError(
            'filename must not escape the temporary directory',
        ) from err
    return resolved


@contextmanager
def _temporary_dispatch_path(
    *,
    filename: str,
    keep_dirs: bool,
) -> Iterator[Path]:
    """
    Yield a safe temporary path for one core-dispatch read/write operation.

    Parameters
    ----------
    filename : str
        Name of the file.
    keep_dirs : bool
        Whether to keep directory structure.

    Yields
    ------
    Path
        Safe temporary path within the temporary directory.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = _safe_tmp_path(
            tmpdir=tmpdir,
            filename=filename,
            keep_dirs=keep_dirs,
        )
        if keep_dirs:
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
        yield tmp_path


# SECTION: FUNCTIONS ======================================================== #


def read_payload_with_core(
    *,
    fmt: FileFormat,
    payload: bytes,
    filename: str,
) -> JSONData:
    """
    Parse payload bytes by materializing a temporary typed file and reading it.
    """
    with _temporary_dispatch_path(
        filename=filename,
        keep_dirs=False,
    ) as tmp_path:
        tmp_path.write_bytes(payload)
        return _core_file(tmp_path, fmt).read()


def write_payload_with_core(
    *,
    fmt: FileFormat,
    data: JSONData,
    filename: str,
) -> tuple[int, bytes]:
    """
    Serialize data by writing to a temporary typed file and returning bytes.
    """
    with _temporary_dispatch_path(
        filename=filename,
        keep_dirs=True,
    ) as tmp_path:
        count = _core_file(tmp_path, fmt).write(data)
        return count, tmp_path.read_bytes()
