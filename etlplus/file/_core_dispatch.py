"""
:mod:`etlplus.file._core_dispatch` module.

Shared helpers that route typed payloads through :mod:`etlplus.file.core`.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ..types import JSONData
from .enums import FileFormat

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _safe_tmp_path(
    *,
    tmpdir: str,
    filename: str,
    keep_dirs: bool,
) -> Path:
    """
    Resolve *filename* under *tmpdir* and block directory traversal.
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
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = _safe_tmp_path(
            tmpdir=tmpdir,
            filename=filename,
            keep_dirs=False,
        )
        tmp_path.write_bytes(payload)
        from .core import File

        return File(tmp_path, fmt).read()


def write_payload_with_core(
    *,
    fmt: FileFormat,
    data: JSONData,
    filename: str,
) -> tuple[int, bytes]:
    """
    Serialize data by writing to a temporary typed file and returning bytes.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = _safe_tmp_path(
            tmpdir=tmpdir,
            filename=filename,
            keep_dirs=True,
        )
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        from .core import File

        count = File(tmp_path, fmt).write(data)
        return count, tmp_path.read_bytes()
