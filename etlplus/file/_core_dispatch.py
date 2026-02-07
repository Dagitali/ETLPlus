"""
:mod:`etlplus.file._core_dispatch` module.

Shared helpers that route typed payloads through :mod:`etlplus.file.core`.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ..types import JSONData
from .enums import FileFormat

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
        tmp_path = Path(tmpdir) / Path(filename).name
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
        tmp_path = Path(tmpdir) / filename
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        from .core import File

        count = File(tmp_path, fmt).write(data)
        return count, tmp_path.read_bytes()
