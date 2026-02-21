"""
:mod:`etlplus.file._archive` module.

Shared helpers for archive-wrapper format inference and validation.
"""

from __future__ import annotations

from typing import cast

from ..types import StrPath
from .enums import CompressionFormat
from .enums import FileFormat
from .enums import infer_file_format_and_compression

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'infer_archive_payload_format',
]


# SECTION: FUNCTIONS ======================================================== #


def infer_archive_payload_format(
    source: StrPath,
    *,
    allowed_compressions: tuple[CompressionFormat | None, ...],
    compression_error: str,
    require_format: bool = True,
) -> FileFormat | None:
    """
    Infer payload format from *source* while enforcing allowed compression.

    Parameters
    ----------
    source : StrPath
        Path or filename used for format/compression inference.
    allowed_compressions : tuple[CompressionFormat | None, ...]
        Compression variants accepted for this inference.
    compression_error : str
        Error message raised when compression is not allowed.
    require_format : bool, optional
        Whether a concrete payload format must be inferred.

    Returns
    -------
    FileFormat | None
        Inferred payload format when available.

    Raises
    ------
    ValueError
        If compression is not allowed or the format is required but missing.
    """
    fmt, compression = infer_file_format_and_compression(source)
    if compression not in allowed_compressions:
        raise ValueError(compression_error)
    if fmt is None and require_format:
        raise ValueError(
            f'Cannot infer file format from compressed file {source!r}',
        )
    return cast(FileFormat | None, fmt)
