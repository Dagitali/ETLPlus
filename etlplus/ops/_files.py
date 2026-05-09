"""
:mod:`etlplus.ops._files` module.

Shared resolved-file helpers for extract/load orchestration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import overload

from ..file import File
from ..file import FileFormat
from ..file._core import FileFormatArg
from ..utils._types import StrPath

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    '_ResolvedFile',
    # Functions
    'resolve_file',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class _ResolvedFile[FormatT]:
    """One resolved file handle plus its effective format."""

    # -- Instance Attributes -- #

    file: File
    file_format: FormatT


# SECTION: FUNCTIONS ======================================================== #


@overload
def resolve_file(
    file_path: StrPath,
    file_format: FileFormatArg,
    *,
    inferred_default: None = None,
    file_cls: type[File] = File,
) -> _ResolvedFile[FileFormat | None]: ...


@overload
def resolve_file(
    file_path: StrPath,
    file_format: FileFormatArg,
    *,
    inferred_default: FileFormat,
    file_cls: type[File] = File,
) -> _ResolvedFile[FileFormat]: ...


def resolve_file(
    file_path: StrPath,
    file_format: FileFormatArg,
    *,
    inferred_default: FileFormat | None = None,
    file_cls: type[File] = File,
) -> _ResolvedFile[FileFormat] | _ResolvedFile[FileFormat | None]:
    """
    Return one resolved file handle plus its effective format.

    Parameters
    ----------
    file_path : StrPath
        Local path or remote URI for the file.
    file_format : FileFormatArg
        Explicit format override. When omitted, use the file object's inferred
        format and optionally fall back to ``inferred_default``.
    inferred_default : FileFormat | None, optional
        Default format to use when inference returns ``None``.
    file_cls : type[File], optional
        File implementation to instantiate. This keeps extract/load call sites
        patchable in unit tests.

    Returns
    -------
    _ResolvedFile[FileFormat] | _ResolvedFile[FileFormat | None]
        Resolved file handle with the effective format.
    """
    if file_format is None:
        file = file_cls(file_path)
        resolved_format = (
            file.file_format if file.file_format is not None else inferred_default
        )
    else:
        resolved_format = FileFormat.coerce(file_format)
        file = file_cls(file_path, resolved_format)

    return _ResolvedFile(
        file=file,
        file_format=resolved_format,
    )
