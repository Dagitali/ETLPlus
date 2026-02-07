"""
:mod:`etlplus.file.stub` module.

Helpers for reading/writing intentionally unsupported (stubbed) formats.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from .base import FileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'StubFileHandlerABC',
    'StubFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class StubFileHandlerABC(FileHandlerABC):
    """
    Base class for placeholder formats that intentionally raise
    :class:`NotImplementedError`.
    """

    # -- Class Attributes -- #

    format: ClassVar[FileFormat]
    category: ClassVar[str] = 'placeholder_stub'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for placeholder reads.
        """
        _ = options
        return read(path, format_name=self.format.value.upper())

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for placeholder writes.
        """
        _ = options
        return write(path, data, format_name=self.format.value.upper())


class StubFile(StubFileHandlerABC):
    """Placeholder handler for STUB."""

    format = FileFormat.STUB


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
    format_name: str = 'Stubbed',
) -> JSONList:
    """
    Raise :class:`NotImplementedError` for stubbed reads.

    Parameters
    ----------
    path : StrPath
        Path to the stubbed file on disk.
    format_name : str
        Human-readable format name.

    Returns
    -------
    JSONList
        The list of dictionaries read from the stubbed file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    path = coerce_path(path)
    _ = path
    raise NotImplementedError(f'{format_name} read is not implemented yet')


def write(
    path: StrPath,
    data: JSONData,
    format_name: str = 'Stubbed',
) -> int:
    """
    Raise :class:`NotImplementedError` for stubbed writes.

    Parameters
    ----------
    path : StrPath
        Path to the stubbed file on disk.
    data : JSONData
        Data to write as stubbed file. Should be a list of dictionaries or a
        single dictionary.
    format_name : str
        Human-readable format name.

    Returns
    -------
    int
        The number of rows written to the stubbed file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    path = coerce_path(path)
    _ = path
    _ = data
    raise NotImplementedError(f'{format_name} write is not implemented yet')
