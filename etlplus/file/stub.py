"""
:mod:`etlplus.file.stub` module.

Helpers for reading/writing intentionally unsupported (stubbed) formats.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import ClassVar
from typing import Never

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
        _ = path
        _ = options
        _raise_not_implemented('read', format_name=self.format.value.upper())

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
        _ = path
        _ = data
        _ = options
        _raise_not_implemented('write', format_name=self.format.value.upper())


class StubFile(StubFileHandlerABC):
    """Placeholder handler for STUB."""

    format = FileFormat.STUB


# SECTION: INTERNAL CONSTANTS =============================================== #


_STUB_HANDLER = StubFile()


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _raise_not_implemented(
    operation: str,
    *,
    format_name: str,
) -> Never:
    """Raise standardized placeholder NotImplementedError messages."""
    raise NotImplementedError(
        f'{format_name} {operation} is not implemented yet',
    )


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
    format_name: str = 'Stubbed',
) -> JSONList:
    """
    Deprecated shim that delegates to :class:`StubFile`.

    Parameters
    ----------
    path : StrPath
        Path to the stubbed file on disk.
    format_name : str
        Deprecated override for human-readable format name.

    Returns
    -------
    JSONList
        The list of dictionaries read from the stubbed file.

    Raises
    ------
    DeprecationWarning
        Always, since module-level wrappers are deprecated.
    NotImplementedError
        Always, since STUB is a placeholder implementation.
    """
    path = coerce_path(path)
    warnings.warn(
        'etlplus.file.stub.read() is deprecated; use StubFile().read() '
        'or other handler instance methods.',
        DeprecationWarning,
        stacklevel=2,
    )
    if format_name != 'Stubbed':
        _raise_not_implemented('read', format_name=format_name)
    return _STUB_HANDLER.read(path)


def write(
    path: StrPath,
    data: JSONData,
    format_name: str = 'Stubbed',
) -> int:
    """
    Deprecated shim that delegates to :class:`StubFile`.

    Parameters
    ----------
    path : StrPath
        Path to the stubbed file on disk.
    data : JSONData
        Data to write as stubbed file. Should be a list of dictionaries or a
        single dictionary.
    format_name : str
        Deprecated override for human-readable format name.

    Returns
    -------
    int
        The number of rows written to the stubbed file.

    Raises
    ------
    DeprecationWarning
        Always, since module-level wrappers are deprecated.
    NotImplementedError
        Always, since STUB is a placeholder implementation.
    """
    path = coerce_path(path)
    warnings.warn(
        'etlplus.file.stub.write() is deprecated; use StubFile().write() '
        'or other handler instance methods.',
        DeprecationWarning,
        stacklevel=2,
    )
    if format_name != 'Stubbed':
        _raise_not_implemented('write', format_name=format_name)
    return _STUB_HANDLER.write(path, data)
