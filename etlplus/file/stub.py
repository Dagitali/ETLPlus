"""
:mod:`etlplus.file.stub` module.

Helpers for reading/writing intentionally unsupported (stubbed) formats.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar
from typing import Never

from ..utils._types import JSONData
from ..utils._types import JSONList
from ._enums import FileFormat
from .base import FileHandlerABC
from .base import ReadOptions
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'StubFileHandlerABC',
    'StubFile',
]


# SECTION: CLASSES ========================================================== #


class StubFileHandlerABC(FileHandlerABC):
    """
    Base class for placeholder formats raising :class:`NotImplementedError`.
    """

    # -- Class Attributes -- #

    format: ClassVar[FileFormat]
    category: ClassVar[str] = 'placeholder_stub'

    # -- Internal Instance Methods -- #

    def _stub_path(
        self,
    ) -> Path:
        """Build a deterministic path used by non-path stub helper methods."""
        return Path(f'ignored.{self.format.value}')

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """Raise :class:`NotImplementedError` for placeholder reads."""
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
        """Raise :class:`NotImplementedError` for placeholder writes."""
        _ = path
        _ = data
        _ = options
        _raise_not_implemented('write', format_name=self.format.value.upper())


class StubFile(StubFileHandlerABC):
    """Placeholder handler for STUB."""

    format = FileFormat.STUB


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _raise_not_implemented(
    operation: str,
    *,
    format_name: str,
) -> Never:
    """
    Raise standardized placeholder NotImplementedError messages.

    Parameters
    ----------
    operation : str
        The operation being attempted (e.g., 'read' or 'write').
    format_name : str
        Human-readable format name.

    Returns
    -------
    Never

    Raises
    ------
    NotImplementedError
        Always raised with a message indicating the unsupported operation and
        format.
    """
    raise NotImplementedError(
        f'{format_name} {operation} is not implemented yet',
    )
