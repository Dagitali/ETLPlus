"""
:mod:`etlplus.file.cfg` module.

Stub helpers for reading/writing config (CFG) files (not implemented yet).

Notes
-----
- A CFG file is a configuration file that may use various syntaxes, such as
    INI, YAML, or custom formats.
- Common cases:
    - INI-style key-value pairs with sections (such as in Python ecosystems,
        using ``configparser``).
    - YAML-like structures with indentation.
    - Custom formats specific to certain applications.
- Rule of thumb:
    - If the file follows a standard format like INI or YAML, consider using
        dedicated parsers.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat
from .stub import StubFileHandlerABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CfgFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class CfgFile(StubFileHandlerABC):
    """
    Stub handler implementation for CFG files.
    """

    format = FileFormat.CFG

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        return super().read(path, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        return super().write(path, data, options=options)


# SECTION: INTERNAL CONSTANTS ============================================== #


_CFG_HANDLER = CfgFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read CFG content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the CFG file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CFG file.
    """
    return _CFG_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to CFG file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the CFG file on disk.
    data : JSONData
        Data to write as CFG file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CFG file.
    """
    return _CFG_HANDLER.write(coerce_path(path), data)
