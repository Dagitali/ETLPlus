"""
:mod:`etlplus.file.sylk` module.

Stub helpers for reading/writing Symbolic Link (SYLK) data files (not
implemented yet).

Notes
-----
- A SYLK file is a text-based file format used to represent spreadsheet
    data, including cell values, formulas, and formatting.
- Common cases:
    - Storing spreadsheet data in a human-readable format.
    - Exchanging data between different spreadsheet applications.
- Rule of thumb:
    - If you need to work with SYLK files, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from . import stub
from ._io import coerce_path
from .base import FileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SylkFile',
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


class SylkFile(FileHandlerABC):
    """
    Handler implementation for SYLK files.
    """

    format = FileFormat.SYLK
    category = 'statistical_dataset'

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read SYLK content from *path*.

        Parameters
        ----------
        path : Path
            Path to the SYLK file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the SYLK file.
        """
        _ = options
        return stub.read(path, format_name='SYLK')

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to SYLK file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the SYLK file on disk.
        data : JSONData
            Data to write as SYLK file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the SYLK file.
        """
        _ = options
        return stub.write(path, data, format_name='SYLK')


# SECTION: INTERNAL CONSTANTS ============================================== #


_SYLK_HANDLER = SylkFile()


def read(
    path: StrPath,
) -> JSONList:
    """
    Read SYLK content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the SYLK file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SYLK file.
    """
    return _SYLK_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to SYLK file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the SYLK file on disk.
    data : JSONData
        Data to write as SYLK file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SYLK file.
    """
    return _SYLK_HANDLER.write(coerce_path(path), data)
