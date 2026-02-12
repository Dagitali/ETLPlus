"""
:mod:`etlplus.file.tab` module.

Helpers for reading/writing "tab"-formatted (TAB) files.

Notes
-----
- A TAB file is not necessarily a TSV file when tabs aren’t actually the
    delimiter that defines the fields, even if the text looks column-aligned.
- Common cases:
    - Fixed-width text (FWF) that uses tabs for alignment.
    - Mixed whitespace (tabs + spaces) as “pretty printing”.
    - Tabs embedded inside quoted fields (or unescaped tabs in free text).
    - Header/metadata lines or multi-line records that break TSV assumptions.
    - Not actually tab-delimited despite the name.
- Rule of thumb:
    - This implementation treats TAB as tab-delimited text.
    - If the file has fixed-width fields, use :mod:`etlplus.file.fwf`.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import read_delimited
from ._io import write_delimited
from .base import DelimitedTextFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TabFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class TabFile(DelimitedTextFileHandlerABC):
    """
    Handler implementation for TAB files.
    """

    # -- Class Attributes -- #

    format = FileFormat.TAB
    delimiter = '\t'

    # -- Instance Methods -- #

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read delimited TAB rows from *path*.

        Parameters
        ----------
        path : Path
            Path to the TAB file on disk.
        options : ReadOptions | None, optional
            Optional read parameters. Extra key ``delimiter`` can override
            :attr:`delimiter`.

        Returns
        -------
        JSONList
            Parsed rows.
        """
        return read_delimited(
            path,
            delimiter=self.delimiter_from_read_options(options),
        )

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write TAB *rows* to *path*.

        Parameters
        ----------
        path : Path
            Path to the TAB file on disk.
        rows : JSONList
            Rows to write.
        options : WriteOptions | None, optional
            Optional write parameters. Extra key ``delimiter`` can override
            :attr:`delimiter`.

        Returns
        -------
        int
            The number of rows written to the TAB file.
        """
        return write_delimited(
            path,
            rows,
            delimiter=self.delimiter_from_write_options(options),
            format_name='TAB',
        )


# SECTION: INTERNAL CONSTANTS =============================================== #

_TAB_HANDLER = TabFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``TabFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the TAB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the TAB file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _TAB_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``TabFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the TAB file on disk.
    data : JSONData
        Data to write as TAB file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the TAB file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _TAB_HANDLER.write,
    )
