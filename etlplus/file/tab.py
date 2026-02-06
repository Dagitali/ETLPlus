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
from ._io import coerce_path
from ._io import normalize_records
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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read TAB content from *path*.

        Parameters
        ----------
        path : Path
            Path to the TAB file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the TAB file.
        """
        return self.read_rows(path, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to TAB file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the TAB file on disk.
        data : JSONData
            Data to write as TAB file.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the TAB file.
        """
        rows = normalize_records(data, 'TAB')
        return self.write_rows(path, rows, options=options)

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
        delimiter = self.delimiter
        if options is not None:
            delimiter = str(options.extras.get('delimiter', delimiter))
        return read_delimited(path, delimiter=delimiter)

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
        delimiter = self.delimiter
        if options is not None:
            delimiter = str(options.extras.get('delimiter', delimiter))
        return write_delimited(
            path,
            rows,
            delimiter=delimiter,
            format_name='TAB',
        )


# SECTION: INTERNAL CONSTANTS ============================================== #


_TAB_HANDLER = TabFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read TAB content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the TAB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the TAB file.
    """
    return _TAB_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to TAB file at *path* and return record count.

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
    return _TAB_HANDLER.write(coerce_path(path), data)
