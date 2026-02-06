"""
:mod:`etlplus.file.tsv` module.

Helpers for reading/writing Tab-Separated Values (TSV) files.

Notes
-----
- A TSV file is a plain text file that uses the tab character (``\t``) to
    separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain tabs
        or special characters.
- Rule of thumb:
    - If the file follows the TSV specification, use this module for
        reading and writing.
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
    'TsvFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class TsvFile(DelimitedTextFileHandlerABC):
    """
    Handler implementation for TSV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.TSV
    delimiter = '\t'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read TSV content from *path*.

        Parameters
        ----------
        path : Path
            Path to the TSV file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the TSV file.
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
        Write *data* to TSV at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the TSV file on disk.
        data : JSONData
            Data to write as TSV.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the TSV file.
        """
        rows = normalize_records(data, 'TSV')
        return self.write_rows(path, rows, options=options)

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read delimited TSV rows from *path*.

        Parameters
        ----------
        path : Path
            Path to the TSV file on disk.
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
        Write TSV *rows* to *path*.

        Parameters
        ----------
        path : Path
            Path to the TSV file on disk.
        rows : JSONList
            Rows to write.
        options : WriteOptions | None, optional
            Optional write parameters. Extra key ``delimiter`` can override
            :attr:`delimiter`.

        Returns
        -------
        int
            The number of rows written to the TSV file.
        """
        delimiter = self.delimiter
        if options is not None:
            delimiter = str(options.extras.get('delimiter', delimiter))
        return write_delimited(
            path,
            rows,
            delimiter=delimiter,
            format_name='TSV',
        )


# SECTION: INTERNAL CONSTANTS ============================================== #


_TSV_HANDLER = TsvFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read TSV content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the TSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the TSV file.
    """
    return _TSV_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to TSV at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the TSV file on disk.
    data : JSONData
        Data to write as TSV. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the TSV file.
    """
    return _TSV_HANDLER.write(coerce_path(path), data)
