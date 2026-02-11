"""
:mod:`etlplus.file.csv` module.

Helpers for reading/writing Comma-Separated Values (CSV) files.

Notes
-----
- A CSV file is a plain text file that uses commas to separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain commas
        or special characters.
- Rule of thumb:
    - If the file follows the CSV specification, use this module for
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
from ._io import warn_deprecated_module_io
from ._io import write_delimited
from .base import DelimitedTextFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CsvFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class CsvFile(DelimitedTextFileHandlerABC):
    """
    Handler implementation for CSV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.CSV
    delimiter = ','

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return CSV content from *path*.

        Parameters
        ----------
        path : Path
            Path to the CSV file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the CSV file.
        """
        return self.read_rows(path, options=options)

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read delimited CSV rows from *path*.

        Parameters
        ----------
        path : Path
            Path to the CSV file on disk.
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

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to CSV at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the CSV file on disk.
        data : JSONData
            Data to write as CSV.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the CSV file.
        """
        rows = normalize_records(data, 'CSV')
        return self.write_rows(path, rows, options=options)

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write CSV *rows* to *path*.

        Parameters
        ----------
        path : Path
            Path to the CSV file on disk.
        rows : JSONList
            Rows to write.
        options : WriteOptions | None, optional
            Optional write parameters. Extra key ``delimiter`` can override
            :attr:`delimiter`.

        Returns
        -------
        int
            The number of rows written to the CSV file.
        """
        return write_delimited(
            path,
            rows,
            delimiter=self.delimiter_from_write_options(options),
            format_name='CSV',
        )


# SECTION: INTERNAL CONSTANTS =============================================== #

_CSV_HANDLER = CsvFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``CsvFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the CSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CSV file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _CSV_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``CsvFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the CSV file on disk.
    data : JSONData
        Data to write as CSV. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CSV file.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _CSV_HANDLER.write(coerce_path(path), data)
