"""
:mod:`etlplus.file.psv` module.

Helpers for reading/writing Pipe-Separated Values (PSV) files.

Notes
-----
- A PSV file is a plain text file that uses the pipe character (`|`) to
    separate values.
- Common cases:
    - Each line in the file represents a single record.
    - The first line often contains headers that define the column names.
    - Values may be enclosed in quotes, especially if they contain pipes
        or special characters.
- Rule of thumb:
    - If the file follows the PSV specification, use this module for
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
    'PsvFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class PsvFile(DelimitedTextFileHandlerABC):
    """
    Handler implementation for PSV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PSV
    delimiter = '|'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return PSV content from *path*.

        Parameters
        ----------
        path : Path
            Path to the PSV file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the PSV file.
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
        Write *data* to PSV at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the PSV file on disk.
        data : JSONData
            Data to write as PSV file.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the PSV file.
        """
        rows = normalize_records(data, 'PSV')
        return self.write_rows(path, rows, options=options)

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read delimited PSV rows from *path*.

        Parameters
        ----------
        path : Path
            Path to the PSV file on disk.
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
        Write PSV *rows* to *path*.

        Parameters
        ----------
        path : Path
            Path to the PSV file on disk.
        rows : JSONList
            Rows to write.
        options : WriteOptions | None, optional
            Optional write parameters. Extra key ``delimiter`` can override
            :attr:`delimiter`.

        Returns
        -------
        int
            The number of rows written to the PSV file.
        """
        delimiter = self.delimiter
        if options is not None:
            delimiter = str(options.extras.get('delimiter', delimiter))
        return write_delimited(
            path,
            rows,
            delimiter=delimiter,
            format_name='PSV',
        )


# SECTION: INTERNAL CONSTANTS ============================================== #


_PSV_HANDLER = PsvFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return PSV content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the PSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PSV file.
    """
    return _PSV_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to PSV at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the PSV file on disk.
    data : JSONData
        Data to write as PSV file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PSV file.
    """
    return _PSV_HANDLER.write(coerce_path(path), data)
