"""
:mod:`etlplus.file.feather` module.

Helpers for reading/writing Apache Arrow Feather (FEATHER) files.

Notes
-----
- A FEATHER file is a binary file format designed for efficient
    on-disk storage of data frames, built on top of Apache Arrow.
- Common cases:
    - Fast read/write operations for data frames.
    - Interoperability between different data analysis tools.
    - Storage of large datasets with efficient compression.
- Rule of thumb:
    - If the file follows the Apache Arrow Feather specification, use this
        module for reading and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..types import JSONData
from ..types import JSONList
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import make_deprecated_module_io
from ._io import normalize_records
from ._io import records_from_table
from .base import ColumnarFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FeatherFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _pandas() -> Any:
    """Return the optional pandas module for Feather operations."""
    return get_pandas('Feather')


def _pyarrow() -> Any:
    """Return the optional pyarrow module."""
    return get_dependency('pyarrow', format_name='Feather')


# SECTION: CLASSES ========================================================== #


class FeatherFile(ColumnarFileHandlerABC):
    """
    Handler implementation for Feather files.
    """

    # -- Class Attributes -- #

    format = FileFormat.FEATHER
    engine_name = 'pandas'

    # -- Instance Methods -- #

    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read a Feather table object from *path*.

        Parameters
        ----------
        path : Path
            Path to the Feather file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        Any
            Columnar table object.
        """
        _ = options
        _ = _pyarrow()
        pandas = _pandas()
        return pandas.read_feather(path)

    def records_to_table(
        self,
        data: JSONData,
    ) -> Any:
        """
        Convert row-oriented records into a Feather table object.

        Parameters
        ----------
        data : JSONData
            Records to convert.

        Returns
        -------
        Any
            Columnar table object.
        """
        records = normalize_records(data, 'Feather')
        _ = _pyarrow()
        pandas = _pandas()
        return pandas.DataFrame.from_records(records)

    def table_to_records(
        self,
        table: Any,
    ) -> JSONList:
        """
        Convert a Feather table object into row-oriented records.

        Parameters
        ----------
        table : Any
            Columnar table object.

        Returns
        -------
        JSONList
            Parsed records.
        """
        return records_from_table(table)

    def write_table(
        self,
        path: Path,
        table: Any,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write a Feather table object to *path*.

        Parameters
        ----------
        path : Path
            Path to the Feather file on disk.
        table : Any
            Columnar table object.
        options : WriteOptions | None, optional
            Optional write parameters.
        """
        _ = options
        table.to_feather(path)


# SECTION: INTERNAL CONSTANTS =============================================== #

_FEATHER_HANDLER = FeatherFile()


# SECTION: FUNCTIONS ======================================================== #


read, write = make_deprecated_module_io(__name__, _FEATHER_HANDLER)
