"""
:mod:`etlplus.file.orc` module.

Helpers for reading/writing Optimized Row Columnar (ORC) files.

Notes
-----
- An ORC file is a columnar storage file format optimized for Big Data
    processing.
- Common cases:
    - Efficient storage and retrieval of large datasets.
    - Integration with big data frameworks like Apache Hive and Apache Spark.
    - Compression and performance optimization for analytical queries.
- Rule of thumb:
    - If the file follows the ORC specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import normalize_records
from ._io import records_from_table
from .base import ColumnarFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'OrcFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class OrcFile(ColumnarFileHandlerABC):
    """
    Handler implementation for ORC files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ORC
    engine_name = 'pandas'

    # -- Instance Methods -- #

    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read an ORC table object from *path*.

        Parameters
        ----------
        path : Path
            Path to the ORC file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        Any
            Columnar table object.
        """
        _ = options
        get_dependency('pyarrow', format_name='ORC')
        pandas = get_pandas('ORC')
        return pandas.read_orc(path)

    def records_to_table(
        self,
        data: JSONData,
    ) -> Any:
        """
        Convert row-oriented records into an ORC table object.

        Parameters
        ----------
        data : JSONData
            Records to convert.

        Returns
        -------
        Any
            Columnar table object.
        """
        records = normalize_records(data, 'ORC')
        get_dependency('pyarrow', format_name='ORC')
        pandas = get_pandas('ORC')
        return pandas.DataFrame.from_records(records)

    def table_to_records(
        self,
        table: Any,
    ) -> JSONList:
        """
        Convert an ORC table object into row-oriented records.

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
        Write an ORC table object to *path*.

        Parameters
        ----------
        path : Path
            Path to the ORC file on disk.
        table : Any
            Columnar table object.
        options : WriteOptions | None, optional
            Optional write parameters.
        """
        _ = options
        table.to_orc(path, index=False)


# SECTION: INTERNAL CONSTANTS =============================================== #

_ORC_HANDLER = OrcFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``OrcFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ORC file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ORC file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _ORC_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``OrcFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ORC file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _ORC_HANDLER.write,
    )
