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
from typing import TYPE_CHECKING
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._io import warn_deprecated_module_io
from .base import ColumnarFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

if TYPE_CHECKING:
    import pyarrow

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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return ORC content from *path*.

        Parameters
        ----------
        path : Path
            Path to the ORC file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the ORC file.
        """
        table = self.read_table(path, options=options)
        return self.table_to_records(table)

    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> object:
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
        object
            Pandas DataFrame-like object.
        """
        _ = options
        get_dependency('pyarrow', format_name='ORC')
        pandas = get_pandas('ORC')
        return pandas.read_orc(path)

    def records_to_table(
        self,
        data: JSONData,
    ) -> object:
        """
        Convert row-oriented records into an ORC table object.

        Parameters
        ----------
        data : JSONData
            Records to convert.

        Returns
        -------
        object
            Pandas DataFrame-like object.
        """
        records = normalize_records(data, 'ORC')
        get_dependency('pyarrow', format_name='ORC')
        pandas = get_pandas('ORC')
        return pandas.DataFrame.from_records(records)

    def table_to_records(
        self,
        table: pyarrow.Table,
    ) -> JSONList:
        """
        Convert an ORC table object into row-oriented records.

        Parameters
        ----------
        table : pyarrow.Table
            Pandas DataFrame-like object.

        Returns
        -------
        JSONList
            Parsed records.
        """
        return cast(JSONList, table.to_dict(orient='records'))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to ORC at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the ORC file on disk.
        data : JSONData
            Data to write.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.
        """
        records = normalize_records(data, 'ORC')
        if not records:
            return 0

        ensure_parent_dir(path)
        table = self.records_to_table(records)
        self.write_table(path, table, options=options)
        return len(records)

    def write_table(
        self,
        path: Path,
        table: pyarrow.Table,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write an ORC table object to *path*.

        Parameters
        ----------
        path : Path
            Path to the ORC file on disk.
        table : pyarrow.Table
            Pandas DataFrame-like object.
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
    Read and return ORC content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the ORC file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ORC file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _ORC_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to ORC at *path* and return record count.

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
    warn_deprecated_module_io(__name__, 'write')
    return _ORC_HANDLER.write(coerce_path(path), data)
