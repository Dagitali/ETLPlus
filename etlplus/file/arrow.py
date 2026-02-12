"""
:mod:`etlplus.file.arrow` module.

Helpers for reading/writing Apache Arrow (ARROW) files.

Notes
-----
- An ARROW file is a binary file format designed for efficient
    columnar data storage and processing.
- Common cases:
    - High-performance data analytics.
    - Interoperability between different data processing systems.
    - In-memory data representation for fast computations.
- Rule of thumb:
    - If the file follows the Apache Arrow specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pyarrow  # type: ignore[import]

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import ensure_parent_dir
from ._io import normalize_records
from .base import ColumnarFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ArrowFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class ArrowFile(ColumnarFileHandlerABC):
    """
    Handler implementation for Arrow IPC files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ARROW
    engine_name = 'pyarrow'

    # -- Instance Methods -- #

    def records_to_table(
        self,
        data: JSONData,
    ) -> pyarrow.Table:
        """
        Convert row-oriented records into an Arrow table object.

        Parameters
        ----------
        data : JSONData
            Records to convert.

        Returns
        -------
        pyarrow.Table
            PyArrow table object.
        """
        records = normalize_records(data, 'ARROW')
        pyarrow_mod = get_dependency('pyarrow', format_name='ARROW')
        return pyarrow_mod.Table.from_pylist(records)

    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> object:
        """
        Read an Arrow table object from *path*.

        Parameters
        ----------
        path : Path
            Path to the Arrow file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        object
            PyArrow table object.
        """
        _ = options
        pyarrow_mod = get_dependency('pyarrow', format_name='ARROW')
        with pyarrow_mod.memory_map(str(path), 'r') as source:
            reader = pyarrow_mod.ipc.open_file(source)
            return reader.read_all()

    def table_to_records(
        self,
        table: pyarrow.Table,
    ) -> JSONList:
        """
        Convert an Arrow table object into row-oriented records.

        Parameters
        ----------
        table : pyarrow.Table
            PyArrow table object.

        Returns
        -------
        JSONList
            Parsed records.
        """
        return cast(JSONList, table.to_pylist())

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to ARROW at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the ARROW file on disk.
        data : JSONData
            Data to write as ARROW.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the ARROW file.
        """
        records = normalize_records(data, 'ARROW')
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
        Write an Arrow table object to *path*.

        Parameters
        ----------
        path : Path
            Path to the Arrow file on disk.
        table : pyarrow.Table
            PyArrow table object.
        options : WriteOptions | None, optional
            Optional write parameters.
        """
        _ = options
        pyarrow_mod = get_dependency('pyarrow', format_name='ARROW')
        with pyarrow_mod.OSFile(str(path), 'wb') as sink:
            with pyarrow_mod.ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)


# SECTION: INTERNAL CONSTANTS =============================================== #

_ARROW_HANDLER = ArrowFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``ArrowFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the Apache Arrow file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the Apache Arrow file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _ARROW_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``ArrowFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ARROW file on disk.
    data : JSONData
        Data to write as ARROW. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the ARROW file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _ARROW_HANDLER.write,
    )
