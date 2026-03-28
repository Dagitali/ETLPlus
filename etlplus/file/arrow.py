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
from typing import Any
from typing import cast

from ..utils.types import JSONData
from ..utils.types import JSONList
from ._enums import FileFormat
from ._imports import get_dependency
from ._io import normalize_records
from .base import ColumnarFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ArrowFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _pyarrow() -> Any:
    """Return the required pyarrow module."""
    return get_dependency(
        'pyarrow',
        format_name='ARROW',
        required=True,
    )


# SECTION: CLASSES ========================================================== #


class ArrowFile(ColumnarFileHandlerABC):
    """Handler implementation for Arrow IPC files."""

    # -- Class Attributes -- #

    format = FileFormat.ARROW
    engine_name = 'pyarrow'

    # -- Instance Methods -- #

    def records_to_table(
        self,
        data: JSONData,
    ) -> Any:
        """
        Convert row-oriented records into an Arrow table object.

        Parameters
        ----------
        data : JSONData
            Records to convert.

        Returns
        -------
        Any
            Columnar table object.
        """
        records = normalize_records(data, self.format_name)
        pyarrow_mod = _pyarrow()
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
        pyarrow_mod = _pyarrow()
        with pyarrow_mod.memory_map(str(path), 'r') as source:
            reader = pyarrow_mod.ipc.open_file(source)
            return reader.read_all()

    def table_to_records(
        self,
        table: Any,
    ) -> JSONList:
        """
        Convert an Arrow table object into row-oriented records.

        Parameters
        ----------
        table : Any
            Columnar table object.

        Returns
        -------
        JSONList
            Parsed records.
        """
        return cast(JSONList, table.to_pylist())

    def write_table(
        self,
        path: Path,
        table: Any,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write an Arrow table object to *path*.

        Parameters
        ----------
        path : Path
            Path to the Arrow file on disk.
        table : Any
            Columnar table object.
        options : WriteOptions | None, optional
            Optional write parameters.
        """
        _ = options
        pyarrow_mod = _pyarrow()
        with pyarrow_mod.OSFile(str(path), 'wb') as sink:
            with pyarrow_mod.ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)
