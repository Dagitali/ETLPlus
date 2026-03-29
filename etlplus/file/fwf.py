"""
:mod:`etlplus.file.fwf` module.

Helpers for reading/writing Fixed-Width Fields (FWF) files.

Notes
-----
- An FWF file is a text file format where each field has a fixed width.
- Common cases:
    - Data files from legacy systems.
    - Reports with aligned columns.
    - Data exchange in mainframe environments.
- Rule of thumb:
    - If the file follows the FWF specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..utils._types import JSONList
from ._enums import FileFormat
from ._imports import get_pandas
from ._io import _open_text_handle
from ._io import ensure_parent_dir
from ._io import records_from_table
from ._io import stringify_value
from .base import ReadOptions
from .base import TextFixedWidthFileHandlerABC
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FwfFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _pandas() -> Any:
    """Return the required pandas module for FWF operations."""
    return get_pandas('FWF')


# SECTION: CLASSES ========================================================== #


class FwfFile(TextFixedWidthFileHandlerABC):
    """Handler implementation for FWF files."""

    # -- Class Attributes -- #

    format = FileFormat.FWF

    # -- Instance Methods -- #

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read row records from FWF content at *path*.

        Parameters
        ----------
        path : Path
            Path to the FWF file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries parsed from the FWF file.
        """
        _ = options
        pandas = _pandas()
        frame = pandas.read_fwf(path)
        return records_from_table(frame)

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write row records to FWF file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the FWF file on disk.
        rows : JSONList
            Row records to write as FWF file.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the FWF file.
        """
        _ = options
        if not rows:
            return 0

        fieldnames = sorted({key for row in rows for key in row})
        if not fieldnames:
            return 0

        widths: dict[str, int] = {name: len(name) for name in fieldnames}
        for row in rows:
            for name in fieldnames:
                widths[name] = max(
                    widths[name],
                    len(stringify_value(row.get(name))),
                )

        ensure_parent_dir(path)
        with _open_text_handle(
            path,
            mode='w',
            encoding=self.default_encoding,
            newline='',
        ) as handle:
            header = ' '.join(name.ljust(widths[name]) for name in fieldnames)
            handle.write(header + '\n')
            for row in rows:
                line = ' '.join(
                    stringify_value(row.get(name)).ljust(widths[name])
                    for name in fieldnames
                )
                handle.write(line + '\n')
        return len(rows)
