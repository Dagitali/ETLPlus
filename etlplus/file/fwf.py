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
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_pandas
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._io import stringify_value
from .base import ReadOptions
from .base import TextFixedWidthFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FwfFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class FwfFile(TextFixedWidthFileHandlerABC):
    """
    Handler implementation for FWF files.
    """

    # -- Class Attributes -- #

    format = FileFormat.FWF

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return FWF content from *path*.

        Parameters
        ----------
        path : Path
            Path to the FWF file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the FWF file.
        """
        return self.read_rows(path, options=options)

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
        pandas = get_pandas('FWF')
        frame = pandas.read_fwf(path)
        return cast(JSONList, frame.to_dict(orient='records'))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to FWF at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the FWF file on disk.
        data : JSONData
            Data to write as FWF file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the FWF file.
        """
        rows = normalize_records(data, 'FWF')
        return self.write_rows(path, rows, options=options)

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
        with path.open(
            'w',
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


# SECTION: INTERNAL CONSTANTS =============================================== #

_FWF_HANDLER = FwfFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``FwfFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the FWF file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the FWF file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _FWF_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``FwfFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the FWF file on disk.
    data : JSONData
        Data to write as FWF file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the FWF file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _FWF_HANDLER.write,
    )
