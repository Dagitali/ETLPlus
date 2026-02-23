"""
:mod:`etlplus.file.xls` module.

Helpers for reading Excel XLS files (write is not supported).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..types import JSONList
from ._imports import get_pandas
from ._io import records_from_table
from .base import ReadOnlySpreadsheetFileHandlerABC
from .base import ReadOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XlsFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _pandas() -> Any:
    """Return the optional pandas module for XLS operations."""
    return get_pandas('XLS')


# SECTION: CLASSES ========================================================== #


class XlsFile(ReadOnlySpreadsheetFileHandlerABC):
    """
    Read-only handler implementation for XLS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.XLS
    engine_name = 'xlrd'

    # -- Instance Methods -- #

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read one XLS sheet from *path*.

        Parameters
        ----------
        path : Path
            Path to the XLS file on disk.
        sheet : str | int
            Sheet selector (name or index).
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the XLS sheet.

        Raises
        ------
        ImportError
            If the optional dependency "xlrd" is not installed.
        """
        _ = options
        pandas = _pandas()
        try:
            frame = pandas.read_excel(path, engine='xlrd', sheet_name=sheet)
        except TypeError:
            frame = pandas.read_excel(path, engine='xlrd')
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                'XLS support requires optional dependency "xlrd".\n'
                'Install with: pip install xlrd',
            ) from e
        return records_from_table(frame)
