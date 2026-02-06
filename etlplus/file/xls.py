"""
:mod:`etlplus.file.xls` module.

Helpers for reading Excel XLS files (write is not supported).
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_pandas
from ._io import coerce_path
from .base import ReadOnlyFileHandlerABC
from .base import ReadOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XlsFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class XlsFile(ReadOnlyFileHandlerABC):
    """
    Read-only handler implementation for XLS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.XLS

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read XLS content from *path*.

        Parameters
        ----------
        path : Path
            Path to the XLS file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the XLS file.

        Raises
        ------
        ImportError
            If the optional dependency "xlrd" is not installed.
        """
        _ = options
        pandas = get_pandas('XLS')
        try:
            frame = pandas.read_excel(path, engine='xlrd')
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                'XLS support requires optional dependency "xlrd".\n'
                'Install with: pip install xlrd',
            ) from e
        return cast(JSONList, frame.to_dict(orient='records'))


# SECTION: INTERNAL CONSTANTS ============================================== #


_XLS_HANDLER = XlsFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read XLS content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the XLS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XLS file.
    """
    return _XLS_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to XLS at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the XLS file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    return _XLS_HANDLER.write(coerce_path(path), data)
