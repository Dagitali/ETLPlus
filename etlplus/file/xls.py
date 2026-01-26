"""
:mod:`etlplus.file.xls` module.

Helpers for reading/writing Excel XLS files.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ._imports import get_pandas

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read XLS content from *path*.

    Parameters
    ----------
    path : Path
        Path to the XLS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XLS file.

    Raises
    ------
    ImportError
        If the optional dependency "xlrd" is not installed.
    """
    pandas = get_pandas('XLS')
    try:
        frame = pandas.read_excel(path, engine='xlrd')
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'XLS support requires optional dependency "xlrd".\n'
            'Install with: pip install xlrd',
        ) from e
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to XLS at *path* and return record count.

    Notes
    -----
    XLS writing is not supported by pandas 2.x. Use XLSX for writes.

    Parameters
    ----------
    path : Path
        Path to the XLS file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    RuntimeError
        If XLS writing is attempted.
    """
    raise RuntimeError('XLS write is not supported; use XLSX instead')
