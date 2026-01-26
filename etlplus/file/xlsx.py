"""
:mod:`etlplus.file.xlsx` module.

Helpers for reading/writing Excel XLSX files.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ._imports import get_pandas
from ._io import normalize_records

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
    Read XLSX content from *path*.

    Parameters
    ----------
    path : Path
        Path to the XLSX file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XLSX file.

    Raises
    ------
    ImportError
        If optional dependencies for XLSX support are missing.
    """
    pandas = get_pandas('XLSX')
    try:
        frame = pandas.read_excel(path)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'XLSX support requires optional dependency "openpyxl".\n'
            'Install with: pip install openpyxl',
        ) from e
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to XLSX at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the XLSX file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    ImportError
        If optional dependencies for XLSX support are missing.
    """
    records = normalize_records(data, 'XLSX')
    if not records:
        return 0

    pandas = get_pandas('XLSX')
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pandas.DataFrame.from_records(records)
    try:
        frame.to_excel(path, index=False)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'XLSX support requires optional dependency "openpyxl".\n'
            'Install with: pip install openpyxl',
        ) from e
    return len(records)
