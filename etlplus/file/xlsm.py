"""
:mod:`etlplus.file.xlsm` module.

Helpers for reading/writing Microsoft Excel Macro-Enabled (XLSM)
spreadsheet files.

Notes
-----
- An XLSM file is a spreadsheet file created using the Microsoft Excel Macro-
    Enabled (Open XML) format.
- Common cases:
    - Reading data from Excel Macro-Enabled spreadsheets.
    - Writing data to Excel Macro-Enabled format for compatibility.
    - Converting XLSM files to more modern formats.
- Rule of thumb:
    - If you need to work with Excel Macro-Enabled spreadsheet files, use this
        module for reading and writing.
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
    Read XLSM content from *path*.

    Parameters
    ----------
    path : Path
        Path to the XLSM file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XLSM file.

    Raises
    ------
    ImportError
        If optional dependencies for XLSM support are missing.
    """
    pandas = get_pandas('XLSM')
    try:
        frame = pandas.read_excel(path)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'XLSM support requires optional dependency "openpyxl".\n'
            'Install with: pip install openpyxl',
        ) from e
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to XLSM file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the XLSM file on disk.
    data : JSONData
        Data to write as XLSM file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the XLSM file.

    Raises
    ------
    ImportError
        If optional dependencies for XLSM support are missing.
    """
    records = normalize_records(data, 'XLSM')
    if not records:
        return 0

    pandas = get_pandas('XLSM')
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pandas.DataFrame.from_records(records)
    try:
        frame.to_excel(path, index=False)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'XLSM support requires optional dependency "openpyxl".\n'
            'Install with: pip install openpyxl',
        ) from e
    return len(records)
