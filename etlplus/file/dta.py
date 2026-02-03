"""
:mod:`etlplus.file.dta` module.

Helpers for reading/writing Stata (DTA) files.

Notes
-----
- A DTA file is a proprietary binary format created by Stata to store datasets
    with variables, labels, and data types.
- Common cases:
    - Statistical analysis workflows.
    - Data sharing in research environments.
    - Interchange between Stata and other analytics tools.
- Rule of thumb:
    - If the file follows the DTA specification, use this module for reading
        and writing.
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
    Read DTA content from *path*.

    Parameters
    ----------
    path : Path
        Path to the DTA file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DTA file.

    Raises
    ------
    ImportError
        If optional dependencies for DTA support are missing.
    """
    pandas = get_pandas('DTA')
    try:
        frame = pandas.read_stata(path)
    except ImportError as err:  # pragma: no cover
        raise ImportError(
            'DTA support may require optional dependency "pyreadstat".\n'
            'Install with: pip install pyreadstat',
        ) from err
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to DTA file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the DTA file on disk.
    data : JSONData
        Data to write as DTA file. Should be a list of dictionaries or a single
        dictionary.

    Returns
    -------
    int
        The number of rows written to the DTA file.

    Raises
    ------
    ImportError
        If optional dependencies for DTA support are missing.
    """
    records = normalize_records(data, 'DTA')
    if not records:
        return 0

    pandas = get_pandas('DTA')
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pandas.DataFrame.from_records(records)
    try:
        frame.to_stata(path, write_index=False)
    except ImportError as err:  # pragma: no cover
        raise ImportError(
            'DTA support may require optional dependency "pyreadstat".\n'
            'Install with: pip install pyreadstat',
        ) from err
    return len(records)
