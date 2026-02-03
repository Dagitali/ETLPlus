"""
:mod:`etlplus.file.rds` module.

Helpers for reading/writing R (RDS) data files.

Notes
-----
- An RDS file is a binary file format used by R to store a single R object,
    such as a data frame, list, or vector.
- Common cases:
    - Storing R objects for later use.
    - Sharing R data between users.
    - Loading R data into Python for analysis.
- Rule of thumb:
    - If the file follows the RDS specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..types import JSONData
from ..types import JSONDict
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import ensure_parent_dir
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL HELPERS ================================================ #


def _coerce_r_object(value: Any, pandas: Any) -> JSONData:
    """Normalize a pyreadr object into JSON-friendly data."""
    if isinstance(value, pandas.DataFrame):
        return value.to_dict(orient='records')
    if isinstance(value, dict):
        return value
    if isinstance(value, list) and all(
        isinstance(item, dict) for item in value
    ):
        return value
    return {'value': value}


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONData:
    """
    Read RDS content from *path*.

    Parameters
    ----------
    path : Path
        Path to the RDS file on disk.

    Returns
    -------
    JSONData
        The structured data read from the RDS file.
    """
    pyreadr = get_dependency('pyreadr', format_name='RDS')
    pandas = get_pandas('RDS')
    result = pyreadr.read_r(str(path))
    if not result:
        return []
    if len(result) == 1:
        value = next(iter(result.values()))
        return _coerce_r_object(value, pandas)
    payload: JSONDict = {}
    for key, value in result.items():
        payload[str(key)] = _coerce_r_object(value, pandas)
    return payload


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to RDS file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the RDS file on disk.
    data : JSONData
        Data to write as RDS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the RDS file.

    Raises
    ------
    ImportError
        If "pyreadr" is not installed with write support.
    """
    pyreadr = get_dependency('pyreadr', format_name='RDS')
    pandas = get_pandas('RDS')
    records = normalize_records(data, 'RDS')
    frame = pandas.DataFrame.from_records(records)
    count = len(records)

    writer = getattr(pyreadr, 'write_rds', None)
    if writer is None:
        raise ImportError(
            'RDS write support requires "pyreadr" with write_rds().',
        )

    ensure_parent_dir(path)
    writer(str(path), frame)
    return count
