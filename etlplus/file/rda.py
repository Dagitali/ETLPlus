"""
:mod:`etlplus.file.rda` module.

Helpers for reading/writing RData workspace/object bundle (RDA) files.

Notes
-----
- A RDA file is a binary file format used by R to store workspace objects,
    including data frames, lists, and other R objects.
- Common cases:
    - Storing R data objects for later use.
    - Sharing R datasets between users.
    - Loading R data into Python for analysis.
- Rule of thumb:
    - If the file follows the RDA specification, use this module for reading
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


# SECTION: INTERNAL FUNCTIONS =============================================== #


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
    Read RDA content from *path*.

    Parameters
    ----------
    path : Path
        Path to the RDA file on disk.

    Returns
    -------
    JSONData
        The structured data read from the RDA file.
    """
    pyreadr = get_dependency('pyreadr', format_name='RDA')
    pandas = get_pandas('RDA')
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
    Write *data* to RDA file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the RDA file on disk.
    data : JSONData
        Data to write as RDA file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the RDA file.

    Raises
    ------
    ImportError
        If "pyreadr" is not installed with write support.
    """
    pyreadr = get_dependency('pyreadr', format_name='RDA')
    pandas = get_pandas('RDA')
    records = normalize_records(data, 'RDA')
    frame = pandas.DataFrame.from_records(records)
    count = len(records)

    writer = getattr(pyreadr, 'write_rdata', None) or getattr(
        pyreadr,
        'write_rda',
        None,
    )
    if writer is None:
        raise ImportError(
            'RDA write support requires "pyreadr" with write_rdata().',
        )

    ensure_parent_dir(path)
    try:
        writer(str(path), frame, df_name='data')
    except TypeError:
        writer(str(path), frame)
    return count
