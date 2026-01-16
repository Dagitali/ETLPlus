"""
:mod:`etlplus.file.feather` module.

Helpers for reading/writing Feather files.
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
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read Feather content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the Feather file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the Feather file.

    Raises
    ------
    ImportError
        When optional dependency "pyarrow" is missing.
    """
    pandas = get_pandas('Feather')
    try:
        frame = pandas.read_feather(path)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'Feather support requires optional dependency "pyarrow".\n'
            'Install with: pip install pyarrow',
        ) from e
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to Feather at ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the Feather file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    ImportError
        When optional dependency "pyarrow" is missing.
    """
    records = normalize_records(data, 'Feather')
    if not records:
        return 0

    pandas = get_pandas('Feather')
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pandas.DataFrame.from_records(records)
    try:
        frame.to_feather(path)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'Feather support requires optional dependency "pyarrow".\n'
            'Install with: pip install pyarrow',
        ) from e
    return len(records)
