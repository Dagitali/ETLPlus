"""
:mod:`etlplus.file.feather` module.

Helpers for reading/writing Apache Arrow Feather (FEATHER) files.

Notes
-----
- A FEATHER file is a binary file format designed for efficient
    on-disk storage of data frames, built on top of Apache Arrow.
- Common cases:
    - Fast read/write operations for data frames.
    - Interoperability between different data analysis tools.
    - Storage of large datasets with efficient compression.
- Rule of thumb:
    - If the file follows the Apache Arrow Feather specification, use this
        module for reading and writing.
"""

from __future__ import annotations

from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read Feather content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the Feather file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the Feather file.
    """
    path = coerce_path(path)
    get_dependency('pyarrow', format_name='Feather')
    pandas = get_pandas('Feather')
    frame = pandas.read_feather(path)
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to Feather at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the Feather file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    path = coerce_path(path)
    records = normalize_records(data, 'Feather')
    if not records:
        return 0

    get_dependency('pyarrow', format_name='Feather')
    pandas = get_pandas('Feather')
    ensure_parent_dir(path)
    frame = pandas.DataFrame.from_records(records)
    frame.to_feather(path)
    return len(records)
