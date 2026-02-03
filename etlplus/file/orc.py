"""
:mod:`etlplus.file.orc` module.

Helpers for reading/writing Optimized Row Columnar (ORC) files.

Notes
-----
- An ORC file is a columnar storage file format optimized for Big Data
    processing.
- Common cases:
    - Efficient storage and retrieval of large datasets.
    - Integration with big data frameworks like Apache Hive and Apache Spark.
    - Compression and performance optimization for analytical queries.
- Rule of thumb:
    - If the file follows the ORC specification, use this module for reading
        and writing.
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
    Read ORC content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the ORC file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ORC file.
    """
    path = coerce_path(path)
    get_dependency('pyarrow', format_name='ORC')
    pandas = get_pandas('ORC')
    frame = pandas.read_orc(path)
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to ORC at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the ORC file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    path = coerce_path(path)
    records = normalize_records(data, 'ORC')
    if not records:
        return 0

    get_dependency('pyarrow', format_name='ORC')
    pandas = get_pandas('ORC')
    ensure_parent_dir(path)
    frame = pandas.DataFrame.from_records(records)
    frame.to_orc(path, index=False)
    return len(records)
