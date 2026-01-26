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
    Read ORC content from *path*.

    Parameters
    ----------
    path : Path
        Path to the ORC file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ORC file.

    Raises
    ------
    ImportError
        When optional dependency "pyarrow" is missing.
    """
    pandas = get_pandas('ORC')
    try:
        frame = pandas.read_orc(path)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'ORC support requires optional dependency "pyarrow".\n'
            'Install with: pip install pyarrow',
        ) from e
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to ORC at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the ORC file on disk.
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
    records = normalize_records(data, 'ORC')
    if not records:
        return 0

    pandas = get_pandas('ORC')
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pandas.DataFrame.from_records(records)
    try:
        frame.to_orc(path, index=False)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'ORC support requires optional dependency "pyarrow".\n'
            'Install with: pip install pyarrow',
        ) from e
    return len(records)
