"""
:mod:`etlplus.file.arrow` module.

Helpers for reading/writing Apache Arrow (ARROW) files.

Notes
-----
- An ARROW file is a binary file format designed for efficient
    columnar data storage and processing.
- Common cases:
    - High-performance data analytics.
    - Interoperability between different data processing systems.
    - In-memory data representation for fast computations.
- Rule of thumb:
    - If the file follows the Apache Arrow specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ._imports import get_optional_module
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _get_pyarrow() -> Any:
    """Return the pyarrow module, importing it on first use."""
    return get_optional_module(
        'pyarrow',
        error_message=(
            'ARROW support requires optional dependency "pyarrow".\n'
            'Install with: pip install pyarrow'
        ),
    )


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read ARROW content from *path*.

    Parameters
    ----------
    path : Path
        Path to the Apache Arrow file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the Apache Arrow file.
    """
    pyarrow = _get_pyarrow()
    with pyarrow.memory_map(str(path), 'r') as source:
        reader = pyarrow.ipc.open_file(source)
        table = reader.read_all()
    return cast(JSONList, table.to_pylist())


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to ARROW at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the ARROW file on disk.
    data : JSONData
        Data to write as ARROW. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the ARROW file.
    """
    records = normalize_records(data, 'ARROW')
    if not records:
        return 0

    pyarrow = _get_pyarrow()
    table = pyarrow.Table.from_pylist(records)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pyarrow.OSFile(str(path), 'wb') as sink:
        with pyarrow.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
    return len(records)
