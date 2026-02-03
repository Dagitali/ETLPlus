"""
:mod:`etlplus.file.fwf` module.

Helpers for reading/writing Fixed-Width Fields (FWF) files.

Notes
-----
- An FWF file is a text file format where each field has a fixed width.
- Common cases:
    - Data files from legacy systems.
    - Reports with aligned columns.
    - Data exchange in mainframe environments.
- Rule of thumb:
    - If the file follows the FWF specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._io import stringify_value

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
    Read FWF content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the FWF file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the FWF file.
    """
    path = coerce_path(path)
    pandas = get_pandas('FWF')
    frame = pandas.read_fwf(path)
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to FWF file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the FWF file on disk.
    data : JSONData
        Data to write as FWF file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the FWF file.
    """
    path = coerce_path(path)
    records = normalize_records(data, 'FWF')
    if not records:
        return 0

    fieldnames = sorted({key for row in records for key in row})
    if not fieldnames:
        return 0

    widths: dict[str, int] = {name: len(name) for name in fieldnames}
    for row in records:
        for name in fieldnames:
            widths[name] = max(
                widths[name],
                len(stringify_value(row.get(name))),
            )

    ensure_parent_dir(path)
    with path.open('w', encoding='utf-8', newline='') as handle:
        header = ' '.join(name.ljust(widths[name]) for name in fieldnames)
        handle.write(header + '\n')
        for row in records:
            line = ' '.join(
                stringify_value(row.get(name)).ljust(widths[name])
                for name in fieldnames
            )
            handle.write(line + '\n')
    return len(records)
