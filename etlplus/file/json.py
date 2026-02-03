"""
:mod:`etlplus.file.json` module.

Helpers for reading/writing JavaScript Object Notation (JSON) files.

Notes
-----
- A JSON file is a widely used data interchange format that uses
    human-readable text to represent structured data.
- Common cases:
    - Data interchange between web applications and servers.
    - Configuration files for applications.
    - Data storage for NoSQL databases.
- Rule of thumb:
    - If the file follows the JSON specification, use this module for
        reading and writing.
"""

from __future__ import annotations

import json

from ..types import JSONData
from ..types import StrPath
from ..utils import count_records
from ._io import coerce_path
from ._io import coerce_record_payload
from ._io import ensure_parent_dir

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read JSON content from *path*.

    Validates that the JSON root is a dict or a list of dicts.

    Parameters
    ----------
    path : StrPath
        Path to the JSON file on disk.

    Returns
    -------
    JSONData
        The structured data read from the JSON file.
    """
    path = coerce_path(path)
    with path.open('r', encoding='utf-8') as handle:
        loaded = json.load(handle)

    return coerce_record_payload(loaded, format_name='JSON')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* as formatted JSON to *path*.

    Parameters
    ----------
    path : StrPath
        Path to the JSON file on disk.
    data : JSONData
        Data to serialize as JSON.

    Returns
    -------
    int
        The number of records written to the JSON file.
    """
    path = coerce_path(path)
    ensure_parent_dir(path)
    with path.open('w', encoding='utf-8') as handle:
        json.dump(
            data,
            handle,
            indent=2,
            ensure_ascii=False,
        )
        handle.write('\n')

    return count_records(data)
