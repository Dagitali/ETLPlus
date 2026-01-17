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
from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..utils import count_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONData:
    """
    Read JSON content from ``path``.

    Validates that the JSON root is a dict or a list of dicts.

    Parameters
    ----------
    path : Path
        Path to the JSON file on disk.

    Returns
    -------
    JSONData
        The structured data read from the JSON file.

    Raises
    ------
    TypeError
        If the JSON root is not an object or an array of objects.
    """
    with path.open('r', encoding='utf-8') as handle:
        loaded = json.load(handle)

    if isinstance(loaded, dict):
        return cast(JSONDict, loaded)
    if isinstance(loaded, list):
        if all(isinstance(item, dict) for item in loaded):
            return cast(JSONList, loaded)
        raise TypeError(
            'JSON array must contain only objects (dicts) when loading file',
        )
    raise TypeError(
        'JSON root must be an object or an array of objects when loading file',
    )


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` as formatted JSON to ``path``.

    Parameters
    ----------
    path : Path
        Path to the JSON file on disk.
    data : JSONData
        Data to serialize as JSON.

    Returns
    -------
    int
        The number of records written to the JSON file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as handle:
        json.dump(
            data,
            handle,
            indent=2,
            ensure_ascii=False,
        )
        handle.write('\n')

    return count_records(data)
