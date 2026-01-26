"""
:mod:`etlplus.file.ndjson` module.

Helpers for reading/writing Newline Delimited JSON (NDJSON) files.

Notes
-----
- An NDJSON file is a format where each line is a separate JSON object.
- Common cases:
    - Streaming JSON data.
    - Log files with JSON entries.
    - Large datasets that are processed line-by-line.
- Rule of thumb:
    - If the file follows the NDJSON specification, use this module for
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
    Read NDJSON content from *path*.

    Parameters
    ----------
    path : Path
        Path to the NDJSON file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the NDJSON file.

    Raises
    ------
    TypeError
        If any line in the NDJSON file is not a JSON object (dict).
    """
    rows: JSONList = []
    with path.open('r', encoding='utf-8') as handle:
        for idx, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            payload = json.loads(text)
            if not isinstance(payload, dict):
                raise TypeError(
                    f'NDJSON lines must be objects (dicts) (line {idx})',
                )
            rows.append(cast(JSONDict, payload))
    return rows


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to NDJSON at *path*.

    Parameters
    ----------
    path : Path
        Path to the NDJSON file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    rows = normalize_records(data, 'NDJSON')

    if not rows:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write('\n')

    return count_records(rows)
