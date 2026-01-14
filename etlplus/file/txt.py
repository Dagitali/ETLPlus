"""
:mod:`etlplus.file.txt` module.

TXT read/write helpers.
"""

from __future__ import annotations

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
) -> JSONList:
    """
    Load TXT content as a list of dictionaries.

    Parameters
    ----------
    path : Path
        Path to the TXT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the TXT file.
    """
    rows: JSONList = []
    with path.open('r', encoding='utf-8') as handle:
        for line in handle:
            text = line.rstrip('\n')
            if text == '':
                continue
            rows.append({'text': text})
    return rows


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to TXT at ``path``.

    Parameters
    ----------
    path : Path
        Path to the TXT file on disk.
    data : JSONData
        Data to write. Expects ``{'text': '...'} `` or a list of those.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    TypeError
        If any item in ``data`` is not a dictionary or if any dictionary
        does not contain a ``'text'`` key.
    """
    rows: JSONList
    if isinstance(data, list):
        if not all(isinstance(item, dict) for item in data):
            raise TypeError('TXT payloads must contain only objects (dicts)')
        rows = cast(JSONList, data)
    else:
        rows = [cast(JSONDict, data)]

    if not rows:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as handle:
        for row in rows:
            if 'text' not in row:
                raise TypeError('TXT payloads must include a "text" key')
            handle.write(str(row['text']))
            handle.write('\n')

    return count_records(rows)
