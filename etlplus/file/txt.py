"""
:mod:`etlplus.file.txt` module.

Helpers for reading/writing text (TXT) files.

Notes
-----
- A TXT file is a plain text file that contains unformatted text.
- Common cases:
    - Each line in the file represents a single piece of text.
    - Lines may vary in length and content.
- Rule of thumb:
    - If the file is a simple text file without specific formatting
        requirements, use this module for reading and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
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
    Read TXT content from *path*.

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
    Write *data* to TXT at *path* and return record count.

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
        If any item in *data* is not a dictionary or if any dictionary
        does not contain a ``'text'`` key.
    """
    rows = normalize_records(data, 'TXT')

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
