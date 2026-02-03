"""
:mod:`etlplus.file.properties` module.

Helpers for reading/writing properties (PROPERTIES) files.

Notes
-----
- A PROPERTIES file is a properties file that typically uses key-value pairs,
    often with a simple syntax.
- Common cases:
    - Java-style properties files with ``key=value`` pairs.
    - INI-style files without sections.
    - Custom formats specific to certain applications.
- Rule of thumb:
    - If the file follows a standard format like INI, consider using
        dedicated parsers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..types import JSONData
from ..types import JSONDict

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _stringify(value: Any) -> str:
    """Normalize properties values into strings."""
    if value is None:
        return ''
    return str(value)


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONData:
    """
    Read PROPERTIES content from *path*.

    Parameters
    ----------
    path : Path
        Path to the PROPERTIES file on disk.

    Returns
    -------
    JSONData
        The structured data read from the PROPERTIES file.
    """
    payload: JSONDict = {}
    for line in path.read_text(encoding='utf-8').splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(('#', '!')):
            continue
        separator_index = -1
        for sep in ('=', ':'):
            if sep in stripped:
                separator_index = stripped.find(sep)
                break
        if separator_index == -1:
            key = stripped
            value = ''
        else:
            key = stripped[:separator_index].strip()
            value = stripped[separator_index + 1:].strip()
        if key:
            payload[key] = value
    return payload


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to PROPERTIES at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the PROPERTIES file on disk.
    data : JSONData
        Data to write as PROPERTIES. Should be a dictionary.

    Returns
    -------
    int
        The number of records written to the PROPERTIES file.

    Raises
    ------
    TypeError
        If *data* is not a dictionary.
    """
    if isinstance(data, list):
        raise TypeError('PROPERTIES payloads must be a dict')
    if not isinstance(data, dict):
        raise TypeError('PROPERTIES payloads must be a dict')

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as handle:
        for key in sorted(data.keys()):
            handle.write(f'{key}={_stringify(data[key])}\n')
    return 1
