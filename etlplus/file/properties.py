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

from ..types import JSONData
from ..types import JSONList
from . import stub

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
    Read PROPERTIES content from *path*.

    Parameters
    ----------
    path : Path
        Path to the PROPERTIES file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PROPERTIES file.
    """
    return stub.read(path, format_name='PROPERTIES')


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
        Data to write as PROPERTIES. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PROPERTIES file.
    """
    return stub.write(path, data, format_name='PROPERTIES')
