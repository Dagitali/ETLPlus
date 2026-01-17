"""
:mod:`etlplus.file.props` module.

Helpers for reading/writing properties (PROPS) files.

Notes
-----
- A “PROPS-formatted” file is a properties file that typically uses
    key-value pairs, often with a simple syntax.
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
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read PROPS content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the PROPS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the PROPS file.
    """
    return stub.read(path, format_name='PROPS')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to PROPS at ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the PROPS file on disk.
    data : JSONData
        Data to write as PROPS. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the PROPS file.
    """
    return stub.write(path, data, format_name='PROPS')
