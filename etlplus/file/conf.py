"""
:mod:`etlplus.file.conf` module.

Helpers for reading/writing config (CONF) files.

Notes
-----
- A CONF file is a configuration file that may use various syntaxes, such as
    INI, YAML, or custom formats.
- Common cases:
    - INI-style key-value pairs with sections.
    - YAML-like structures with indentation.
    - Custom formats specific to certain applications (such as Unix-like
        systems, where ``.conf`` is a strong convention for "This is a
        configuration file").
- Rule of thumb:
    - If the file follows a standard format like INI or YAML, consider using
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
    Read CONF content from *path*.

    Parameters
    ----------
    path : Path
        Path to the CONF file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CONF file.
    """
    return stub.read(path, format_name='CONF')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to CONF at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the CONF file on disk.
    data : JSONData
        Data to write as CONF. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CONF file.
    """
    return stub.write(path, data, format_name='CONF')
