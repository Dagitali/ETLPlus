"""
:mod:`etlplus.file.jinja2` module.

Helpers for reading/writing compressed Jinja2 (JINJA2) template files.

Notes
-----
- A JINJA2 file is a text file used for generating HTML or other text formats
    by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Jinja2 template files, use this module for
        reading and writing.
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
    Read JINJA2 content from *path*.

    Parameters
    ----------
    path : Path
        Path to the JINJA2 file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the JINJA2 file.
    """
    return stub.read(path, format_name='JINJA2')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to JINJA2 file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the JINJA2 file on disk.
    data : JSONData
        Data to write as JINJA2 file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the JINJA2 file.
    """
    return stub.write(path, data, format_name='JINJA2')
