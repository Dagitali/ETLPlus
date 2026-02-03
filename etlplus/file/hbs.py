"""
:mod:`etlplus.file.hbs` module.

Stub helpers for reading/writing Handlebars (HBS) template files (not
implemented yet).

Notes
-----
- A Handlebars (HBS) template file is a text file used for generating HTML or
    other text formats by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Handlebars template files, use this module for
        reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from . import stub
from ._io import coerce_path

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read ZSAV content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the HBS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the HBS file.
    """
    return stub.read(path, format_name='HBS')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to HBS file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the HBS file on disk.
    data : JSONData
        Data to write as HBS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the HBS file.
    """
    path = coerce_path(path)
    return stub.write(path, data, format_name='HBS')
