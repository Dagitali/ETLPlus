"""
:mod:`etlplus.file.cfg` module.

Stub helpers for reading/writing config (CFG) files (not implemented yet).

Notes
-----
- A CFG file is a configuration file that may use various syntaxes, such as
    INI, YAML, or custom formats.
- Common cases:
    - INI-style key-value pairs with sections (such as in Python ecosystems,
        using ``configparser``).
    - YAML-like structures with indentation.
    - Custom formats specific to certain applications.
- Rule of thumb:
    - If the file follows a standard format like INI or YAML, consider using
        dedicated parsers.
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
    Read CFG content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the CFG file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CFG file.
    """
    return stub.read(path, format_name='CFG')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to CFG file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the CFG file on disk.
    data : JSONData
        Data to write as CFG file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CFG file.
    """
    path = coerce_path(path)
    return stub.write(path, data, format_name='CFG')
