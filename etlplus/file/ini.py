"""
:mod:`etlplus.file.ini` module.

Helpers for reading/writing initialization (INI) files.

Notes
-----
- An INI file is a simple configuration file format that uses sections,
    properties, and values.
- Common cases:
    - Sections are denoted by square brackets (e.g., ``[section]``).
    - Properties are key-value pairs (e.g., ``key=value``).
    - Comments are often indicated by semicolons (``;``) or hash symbols
        (``#``).
- Rule of thumb:
    - If the file follows the INI specification, use this module for
        reading and writing.
"""

from __future__ import annotations

import configparser
from pathlib import Path

from ..types import JSONData
from ..types import JSONDict
from ._io import ensure_parent_dir
from ._io import require_dict_payload
from ._io import stringify_value

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONData:
    """
    Read INI content from *path*.

    Parameters
    ----------
    path : Path
        Path to the INI file on disk.

    Returns
    -------
    JSONData
        The structured data read from the INI file.
    """
    parser = configparser.ConfigParser()
    parser.read(path, encoding='utf-8')

    payload: JSONDict = {}
    if parser.defaults():
        payload['DEFAULT'] = dict(parser.defaults())
    defaults = dict(parser.defaults())
    for section in parser.sections():
        raw_section = dict(parser.items(section))
        for key in defaults:
            raw_section.pop(key, None)
        payload[section] = raw_section
    return payload


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to INI at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the INI file on disk.
    data : JSONData
        Data to write as INI. Should be a dictionary.

    Returns
    -------
    int
        The number of records written to the INI file.

    Raises
    ------
    TypeError
        If *data* is not a dictionary.
    """
    payload = require_dict_payload(data, format_name='INI')

    parser = configparser.ConfigParser()
    for section, values in payload.items():
        if section == 'DEFAULT':
            if isinstance(values, dict):
                parser['DEFAULT'] = {
                    key: stringify_value(value)
                    for key, value in values.items()
                }
            else:
                raise TypeError('INI DEFAULT section must be a dict')
            continue
        if not isinstance(values, dict):
            raise TypeError('INI sections must map to dicts')
        parser[section] = {
            key: stringify_value(value) for key, value in values.items()
        }

    ensure_parent_dir(path)
    with path.open('w', encoding='utf-8', newline='') as handle:
        parser.write(handle)
    return 1
