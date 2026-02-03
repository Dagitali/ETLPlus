"""
:mod:`etlplus.file.toml` module.

Helpers for reading/writing Tom's Obvious Minimal Language (TOML) files.

Notes
-----
- A TOML file is a configuration file that uses the TOML syntax.
- Common cases:
    - Simple key-value pairs.
    - Nested tables and arrays.
    - Data types such as strings, integers, floats, booleans, dates, and
        arrays.
- Rule of thumb:
    - If the file follows the TOML specification, use this module for
        reading and writing.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ._imports import get_optional_module
from ._io import ensure_parent_dir
from ._io import require_dict_payload

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
    Read TOML content from *path*.

    Parameters
    ----------
    path : Path
        Path to the TOML file on disk.

    Returns
    -------
    JSONData
        The structured data read from the TOML file.

    Raises
    ------
    TypeError
        If the TOML root is not a table (dictionary).
    """
    payload = tomllib.loads(path.read_text(encoding='utf-8'))
    if isinstance(payload, dict):
        return payload
    raise TypeError('TOML root must be a table (dict)')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to TOML at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the TOML file on disk.
    data : JSONData
        Data to write as TOML. Should be a dictionary.

    Returns
    -------
    int
        The number of records written to the TOML file.
   """
    payload = require_dict_payload(data, format_name='TOML')

    toml_writer: Any
    try:
        toml_writer = get_optional_module(
            'tomli_w',
            error_message=(
                'TOML write support requires optional dependency "tomli_w".\n'
                'Install with: pip install tomli-w'
            ),
        )
        content = toml_writer.dumps(cast(JSONDict, payload))
    except ImportError:
        toml = get_optional_module(
            'toml',
            error_message=(
                'TOML write support requires optional dependency "tomli_w" '
                'or "toml".\n'
                'Install with: pip install tomli-w'
            ),
        )
        content = toml.dumps(cast(JSONDict, payload))

    ensure_parent_dir(path)
    path.write_text(content, encoding='utf-8')
    return 1
