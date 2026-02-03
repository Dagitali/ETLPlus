"""
:mod:`etlplus.file.proto` module.

Helpers for reading/writing Protocol Buffers schema (PROTO) files.

Notes
-----
- A PROTO file defines the structure of Protocol Buffers messages.
- Common cases:
    - Defining message formats for data interchange.
    - Generating code for serialization/deserialization.
    - Documenting data structures in distributed systems.
- Rule of thumb:
    - If the file follows the Protocol Buffers schema specification, use this
        module for reading and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ._io import ensure_parent_dir
from ._io import require_dict_payload
from ._io import require_str_key

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
    Read PROTO content from *path*.

    Parameters
    ----------
    path : Path
        Path to the PROTO file on disk.

    Returns
    -------
    JSONData
        The structured data read from the PROTO file.
    """
    return {'schema': path.read_text(encoding='utf-8')}


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to PROTO at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the PROTO file on disk.
    data : JSONData
        Data to write as PROTO. Should be a dictionary with ``schema``.

    Returns
    -------
    int
        The number of records written to the PROTO file.
    """
    payload = require_dict_payload(data, format_name='PROTO')
    schema = require_str_key(payload, format_name='PROTO', key='schema')

    ensure_parent_dir(path)
    path.write_text(schema, encoding='utf-8')
    return 1
