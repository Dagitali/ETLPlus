"""
:mod:`etlplus.file.msgpack` module.

Helpers for reading/writing MessagePack (MSGPACK) files.

Notes
-----
- A MsgPack file is a binary serialization format that is more compact than
    JSON.
- Common cases:
    - Efficient data storage and transmission.
    - Inter-process communication.
    - Data serialization in performance-critical applications.
- Rule of thumb:
    - If the file follows the MsgPack specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..types import JSONData
from ._imports import get_optional_module
from ._io import coerce_record_payload
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _get_msgpack() -> Any:
    """Return the msgpack module, importing it on first use."""
    return get_optional_module(
        'msgpack',
        error_message=(
            'MSGPACK support requires optional dependency "msgpack".\n'
            'Install with: pip install msgpack'
        ),
    )


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONData:
    """
    Read MsgPack content from *path*.

    Parameters
    ----------
    path : Path
        Path to the MsgPack file on disk.

    Returns
    -------
    JSONData
        The structured data read from the MsgPack file.
    """
    msgpack = _get_msgpack()
    with path.open('rb') as handle:
        payload = msgpack.unpackb(handle.read(), raw=False)
    return coerce_record_payload(payload, format_name='MSGPACK')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to MsgPack at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the MsgPack file on disk.
    data : JSONData
        Data to write as MsgPack. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MsgPack file.
    """
    msgpack = _get_msgpack()
    records = normalize_records(data, 'MSGPACK')
    payload: JSONData = records if isinstance(data, list) else records[0]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as handle:
        handle.write(msgpack.packb(payload, use_bin_type=True))
    return len(records)
