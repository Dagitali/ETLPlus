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

from ..types import JSONData
from ..types import StrPath
from ._imports import get_dependency
from ._io import coerce_path
from ._io import coerce_record_payload
from ._io import ensure_parent_dir
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read MsgPack content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the MsgPack file on disk.

    Returns
    -------
    JSONData
        The structured data read from the MsgPack file.
    """
    path = coerce_path(path)
    msgpack = get_dependency('msgpack', format_name='MSGPACK')
    with path.open('rb') as handle:
        payload = msgpack.unpackb(handle.read(), raw=False)
    return coerce_record_payload(payload, format_name='MSGPACK')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to MsgPack at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the MsgPack file on disk.
    data : JSONData
        Data to write as MsgPack. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MsgPack file.
    """
    path = coerce_path(path)
    msgpack = get_dependency('msgpack', format_name='MSGPACK')
    records = normalize_records(data, 'MSGPACK')
    payload: JSONData = records if isinstance(data, list) else records[0]
    ensure_parent_dir(path)
    with path.open('wb') as handle:
        handle.write(msgpack.packb(payload, use_bin_type=True))
    return len(records)
