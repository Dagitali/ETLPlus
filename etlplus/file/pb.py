"""
:mod:`etlplus.file.pb` module.

Helpers for reading/writing Protocol Buffers binary (PB) files.

Notes
-----
- A PB file contains Protocol Buffers (Protobuf) binary-encoded messages.
- Common cases:
    - Serialized payloads emitted by services or SDKs.
    - Binary payload dumps for debugging or transport.
- Rule of thumb:
    - Use this module when you need to store or transport raw protobuf bytes.
"""

from __future__ import annotations

import base64

from ..types import JSONData
from ..types import StrPath
from ._io import coerce_path
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
    path: StrPath,
) -> JSONData:
    """
    Read PB content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the PB file on disk.

    Returns
    -------
    JSONData
        The structured data read from the PB file.
    """
    path = coerce_path(path)
    payload = path.read_bytes()
    encoded = base64.b64encode(payload).decode('ascii')
    return {'payload_base64': encoded}


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to PB at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the PB file on disk.
    data : JSONData
        Data to write as PB. Should be a dictionary with ``payload_base64``.

    Returns
    -------
    int
        The number of records written to the PB file.
    """
    path = coerce_path(path)
    payload = require_dict_payload(data, format_name='PB')
    payload_base64 = require_str_key(
        payload,
        format_name='PB',
        key='payload_base64',
    )

    decoded = base64.b64decode(payload_base64.encode('ascii'))
    ensure_parent_dir(path)
    path.write_bytes(decoded)
    return 1
