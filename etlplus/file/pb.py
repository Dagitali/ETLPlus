"""
:mod:`etlplus.file.pb` module.

Helpers for reading/writing Protocol Buffers binary (PB) files.

Notes
-----
- A PB file contains Protocol Buffers (Protobuff) binary-encoded messages.
- Common cases:
    - Serialized payloads emitted by services or SDKs.
    - Binary payload dumps for debugging or transport.
- Rule of thumb:
    - Use this module when you need to store or transport raw protobuf bytes.
"""

from __future__ import annotations

import base64
from pathlib import Path

from ..types import JSONData

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
    Read PB content from *path*.

    Parameters
    ----------
    path : Path
        Path to the PB file on disk.

    Returns
    -------
    JSONData
        The structured data read from the PB file.
    """
    payload = path.read_bytes()
    encoded = base64.b64encode(payload).decode('ascii')
    return {'payload_base64': encoded}


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to PB at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the PB file on disk.
    data : JSONData
        Data to write as PB. Should be a dictionary with ``payload_base64``.

    Returns
    -------
    int
        The number of records written to the PB file.
    """
    if isinstance(data, list):
        raise TypeError('PB payloads must be a dict')
    if not isinstance(data, dict):
        raise TypeError('PB payloads must be a dict')

    payload_base64 = data.get('payload_base64')
    if not isinstance(payload_base64, str):
        raise TypeError('PB payloads must include a "payload_base64" string')

    payload = base64.b64decode(payload_base64.encode('ascii'))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return 1
