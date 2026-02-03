"""
:mod:`etlplus.file.cbor` module.

Helpers for reading/writing Concise Binary Object Representation (CBOR) files.

Notes
-----
- A CBOR file is a binary data format designed for small code size and message
    size, suitable for constrained environments.
- Common cases:
    - IoT data interchange.
    - Efficient data serialization.
    - Storage of structured data in a compact binary form.
- Rule of thumb:
    - If the file follows the CBOR specification, use this module for reading
        and writing.
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
    Read CBOR content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the CBOR file on disk.

    Returns
    -------
    JSONData
        The structured data read from the CBOR file.
    """
    path = coerce_path(path)
    cbor2 = get_dependency('cbor2', format_name='CBOR')
    with path.open('rb') as handle:
        payload = cbor2.loads(handle.read())
    return coerce_record_payload(payload, format_name='CBOR')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to CBOR file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the CBOR file on disk.
    data : JSONData
        Data to write as CBOR file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CBOR file.
    """
    path = coerce_path(path)
    cbor2 = get_dependency('cbor2', format_name='CBOR')
    records = normalize_records(data, 'CBOR')
    payload: JSONData = records if isinstance(data, list) else records[0]
    ensure_parent_dir(path)
    with path.open('wb') as handle:
        handle.write(cbor2.dumps(payload))
    return len(records)
