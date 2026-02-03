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


def _get_cbor() -> Any:
    """Return the cbor2 module, importing it on first use."""
    return get_optional_module(
        'cbor2',
        error_message=(
            'CBOR support requires optional dependency "cbor2".\n'
            'Install with: pip install cbor2'
        ),
    )


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONData:
    """
    Read CBOR content from *path*.

    Parameters
    ----------
    path : Path
        Path to the CBOR file on disk.

    Returns
    -------
    JSONData
        The structured data read from the CBOR file.
    """
    cbor2 = _get_cbor()
    with path.open('rb') as handle:
        payload = cbor2.loads(handle.read())
    return coerce_record_payload(payload, format_name='CBOR')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to CBOR at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the CBOR file on disk.
    data : JSONData
        Data to write as CBOR. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CBOR file.
    """
    cbor2 = _get_cbor()
    records = normalize_records(data, 'CBOR')
    payload: JSONData = records if isinstance(data, list) else records[0]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as handle:
        handle.write(cbor2.dumps(payload))
    return len(records)
