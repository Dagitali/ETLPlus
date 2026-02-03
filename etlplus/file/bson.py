"""
:mod:`etlplus.file.bson` module.

Helpers for reading/writing Binary JSON (BSON) files.

Notes
-----
- A BSON file is a binary-encoded serialization of JSON-like documents.
- Common cases:
    - Data storage in MongoDB.
    - Efficient data interchange between systems.
    - Handling of complex data types not supported in standard JSON.
- Rule of thumb:
    - If the file follows the BSON specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ._imports import get_dependency
from ._io import ensure_parent_dir
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _decode_all(
    bson_module: Any,
    payload: bytes,
) -> list[dict[str, Any]]:
    """
    Decode all BSON documents from raw payload bytes.

    Parameters
    ----------
    bson_module : Any
        The imported BSON module to use for decoding.
    payload : bytes
        Raw bytes read from the BSON file.

    Returns
    -------
    list[dict[str, Any]]
        List of decoded BSON documents as dictionaries.

    Raises
    ------
    AttributeError
        If the bson module lacks the required :meth:`decode_all()` method.
    """
    if hasattr(bson_module, 'decode_all'):
        return bson_module.decode_all(payload)
    if hasattr(bson_module, 'BSON'):
        return bson_module.BSON.decode_all(payload)
    raise AttributeError('bson module lacks decode_all()')


def _encode_doc(
    bson_module: Any,
    doc: dict[str, Any],
) -> bytes:
    """
    Encode a single BSON document to bytes.

    Parameters
    ----------
    bson_module : Any
        The imported BSON module to use for encoding.
    doc : dict[str, Any]
        The BSON document to encode.

    Returns
    -------
    bytes
        The encoded BSON document as bytes.

    Raises
    ------
    AttributeError
        If the bson module lacks the required :meth:`encode()` method.
    """
    if hasattr(bson_module, 'encode'):
        return bson_module.encode(doc)
    if hasattr(bson_module, 'BSON'):
        return bson_module.BSON.encode(doc)
    raise AttributeError('bson module lacks encode()')


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read BSON content from *path*.

    Parameters
    ----------
    path : Path
        Path to the BSON file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the BSON file.
    """
    bson = get_dependency('bson', format_name='BSON', pip_name='pymongo')
    with path.open('rb') as handle:
        payload = handle.read()
    docs = _decode_all(bson, payload)
    return cast(JSONList, docs)


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to BSON at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the BSON file on disk.
    data : JSONData
        Data to write as BSON. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the BSON file.
    """
    bson = get_dependency('bson', format_name='BSON', pip_name='pymongo')
    records = normalize_records(data, 'BSON')
    if not records:
        return 0

    ensure_parent_dir(path)
    with path.open('wb') as handle:
        for record in records:
            handle.write(_encode_doc(bson, record))
    return len(records)
