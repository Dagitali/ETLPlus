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

from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ._imports import get_dependency
from ._io import make_deprecated_module_read
from ._io import make_deprecated_module_write
from ._io import normalize_records
from .base import BinarySerializationFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'BsonFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _bson() -> Any:
    """Return the optional bson module."""
    return get_dependency('bson', format_name='BSON', pip_name='pymongo')


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


# SECTION: CLASSES ========================================================== #


class BsonFile(BinarySerializationFileHandlerABC):
    """
    Handler implementation for BSON files.
    """

    # -- Class Attributes -- #

    format = FileFormat.BSON

    # -- Instance Methods -- #

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Serialize records to BSON bytes.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        bytes
            Serialized BSON payload bytes.
        """
        _ = options
        bson = _bson()
        records = normalize_records(data, 'BSON')
        chunks = [_encode_doc(bson, record) for record in records]
        return b''.join(chunks)

    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse BSON bytes into record lists.

        Parameters
        ----------
        payload : bytes
            Raw BSON payload bytes.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed records.
        """
        _ = options
        bson = _bson()
        docs = _decode_all(bson, payload)
        return cast(JSONList, docs)


# SECTION: INTERNAL CONSTANTS =============================================== #


_BSON_HANDLER = BsonFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _BSON_HANDLER)
write = make_deprecated_module_write(__name__, _BSON_HANDLER)
