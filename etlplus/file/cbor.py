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

from typing import Any

from ..types import JSONData
from ._imports import get_dependency
from ._io import coerce_record_payload
from ._io import make_deprecated_module_io
from ._io import normalize_records
from .base import BinarySerializationFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CborFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _cbor2() -> Any:
    """Return the optional cbor2 module."""
    return get_dependency('cbor2', format_name='CBOR')


# SECTION: CLASSES ========================================================== #


class CborFile(BinarySerializationFileHandlerABC):
    """
    Handler implementation for CBOR files.
    """

    # -- Class Attributes -- #

    format = FileFormat.CBOR

    # -- Instance Methods -- #

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Serialize structured records to CBOR bytes.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        bytes
            Serialized CBOR payload bytes.
        """
        _ = options
        cbor2 = _cbor2()
        records = normalize_records(data, 'CBOR')
        payload: JSONData = records if isinstance(data, list) else records[0]
        return cbor2.dumps(payload)

    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse CBOR bytes into structured records.

        Parameters
        ----------
        payload : bytes
            Raw CBOR payload bytes.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """
        _ = options
        cbor2 = _cbor2()
        decoded = cbor2.loads(payload)
        return coerce_record_payload(decoded, format_name='CBOR')


# SECTION: INTERNAL CONSTANTS =============================================== #

_CBOR_HANDLER = CborFile()


# SECTION: FUNCTIONS ======================================================== #


read, write = make_deprecated_module_io(__name__, _CBOR_HANDLER)
