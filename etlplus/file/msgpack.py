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
    'MsgpackFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _msgpack() -> Any:
    """Return the optional msgpack module."""
    return get_dependency('msgpack', format_name='MSGPACK')


# SECTION: CLASSES ========================================================== #


class MsgpackFile(BinarySerializationFileHandlerABC):
    """
    Handler implementation for MessagePack files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MSGPACK

    # -- Instance Methods -- #

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Serialize structured records to MsgPack bytes.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        bytes
            Serialized MsgPack payload bytes.
        """
        _ = options
        msgpack = _msgpack()
        records = normalize_records(data, 'MSGPACK')
        payload: JSONData = records if isinstance(data, list) else records[0]
        return msgpack.packb(payload, use_bin_type=True)

    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse MsgPack bytes into structured records.

        Parameters
        ----------
        payload : bytes
            Raw MsgPack payload bytes.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """
        _ = options
        msgpack = _msgpack()
        decoded = msgpack.unpackb(payload, raw=False)
        return coerce_record_payload(decoded, format_name='MSGPACK')


# SECTION: INTERNAL CONSTANTS =============================================== #


_MSGPACK_HANDLER = MsgpackFile()


# SECTION: FUNCTIONS ======================================================== #


read, write = make_deprecated_module_io(__name__, _MSGPACK_HANDLER)
