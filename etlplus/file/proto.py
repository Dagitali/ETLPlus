"""
:mod:`etlplus.file.proto` module.

Helpers for reading/writing Protocol Buffers schema (PROTO) files.

Notes
-----
- A PROTO file defines the structure of Protocol Buffers messages.
- Common cases:
    - Defining message formats for data interchange.
    - Generating code for serialization/deserialization.
    - Documenting data structures in distributed systems.
- Rule of thumb:
    - If the file follows the Protocol Buffers schema specification, use this
        module for reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ._io import make_deprecated_module_read
from ._io import make_deprecated_module_write
from ._io import require_dict_payload
from ._io import require_str_key
from .base import BinarySerializationFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ProtoFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class ProtoFile(BinarySerializationFileHandlerABC):
    """
    Handler implementation for Protocol Buffers schema files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PROTO

    # -- Instance Methods -- #

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Serialize schema dictionary payload into bytes.

        Parameters
        ----------
        data : JSONData
            Payload dictionary with ``schema`` key.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        bytes
            Encoded schema bytes.
        """
        encoding = self.encoding_from_options(options)
        payload = require_dict_payload(data, format_name='PROTO')
        schema = require_str_key(payload, format_name='PROTO', key='schema')
        return schema.encode(encoding)

    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse PROTO bytes into schema dictionary payload.

        Parameters
        ----------
        payload : bytes
            Raw schema bytes.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Payload dictionary with ``schema`` key.
        """
        encoding = self.encoding_from_options(options)
        return {'schema': payload.decode(encoding)}


# SECTION: INTERNAL CONSTANTS =============================================== #


_PROTO_HANDLER = ProtoFile()


# SECTION: FUNCTIONS ======================================================== #


read = make_deprecated_module_read(__name__, _PROTO_HANDLER)
write = make_deprecated_module_write(__name__, _PROTO_HANDLER)
