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

from ..utils import require_dict_payload
from ..utils import require_str_key
from ..utils._types import JSONData
from ._enums import FileFormat
from .base import BinarySerializationFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PbFile',
]


# SECTION: CLASSES ========================================================== #


class PbFile(BinarySerializationFileHandlerABC):
    """Handler implementation for Protocol Buffers binary payload files."""

    # -- Class Attributes -- #

    format = FileFormat.PB

    # -- Instance Methods -- #

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Serialize base64-wrapped PB payload dictionary to bytes.

        Parameters
        ----------
        data : JSONData
            Payload dictionary with ``payload_base64`` key.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        bytes
            Raw protobuf payload bytes.
        """
        _ = options
        payload = require_dict_payload(data, format_name='PB')
        payload_base64 = require_str_key(
            payload,
            format_name='PB',
            key='payload_base64',
        )
        return base64.b64decode(payload_base64.encode('ascii'))

    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse PB bytes into base64-wrapped JSON payload.

        Parameters
        ----------
        payload : bytes
            Raw protobuf payload bytes.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Payload dictionary with ``payload_base64`` key.
        """
        _ = options
        encoded = base64.b64encode(payload).decode('ascii')
        return {'payload_base64': encoded}
