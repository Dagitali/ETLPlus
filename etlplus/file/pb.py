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
from pathlib import Path

from ..types import JSONData
from ..types import StrPath
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import require_dict_payload
from ._io import require_str_key
from ._io import warn_deprecated_module_io
from .base import BinarySerializationFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PbFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class PbFile(BinarySerializationFileHandlerABC):
    """
    Handler implementation for Protocol Buffers binary payload files.
    """

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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return PB content from *path*.

        Parameters
        ----------
        path : Path
            Path to the PB file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the PB file.
        """
        _ = options
        return self.loads_bytes(path.read_bytes())

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to PB at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the PB file on disk.
        data : JSONData
            Data to write as PB. Should be a dictionary with
            ``payload_base64``.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of records written to the PB file.
        """
        payload = self.dumps_bytes(data, options=options)
        ensure_parent_dir(path)
        path.write_bytes(payload)
        return 1


# SECTION: INTERNAL CONSTANTS =============================================== #

_PB_HANDLER = PbFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read and return PB content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the PB file on disk.

    Returns
    -------
    JSONData
        The structured data read from the PB file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _PB_HANDLER.read(coerce_path(path))


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
    warn_deprecated_module_io(__name__, 'write')
    return _PB_HANDLER.write(coerce_path(path), data)
