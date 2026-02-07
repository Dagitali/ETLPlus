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

from pathlib import Path

from ..types import JSONData
from ..types import StrPath
from ._io import coerce_path
from ._io import ensure_parent_dir
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
        encoding = self.encoding_from_write_options(options)
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
        encoding = self.encoding_from_read_options(options)
        return {'schema': payload.decode(encoding)}

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return PROTO content from *path*.

        Parameters
        ----------
        path : Path
            Path to the PROTO file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the PROTO file.
        """
        return self.loads_bytes(path.read_bytes(), options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to PROTO at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the PROTO file on disk.
        data : JSONData
            Data to write as PROTO. Should be a dictionary with ``schema``.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of records written to the PROTO file.
        """
        payload = self.dumps_bytes(data, options=options)
        ensure_parent_dir(path)
        path.write_bytes(payload)
        return 1


# SECTION: INTERNAL CONSTANTS ============================================== #


_PROTO_HANDLER = ProtoFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read and return PROTO content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the PROTO file on disk.

    Returns
    -------
    JSONData
        The structured data read from the PROTO file.
    """
    return _PROTO_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to PROTO at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the PROTO file on disk.
    data : JSONData
        Data to write as PROTO. Should be a dictionary with ``schema``.

    Returns
    -------
    int
        The number of records written to the PROTO file.
    """
    return _PROTO_HANDLER.write(coerce_path(path), data)
