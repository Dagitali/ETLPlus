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

from pathlib import Path

from ..types import JSONData
from ..types import StrPath
from ._imports import get_dependency
from ._io import coerce_path
from ._io import coerce_record_payload
from ._io import ensure_parent_dir
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
        msgpack = get_dependency('msgpack', format_name='MSGPACK')
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
        msgpack = get_dependency('msgpack', format_name='MSGPACK')
        decoded = msgpack.unpackb(payload, raw=False)
        return coerce_record_payload(decoded, format_name='MSGPACK')

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read MsgPack content from *path*.

        Parameters
        ----------
        path : Path
            Path to the MsgPack file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the MsgPack file.
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
        Write *data* to MsgPack at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the MsgPack file on disk.
        data : JSONData
            Data to write as MsgPack.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the MsgPack file.
        """
        records = normalize_records(data, 'MSGPACK')
        payload = self.dumps_bytes(data, options=options)
        ensure_parent_dir(path)
        path.write_bytes(payload)
        return len(records)


# SECTION: INTERNAL CONSTANTS ============================================== #


_MSGPACK_HANDLER = MsgpackFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read MsgPack content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the MsgPack file on disk.

    Returns
    -------
    JSONData
        The structured data read from the MsgPack file.
    """
    return _MSGPACK_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to MsgPack at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the MsgPack file on disk.
    data : JSONData
        Data to write as MsgPack. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MsgPack file.
    """
    return _MSGPACK_HANDLER.write(coerce_path(path), data)
