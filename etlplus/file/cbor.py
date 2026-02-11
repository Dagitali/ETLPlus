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

from ..types import JSONData
from ..types import StrPath
from ._imports import get_dependency
from ._io import coerce_path
from ._io import coerce_record_payload
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._io import warn_deprecated_module_io
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
        cbor2 = get_dependency('cbor2', format_name='CBOR')
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
        cbor2 = get_dependency('cbor2', format_name='CBOR')
        decoded = cbor2.loads(payload)
        return coerce_record_payload(decoded, format_name='CBOR')

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return CBOR content from *path*.

        Parameters
        ----------
        path : Path
            Path to the CBOR file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the CBOR file.
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
        Write *data* to CBOR at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the CBOR file on disk.
        data : JSONData
            Data to write as CBOR file.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the CBOR file.
        """
        records = normalize_records(data, 'CBOR')
        payload = self.dumps_bytes(data, options=options)
        ensure_parent_dir(path)
        path.write_bytes(payload)
        return len(records)


# SECTION: INTERNAL CONSTANTS =============================================== #

_CBOR_HANDLER = CborFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read and return CBOR content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the CBOR file on disk.

    Returns
    -------
    JSONData
        The structured data read from the CBOR file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _CBOR_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to CBOR at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the CBOR file on disk.
    data : JSONData
        Data to write as CBOR file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CBOR file.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _CBOR_HANDLER.write(coerce_path(path), data)
