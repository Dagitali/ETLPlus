"""
:mod:`etlplus.file.avro` module.

Helpers for reading/writing Apache Avro (AVRO) files.

Notes
-----
- An AVRO file is a binary file format designed for efficient
    on-disk storage of data, with a schema definition.
- Common cases:
    - Data serialization for distributed systems.
    - Interoperability between different programming languages.
    - Storage of large datasets with schema evolution support.
- Rule of thumb:
    - If the file follows the Apache Avro specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from io import BytesIO
from typing import Any
from typing import cast

from ..utils import RecordPayloadParser
from ..utils._types import JSONData
from ..utils._types import JSONDict
from ..utils._types import JSONList
from ._enums import FileFormat
from ._imports import get_dependency
from .base import BinarySerializationFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AvroFile',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_PRIMITIVE_TYPES: tuple[type, ...] = (
    bool,
    int,
    float,
    str,
    bytes,
    bytearray,
)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _fastavro() -> Any:
    """Return the required fastavro module."""
    return get_dependency(
        'fastavro',
        format_name='AVRO',
        required=True,
    )


def _infer_schema(records: JSONList) -> dict[str, Any]:
    """
    Infer a basic Avro schema from record payloads.

    Only primitive field values are supported; complex values raise TypeError.
    """
    field_names = sorted({key for record in records for key in record})
    fields: list[dict[str, Any]] = []
    for name in field_names:
        types: list[str] = []
        for record in records:
            value = record.get(name)
            if value is None:
                types.append('null')
                continue
            if isinstance(value, dict | list):
                raise TypeError(
                    'AVRO payloads must contain only primitive values',
                )
            if not isinstance(value, _PRIMITIVE_TYPES):
                raise TypeError(
                    'AVRO payloads must contain only primitive values',
                )
            types.append(cast(str, _infer_value_type(value)))
        fields.append({'name': name, 'type': _merge_types(types)})

    return {
        'name': 'etlplus_record',
        'type': 'record',
        'fields': fields,
    }


def _infer_value_type(value: object) -> str | list[str]:
    """
    Infer the Avro type for a primitive value.

    Raises TypeError for unsupported types.
    """
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'boolean'
    if isinstance(value, int):
        return 'long'
    if isinstance(value, float):
        return 'double'
    if isinstance(value, str):
        return 'string'
    if isinstance(value, (bytes, bytearray)):
        return 'bytes'
    raise TypeError('AVRO payloads must contain only primitive values')


def _merge_types(types: list[str]) -> str | list[str]:
    """Return a stable Avro type union for a list of types."""
    unique = list(dict.fromkeys(types))
    if len(unique) == 1:
        return unique[0]
    ordered = ['null'] + sorted(t for t in unique if t != 'null')
    return ordered


# SECTION: CLASSES ========================================================== #


class AvroFile(BinarySerializationFileHandlerABC):
    """Handler implementation for AVRO files."""

    # -- Class Attributes -- #

    format = FileFormat.AVRO

    # -- Instance Methods -- #

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Serialize records into AVRO payload bytes.

        Parameters
        ----------
        data : JSONData
            Data to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        bytes
            Serialized AVRO payload.
        """
        _ = options
        records = RecordPayloadParser('AVRO').normalize(data)
        if not records:
            return b''

        fastavro = _fastavro()
        schema = _infer_schema(records)
        parsed_schema = fastavro.parse_schema(schema)

        with BytesIO() as handle:
            fastavro.writer(handle, parsed_schema, records)
            return handle.getvalue()

    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Parse AVRO payload bytes into structured records.

        Parameters
        ----------
        payload : bytes
            AVRO binary payload.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Parsed records.
        """
        _ = options
        fastavro = _fastavro()
        with BytesIO(payload) as handle:
            reader = fastavro.reader(handle)
            return [cast(JSONDict, record) for record in reader]
