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

from pathlib import Path
from typing import Any
from typing import cast

from etlplus.file._imports import get_fastavro

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
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


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read AVRO content from *path*.

    Parameters
    ----------
    path : Path
        Path to the AVRO file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the AVRO file.
    """
    fastavro = get_fastavro()
    with path.open('rb') as handle:
        reader = fastavro.reader(handle)
        return [cast(JSONDict, record) for record in reader]


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to AVRO at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the AVRO file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    records = normalize_records(data, 'AVRO')
    if not records:
        return 0

    fastavro = get_fastavro()
    schema = _infer_schema(records)
    parsed_schema = fastavro.parse_schema(schema)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as handle:
        fastavro.writer(handle, parsed_schema, records)

    return len(records)
