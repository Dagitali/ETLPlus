"""
:mod:`etlplus.file._io` module.

Shared helpers for record normalization and delimited text formats.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList

# SECTION: FUNCTIONS ======================================================== #


def coerce_record_payload(
    payload: Any,
    *,
    format_name: str,
) -> JSONData:
    """
    Validate that *payload* is an object or list of objects.

    Parameters
    ----------
    payload : Any
        Parsed payload to validate.
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    JSONData
        *payload* when it is a dict or a list of dicts.

    Raises
    ------
    TypeError
        If the payload is not a dict or list of dicts.
    """
    if isinstance(payload, dict):
        return cast(JSONDict, payload)
    if isinstance(payload, list):
        if all(isinstance(item, dict) for item in payload):
            return cast(JSONList, payload)
        raise TypeError(
            f'{format_name} array must contain only objects (dicts)',
        )
    raise TypeError(
        f'{format_name} root must be an object or an array of objects',
    )


def normalize_records(
    data: JSONData,
    format_name: str,
) -> JSONList:
    """
    Normalize payloads into a list of dictionaries.

    Parameters
    ----------
    data : JSONData
        Input payload to normalize.
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    JSONList
        Normalized list of dictionaries.

    Raises
    ------
    TypeError
        If a list payload contains non-dict items.
    """
    if isinstance(data, list):
        if not all(isinstance(item, dict) for item in data):
            raise TypeError(
                f'{format_name} payloads must contain only objects (dicts)',
            )
        return cast(JSONList, data)
    return [cast(JSONDict, data)]


def read_delimited(
    path: Path,
    *,
    delimiter: str,
) -> JSONList:
    """
    Read delimited content from *path*.

    Parameters
    ----------
    path : Path
        Path to the delimited file on disk.
    delimiter : str
        Delimiter character for parsing.

    Returns
    -------
    JSONList
        The list of dictionaries read from the delimited file.
    """
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader: csv.DictReader[str] = csv.DictReader(
            handle,
            delimiter=delimiter,
        )
        rows: JSONList = []
        for row in reader:
            if not any(row.values()):
                continue
            rows.append(cast(JSONDict, dict(row)))
    return rows


def write_delimited(
    path: Path,
    data: JSONData,
    *,
    delimiter: str,
) -> int:
    """
    Write *data* to a delimited file and return record count.

    Parameters
    ----------
    path : Path
        Path to the delimited file on disk.
    data : JSONData
        Data to write as delimited rows.
    delimiter : str
        Delimiter character for writing.

    Returns
    -------
    int
        The number of rows written.
    """
    rows: list[JSONDict]
    if isinstance(data, list):
        rows = [row for row in data if isinstance(row, dict)]
    else:
        rows = [data]

    if not rows:
        return 0

    fieldnames = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            delimiter=delimiter,
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})

    return len(rows)
