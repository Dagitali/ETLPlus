"""
:mod:`etlplus.utils._data` module.

Data-oriented utility helpers.
"""

from __future__ import annotations

import json
from typing import Any
from typing import cast

from ._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions (data utilities)
    'count_records',
    'parse_json',
    'print_json',
    'serialize_json',
]


# SECTION: FUNCTIONS ======================================================== #


def count_records(
    data: JSONData,
) -> int:
    """
    Return a consistent record count for JSON-like data payloads.

    Lists are treated as multiple records; dicts as a single record.

    Parameters
    ----------
    data : JSONData
        Data payload to count records for.

    Returns
    -------
    int
        Number of records in `data`.
    """
    return len(data) if isinstance(data, list) else 1


def parse_json(
    text: str,
) -> JSONData:
    """
    Parse JSON text and surface a concise error when it fails.

    Parameters
    ----------
    text : str
        The JSON text to parse.

    Returns
    -------
    JSONData
        The parsed JSON data.

    Raises
    ------
    ValueError
        When the JSON text is invalid.
    """
    try:
        return cast(JSONData, json.loads(text))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f'Invalid JSON payload: {exc.msg} (pos {exc.pos})',
        ) from exc


def serialize_json(
    obj: Any,
    *,
    pretty: bool = False,
    sort_keys: bool = False,
) -> str:
    """
    Serialize *obj* as UTF-8 JSON without ASCII escaping.

    Parameters
    ----------
    obj : Any
        Object to serialize as JSON.
    pretty : bool, optional
        Whether to format output with indentation. Default is ``False``.
    sort_keys : bool, optional
        Whether to sort mapping keys for stable output. Default is ``False``.

    Returns
    -------
    str
        Serialized JSON text.
    """
    kwargs: dict[str, Any] = {
        'ensure_ascii': False,
        'sort_keys': sort_keys,
    }
    if pretty:
        kwargs['indent'] = 2
    else:
        kwargs['separators'] = (',', ':')
    return json.dumps(obj, **kwargs)


def print_json(
    obj: Any,
) -> None:
    """
    Pretty-print *obj* as UTF-8 JSON without ASCII escaping.

    Parameters
    ----------
    obj : Any
        Object to serialize as JSON.

    Returns
    -------
    None
        This helper writes directly to STDOUT.
    """
    print(serialize_json(obj, pretty=True))
