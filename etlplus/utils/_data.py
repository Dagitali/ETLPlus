"""
:mod:`etlplus.utils._data` module.

Data-oriented utility helpers.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal
from typing import TextIO
from typing import TypeGuard
from typing import cast

from ._types import JSONData
from ._types import JSONDict
from ._types import JSONList

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'JsonCodec',
    'RecordCounter',
    # Functions
    'coerce_record_payload',
    'normalize_records',
    'require_dict_payload',
    'require_str_key',
    'stringify_value',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _is_json_data(
    value: object,
) -> TypeGuard[JSONData]:
    """Return whether *value* matches the JSONData runtime shape."""
    return isinstance(value, dict) or _is_object_list(value)


def _is_object_list(
    value: object,
) -> TypeGuard[JSONList]:
    """Return whether *value* is a list of dictionary objects."""
    return isinstance(value, list) and all(isinstance(item, dict) for item in value)


# SECTION: FUNCTIONS ======================================================== #


def coerce_record_payload(
    payload: object,
    *,
    format_name: str,
) -> JSONData:
    """
    Validate that *payload* is an object or list of objects.

    Parameters
    ----------
    payload : object
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
    if _is_object_list(payload):
        return payload
    if isinstance(payload, list):
        raise TypeError(
            f'{format_name} array must contain only objects (dicts)',
        )
    raise TypeError(
        f'{format_name} root must be an object or an array of objects',
    )


def normalize_records(
    data: object,
    format_name: str,
) -> JSONList:
    """
    Normalize payloads into a list of dictionaries.

    Parameters
    ----------
    data : object
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
        If the payload is not a dict or a list of dicts.
    """
    if _is_object_list(data):
        return data
    if isinstance(data, list):
        raise TypeError(
            f'{format_name} payloads must contain only objects (dicts)',
        )
    if isinstance(data, dict):
        return [cast(JSONDict, data)]
    raise TypeError(
        f'{format_name} payloads must be an object or an array of objects',
    )


def require_dict_payload(
    data: object,
    *,
    format_name: str,
) -> JSONDict:
    """
    Validate that *data* is a dictionary payload.

    Parameters
    ----------
    data : object
        Input payload to validate.
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    JSONDict
        Validated dictionary payload.

    Raises
    ------
    TypeError
        If the payload is not a dictionary.
    """
    if isinstance(data, dict):
        return cast(JSONDict, data)
    raise TypeError(f'{format_name} payloads must be a dict')


def require_str_key(
    payload: JSONDict,
    *,
    format_name: str,
    key: str,
) -> str:
    """
    Require a string value for *key* in *payload*.

    Parameters
    ----------
    payload : JSONDict
        Dictionary payload to inspect.
    format_name : str
        Human-readable format name for error messages.
    key : str
        Key to extract.

    Returns
    -------
    str
        The string value for *key*.

    Raises
    ------
    TypeError
        If the key is missing or not a string.
    """
    value = payload.get(key)
    if not isinstance(value, str):
        raise TypeError(
            f'{format_name} payloads must include a "{key}" string',
        )
    return value


def stringify_value(value: object) -> str:
    """
    Normalize configuration-like values into strings.

    Parameters
    ----------
    value : object
        Value to normalize.

    Returns
    -------
    str
        Stringified value (``''`` for ``None``).
    """
    if value is None:
        return ''
    return str(value)


# SECTION: CLASSES ========================================================== #


class RecordCounter:
    """Centralize record-count semantics for JSON-like ETL payloads."""

    # -- Static Methods -- #

    @staticmethod
    def count(
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
        if isinstance(data, list):
            return len(data)
        return 1


class JsonCodec:
    """Centralize JSON parse, render, and print behavior."""

    # -- Class Methods -- #

    @classmethod
    def decode(
        cls,
        text: str,
    ) -> object:
        """
        Decode JSON text without applying ETL payload shape validation.

        Parameters
        ----------
        text : str
            The JSON text to decode.

        Returns
        -------
        object
            The raw decoded JSON value.
        """
        return json.loads(text)

    @classmethod
    def default(
        cls,
        value: object,
    ) -> object:
        """Return a JSON fallback for common ETL scalar types."""
        if isinstance(value, date | datetime | time):
            return cls.isoformat(value)
        if isinstance(value, Decimal):
            return str(value)
        return str(value)

    @classmethod
    def parse(
        cls,
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
            If *text* is not valid JSON.

        Notes
        -----
        This wrapper preserves the concise :class:`ValueError` raised by the
        internal JSON codec when decoding fails.
        """
        try:
            data = cls.decode(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f'Invalid JSON payload: {exc.msg} (pos {exc.pos})',
            ) from exc
        if not _is_json_data(data):
            raise ValueError('JSON payload must be an object or array of objects')
        return data

    @classmethod
    def print(
        cls,
        obj: object,
        *,
        stream: TextIO | None = None,
    ) -> None:
        """
        Pretty-print *obj* as UTF-8 JSON without ASCII escaping.

        Parameters
        ----------
        obj : object
            Object to serialize as JSON.
        stream : TextIO | None, optional
            Destination stream. Defaults to :data:`sys.stdout`.

        Returns
        -------
        None
            This helper writes directly to STDOUT.
        """
        print(
            cls.serialize(obj, pretty=True),
            file=sys.stdout if stream is None else stream,
        )

    @classmethod
    def serialize(
        cls,
        obj: object,
        *,
        compact: bool = True,
        default: Callable[[object], object] | None = None,
        pretty: bool = False,
        sort_keys: bool = False,
    ) -> str:
        """
        Serialize *obj* as UTF-8 JSON without ASCII escaping.

        Parameters
        ----------
        obj : object
            Object to serialize as JSON.
        compact : bool, optional
            Whether to remove optional whitespace when not pretty-printing.
            Default is ``True``.
        default : Callable[[object], object] | None, optional
            Optional JSON fallback serializer for non-standard values.
        pretty : bool, optional
            Whether to format output with indentation. Default is ``False``.
        sort_keys : bool, optional
            Whether to sort mapping keys for stable output. Default is ``False``.

        Returns
        -------
        str
            Serialized JSON text.
        """
        return json.dumps(
            obj,
            ensure_ascii=False,
            sort_keys=sort_keys,
            indent=2 if pretty else None,
            separators=(',', ':') if compact and not pretty else None,
            default=default,
        )

    # -- Static Methods -- #

    @staticmethod
    def isoformat(value: date | datetime | time) -> str:
        """Return stable ISO text for date-like values."""
        match value:
            case datetime() | time():
                return value.isoformat(timespec='microseconds')
            case _:
                return value.isoformat()
