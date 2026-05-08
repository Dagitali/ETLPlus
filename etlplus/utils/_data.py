"""
:mod:`etlplus.utils._data` module.

Data-oriented utility helpers.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
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
    # Data Classes
    'RecordPayloadParser',
    # Functions
    'count_records',
    'stringify_value',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _is_object_list(
    value: object,
) -> TypeGuard[JSONList]:
    """Return whether *value* is a list of dictionary objects."""
    return isinstance(value, list) and all(isinstance(item, dict) for item in value)


def _record_payload_or_none(
    value: object,
) -> JSONData | None:
    """Return one validated record payload or ``None`` for unsupported shapes."""
    if isinstance(value, dict):
        return cast(JSONDict, value)
    if _is_object_list(value):
        return value
    return None


# SECTION: FUNCTIONS ======================================================== #


def count_records(
    data: JSONData,
) -> int:
    """
    Return a consistent record count for JSON-like data payloads.

    Parameters
    ----------
    data : JSONData
        Data payload to count records for.

    Returns
    -------
    int
        Lists count as multiple records; dictionaries count as one record.
    """
    return len(data) if isinstance(data, list) else 1


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


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class JsonCodec:
    """
    Centralize JSON parse, render, and print behavior.

    Attributes
    ----------
    compact : bool
        Whether to remove optional whitespace when not pretty-printing.
    pretty : bool
        Whether to format output with indentation.
    sort_keys : bool
        Whether to sort mapping keys for stable output.
    default_serializer : Callable[[object], object] | None
        Optional JSON fallback serializer for non-standard values.
    """

    # -- Instance Attributes -- #

    compact: bool = True
    pretty: bool = False
    sort_keys: bool = False
    default_serializer: Callable[[object], object] | None = None

    # -- Class Methods -- #

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

        """
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f'Invalid JSON payload: {exc.msg} (pos {exc.pos})',
            ) from exc
        record_payload = _record_payload_or_none(data)
        if record_payload is not None:
            return record_payload
        raise ValueError('JSON payload must be an object or array of objects')

    # -- Instance Methods -- #

    def print(
        self,
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
            self.serialize(obj),
            file=sys.stdout if stream is None else stream,
        )

    def serialize(
        self,
        obj: object,
        *,
        default: Callable[[object], object] | None = None,
    ) -> str:
        """
        Serialize *obj* as UTF-8 JSON without ASCII escaping.

        Parameters
        ----------
        obj : object
            Object to serialize as JSON.
        default : Callable[[object], object] | None, optional
            Optional one-call override for the configured fallback serializer.

        Returns
        -------
        str
            Serialized JSON text.
        """
        return json.dumps(
            obj,
            ensure_ascii=False,
            sort_keys=self.sort_keys,
            indent=2 if self.pretty else None,
            separators=(',', ':') if self.compact and not self.pretty else None,
            default=self.default_serializer if default is None else default,
        )

    @staticmethod
    def default(
        value: object,
    ) -> object:
        """
        Return a JSON fallback for common ETL scalar types.

        Parameters
        ----------
        value : object
            The value to serialize.

        Returns
        -------
        object
            A JSON-serializable fallback for *value*.
        """
        if isinstance(value, date | datetime | time):
            return JsonCodec.isoformat(value)
        return str(value)

    @staticmethod
    def isoformat(
        value: date | datetime | time,
    ) -> str:
        """
        Return stable ISO text for date-like values.

        Parameters
        ----------
        value : date | datetime | time
            The date-like value to serialize.

        Returns
        -------
        str
            The ISO-formatted string representation of *value*.
        """
        match value:
            case datetime() | time():
                return value.isoformat(timespec='microseconds')
            case _:
                return value.isoformat()


@dataclass(frozen=True, slots=True)
class RecordPayloadParser:
    """
    Validate and normalize JSON-record payloads for one data format.

    Attributes
    ----------
    format_name : str
        Human-readable format name used in validation error messages.
    """

    # -- Instance Attributes -- #

    format_name: str

    # -- Internal Instance Methods -- #

    def _invalid_payload_error(
        self,
        payload: object,
        *,
        mixed_list_message: str,
        invalid_root_message: str,
    ) -> TypeError:
        """Return the payload-shape error matching *payload*."""
        message = (
            mixed_list_message if isinstance(payload, list) else invalid_root_message
        )
        return TypeError(
            f'{self.format_name} {message}',
        )

    # -- Instance Methods -- #

    def coerce(
        self,
        payload: object,
    ) -> JSONData:
        """
        Validate that *payload* is an object or list of objects.

        Parameters
        ----------
        payload : object
            Parsed payload to validate.

        Returns
        -------
        JSONData
            *payload* when it is a dict or a list of dicts.

        Raises
        ------
        TypeError
            If the payload is not a dict or list of dicts.
        """
        record_payload = _record_payload_or_none(payload)
        if record_payload is not None:
            return record_payload
        raise self._invalid_payload_error(
            payload,
            mixed_list_message='array must contain only objects (dicts)',
            invalid_root_message='root must be an object or an array of objects',
        )

    def normalize(
        self,
        data: object,
    ) -> JSONList:
        """
        Normalize a payload into a list of dictionaries.

        Parameters
        ----------
        data : object
            Input payload to normalize.

        Returns
        -------
        JSONList
            Normalized list of dictionaries.

        Raises
        ------
        TypeError
            If the payload is not a dict or a list of dicts.
        """
        record_payload = _record_payload_or_none(data)
        if isinstance(record_payload, list):
            return record_payload
        if record_payload is not None:
            return [record_payload]
        raise self._invalid_payload_error(
            data,
            mixed_list_message='payloads must contain only objects (dicts)',
            invalid_root_message='payloads must be an object or an array of objects',
        )

    def require_dict(
        self,
        data: object,
    ) -> JSONDict:
        """
        Validate that *data* is a dictionary payload.

        Parameters
        ----------
        data : object
            Input payload to validate.

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
        raise TypeError(f'{self.format_name} payloads must be a dict')

    def require_str_key(
        self,
        payload: JSONDict,
        key: str,
    ) -> str:
        """
        Require a string value for *key* in *payload*.

        Parameters
        ----------
        payload : JSONDict
            Dictionary payload to inspect.
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
                f'{self.format_name} payloads must include a "{key}" string',
            )
        return value
