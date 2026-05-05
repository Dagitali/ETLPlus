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

from ._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'JsonCodec',
    'RecordCounter',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _is_json_data(
    value: object,
) -> TypeGuard[JSONData]:
    """Return whether *value* matches the JSONData runtime shape."""
    return isinstance(value, dict) or (
        isinstance(value, list) and all(isinstance(item, dict) for item in value)
    )


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
        match data:
            case list():
                return len(data)
            case _:
                return 1


class JsonCodec:
    """Centralize JSON parse, render, and print behavior."""

    # -- Class Methods -- #

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
        internal JSON codec when decoding fails.`
        """
        try:
            data = json.loads(text)
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
        kwargs: dict[str, object] = {
            'ensure_ascii': False,
            'sort_keys': sort_keys,
            'indent': 2 if pretty else None,
        }
        if compact and not pretty:
            kwargs['separators'] = (',', ':')
        return json.dumps(
            obj,
            default=default,
            **kwargs,  # type: ignore[arg-type]
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
