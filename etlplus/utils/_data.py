"""
:mod:`etlplus.utils._data` module.

Data-oriented utility helpers.
"""

from __future__ import annotations

import json
import sys
from typing import Any
from typing import TextIO
from typing import cast

from ._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'JsonCodec',
    'RecordCounter',
]


# SECTION: INTERNAL CLASSES ================================================= #


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
            return cast(JSONData, json.loads(text))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f'Invalid JSON payload: {exc.msg} (pos {exc.pos})',
            ) from exc

    @classmethod
    def print(
        cls,
        obj: Any,
        *,
        stream: TextIO | None = None,
    ) -> None:
        """
        Pretty-print *obj* as UTF-8 JSON without ASCII escaping.

        Parameters
        ----------
        obj : Any
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
            'indent': 2 if pretty else None,
        }
        if not pretty:
            kwargs['separators'] = (',', ':')
        return json.dumps(obj, **kwargs)
