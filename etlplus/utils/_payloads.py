"""
:mod:`etlplus.utils._payloads` module.

Generic payload parsing and file-hydration helpers shared across ETLPlus.
"""

from __future__ import annotations

import csv
import os
from io import StringIO
from typing import cast

from ..file import File
from ..file import FileFormat
from . import JsonCodec
from ._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'infer_payload_format',
    'materialize_file_payload',
    'parse_text_payload',
]


# SECTION: FUNCTIONS ======================================================== #


def infer_payload_format(
    text: str,
) -> str:
    """
    Infer JSON vs CSV from payload text.

    Parameters
    ----------
    text : str
        The payload text to analyze.

    Returns
    -------
    str
        The inferred format: either 'json' or 'csv'.
    """
    stripped = text.lstrip()
    if stripped.startswith(('{', '[')):
        return 'json'
    return 'csv'


def materialize_file_payload(
    source: object,
    *,
    format_hint: str | None,
    format_explicit: bool,
) -> JSONData | object:
    """
    Return structured payloads when *source* references a file.

    Parameters
    ----------
    source : object
        The source payload, potentially a file path.
    format_hint : str | None
        An optional format hint (e.g., 'json', 'csv').
    format_explicit : bool
        Whether the format hint was explicitly provided.

    Returns
    -------
    JSONData | object
        The materialized payload if a file was read, otherwise the original
        source.

    Raises
    ------
    FileNotFoundError
        When the specified file does not exist.
    """
    if isinstance(source, (dict, list)):
        return cast(JSONData, source)
    if not isinstance(source, (str, os.PathLike)):
        return source

    normalized_hint = (format_hint or '').strip().lower()
    file: File | None = None

    if format_explicit and normalized_hint:
        try:
            file = File(source, FileFormat(normalized_hint))
        except ValueError:
            file = None
    else:
        file = File(source)

    if file is None or file.file_format is None:
        return source
    if not file.exists():
        if isinstance(source, str):
            stripped = source.lstrip()
            if (
                stripped.startswith(('{', '['))
                or '\n' in source
                or (normalized_hint == 'csv' and ',' in source)
            ):
                return parse_text_payload(source, format_hint)
        raise FileNotFoundError(f'File not found: {source}')
    return cast(JSONData, file.read())


def parse_text_payload(
    text: str,
    fmt_hint: str | None,
) -> JSONData | str:
    """
    Parse JSON/CSV text into a Python payload.

    Parameters
    ----------
    text : str
        The text payload to parse.
    fmt_hint : str | None
        An optional format hint (e.g., 'json', 'csv').

    Returns
    -------
    JSONData | str
        The parsed payload as JSON data or raw text.
    """
    effective = (fmt_hint or '').strip().lower() or infer_payload_format(text)
    if effective == 'json':
        return JsonCodec.parse(text)
    if effective == 'csv':
        return [dict(row) for row in csv.DictReader(StringIO(text))]
    return text
