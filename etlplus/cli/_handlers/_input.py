"""
:mod:`etlplus.cli._handlers._input` module.

Input and payload-hydration helpers shared by CLI handlers.
"""

from __future__ import annotations

import csv
import io as _io
import os
import sys
from pathlib import Path
from typing import cast

from ...file import File
from ...file import FileFormat
from ...utils import parse_json
from ...utils._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'infer_payload_format',
    'materialize_file_payload',
    'parse_text_payload',
    'read_csv_rows',
    'read_stdin_text',
    'resolve_cli_payload',
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
    if stripped.startswith('{') or stripped.startswith('['):
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
            hint = (format_hint or '').strip().lower()
            if (
                stripped.startswith(('{', '['))
                or '\n' in source
                or (hint == 'csv' and ',' in source)
            ):
                return parse_text_payload(source, format_hint)
        raise FileNotFoundError(f'File not found: {source}')
    return cast(JSONData, file.read())


def parse_text_payload(
    text: str,
    fmt: str | None,
) -> JSONData | str:
    """
    Parse JSON/CSV text into a Python payload.

    Parameters
    ----------
    text : str
        The text payload to parse.
    fmt : str | None
        An optional format hint (e.g., 'json', 'csv').

    Returns
    -------
    JSONData | str
        The parsed payload as JSON data or raw text.
    """
    effective = (fmt or '').strip().lower() or infer_payload_format(text)
    if effective == 'json':
        return parse_json(text)
    if effective == 'csv':
        reader = csv.DictReader(_io.StringIO(text))
        return [dict(row) for row in reader]
    return text


def read_csv_rows(
    path: Path,
) -> list[dict[str, str]]:
    """
    Read CSV rows into dictionaries.

    Parameters
    ----------
    path : Path
        The path to the CSV file.

    Returns
    -------
    list[dict[str, str]]
        The list of CSV rows as dictionaries.
    """
    with path.open(newline='', encoding='utf-8') as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def read_stdin_text() -> str:
    """Return entire STDIN payload."""
    return sys.stdin.read()


def resolve_cli_payload(
    source: object,
    *,
    format_hint: str | None,
    format_explicit: bool,
    hydrate_files: bool = True,
) -> JSONData | object:
    """
    Normalize CLI-provided payloads, honoring STDIN and inline data.

    Parameters
    ----------
    source : object
        The source payload, potentially STDIN or a file path.
    format_hint : str | None
        An optional format hint (e.g., 'json', 'csv').
    format_explicit : bool
        Whether the format hint was explicitly provided.
    hydrate_files : bool, optional
        Whether to materialize file-based payloads. Default is True.

    Returns
    -------
    JSONData | object
        The resolved payload.
    """
    if isinstance(source, (os.PathLike, str)) and str(source) == '-':
        text = read_stdin_text()
        return parse_text_payload(text, format_hint)

    if not hydrate_files:
        return source

    return materialize_file_payload(
        source,
        format_hint=format_hint,
        format_explicit=format_explicit,
    )
