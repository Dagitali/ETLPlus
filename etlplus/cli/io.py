"""
:mod:`etlplus.cli.io` module.

Shared I/O helpers for CLI handlers (STDIN/STDOUT, payload hydration).
"""

from __future__ import annotations

import csv
import io as _io
import json
import os
import sys
from pathlib import Path
from typing import Any
from typing import cast

from ..file import File
from ..file import FileFormat
from ..types import JSONData
from ..utils import print_json

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'emit_json',
    'emit_or_write',
    'infer_payload_format',
    'materialize_file_payload',
    'parse_json_payload',
    'parse_text_payload',
    'read_csv_rows',
    'read_stdin_text',
    'resolve_cli_payload',
    'write_json_output',
]


# SECTION: FUNCTIONS ======================================================== #


def emit_json(
    data: Any,
    *,
    pretty: bool,
) -> None:
    """
    Emit JSON honoring pretty/compact preference.

    Parameters
    ----------
    data : Any
        Data to serialize as JSON.
    pretty : bool
        Whether to pretty-print JSON output.
    """
    if pretty:
        print_json(data)
        return
    dumped = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
    print(dumped)


def emit_or_write(
    data: Any,
    output_path: str | None,
    *,
    pretty: bool,
    success_message: str,
) -> None:
    """
    Emit JSON or persist to disk based on *output_path*.

    Parameters
    ----------
    data : Any
        The data to serialize.
    output_path : str | None
        Target file path; when falsy or ``'-'`` data is emitted to STDOUT.
    pretty : bool
        Whether to pretty-print JSON emission.
    success_message : str
        Message printed when writing to disk succeeds.
    """
    if write_json_output(
        data,
        output_path,
        success_message=success_message,
    ):
        return
    emit_json(data, pretty=pretty)


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

    path = Path(source)

    normalized_hint = (format_hint or '').strip().lower()
    fmt: FileFormat | None = None

    if format_explicit and normalized_hint:
        try:
            fmt = FileFormat(normalized_hint)
        except ValueError:
            fmt = None
    elif not format_explicit:
        suffix = path.suffix.lower().lstrip('.')
        if suffix:
            try:
                fmt = FileFormat(suffix)
            except ValueError:
                fmt = None

    if fmt is None:
        return source
    if not path.exists():
        if isinstance(source, str):
            stripped = source.lstrip()
            hint = (format_hint or '').strip().lower()
            if (
                stripped.startswith(('{', '['))
                or '\n' in source
                or (hint == 'csv' and ',' in source)
            ):
                return parse_text_payload(source, format_hint)
        raise FileNotFoundError(f'File not found: {path}')
    if fmt == FileFormat.CSV:
        return read_csv_rows(path)
    return File(path, fmt).read()


def parse_json_payload(text: str) -> JSONData:
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
    except json.JSONDecodeError as e:
        raise ValueError(
            f'Invalid JSON payload: {e.msg} (pos {e.pos})',
        ) from e


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
        return parse_json_payload(text)
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


def write_json_output(
    data: Any,
    output_path: str | None,
    *,
    success_message: str,
) -> bool:
    """
    Persist JSON data to disk when output path provided.

    Parameters
    ----------
    data : Any
        The data to serialize as JSON.
    output_path : str | None
        The output file path, or None/'-' to skip writing.
    success_message : str
        The message to print upon successful write.

    Returns
    -------
    bool
        True if data was written to disk; False if not.
    """
    if not output_path or output_path == '-':
        return False
    File(Path(output_path), FileFormat.JSON).write(data)
    print(f'{success_message} {output_path}')
    return True
