"""
:mod:`etlplus.cli._handlers._output` module.

Output helpers shared by CLI handler implementations.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from ...file import File
from ...file import FileFormat
from ...utils import print_json
from ...utils import serialize_json
from ...utils._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'emit_json',
    'emit_markdown_table',
    'emit_json_payload',
    'emit_or_write',
    'write_json_output',
    'write_file_payload',
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
    print(serialize_json(data))


def emit_markdown_table(
    rows: Sequence[Mapping[str, Any]],
    *,
    columns: Sequence[str],
) -> None:
    """
    Emit rows as a Markdown table.

    Parameters
    ----------
    rows : Sequence[Mapping[str, Any]]
        Table rows to emit.
    columns : Sequence[str]
        Ordered column names to include in the table.
    """

    def _format_cell(value: Any) -> str:
        if value is None:
            return ''
        if isinstance(value, (dict, list)):
            rendered = serialize_json(value, sort_keys=True)
        else:
            rendered = str(value)
        return rendered.replace('|', '\\|').replace('\n', '<br>')

    header = '| ' + ' | '.join(columns) + ' |'
    separator = '| ' + ' | '.join('---' for _ in columns) + ' |'
    print(header)
    print(separator)
    for row in rows:
        print(
            '| '
            + ' | '.join(_format_cell(row.get(column)) for column in columns)
            + ' |',
        )


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


def emit_json_payload(
    payload: Any,
    *,
    pretty: bool,
    exit_code: int = 0,
) -> int:
    """
    Emit one JSON payload and return *exit_code*.

    Parameters
    ----------
    payload : Any
        The JSON-serializable payload to emit.
    pretty : bool
        Whether to pretty-print the JSON output.
    exit_code : int, optional
        The exit code to return after emitting the payload. Defaults to 0.

    Returns
    -------
    int
        The provided *exit_code* after emitting the JSON payload.
    """
    emit_json(payload, pretty=pretty)
    return exit_code


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
    File(output_path, FileFormat.JSON).write(data)
    print(f'{success_message} {output_path}')
    return True


def write_file_payload(
    payload: JSONData,
    target: str,
    *,
    format_hint: str | None,
) -> None:
    """
    Write a JSON-like payload to *target* using *format_hint* when given.

    Parameters
    ----------
    payload : JSONData
        The JSON-like data to write.
    target : str
        The file path to write the payload.
    format_hint : str | None
        An optional hint for the file format. If None, the format is inferred.

    Returns
    -------
    None
    """
    file_format = FileFormat.coerce(format_hint) if format_hint else None
    File(target, file_format=file_format).write(payload)
