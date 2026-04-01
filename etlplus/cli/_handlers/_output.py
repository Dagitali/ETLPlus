"""
:mod:`etlplus.cli._handlers._output` module.

Output helpers shared by CLI handler implementations.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from typing import cast

from ...file import File
from ...file import FileFormat
from ...runtime import ReadinessReportBuilder
from ...utils import print_json
from ...utils import serialize_json
from ...utils._types import JSONData
from . import _lifecycle
from ._history import HistoryView

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'complete_output',
    'emit_json',
    'emit_markdown_table',
    'emit_history_payload',
    'emit_json_payload',
    'emit_or_write',
    'emit_readiness_report',
    'emit_render_output',
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


def emit_history_payload(
    payload: Any,
    *,
    columns: tuple[str, ...],
    pretty: bool,
    table: bool = False,
    json_output: bool = False,
    table_rows: list[dict[str, Any]] | None = None,
    exit_code: int = 0,
) -> int:
    """
    Emit history data as JSON or a Markdown table.

    Parameters
    ----------
    payload : Any
        The history data to emit.
    columns : tuple[str, ...]
        The columns to include in the table output.
    pretty : bool
        Whether to pretty-print the JSON output.
    table : bool, optional
        Whether to emit the data as a Markdown table. Defaults to False.
    json_output : bool, optional
        Whether to emit the data as JSON. Defaults to False.
    table_rows : list[dict[str, Any]] | None, optional
        The rows to include in the table output. Defaults to None.
    exit_code : int, optional
        The exit code to return after emitting the payload. Defaults to 0.

    Returns
    -------
    int
        The provided *exit_code* after emitting the payload.
    """
    HistoryView.validate_output_mode(json_output=json_output, table=table)
    if table:
        emit_markdown_table(
            table_rows
            if table_rows is not None
            else cast(list[dict[str, Any]], payload),
            columns=columns,
        )
        return exit_code
    return emit_json_payload(payload, pretty=pretty, exit_code=exit_code)


def complete_output(
    context: _lifecycle.CommandContext,
    payload: Any,
    *,
    mode: str,
    pretty: bool = True,
    output_path: str | None = None,
    format_hint: str | None = None,
    success_message: str | None = None,
    **fields: Any,
) -> int:
    """
    Emit completion for *context* and route the payload by output mode.

    Parameters
    ----------
    context : _lifecycle.CommandContext
        The command context for the completed command.
    payload : Any
        The JSON-serializable payload to emit or write.
    mode : str
        The output mode, one of "json", "or_write", "file", or "json_file".
    pretty : bool, optional
        Whether to pretty-print the JSON output when *mode* is "json" or
        "json_file". Defaults to True.
    output_path : str | None, optional
        The file path to write the payload when *mode* is "file" or
        "json_file". Defaults to ``None``, which writes to STDOUT for
        "json_file" mode and is ignored for "file" mode.
    format_hint : str | None, optional
        An optional hint for the file format when *mode* is "file". Ignored
        for other modes. Defaults to ``None``.
    success_message : str | None, optional
        An optional message to print upon successful output when *mode* is
        "or_write" or "file". Ignored for other modes. Defaults to ``None``.
    **fields : Any
        Additional fields to include in the emitted lifecycle event payload.

    Returns
    -------
    int
        The exit code to return after emitting the payload, typically 0 for
        success or 1 for failure depending on the context and output mode.

    Raises
    ------
    AssertionError
        If *mode* is not one of the supported output modes.
    """
    _lifecycle.complete_command(context, **fields)
    match mode:
        case 'json':
            return emit_json_payload(payload, pretty=pretty)
        case 'or_write':
            emit_or_write(
                payload,
                output_path,
                pretty=pretty,
                success_message=cast(str, success_message),
            )
            return 0
        case 'file':
            target = cast(str, output_path)
            write_file_payload(
                cast(JSONData, payload),
                target,
                format_hint=format_hint,
            )
            print(f'{cast(str, success_message)} {target}')
            return 0
        case 'json_file':
            write_json_output(
                payload,
                cast(str, output_path),
                success_message=cast(str, success_message),
            )
            return 0
        case _:
            raise AssertionError(f'Unsupported completion mode: {mode!r}')


def emit_render_output(
    rendered_chunks: list[str],
    *,
    output_path: str | None,
    pretty: bool,
    quiet: bool,
    schema_count: int,
) -> int:
    """
    Write rendered SQL to a file or STDOUT.

    Parameters
    ----------
    rendered_chunks : list[str]
        The list of rendered SQL chunks to emit.
    output_path : str | None
        The file path to write the rendered SQL. If None or '-', writes to
        STDOUT.
    pretty : bool
        Whether to pretty-print the SQL output. Defaults to True.
    quiet : bool
        Whether to suppress informational messages. Defaults to False.
    schema_count : int
        The number of schemas rendered.

    Returns
    -------
    int
        The exit code, typically 0 for success.
    """
    sql_text = '\n'.join(chunk.rstrip() for chunk in rendered_chunks).rstrip() + '\n'
    rendered_output = sql_text if pretty else sql_text.rstrip('\n')
    if output_path and output_path != '-':
        Path(output_path).write_text(rendered_output, encoding='utf-8')
        if not quiet:
            print(f'Rendered {schema_count} schema(s) to {output_path}')
        return 0

    print(rendered_output, end='')
    return 0


def emit_readiness_report(
    *,
    config: str | None,
    pretty: bool,
) -> int:
    """
    Build and emit one readiness report.

    Parameters
    ----------
    config : str | None
        The path to the configuration file. If None, uses the default
        configuration.
    pretty : bool
        Whether to pretty-print the JSON output. Defaults to True.

    Returns
    -------
    int
        The exit code, 0 if the report status is 'ok', 1 otherwise.
    """
    report = ReadinessReportBuilder.build(config_path=config)
    return emit_json_payload(
        report,
        pretty=pretty,
        exit_code=0 if report.get('status') == 'ok' else 1,
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
