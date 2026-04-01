"""
:mod:`etlplus.cli._handlers.history` module.

History, report, and status handler implementations for the CLI facade.
"""

from __future__ import annotations

from time import sleep
from typing import Any
from typing import Literal
from typing import cast

from . import _output
from ._history_report import REPORT_TABLE_COLUMNS
from ._history_report import HistoryReportBuilder
from ._history_view import HISTORY_TABLE_COLUMNS
from ._history_view import HistoryView

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'emit_follow_history',
    'history_handler',
    'load_history_records',
    'report_handler',
    'status_handler',
]


# SECTION: FUNCTIONS ======================================================== #


def _emit_history_payload(
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
        The columns to include in the Markdown table.
    pretty : bool
        Whether to pretty-print JSON output.
    table : bool, optional
        Whether to emit a Markdown table. Default is ``False``.
    json_output : bool, optional
        Whether to emit JSON output. Default is ``False``.
    table_rows : list[dict[str, Any]] | None, optional
        Optional precomputed table rows. Default is ``None``.
    exit_code : int, optional
        CLI exit code to return. Default is ``0``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    HistoryView.validate_output_mode(json_output=json_output, table=table)
    if table:
        _output.emit_markdown_table(
            table_rows
            if table_rows is not None
            else cast(list[dict[str, Any]], payload),
            columns=columns,
        )
        return exit_code
    return _output.emit_json_payload(payload, pretty=pretty, exit_code=exit_code)


def load_history_records(
    *,
    job: str | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    raw: bool = False,
) -> list[dict[str, Any]]:
    """
    Load filtered history records for CLI read commands.

    Parameters
    ----------
    job : str | None, optional
        Optional job name to filter records. Default is ``None``.
    run_id : str | None, optional
        Optional run ID to filter records. Default is ``None``.
    since : str | None, optional
        Optional ISO 8601 timestamp to filter records created after the given
        time.  Default is ``None``.
    until : str | None, optional
        Optional ISO 8601 timestamp to filter records created before the given
        time.  Default is ``None``.
    status : str | None, optional
        Optional status to filter records. Default is ``None``.
    limit : int | None, optional
        Optional maximum number of records to load. Default is ``None``.
    raw : bool, optional
        Whether to load raw records instead of normalized runs. Default is
        ``False``.

    Returns
    -------
    list[dict[str, Any]]
        History records matching the specified filters.
    """
    load_kwargs: dict[str, Any] = {'raw': raw}
    load_kwargs.update(
        {
            key: value
            for key, value in {
                'job': job,
                'limit': limit,
                'run_id': run_id,
                'since': since,
                'until': until,
                'status': status,
            }.items()
            if value is not None
        },
    )
    return HistoryView.load_records(**load_kwargs)


def emit_follow_history(
    *,
    job: str | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    limit: int | None = None,
) -> int:
    """
    Stream newly observed raw history records until interrupted.

    Parameters
    ----------
    job : str | None, optional
        Optional job name filter. Default is ``None``.
    run_id : str | None, optional
        Optional run ID filter. Default is ``None``.
    since : str | None, optional
        Optional ISO 8601 timestamp filter to load records created after the
        given time.  Default is ``None``.
    until : str | None, optional
        Optional ISO 8601 timestamp filter to load records created before the
        given time.  Default is ``None``.
    status : str | None, optional
        Optional status filter. Default is ``None``.
    limit : int | None, optional
        Optional maximum number of records to load per iteration. Default is
        ``None``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    seen: set[str] = set()
    try:
        while True:
            records = load_history_records(
                job=job,
                limit=limit,
                raw=True,
                run_id=run_id,
                since=since,
                until=until,
                status=status,
            )
            for record in reversed(records):
                fingerprint = HistoryView.fingerprint(record)
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                _output.emit_json_payload(record, pretty=False)
            sleep(1.0)
    except KeyboardInterrupt:
        return 0


def history_handler(
    *,
    job: str | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    follow: bool = False,
    raw: bool = False,
    table: bool = False,
    json_output: bool = False,
    pretty: bool = True,
) -> int:
    """
    Emit persisted local run history.

    Parameters
    ----------
    job : str | None, optional
        Optional job name filter. Default is ``None``.
    run_id : str | None, optional
        Optional run ID filter. Default is ``None``.
    since : str | None, optional
        Optional ISO 8601 timestamp filter to load records created after the
        given time.  Default is ``None``.
    until : str | None, optional
        Optional ISO 8601 timestamp filter to load records created before the
        given time.  Default is ``None``.
    status : str | None, optional
        Optional status filter. Default is ``None``.
    limit : int | None, optional
        Optional maximum number of records to load. Default is ``None``.
    follow : bool, optional
        Whether to stream newly observed records until interrupted. Default is
        ``False``.
    raw : bool, optional
        Whether to load raw records instead of normalized runs. Default is
        ``False``.
    table : bool, optional
        Whether to emit output as a human-friendly table instead of JSON.
        Default is ``False``.
    json_output : bool, optional
        Whether to emit JSON instead of a human-friendly table. Default is
        ``False``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    if follow:
        return emit_follow_history(
            job=job,
            limit=limit,
            run_id=run_id,
            since=since,
            until=until,
            status=status,
        )

    return _emit_history_payload(
        load_history_records(
            job=job,
            limit=limit,
            raw=raw,
            run_id=run_id,
            since=since,
            until=until,
            status=status,
        ),
        columns=HISTORY_TABLE_COLUMNS,
        pretty=pretty,
        table=table,
        json_output=json_output,
    )


def report_handler(
    *,
    group_by: Literal['day', 'job', 'status'] = 'job',
    job: str | None = None,
    since: str | None = None,
    until: str | None = None,
    table: bool = False,
    json_output: bool = False,
    pretty: bool = True,
) -> int:
    """
    Emit a grouped history report derived from normalized runs.

    Parameters
    ----------
    group_by : Literal['day', 'job', 'status'], optional
        The field by which to group the report. Default is 'job'.
    job : str | None, optional
        Optional job name filter. Default is ``None``.
    since : str | None, optional
        Optional ISO 8601 timestamp filter to load records created after the
        given time.  Default is ``None``.
    until : str | None, optional
        Optional ISO 8601 timestamp filter to load records created before the
        given time.  Default is ``None``.
    table : bool, optional
        Whether to emit output as a human-friendly table instead of JSON.
        Default is ``False``.
    json_output : bool, optional
        Whether to emit JSON instead of a human-friendly table. Default is
        ``False``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    report = HistoryReportBuilder.build(
        load_history_records(
            job=job,
            since=since,
            until=until,
        ),
        group_by=group_by,
    )
    return _emit_history_payload(
        report,
        columns=REPORT_TABLE_COLUMNS,
        pretty=pretty,
        table=table,
        json_output=json_output,
        table_rows=cast(list[dict[str, Any]], report['rows']),
    )


def status_handler(
    *,
    job: str | None = None,
    run_id: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Emit the latest normalized run matching the given filters.

    Parameters
    ----------
    job : str | None, optional
        Optional job name filter. Default is ``None``.
    run_id : str | None, optional
        Optional run ID filter. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    records = load_history_records(
        job=job,
        limit=1,
        run_id=run_id,
    )
    if not records:
        return _output.emit_json_payload({}, pretty=pretty, exit_code=1)
    return _output.emit_json_payload(records[0], pretty=pretty)
