"""
:mod:`etlplus.cli._handlers.history` module.

History, report, and status handler implementations for the CLI facade.
"""

from __future__ import annotations

from time import sleep
from typing import Any
from typing import Literal
from typing import cast

from .._history import HISTORY_TABLE_COLUMNS
from .._history import REPORT_TABLE_COLUMNS
from .._history import HistoryReportBuilder
from .._history import HistoryView
from . import output as _output

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


def load_history_records(
    *,
    raw: bool = False,
    job: str | None = None,
    limit: int | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """
    Load filtered history records for CLI read commands.

    Parameters
    ----------
    raw : bool, optional
        Whether to load raw records instead of normalized runs. Default is
        ``False``.
    job : str | None, optional
        Optional job name to filter records. Default is ``None``.
    limit : int | None, optional
        Optional maximum number of history records to load. Default is
        ``None``.
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

    Returns
    -------
    list[dict[str, Any]]
        A list of history records matching the specified filters.
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
    limit: int | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
) -> int:
    """
    Stream newly observed raw history records until interrupted.

    Parameters
    ----------
    job : str | None, optional
        Optional job name filter. Default is ``None``.
    limit : int | None, optional
        Optional maximum number of records to load per iteration. Default
        is ``None``.
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

    Returns
    -------
    int
        Exit code ``0`` on successful interruption; non-zero on error.
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
    follow: bool = False,
    job: str | None = None,
    json_output: bool = False,
    limit: int | None = None,
    raw: bool = False,
    pretty: bool = True,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    table: bool = False,
) -> int:
    """
    Emit persisted local run history.

    Parameters
    ----------
    follow : bool, optional
        Whether to stream newly observed records until interrupted. Default is
        ``False``.
    job : str | None, optional
        Optional job name filter. Default is ``None``.
    json_output : bool, optional
        Whether to emit output as JSON instead of a human-friendly table.
        Default is ``False``.
    limit : int | None, optional
        Optional maximum number of records to load. Default is ``None``.
    raw : bool, optional
        Whether to load raw records instead of normalized runs. Default is
        ``False``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.
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
    table : bool, optional
        Whether to emit output as a human-friendly table instead of JSON.
        Default is ``False``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
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

    return _output.emit_history_payload(
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
    json_output: bool = False,
    pretty: bool = True,
    since: str | None = None,
    table: bool = False,
    until: str | None = None,
) -> int:
    """
    Emit a grouped history report derived from normalized runs.

    Parameters
    ----------
    group_by : {'day', 'job', 'status'}, optional
        The field by which to group the report. Default is 'job'.
    job : str | None, optional
        Optional job name filter. Default is ``None``.
    json_output : bool, optional
        Whether to emit output as JSON instead of a human-friendly table.
        Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.
    since : str | None, optional
        Optional ISO 8601 timestamp filter to load records created after the
        given time.  Default is ``None``.
    table : bool, optional
        Whether to emit output as a human-friendly table instead of JSON.
        Default is ``False``.
    until : str | None, optional
        Optional ISO 8601 timestamp filter to load records created before the
        given time.  Default is ``None``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    report = HistoryReportBuilder.build(
        load_history_records(
            job=job,
            since=since,
            until=until,
        ),
        group_by=group_by,
    )
    return _output.emit_history_payload(
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
    pretty: bool = True,
    run_id: str | None = None,
) -> int:
    """
    Emit the latest normalized run matching the given filters.

    Parameters
    ----------
    job : str | None, optional
        Optional job name filter. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.
    run_id : str | None, optional
        Optional run ID filter. Default is ``None``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    records = load_history_records(
        job=job,
        limit=1,
        run_id=run_id,
    )
    if not records:
        return _output.emit_json_payload({}, pretty=pretty, exit_code=1)
    return _output.emit_json_payload(records[0], pretty=pretty)
