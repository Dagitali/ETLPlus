"""
:mod:`etlplus.cli._handlers.history` module.

History-related handlers and helpers.
"""

from __future__ import annotations

from time import sleep
from typing import Any
from typing import Literal
from typing import cast

from .._history import HISTORY_TABLE_COLUMNS as _HISTORY_TABLE_COLUMNS
from .._history import REPORT_TABLE_COLUMNS as _REPORT_TABLE_COLUMNS
from .._history import HistoryReportBuilder
from .._history import HistoryView
from .common import _emit_json_or_table
from .common import _emit_json_payload

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'history_handler',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _load_history_records(
    *,
    raw: bool,
    job: str | None = None,
    limit: int | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Load, filter, and sort history records for CLI read commands."""
    return HistoryView.load_records(
        raw=raw,
        job=job,
        limit=limit,
        run_id=run_id,
        since=since,
        until=until,
        status=status,
    )


def _emit_follow_history(
    *,
    job: str | None = None,
    limit: int | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
) -> int:
    """Stream newly observed raw history records until interrupted."""
    seen: set[str] = set()
    try:
        while True:
            records = _load_history_records(
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
                _emit_json_payload(record, pretty=False)
            sleep(1.0)
    except KeyboardInterrupt:
        return 0


# SECTION: FUNCTIONS ======================================================== #


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
        Whether to stream newly observed raw history records until interrupted.
    job : str | None, optional
        Optional job name to filter history records by.
    json_output : bool, optional
        Whether to emit output as JSON (if ``False``, output will be emitted as
        a table if possible, or as JSON if the table format is not supported
        for the current output).
    limit : int | None, optional
        Optional maximum number of history records to emit (if not following).
    raw : bool, optional
        Whether to load raw persisted history records (if ``False``, normalized
        records will be loaded instead, which may involve additional processing
        and filtering but will ensure consistent formatting and field
        presence).
    pretty : bool, optional
        Whether to pretty-print JSON output.
    run_id : str | None, optional
        Optional run ID to filter history records by.
    since : str | None, optional
        Optional ISO 8601 timestamp to filter history records to those observed
        since the given time.
    until : str | None, optional
        Optional ISO 8601 timestamp to filter history records to those observed
        until the given time.
    status : str | None, optional
        Optional status value to filter history records by.
    table : bool, optional
        Whether to emit output as a table if possible (if ``False``, output
        will be emitted as JSON regardless of table support for the current
        output). Ignored if *json_output* is ``True``.

    Returns
    -------
    int
        Exit code (0 if history was emitted successfully, non-zero if any
        errors occurred).
    """
    HistoryView.validate_output_mode(json_output=json_output, table=table)
    if follow:
        return _emit_follow_history(
            job=job,
            limit=limit,
            run_id=run_id,
            since=since,
            status=status,
            until=until,
        )
    records = _load_history_records(
        job=job,
        limit=limit,
        raw=raw,
        run_id=run_id,
        since=since,
        until=until,
        status=status,
    )
    return _emit_json_or_table(
        records,
        columns=_HISTORY_TABLE_COLUMNS,
        pretty=pretty,
        table=table,
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
    Emit a grouped history report derived from normalized persisted runs.

    Parameters
    ----------
    group_by : Literal['day', 'job', 'status'], optional
        The field by which to group the history report.
    job : str | None, optional
        Optional job name to filter history records by.
    json_output : bool, optional
        Whether to emit output as JSON (if ``False``, output will be emitted as
        a table if possible, or as JSON if the table format is not supported
        for the current output).
    pretty : bool, optional
        Whether to pretty-print JSON output.
    since : str | None, optional
        Optional ISO 8601 timestamp to filter history records to those observed
        since the given time.
    table : bool, optional
        Whether to emit output as a table if possible (if ``False``, output
        will be emitted as JSON regardless of table support for the current
        output). Ignored if *json_output* is ``True``.
    until : str | None, optional
        Optional ISO 8601 timestamp to filter history records to those observed
        until the given time.

    Returns
    -------
    int
        Exit code (0 if report was emitted successfully, non-zero if any
        errors occurred).
    """
    HistoryView.validate_output_mode(json_output=json_output, table=table)
    records = HistoryView.load_records(
        job=job,
        raw=False,
        since=since,
        until=until,
    )
    report = HistoryReportBuilder.build(
        records,
        group_by=cast(Literal['day', 'job', 'status'], group_by),
    )
    return _emit_json_or_table(
        report,
        columns=_REPORT_TABLE_COLUMNS,
        pretty=pretty,
        table=table,
        table_rows=cast(list[dict[str, Any]], report['rows']),
    )


def status_handler(
    *,
    job: str | None = None,
    pretty: bool = True,
    run_id: str | None = None,
) -> int:
    """
    Emit the latest normalized run matching the given status filters.

    Parameters
    ----------
    job : str | None, optional
        Optional job name to filter history records by.
    pretty : bool, optional
        Whether to pretty-print JSON output.
    run_id : str | None, optional
        Optional run ID to filter history records by.

    Returns
    -------
    int
        Exit code (0 if the latest run was emitted successfully, non-zero if any
        errors occurred).
    """
    records = HistoryView.load_records(
        job=job,
        limit=1,
        raw=False,
        run_id=run_id,
    )
    if not records:
        return _emit_json_payload({}, pretty=pretty, exit_code=1)
    return _emit_json_payload(records[0], pretty=pretty)
