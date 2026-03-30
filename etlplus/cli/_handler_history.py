"""
:mod:`etlplus.cli._handler_history` module.

History, report, and status handler implementations for the CLI facade.
"""

from __future__ import annotations

from typing import Any
from typing import Literal
from typing import cast

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
    history_view: Any,
    raw: bool = False,
    job: str | None = None,
    limit: int | None = None,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """
    Load, filter, and sort history records for CLI read commands.

    Parameters
    ----------
    history_view : Any
        History view instance to load records from.
    raw : bool, optional
        Whether to load raw records instead of normalized runs.
    job : str | None, optional
        Optional job name filter.
    limit : int | None, optional
        Optional maximum number of records to load.
    run_id : str | None, optional
        Optional run ID filter.
    since : str | None, optional
        Optional ISO 8601 timestamp filter to load records started after the
        given time.
    until : str | None, optional
        Optional ISO 8601 timestamp filter to load records started before the
        given time.
    status : str | None, optional
        Optional status filter.

    Returns
    -------
    list[dict[str, Any]]
        A list of history records matching the specified filters.
    """
    load_kwargs: dict[str, Any] = {'raw': raw}
    for key, value in (
        ('job', job),
        ('limit', limit),
        ('run_id', run_id),
        ('since', since),
        ('until', until),
        ('status', status),
    ):
        if value is not None:
            load_kwargs[key] = value
    return history_view.load_records(
        **load_kwargs,
    )


def emit_follow_history(
    *,
    load_history_records_fn: Any,
    history_view: Any,
    emit_json_payload_fn: Any,
    sleep_fn: Any,
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
    load_history_records_fn: Any
        Function to load history records with the same signature as
        :func:`load_history_records`.
    history_view: Any
        History view instance to fingerprint records with.
    emit_json_payload_fn: Any
        Function to emit JSON payloads with the same signature as
        :func:`etlplus.cli._handler_common.emit_json_payload`.
    sleep_fn: Any
        Function to sleep for a given number of seconds, like
        :func:`time.sleep`.
    job : str | None, optional
        Optional job name filter.
    limit : int | None, optional
        Optional maximum number of records to load per poll.
    run_id : str | None, optional
        Optional run ID filter.
    since : str | None, optional
        Optional ISO 8601 timestamp filter to load records started after the
        given time.
    until : str | None, optional
        Optional ISO 8601 timestamp filter to load records started before the
        given time.
    status : str | None, optional
        Optional status filter.

    Returns
    -------
    int
        Exit code. ``0`` indicates success, non-zero indicates failure.
    """
    seen: set[str] = set()
    try:
        while True:
            records = load_history_records_fn(
                job=job,
                limit=limit,
                raw=True,
                run_id=run_id,
                since=since,
                until=until,
                status=status,
            )
            for record in reversed(records):
                fingerprint = history_view.fingerprint(record)
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                emit_json_payload_fn(record, pretty=False)
            sleep_fn(1.0)
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
    emit_follow_history_fn: Any,
    emit_history_payload_fn: Any,
    load_history_records_fn: Any,
    columns: tuple[str, ...],
) -> int:
    """
    Emit persisted local run history.

    Parameters
    ----------
    follow : bool, optional
        Whether to stream newly observed records until interrupted. Default is
        ``False``.
    job : str | None, optional
        Optional job name filter.
    json_output : bool, optional
        Whether to emit JSON output instead of a human-friendly table. Default
        is ``False``.
    limit : int | None, optional
        Optional maximum number of records to load. Default is ``None`` (no
        limit).
    raw : bool, optional
        Whether to load raw records instead of normalized runs. Default is
        ``False``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.
    run_id : str | None, optional
        Optional run ID filter. Default is ``None`` (no filter).
    since : str | None, optional
        Optional ISO 8601 timestamp filter to load records started after the
        given time. Default is ``None`` (no filter).
    until : str | None, optional
        Optional ISO 8601 timestamp filter to load records started before the
        given time. Default is ``None`` (no filter).
    status : str | None, optional
        Optional status filter. Default is ``None`` (no filter).
    table : bool, optional
        Whether to format output as a human-friendly table. Default is
        ``False``.
    emit_follow_history_fn: Any
        Function to emit follow history with the same signature as
        :func:`emit_follow_history`.
    emit_history_payload_fn: Any
        Function to emit history payloads with the same signature as
        :func:`etlplus.cli._handler_common.emit_history_payload`.
    load_history_records_fn: Any
        Function to load history records with the same signature as
        :func:`load_history_records`.
    columns: tuple[str, ...]
        Column names to include in table output. Ignored if *table* is
        ``False``. Default is an empty tuple.

    Returns
    -------
    int
        Exit code. ``0`` indicates success, non-zero indicates failure.
    """
    if follow:
        return emit_follow_history_fn(
            job=job,
            limit=limit,
            run_id=run_id,
            since=since,
            status=status,
            until=until,
        )
    return emit_history_payload_fn(
        load_history_records_fn(
            job=job,
            limit=limit,
            raw=raw,
            run_id=run_id,
            since=since,
            until=until,
            status=status,
        ),
        columns=columns,
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
    load_history_records_fn: Any,
    report_builder: Any,
    emit_history_payload_fn: Any,
    columns: tuple[str, ...],
) -> int:
    """
    Emit a grouped history report derived from normalized persisted runs.

    Parameters
    ----------
    group_by : Literal['day', 'job', 'status'], optional
        The field by which to group the report. Default is 'job'.
    job : str | None, optional
        Optional job name filter.
    json_output : bool, optional
        Whether to emit JSON output instead of a human-friendly table. Default
        is ``False``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.
    since : str | None, optional
        Optional ISO 8601 timestamp filter to load records started after the
        given time. Default is ``None`` (no filter).
    table : bool, optional
        Whether to format output as a human-friendly table. Default is
        ``False``.
    until : str | None, optional
        Optional ISO 8601 timestamp filter to load records started before the
        given time. Default is ``None`` (no filter).
    load_history_records_fn: Any
        Function to load history records with the same signature as
        :func:`load_history_records`.
    report_builder: Any
        Function to build the report with the same signature as
        :func:`report_builder`.
    emit_history_payload_fn: Any
        Function to emit history payloads with the same signature as
        :func:`etlplus.cli._handler_common.emit_history_payload`.
    columns: tuple[str, ...]
        Column names to include in table output. Ignored if *table* is
        ``False``. Default is an empty tuple.

    Returns
    -------
    int
        Exit code. ``0`` indicates success, non-zero indicates failure.
    """
    report = report_builder.build(
        load_history_records_fn(
            job=job,
            since=since,
            until=until,
        ),
        group_by=group_by,
    )
    return emit_history_payload_fn(
        report,
        columns=columns,
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
    load_history_records_fn: Any,
    emit_json_payload_fn: Any,
) -> int:
    """
    Emit the latest normalized run matching the given status filters.

    Parameters
    ----------
    job : str | None, optional
        Optional job name filter.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.
    run_id : str | None, optional
        Optional run ID filter.
    load_history_records_fn: Any
        Function to load history records with the same signature as
        :func:`load_history_records`.
    emit_json_payload_fn: Any
        Function to emit JSON payloads with the same signature as
        :func:`etlplus.cli._handler_common.emit_json_payload`.

    Returns
    -------
    int
        Exit code. ``0`` indicates success, non-zero indicates failure.
    """
    records = load_history_records_fn(
        job=job,
        limit=1,
        run_id=run_id,
    )
    if not records:
        return emit_json_payload_fn({}, pretty=pretty, exit_code=1)
    return emit_json_payload_fn(records[0], pretty=pretty)
