"""
:mod:`etlplus.cli._history` module.

Internal history and report helpers shared by CLI handlers.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from typing import Literal
from typing import cast

from ..history import HistoryStore
from ..utils.data import serialize_json

# SECTION: TYPE ALIASES ===================================================== #


type HistoryRecord = Mapping[str, Any]


# SECTION: CONSTANTS ======================================================== #


HISTORY_TABLE_COLUMNS = (
    'run_id',
    'status',
    'job_name',
    'pipeline_name',
    'started_at',
    'finished_at',
    'duration_ms',
)


REPORT_TABLE_COLUMNS = (
    'group',
    'runs',
    'succeeded',
    'failed',
    'running',
    'other',
    'success_rate_pct',
    'avg_duration_ms',
    'min_duration_ms',
    'max_duration_ms',
    'last_started_at',
)


# STATUS: CLASSES =========================================================== #


class HistoryView:
    """Shared query helpers for persisted history records."""

    # -- Static Methods -- #

    @staticmethod
    def fingerprint(
        record: HistoryRecord,
    ) -> str:
        """
        Return a stable fingerprint for a persisted history record.

        The fingerprint is used to identify related start and finish events
        that should be merged into one run record for CLI output. The
        fingerprint is not guaranteed to be unique across all records, but it
        should be stable for related events of the same run and different
        enough across unrelated runs to avoid collisions in practice.
        """
        return serialize_json(record, sort_keys=True)

    @staticmethod
    def parse_timestamp(
        value: object,
    ) -> datetime | None:
        """
        Parse an ISO-8601 timestamp used in persisted history records.

        The parser should be lenient to accommodate different timestamp
        formats. The expected format is a subset of ISO-8601 that includes date
        and time components, with optional timezone information. The parser
        should also handle timestamps that end with 'Z' to indicate UTC time,
        as well as timestamps that include timezone offsets. If the input value
        is not a valid timestamp string, the function should return ``None``.

        Examples of valid timestamp formats include:
        - "2023-01-01T12:00:00Z"
        - "2023-01-01T12:00:00+00:00"
        - "2023-01-01T12:00:00-05:00"
        - "2023-01-01T12:00:00"

        Parameters
        ----------
        value : object
            The value to parse as a timestamp.

        Returns
        -------
        datetime | None
            A datetime object if parsing is successful, or ``None`` if the
            input value is not a valid timestamp string.
        """
        if not isinstance(value, str) or not value:
            return None
        normalized = value.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    @staticmethod
    def record_timestamp(
        record: HistoryRecord,
    ) -> datetime | None:
        """
        Return the normalized timestamp used for record filtering.

        The timestamp is derived from the record's :attr:`started_at` or
        :attr:`finished_at` fields, whichever is available. If neither field
        contains a valid timestamp, the function returns ``None``. This allows
        for consistent filtering of history records based on their relevant
        timestamps, even if the original record formats vary.

        Parameters
        ----------
        record : HistoryRecord
            The history record from which to extract the timestamp.

        Returns
        -------
        datetime | None
            A datetime object if a valid timestamp is found, otherwise ``None``.
        """
        return HistoryView.parse_timestamp(
            record.get('started_at') or record.get('finished_at'),
        )

    @staticmethod
    def sort_key(
        record: HistoryRecord,
    ) -> tuple[str, str]:
        """
        Return a reverse-sortable key for history records.

        The key is a tuple of (timestamp, run_id), where the timestamp is
        derived from the record's :attr:`started_at` or :attr:`finished_at`
        fields, and the run_id is derived from the record's :attr:`run_id`
        field. This allows for consistent sorting of history records in reverse
        chronological order.

        Parameters
        ----------
        record : HistoryRecord
            The history record from which to extract the sort key.

        Returns
        -------
        tuple[str, str]
            A tuple containing the timestamp and run_id for sorting.
        """
        timestamp = cast(
            str,
            record.get('started_at') or record.get('finished_at') or '',
        )
        run_id = cast(str, record.get('run_id') or '')
        return (timestamp, run_id)

    @staticmethod
    def validate_output_mode(
        *,
        json_output: bool,
        table: bool,
    ) -> None:
        """
        Validate that at most one explicit history output mode was requested.

        The CLI supports multiple output modes for history commands, such as
        JSON and table formats. This function checks that the user did not
        request more than one explicit output mode at the same time, which
        would be ambiguous. If multiple modes are requested, the function
        raises a :class:`ValueError` to prompt the user to choose only one.

        Parameters
        ----------
        json_output : bool
            Whether JSON output mode was requested.
        table : bool
            Whether table output mode was requested.
        """
        if json_output and table:
            raise ValueError('choose either json output or table output, not both')

    # -- Class Methods -- #

    @classmethod
    def load_records(
        cls,
        *,
        raw: bool,
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
        raw : bool
            Whether to load raw history records.
        job : str | None, optional
            Filter records by job name.
        limit : int | None, optional
            Limit the number of records returned.
        run_id : str | None, optional
            Filter records by run ID.
        since : str | None, optional
            Filter records starting from this timestamp.
        until : str | None, optional
            Filter records up to this timestamp.
        status : str | None, optional
            Filter records by status.

        Returns
        -------
        list[dict[str, Any]]
            A list of filtered and sorted history records.
        """
        records = sorted(
            cls._matching_records(
                raw=raw,
                job=job,
                run_id=run_id,
                since=cls.parse_timestamp(since),
                until=cls.parse_timestamp(until),
                status=status,
            ),
            key=cls.sort_key,
            reverse=True,
        )
        if limit is None:
            return records
        return records[:limit]

    @classmethod
    def matches(
        cls,
        record: HistoryRecord,
        *,
        job: str | None = None,
        run_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        status: str | None = None,
    ) -> bool:
        """
        Return whether a history record matches CLI filter values.

        Parameters
        ----------
        record : HistoryRecord
            The history record to check.
        job : str | None, optional
            Filter by job name.
        run_id : str | None, optional
            Filter by run ID.
        since : datetime | None, optional
            Filter records starting from this timestamp.
        until : datetime | None, optional
            Filter records up to this timestamp.
        status : str | None, optional
            Filter by status.

        Returns
        -------
        bool
            True if the record matches the filters, False otherwise.
        """
        if job is not None and record.get('job_name') != job:
            return False
        if run_id is not None and record.get('run_id') != run_id:
            return False
        if status is not None and record.get('status') != status:
            return False
        record_timestamp = cls.record_timestamp(record)
        if since is not None:
            if record_timestamp is None or record_timestamp < since:
                return False
        if until is not None:
            if record_timestamp is None or record_timestamp > until:
                return False
        return True

    @classmethod
    def _matching_records(
        cls,
        *,
        raw: bool,
        job: str | None,
        run_id: str | None,
        since: datetime | None,
        until: datetime | None,
        status: str | None,
    ) -> list[dict[str, Any]]:
        """
        Return materialized history records that match the given filters.

        Parameters
        ----------
        raw : bool
            Whether to load raw history records.
        job : str | None, optional
            Filter records by job name.
        run_id : str | None, optional
            Filter records by run ID.
        since : datetime | None, optional
            Filter records starting from this timestamp.
        until : datetime | None, optional
            Filter records up to this timestamp.
        status : str | None, optional
            Filter records by status.

        Returns
        -------
        list[dict[str, Any]]
            A list of filtered history records.
        """
        history_store = HistoryStore.from_environment()
        records_iter = (
            history_store.iter_records() if raw else history_store.iter_runs()
        )
        return [
            dict(record)
            for record in records_iter
            if cls.matches(
                record,
                job=job,
                run_id=run_id,
                since=since,
                until=until,
                status=status,
            )
        ]


class HistoryReportBuilder:
    """Aggregation helpers for grouped history-report output."""

    # -- Static Methods -- #

    @staticmethod
    def increment_metric(
        bucket: dict[str, Any],
        key: str,
        amount: int = 1,
    ) -> None:
        """
        Increment an integer metric stored inside a mutable report bucket.

        Parameters
        ----------
        bucket : dict[str, Any]
            The report bucket to update.
        key : str
            The key of the metric to increment.
        amount : int, optional
            The amount to increment by (default is 1).
        """
        bucket[key] = int(bucket.get(key) or 0) + amount

    @staticmethod
    def report_group_key(
        record: Mapping[str, Any],
        *,
        group_by: Literal['day', 'job', 'status'],
    ) -> str:
        """
        Return the grouping key for one normalized history record.

        Parameters
        ----------
        record : Mapping[str, Any]
            The normalized history record.
        group_by : Literal['day', 'job', 'status']
            The grouping criterion.

        Returns
        -------
        str
            The grouping key for the record.
        """
        if group_by == 'job':
            return cast(str, record.get('job_name') or '(no job)')
        if group_by == 'status':
            return cast(str, record.get('status') or '(unknown)')
        timestamp = cast(
            str,
            record.get('started_at') or record.get('finished_at') or '',
        )
        return timestamp.split('T', maxsplit=1)[0] if 'T' in timestamp else '(unknown)'

    @staticmethod
    def success_rate_pct(
        succeeded: int,
        runs: int,
    ) -> float | None:
        """
        Return the success-rate percentage for the given counters.

        Parameters
        ----------
        succeeded : int
            The number of successful runs.
        runs : int
            The total number of runs.

        Returns
        -------
        float | None
            The success-rate percentage, or None if there are no runs.
        """
        if runs <= 0:
            return None
        return round((succeeded / runs) * 100, 2)

    # -- Class Methods -- #

    @classmethod
    def build(
        cls,
        records: list[dict[str, Any]],
        *,
        group_by: Literal['day', 'job', 'status'],
    ) -> dict[str, Any]:
        """
        Aggregate normalized history records into a grouped report.

        Parameters
        ----------
        records : list[dict[str, Any]]
            The list of normalized history records.
        group_by : Literal['day', 'job', 'status']
            The grouping criterion.

        Returns
        -------
        dict[str, Any]
            The aggregated report.
        """
        rows_by_group: dict[str, dict[str, Any]] = {}
        summary: dict[str, Any] = {
            'avg_duration_ms': None,
            'failed': 0,
            'max_duration_ms': None,
            'min_duration_ms': None,
            'other': 0,
            'running': 0,
            'runs': len(records),
            'success_rate_pct': None,
            'succeeded': 0,
            'total_duration_ms': 0,
            'duration_samples': 0,
        }

        for record in records:
            status = cast(str, record.get('status') or '')
            if status in ('succeeded', 'failed', 'running'):
                cls.increment_metric(summary, status)
            else:
                cls.increment_metric(summary, 'other')

            key = cls.report_group_key(record, group_by=group_by)
            row = cast(
                dict[str, Any],
                rows_by_group.setdefault(
                    key,
                    {
                        'avg_duration_ms': None,
                        'duration_samples': 0,
                        'group': key,
                        'last_started_at': None,
                        'max_duration_ms': None,
                        'min_duration_ms': None,
                        'runs': 0,
                        'succeeded': 0,
                        'failed': 0,
                        'running': 0,
                        'other': 0,
                        'success_rate_pct': None,
                        'total_duration_ms': 0,
                    },
                ),
            )
            cls.increment_metric(row, 'runs')
            if status in ('succeeded', 'failed', 'running'):
                cls.increment_metric(row, status)
            else:
                cls.increment_metric(row, 'other')

            duration_ms = record.get('duration_ms')
            if isinstance(duration_ms, int):
                cls.update_duration_metrics(row, duration_ms)
                cls.update_duration_metrics(summary, duration_ms)

            started_at = record.get('started_at')
            if isinstance(started_at, str) and (
                row['last_started_at'] is None or started_at > row['last_started_at']
            ):
                row['last_started_at'] = started_at

        rows = sorted(rows_by_group.values(), key=lambda item: cast(str, item['group']))
        for row in rows:
            samples = cast(int, row.pop('duration_samples'))
            total_duration = cast(int, row.pop('total_duration_ms'))
            row['avg_duration_ms'] = (
                int(total_duration / samples) if samples > 0 else None
            )
            row['success_rate_pct'] = cls.success_rate_pct(
                int(row['succeeded']),
                int(row['runs']),
            )

        summary_samples = cast(int, summary.pop('duration_samples'))
        summary_total_duration = cast(int, summary.pop('total_duration_ms'))
        summary['avg_duration_ms'] = (
            int(summary_total_duration / summary_samples)
            if summary_samples > 0
            else None
        )
        summary['success_rate_pct'] = cls.success_rate_pct(
            int(summary['succeeded']),
            int(summary['runs']),
        )

        return {
            'group_by': group_by,
            'rows': rows,
            'summary': summary,
        }

    @classmethod
    def update_duration_metrics(
        cls,
        bucket: dict[str, Any],
        duration_ms: int,
    ) -> None:
        """
        Update duration-related metrics for one report bucket.

        Parameters
        ----------
        bucket : dict[str, Any]
            The report bucket to update.
        duration_ms : int
            The duration in milliseconds to incorporate into the metrics.
        """
        cls.increment_metric(bucket, 'total_duration_ms', duration_ms)
        cls.increment_metric(bucket, 'duration_samples', 1)
        current_min = bucket.get('min_duration_ms')
        current_max = bucket.get('max_duration_ms')
        bucket['min_duration_ms'] = (
            duration_ms if current_min is None else min(int(current_min), duration_ms)
        )
        bucket['max_duration_ms'] = (
            duration_ms if current_max is None else max(int(current_max), duration_ms)
        )
