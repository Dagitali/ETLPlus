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
        """Return a stable fingerprint for a persisted history record."""
        return serialize_json(record, sort_keys=True)

    @staticmethod
    def parse_timestamp(
        value: object,
    ) -> datetime | None:
        """Parse an ISO-8601 timestamp used in persisted history records."""
        if not isinstance(value, str) or not value:
            return None
        normalized = value.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    @staticmethod
    def sort_key(
        record: Mapping[str, Any],
    ) -> tuple[str, str]:
        """Return a reverse-sortable key for history records."""
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
        """Validate that at most one explicit history output mode was requested."""
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
        """Load, filter, and sort history records for CLI read commands."""
        parsed_since = cls.parse_timestamp(since)
        parsed_until = cls.parse_timestamp(until)
        history_store = HistoryStore.from_environment()
        records_iter = (
            history_store.iter_records() if raw else history_store.iter_runs()
        )
        records = [
            dict(record)
            for record in records_iter
            if cls.matches(
                record,
                job=job,
                run_id=run_id,
                since=parsed_since,
                until=parsed_until,
                status=status,
            )
        ]
        records.sort(key=cls.sort_key, reverse=True)
        if limit is not None:
            records = records[:limit]
        return records

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
        """Return whether a history record matches CLI filter values."""
        if job is not None and record.get('job_name') != job:
            return False
        if run_id is not None and record.get('run_id') != run_id:
            return False
        if status is not None and record.get('status') != status:
            return False
        record_timestamp = cls.parse_timestamp(
            record.get('started_at') or record.get('finished_at'),
        )
        if since is not None:
            if record_timestamp is None or record_timestamp < since:
                return False
        if until is not None:
            if record_timestamp is None or record_timestamp > until:
                return False
        return True


class HistoryReportBuilder:
    """Aggregation helpers for grouped history-report output."""

    # -- Static Methods -- #

    @staticmethod
    def increment_metric(
        bucket: dict[str, Any],
        key: str,
        amount: int = 1,
    ) -> None:
        """Increment an integer metric stored inside a mutable report bucket."""
        bucket[key] = int(bucket.get(key) or 0) + amount

    @staticmethod
    def report_group_key(
        record: Mapping[str, Any],
        *,
        group_by: Literal['day', 'job', 'status'],
    ) -> str:
        """Return the grouping key for one normalized history record."""
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
        """Return the success-rate percentage for the given counters."""
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
        """Aggregate normalized history records into a grouped report."""
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
        """Update duration-related metrics for one report bucket."""
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
