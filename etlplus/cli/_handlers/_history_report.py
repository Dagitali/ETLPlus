"""
:mod:`etlplus.cli._handlers._history_report` module.

Grouped history-report helpers shared by CLI handlers.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import Literal
from typing import cast

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'REPORT_TABLE_COLUMNS',
    # Classes
    'HistoryReportBuilder',
]


# SECTION: CONSTANTS ======================================================== #


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


# SECTION: CLASSES ========================================================== #


class HistoryReportBuilder:
    """Aggregation helpers for grouped history-report output."""

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
        group_by: Literal['day', 'job', 'pipeline', 'run', 'status'],
    ) -> str:
        """
        Return the grouping key for one normalized history record.

        Parameters
        ----------
        record : Mapping[str, Any]
            The normalized history record.
        group_by : Literal['day', 'job', 'pipeline', 'run', 'status']
            The grouping criterion.

        Returns
        -------
        str
            The grouping key for the record.
        """
        if group_by == 'job':
            return cast(str, record.get('job_name') or '(no job)')
        if group_by == 'pipeline':
            return cast(str, record.get('pipeline_name') or '(no pipeline)')
        if group_by == 'run':
            return cast(str, record.get('run_id') or '(unknown run)')
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

    @classmethod
    def build(
        cls,
        records: list[dict[str, Any]],
        *,
        group_by: Literal['day', 'job', 'pipeline', 'run', 'status'],
    ) -> dict[str, Any]:
        """
        Aggregate normalized history records into a grouped report.

        Parameters
        ----------
        records : list[dict[str, Any]]
            The list of normalized history records.
        group_by : Literal['day', 'job', 'pipeline', 'run', 'status']
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
