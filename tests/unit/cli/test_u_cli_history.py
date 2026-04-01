"""
:mod:`tests.unit.cli.test_u_cli_history` module.

Unit tests for split history support modules under :mod:`etlplus.cli._handlers`.
"""

from __future__ import annotations

from etlplus.cli._handlers._history_report import HistoryReportBuilder
from etlplus.cli._handlers._history_view import HistoryView

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHistoryReportBuilder:
    """
    Unit tests for
    :class:`etlplus.cli._handlers._history_report.HistoryReportBuilder`.
    """

    def test_success_rate_pct_returns_none_without_runs(self) -> None:
        """Success rate is undefined when there are no runs."""
        assert HistoryReportBuilder.success_rate_pct(0, 0) is None

    def test_build_counts_other_statuses_and_ignores_non_int_durations(self) -> None:
        """Report building should bucket unknown statuses under ``other``."""
        report = HistoryReportBuilder.build(
            [
                {
                    'duration_ms': '1000',
                    'job_name': 'job-a',
                    'run_id': 'run-1',
                    'started_at': None,
                    'status': 'queued',
                },
            ],
            group_by='job',
        )

        assert report == {
            'group_by': 'job',
            'rows': [
                {
                    'avg_duration_ms': None,
                    'failed': 0,
                    'group': 'job-a',
                    'last_started_at': None,
                    'max_duration_ms': None,
                    'min_duration_ms': None,
                    'other': 1,
                    'running': 0,
                    'runs': 1,
                    'success_rate_pct': 0.0,
                    'succeeded': 0,
                },
            ],
            'summary': {
                'avg_duration_ms': None,
                'failed': 0,
                'max_duration_ms': None,
                'min_duration_ms': None,
                'other': 1,
                'running': 0,
                'runs': 1,
                'success_rate_pct': 0.0,
                'succeeded': 0,
            },
        }


class TestHistoryView:
    """Unit tests for :class:`etlplus.cli._handlers._history_view.HistoryView`."""

    def test_matches_rejects_records_after_until(self) -> None:
        """Upper-bound timestamp filters should reject later records."""
        assert (
            HistoryView.matches(
                {
                    'run_id': 'run-1',
                    'started_at': '2026-03-24T00:00:00Z',
                    'status': 'succeeded',
                },
                until=HistoryView.parse_timestamp('2026-03-23T23:59:59Z'),
            )
            is False
        )

    def test_matches_rejects_status_mismatch(self) -> None:
        """Status filters should reject records with a different status."""
        assert (
            HistoryView.matches(
                {'status': 'failed'},
                status='succeeded',
            )
            is False
        )

    def test_parse_timestamp_returns_none_for_invalid_value(self) -> None:
        """Invalid ISO timestamps should be treated as missing."""
        assert HistoryView.parse_timestamp('not-a-timestamp') is None
