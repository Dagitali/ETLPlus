"""
:mod:`tests.unit.cli.test_u_cli_history` module.

Unit tests for split history support modules under :mod:`etlplus.cli._handlers`.
"""

from __future__ import annotations

from typing import Literal

import pytest

from etlplus.cli._handlers._history_report import HistoryReportBuilder
from etlplus.cli._handlers._history_view import HISTORY_TABLE_COLUMNS
from etlplus.cli._handlers._history_view import JOB_HISTORY_TABLE_COLUMNS
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

    def test_report_group_key_supports_pipeline_and_run_dimensions(self) -> None:
        """Grouping should expose the pipeline and run identifier dimensions."""
        record = {
            'pipeline_name': 'pipeline-a',
            'run_id': 'run-123',
        }

        assert HistoryReportBuilder.report_group_key(record, group_by='pipeline') == (
            'pipeline-a'
        )
        assert HistoryReportBuilder.report_group_key(record, group_by='run') == (
            'run-123'
        )


class TestHistoryView:
    """Unit tests for :class:`etlplus.cli._handlers._history_view.HistoryView`."""

    @pytest.mark.parametrize(
        ('level', 'expected'),
        [
            pytest.param('run', HISTORY_TABLE_COLUMNS, id='run'),
            pytest.param('job', JOB_HISTORY_TABLE_COLUMNS, id='job'),
        ],
    )
    def test_table_columns_returns_expected_columns(
        self,
        level: Literal['run', 'job'],
        expected: tuple[str, ...],
    ) -> None:
        """Table-column lookup should follow the requested history level."""
        assert HistoryView.table_columns(level) == expected

    def test_matches_accepts_job_level_pipeline_record(self) -> None:
        """
        Test that job-level records match level and pipeline filters directly.
        """
        assert HistoryView.matches(
            {
                'job_name': 'seed',
                'pipeline_name': 'pipeline-a',
                'record_level': 'job',
                'run_id': 'run-1',
                'started_at': '2026-03-24T00:00:00Z',
                'status': 'succeeded',
            },
            level='job',
            pipeline='pipeline-a',
        )

    @pytest.mark.parametrize(
        ('record', 'filters'),
        [
            pytest.param(
                {
                    'run_id': 'run-1',
                    'started_at': '2026-03-24T00:00:00Z',
                    'status': 'succeeded',
                },
                {'until': HistoryView.parse_timestamp('2026-03-23T23:59:59Z')},
                id='after-until',
            ),
            pytest.param(
                {'status': 'failed'},
                {'status': 'succeeded'},
                id='status-mismatch',
            ),
            pytest.param(
                {'record_level': 'job', 'status': 'succeeded'},
                {'level': 'run'},
                id='level-mismatch',
            ),
        ],
    )
    def test_matches_rejects_filtered_records(
        self,
        record: dict[str, object],
        filters: dict[str, object],
    ) -> None:
        """
        Test that filtering rejects records that fall outside the criteria.
        """
        assert HistoryView.matches(record, **filters) is False  # type: ignore[arg-type]

    def test_parse_timestamp_returns_none_for_invalid_value(self) -> None:
        """Test that invalid ISO timestamps are treated as missing."""
        assert HistoryView.parse_timestamp('not-a-timestamp') is None
