"""
:mod:`tests.unit.cli._handlers.test_u_cli_handlers_history_report` module.

Direct unit tests for report aggregation entry points in
:mod:`etlplus.cli._handlers.history`.
"""

from __future__ import annotations

from typing import Any
from typing import Literal

import pytest

from etlplus.cli._handlers import _history_report as history_report_mod
from etlplus.cli._handlers import history as history_mod

from ..conftest import CaptureIo
from ..conftest import assert_emit_json
from .pytest_cli_history_support import assert_emit_markdown_table
from .pytest_cli_history_support import patch_history_store_records
from .pytest_cli_history_support import report_row
from .pytest_cli_history_support import report_summary

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestReportHandler:
    """Unit tests for :func:`report_handler`."""

    @pytest.mark.parametrize(
        ('records', 'group_by', 'kwargs', 'expected'),
        [
            pytest.param(
                [
                    {
                        'duration_ms': 3000,
                        'finished_at': '2026-03-23T00:00:05Z',
                        'job_name': 'job-a',
                        'run_id': 'run-1',
                        'started_at': '2026-03-23T00:00:00Z',
                        'status': 'succeeded',
                    },
                    {
                        'duration_ms': 1000,
                        'finished_at': '2026-03-24T00:00:05Z',
                        'job_name': 'job-a',
                        'run_id': 'run-2',
                        'started_at': '2026-03-24T00:00:00Z',
                        'status': 'failed',
                    },
                ],
                'job',
                {
                    'job': 'job-a',
                    'since': '2026-03-23T00:00:00Z',
                    'until': '2026-03-24T23:59:59Z',
                },
                {
                    'group_by': 'job',
                    'rows': [
                        report_row(
                            avg_duration_ms=2000,
                            failed=1,
                            group='job-a',
                            last_started_at='2026-03-24T00:00:00Z',
                            max_duration_ms=3000,
                            min_duration_ms=1000,
                            runs=2,
                            success_rate_pct=50.0,
                            succeeded=1,
                        ),
                    ],
                    'summary': report_summary(
                        avg_duration_ms=2000,
                        failed=1,
                        max_duration_ms=3000,
                        min_duration_ms=1000,
                        runs=2,
                        success_rate_pct=50.0,
                        succeeded=1,
                    ),
                },
                id='group-by-job',
            ),
            pytest.param(
                [
                    {
                        'duration_ms': 3000,
                        'finished_at': '2026-03-23T00:00:05Z',
                        'job_name': 'job-a',
                        'run_id': 'run-1',
                        'started_at': '2026-03-23T00:00:00Z',
                        'status': 'succeeded',
                    },
                    {
                        'duration_ms': 1000,
                        'finished_at': '2026-03-23T01:00:05Z',
                        'job_name': 'job-b',
                        'run_id': 'run-2',
                        'started_at': '2026-03-23T01:00:00Z',
                        'status': 'failed',
                    },
                ],
                'day',
                {},
                {
                    'group_by': 'day',
                    'rows': [
                        report_row(
                            avg_duration_ms=2000,
                            failed=1,
                            group='2026-03-23',
                            last_started_at='2026-03-23T01:00:00Z',
                            max_duration_ms=3000,
                            min_duration_ms=1000,
                            runs=2,
                            success_rate_pct=50.0,
                            succeeded=1,
                        ),
                    ],
                    'summary': report_summary(
                        avg_duration_ms=2000,
                        failed=1,
                        max_duration_ms=3000,
                        min_duration_ms=1000,
                        runs=2,
                        success_rate_pct=50.0,
                        succeeded=1,
                    ),
                },
                id='group-by-day',
            ),
        ],
    )
    def test_emits_grouped_report_as_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
        records: list[dict[str, object]],
        group_by: Literal['day', 'job', 'pipeline', 'run', 'status'],
        kwargs: dict[str, Any],
        expected: dict[str, object],
    ) -> None:
        """Test that report aggregates normalized runs into JSON output."""
        patch_history_store_records(monkeypatch, records)

        assert (
            history_mod.report_handler(group_by=group_by, pretty=False, **kwargs) == 0
        )
        assert_emit_json(capture_io, expected, pretty=False)

    def test_emits_grouped_report_as_markdown_table(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that report renders grouped rows as a Markdown table."""
        patch_history_store_records(
            monkeypatch,
            [
                {
                    'duration_ms': 3000,
                    'finished_at': '2026-03-23T00:00:05Z',
                    'job_name': 'job-a',
                    'run_id': 'run-1',
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
            ],
        )

        assert history_mod.report_handler(group_by='status', table=True) == 0

        assert_emit_markdown_table(
            capture_io,
            [
                report_row(
                    avg_duration_ms=3000,
                    group='succeeded',
                    last_started_at='2026-03-23T00:00:00Z',
                    max_duration_ms=3000,
                    min_duration_ms=3000,
                    runs=1,
                    success_rate_pct=100.0,
                    succeeded=1,
                ),
            ],
            columns=history_report_mod.REPORT_TABLE_COLUMNS,
        )

    def test_emits_pipeline_grouped_report_for_job_level_records(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that job-level reports support grouping by pipeline name."""
        patch_history_store_records(
            monkeypatch,
            [
                {
                    'duration_ms': 3000,
                    'finished_at': '2026-03-23T00:00:05Z',
                    'job_name': 'job-a',
                    'pipeline_name': 'pipeline-a',
                    'record_level': 'job',
                    'run_id': 'run-1',
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
                {
                    'duration_ms': 1000,
                    'finished_at': '2026-03-23T01:00:05Z',
                    'job_name': 'job-b',
                    'pipeline_name': 'pipeline-a',
                    'record_level': 'job',
                    'run_id': 'run-1',
                    'started_at': '2026-03-23T01:00:00Z',
                    'status': 'failed',
                },
            ],
        )

        assert (
            history_mod.report_handler(
                group_by='pipeline',
                level='job',
                pretty=False,
            )
            == 0
        )

        assert_emit_json(
            capture_io,
            {
                'group_by': 'pipeline',
                'rows': [
                    report_row(
                        avg_duration_ms=2000,
                        failed=1,
                        group='pipeline-a',
                        last_started_at='2026-03-23T01:00:00Z',
                        max_duration_ms=3000,
                        min_duration_ms=1000,
                        runs=2,
                        success_rate_pct=50.0,
                        succeeded=1,
                    ),
                ],
                'summary': report_summary(
                    avg_duration_ms=2000,
                    failed=1,
                    max_duration_ms=3000,
                    min_duration_ms=1000,
                    runs=2,
                    success_rate_pct=50.0,
                    succeeded=1,
                ),
            },
            pretty=False,
        )

    def test_uses_history_view_and_report_builder_directly(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that report delegates through the direct class-based helpers."""
        recorded: dict[str, object] = {}

        def fake_validate_output_mode(*, json_output: bool, table: bool) -> None:
            recorded['validate'] = (json_output, table)

        def fake_load_records(
            cls: type[history_mod.HistoryView],
            **kwargs: object,
        ) -> list[dict[str, object]]:
            recorded['load'] = kwargs
            return [
                {
                    'duration_ms': 1000,
                    'job_name': 'job-a',
                    'run_id': 'run-1',
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
            ]

        def fake_build(
            cls: type[history_mod.HistoryReportBuilder],
            records: list[dict[str, object]],
            *,
            group_by: str,
        ) -> dict[str, object]:
            recorded['build'] = (records, group_by)
            return {
                'group_by': group_by,
                'rows': [{'group': 'job-a', 'runs': 1}],
                'summary': {'runs': 1},
            }

        monkeypatch.setattr(
            history_mod.HistoryView,
            'validate_output_mode',
            staticmethod(fake_validate_output_mode),
        )
        monkeypatch.setattr(
            history_mod.HistoryView,
            'load_records',
            classmethod(fake_load_records),
        )
        monkeypatch.setattr(
            history_mod.HistoryReportBuilder,
            'build',
            classmethod(fake_build),
        )

        assert (
            history_mod.report_handler(
                group_by='job',
                job='job-a',
                json_output=True,
                pretty=False,
                since='2026-03-23T00:00:00Z',
                until='2026-03-24T00:00:00Z',
            )
            == 0
        )

        assert recorded['validate'] == (True, False)
        assert recorded['load'] == {
            'job': 'job-a',
            'level': 'run',
            'raw': False,
            'since': '2026-03-23T00:00:00Z',
            'until': '2026-03-24T00:00:00Z',
        }
        assert recorded['build'] == (
            [
                {
                    'duration_ms': 1000,
                    'job_name': 'job-a',
                    'run_id': 'run-1',
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
            ],
            'job',
        )
        assert_emit_json(
            capture_io,
            {
                'group_by': 'job',
                'rows': [{'group': 'job-a', 'runs': 1}],
                'summary': {'runs': 1},
            },
            pretty=False,
        )
