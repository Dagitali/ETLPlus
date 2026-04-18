"""
:mod:`tests.unit.cli._handlers.test_u_cli_handlers_history_view` module.

Direct unit tests for history view and status entry points in
:mod:`etlplus.cli._handlers.history`.
"""

from __future__ import annotations

from typing import Any

import pytest

from etlplus.cli._handlers import _history_view as history_view_mod
from etlplus.cli._handlers import history as history_mod

from ..conftest import CaptureIo
from ..conftest import assert_emit_json
from .pytest_cli_history_support import assert_emit_markdown_table
from .pytest_cli_history_support import normalized_job
from .pytest_cli_history_support import normalized_run
from .pytest_cli_history_support import patch_history_store_records

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHistoryHandler:
    """Unit tests for :func:`history_handler`."""

    @pytest.mark.parametrize(
        ('records', 'kwargs', 'pretty', 'expected'),
        [
            pytest.param(
                [
                    {
                        'config_path': 'pipeline.yml',
                        'job_name': 'job-a',
                        'run_id': 'run-123',
                        'started_at': '2026-03-23T00:00:00Z',
                        'status': 'running',
                    },
                    {
                        'duration_ms': 5000,
                        'finished_at': '2026-03-23T00:00:05Z',
                        'result_summary': {'rows': 10},
                        'run_id': 'run-123',
                        'status': 'succeeded',
                    },
                ],
                {},
                True,
                [
                    normalized_run(
                        config_path='pipeline.yml',
                        duration_ms=5000,
                        finished_at='2026-03-23T00:00:05Z',
                        job_name='job-a',
                        result_summary={'rows': 10},
                        run_id='run-123',
                        started_at='2026-03-23T00:00:00Z',
                        status='succeeded',
                    ),
                ],
                id='normalized-default',
            ),
            pytest.param(
                [
                    {
                        'run_id': 'run-1',
                        'started_at': '2026-03-23T00:00:00Z',
                        'status': 'running',
                    },
                    {
                        'finished_at': '2026-03-23T00:00:05Z',
                        'run_id': 'run-2',
                        'status': 'succeeded',
                    },
                ],
                {'raw': True, 'limit': 1},
                False,
                [
                    {
                        'finished_at': '2026-03-23T00:00:05Z',
                        'run_id': 'run-2',
                        'status': 'succeeded',
                    },
                ],
                id='raw-limit',
            ),
            pytest.param(
                [
                    {
                        'finished_at': '2026-03-23T00:00:05Z',
                        'job_name': 'job-a',
                        'run_id': 'run-2',
                        'started_at': '2026-03-23T00:00:00Z',
                        'status': 'succeeded',
                    },
                ],
                {'json_output': True},
                False,
                [
                    normalized_run(
                        finished_at='2026-03-23T00:00:05Z',
                        job_name='job-a',
                        run_id='run-2',
                        started_at='2026-03-23T00:00:00Z',
                        status='succeeded',
                    ),
                ],
                id='explicit-json',
            ),
        ],
    )
    def test_emits_json_payload_for_supported_history_modes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
        records: list[dict[str, object]],
        kwargs: dict[str, Any],
        pretty: bool,
        expected: list[dict[str, object]],
    ) -> None:
        """
        Test that history emits JSON for default, raw, and explicit JSON modes.
        """
        patch_history_store_records(monkeypatch, records)

        assert history_mod.history_handler(pretty=pretty, **kwargs) == 0
        assert_emit_json(capture_io, expected, pretty=pretty)

    def test_filters_job_level_history_and_emits_job_table_columns(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that job-level history uses per-job records and table columns.
        """
        patch_history_store_records(
            monkeypatch,
            [
                {
                    'duration_ms': 1000,
                    'finished_at': '2026-03-23T00:00:01Z',
                    'job_name': 'seed',
                    'pipeline_name': 'pipeline-a',
                    'record_level': 'job',
                    'result_status': 'success',
                    'run_id': 'run-1',
                    'sequence_index': 0,
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
                {
                    'finished_at': '2026-03-23T00:00:02Z',
                    'job_name': 'publish',
                    'pipeline_name': 'pipeline-b',
                    'record_level': 'job',
                    'run_id': 'run-2',
                    'sequence_index': 1,
                    'started_at': '2026-03-23T00:00:01Z',
                    'status': 'skipped',
                },
            ],
        )

        assert (
            history_mod.history_handler(
                level='job',
                pipeline='pipeline-a',
                table=True,
                pretty=False,
            )
            == 0
        )

        assert_emit_markdown_table(
            capture_io,
            [
                {
                    'duration_ms': 1000,
                    'error_message': None,
                    'error_type': None,
                    'finished_at': '2026-03-23T00:00:01Z',
                    'job_name': 'seed',
                    'pipeline_name': 'pipeline-a',
                    'records_in': None,
                    'records_out': None,
                    'result_status': 'success',
                    'result_summary': None,
                    'run_id': 'run-1',
                    'sequence_index': 0,
                    'skipped_due_to': None,
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
            ],
            columns=history_view_mod.JOB_HISTORY_TABLE_COLUMNS,
        )

    def test_filters_normalized_runs_and_emits_markdown_table(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that history filters normalized runs before table rendering."""
        patch_history_store_records(
            monkeypatch,
            [
                {
                    'job_name': 'job-a',
                    'run_id': 'run-1',
                    'started_at': '2026-03-22T00:00:00Z',
                    'status': 'running',
                },
                {
                    'finished_at': '2026-03-22T00:00:10Z',
                    'job_name': 'job-a',
                    'run_id': 'run-1',
                    'status': 'failed',
                },
                {
                    'finished_at': '2026-03-23T00:00:05Z',
                    'job_name': 'job-b',
                    'run_id': 'run-2',
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
            ],
        )

        assert (
            history_mod.history_handler(
                job='job-a',
                status='failed',
                table=True,
                pretty=False,
            )
            == 0
        )

        assert_emit_markdown_table(
            capture_io,
            [
                normalized_run(
                    finished_at='2026-03-22T00:00:10Z',
                    job_name='job-a',
                    run_id='run-1',
                    started_at='2026-03-22T00:00:00Z',
                    status='failed',
                ),
            ],
            columns=history_view_mod.HISTORY_TABLE_COLUMNS,
        )

    def test_filters_raw_job_level_records_by_pipeline_and_status(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that raw history mode can target job-level records explicitly.
        """
        patch_history_store_records(
            monkeypatch,
            [
                {
                    'job_name': 'seed',
                    'pipeline_name': 'pipeline-a',
                    'record_level': 'job',
                    'run_id': 'run-1',
                    'sequence_index': 0,
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
                {
                    'job_name': 'publish',
                    'pipeline_name': 'pipeline-a',
                    'reason': 'upstream_failed',
                    'record_level': 'job',
                    'run_id': 'run-1',
                    'sequence_index': 1,
                    'skipped_due_to': ['seed'],
                    'started_at': '2026-03-23T00:00:01Z',
                    'status': 'skipped',
                },
                {
                    'job_name': 'notify',
                    'pipeline_name': 'pipeline-b',
                    'record_level': 'job',
                    'run_id': 'run-2',
                    'sequence_index': 0,
                    'started_at': '2026-03-23T00:00:02Z',
                    'status': 'skipped',
                },
            ],
        )

        assert (
            history_mod.history_handler(
                raw=True,
                level='job',
                pipeline='pipeline-a',
                status='skipped',
                pretty=False,
            )
            == 0
        )

        assert_emit_json(
            capture_io,
            [
                {
                    'job_name': 'publish',
                    'pipeline_name': 'pipeline-a',
                    'reason': 'upstream_failed',
                    'run_id': 'run-1',
                    'sequence_index': 1,
                    'skipped_due_to': ['seed'],
                    'started_at': '2026-03-23T00:00:01Z',
                    'status': 'skipped',
                },
            ],
            pretty=False,
        )

    def test_filters_raw_records_by_run_id_and_since(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that raw history mode filters by run identifier and timestamp.
        """
        patch_history_store_records(
            monkeypatch,
            [
                {
                    'run_id': 'run-1',
                    'started_at': '2026-03-22T00:00:00Z',
                    'status': 'running',
                },
                {
                    'finished_at': '2026-03-23T00:00:05Z',
                    'run_id': 'run-2',
                    'status': 'succeeded',
                },
                {
                    'finished_at': '2026-03-24T00:00:05Z',
                    'run_id': 'run-2',
                    'status': 'failed',
                },
            ],
        )

        assert (
            history_mod.history_handler(
                raw=True,
                run_id='run-2',
                since='2026-03-24T00:00:00Z',
                until='2026-03-24T23:59:59Z',
                pretty=False,
            )
            == 0
        )

        assert_emit_json(
            capture_io,
            [
                {
                    'finished_at': '2026-03-24T00:00:05Z',
                    'run_id': 'run-2',
                    'status': 'failed',
                },
            ],
            pretty=False,
        )

    def test_follow_emits_new_raw_records_until_interrupted(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that follow mode streams only newly observed raw history records.
        """
        call_count = {'value': 0}

        def fake_load_history_records(**kwargs: object) -> list[dict[str, object]]:
            call_count['value'] += 1
            assert kwargs['raw'] is True
            if call_count['value'] == 1:
                return [
                    {
                        'run_id': 'run-1',
                        'started_at': '2026-03-23T00:00:00Z',
                        'status': 'running',
                    },
                ]
            return [
                {
                    'run_id': 'run-1',
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'running',
                },
                {
                    'finished_at': '2026-03-23T00:00:05Z',
                    'run_id': 'run-1',
                    'status': 'succeeded',
                },
            ]

        def fake_sleep(_seconds: float) -> None:
            if call_count['value'] >= 2:
                raise KeyboardInterrupt

        monkeypatch.setattr(
            history_mod,
            'load_history_records',
            fake_load_history_records,
        )
        monkeypatch.setattr(history_mod, 'sleep', fake_sleep)

        assert history_mod.history_handler(follow=True, raw=True, pretty=True) == 0

        assert capture_io['emit_json'] == [
            (
                (
                    {
                        'run_id': 'run-1',
                        'started_at': '2026-03-23T00:00:00Z',
                        'status': 'running',
                    },
                ),
                {'pretty': False},
            ),
            (
                (
                    {
                        'finished_at': '2026-03-23T00:00:05Z',
                        'run_id': 'run-1',
                        'status': 'succeeded',
                    },
                ),
                {'pretty': False},
            ),
        ]

    def test_follow_passes_job_level_filters_to_raw_history_loader(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that follow mode forwards job-level raw-history filters.
        """
        recorded_calls: list[dict[str, object]] = []

        def fake_load_history_records(**kwargs: object) -> list[dict[str, object]]:
            recorded_calls.append(dict(kwargs))
            raise KeyboardInterrupt

        monkeypatch.setattr(
            history_mod,
            'load_history_records',
            fake_load_history_records,
        )

        assert (
            history_mod.history_handler(
                follow=True,
                raw=True,
                level='job',
                pipeline='pipeline-a',
                status='skipped',
                pretty=False,
            )
            == 0
        )
        assert recorded_calls == [
            {
                'level': 'job',
                'job': None,
                'limit': None,
                'pipeline': 'pipeline-a',
                'raw': True,
                'run_id': None,
                'since': None,
                'status': 'skipped',
                'until': None,
            },
        ]

    def test_rejects_conflicting_output_modes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that history rejects conflicting JSON and table output flags."""
        patch_history_store_records(monkeypatch, [])

        with pytest.raises(ValueError, match='choose either json output'):
            history_mod.history_handler(json_output=True, table=True)


class TestHistoryHelperFunctions:
    """Unit tests for direct history helper seams."""

    def test_load_history_records_delegates_through_query_loader(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that top-level history loading delegates through
        :class:`_HistoryQuery`.
        """
        recorded: dict[str, object] = {}

        def fake_load(
            self: history_mod._HistoryQuery,
            *,
            raw: bool = False,
            limit: int | None = None,
        ) -> list[dict[str, object]]:
            recorded['query'] = self
            recorded['raw'] = raw
            recorded['limit'] = limit
            return [{'run_id': 'run-1'}]

        monkeypatch.setattr(history_mod._HistoryQuery, 'load', fake_load)

        assert history_mod.load_history_records(
            level='job',
            job='seed',
            pipeline='pipeline-a',
            run_id='run-1',
            since='2026-03-23T00:00:00Z',
            until='2026-03-24T00:00:00Z',
            status='skipped',
            limit=3,
            raw=True,
        ) == [{'run_id': 'run-1'}]
        query = recorded['query']
        assert isinstance(query, history_mod._HistoryQuery)
        assert query == history_mod._HistoryQuery(
            level='job',
            job='seed',
            pipeline='pipeline-a',
            run_id='run-1',
            since='2026-03-23T00:00:00Z',
            until='2026-03-24T00:00:00Z',
            status='skipped',
        )
        assert recorded['raw'] is True
        assert recorded['limit'] == 3


class TestStatusHandler:
    """Unit tests for :func:`status_handler`."""

    def test_emits_latest_matching_normalized_job_for_job_level_status(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that job-level status emits the full normalized DAG job record
        shape.
        """
        patch_history_store_records(
            monkeypatch,
            [
                {
                    'duration_ms': 1000,
                    'finished_at': '2026-03-23T00:00:01Z',
                    'job_name': 'seed',
                    'pipeline_name': 'pipeline-a',
                    'record_level': 'job',
                    'result_status': 'success',
                    'run_id': 'run-1',
                    'sequence_index': 0,
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
            ],
        )

        assert (
            history_mod.status_handler(level='job', pretty=False, run_id='run-1') == 0
        )
        assert_emit_json(
            capture_io,
            normalized_job(
                duration_ms=1000,
                finished_at='2026-03-23T00:00:01Z',
                job_name='seed',
                pipeline_name='pipeline-a',
                result_status='success',
                run_id='run-1',
                sequence_index=0,
                started_at='2026-03-23T00:00:00Z',
                status='succeeded',
            ),
            pretty=False,
        )

    @pytest.mark.parametrize(
        ('records', 'kwargs', 'pretty', 'expected_exit', 'expected_payload'),
        [
            pytest.param(
                [
                    {
                        'finished_at': '2026-03-22T00:00:05Z',
                        'job_name': 'job-a',
                        'run_id': 'run-1',
                        'started_at': '2026-03-22T00:00:00Z',
                        'status': 'failed',
                    },
                    {
                        'finished_at': '2026-03-23T00:00:05Z',
                        'job_name': 'job-a',
                        'run_id': 'run-2',
                        'started_at': '2026-03-23T00:00:00Z',
                        'status': 'succeeded',
                    },
                ],
                {'job': 'job-a'},
                True,
                0,
                normalized_run(
                    finished_at='2026-03-23T00:00:05Z',
                    job_name='job-a',
                    run_id='run-2',
                    started_at='2026-03-23T00:00:00Z',
                    status='succeeded',
                ),
                id='latest-match',
            ),
            pytest.param(
                [],
                {'run_id': 'missing'},
                False,
                1,
                {},
                id='missing-match',
            ),
        ],
    )
    def test_emits_latest_matching_normalized_run(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
        records: list[dict[str, object]],
        kwargs: dict[str, Any],
        pretty: bool,
        expected_exit: int,
        expected_payload: dict[str, object],
    ) -> None:
        """Test that status emits the latest match or an empty miss payload."""
        patch_history_store_records(monkeypatch, records)

        assert history_mod.status_handler(pretty=pretty, **kwargs) == expected_exit
        assert_emit_json(capture_io, expected_payload, pretty=pretty)

    def test_uses_history_view_load_records_directly(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that status delegates through the direct class-based loader."""
        recorded: dict[str, object] = {}

        def fake_load_records(
            cls: type[history_mod.HistoryView],
            **kwargs: object,
        ) -> list[dict[str, object]]:
            recorded['load'] = kwargs
            return [
                {
                    'run_id': 'run-9',
                    'started_at': '2026-03-23T00:00:00Z',
                    'status': 'succeeded',
                },
            ]

        monkeypatch.setattr(
            history_mod.HistoryView,
            'load_records',
            classmethod(fake_load_records),
        )

        assert (
            history_mod.status_handler(job='job-a', pretty=False, run_id='run-9') == 0
        )

        assert recorded['load'] == {
            'job': 'job-a',
            'level': 'run',
            'limit': 1,
            'raw': False,
            'run_id': 'run-9',
        }
        assert_emit_json(
            capture_io,
            {
                'run_id': 'run-9',
                'started_at': '2026-03-23T00:00:00Z',
                'status': 'succeeded',
            },
            pretty=False,
        )
