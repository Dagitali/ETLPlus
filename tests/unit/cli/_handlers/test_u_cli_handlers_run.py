"""
:mod:`tests.unit.cli._handlers.test_u_cli_handlers_run` module.

Direct unit tests for :mod:`etlplus.cli._handlers.run`.
"""

from __future__ import annotations

import pytest

from etlplus.cli._handlers import run as run_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestFailureMessage:
    """Unit tests for :func:`etlplus.cli._handlers.run._failure_message`."""

    @pytest.mark.parametrize(
        ('result', 'expected'),
        [
            pytest.param(
                'not-a-mapping',
                None,
                id='non-mapping',
            ),
            pytest.param(
                {
                    'status': 'failed',
                    'failed_jobs': ['seed', 'publish'],
                    'skipped_jobs': ['notify'],
                },
                '2 job(s) failed and 1 job(s) were skipped during DAG execution',
                id='plural-failure-and-skips',
            ),
            pytest.param(
                {
                    'status': 'partial_success',
                    'failed_jobs': 'bad-shape',
                    'skipped_jobs': None,
                },
                'DAG execution failed',
                id='fallback-message',
            ),
        ],
    )
    def test_failure_message_variants(
        self,
        result: object,
        expected: str | None,
    ) -> None:
        """
        Test that failure messaging covers non-mapping, plural, and fallback
        cases.
        """
        assert run_mod._failure_message(result) == expected


class TestJobRunPersistence:
    """Unit tests for per-job DAG history persistence helpers."""

    @pytest.mark.parametrize(
        ('item', 'expected'),
        [
            pytest.param(
                {'result': None},
                None,
                id='missing-summary',
            ),
            pytest.param(
                {
                    'reason': 'upstream_failed',
                    'result': object(),
                },
                {'reason': 'upstream_failed'},
                id='unsupported-result-falls-back',
            ),
            pytest.param(
                {'reason': 'upstream_failed'},
                {'reason': 'upstream_failed'},
                id='reason-only',
            ),
            pytest.param(
                {'skipped_due_to': ['seed', 3]},
                {'skipped_due_to': ['seed']},
                id='skipped-only',
            ),
        ],
    )
    def test_coerce_job_result_summary_handles_fallback_variants(
        self,
        item: dict[str, object],
        expected: object,
    ) -> None:
        """Fallback job summaries should normalize only supported payloads."""
        assert run_mod._coerce_job_result_summary(item) == expected

    @pytest.mark.parametrize(
        'item',
        [
            pytest.param({'status': 'succeeded'}, id='missing-job'),
            pytest.param({'job': 'seed'}, id='missing-status'),
        ],
    )
    def test_job_run_record_rejects_missing_required_fields(
        self,
        item: dict[str, object],
    ) -> None:
        """Job-run records require both non-empty job names and statuses."""
        assert (
            run_mod._job_run_record(
                fallback_index=0,
                item=item,
                pipeline_name='pipeline-a',
                run_id='run-123',
            )
            is None
        )

    @pytest.mark.parametrize(
        'result',
        [
            pytest.param(None, id='missing-result'),
            pytest.param({'executed_jobs': 'bad-shape'}, id='invalid-executed-jobs'),
            pytest.param({'executed_jobs': ['bad-row']}, id='non-mapping-item'),
        ],
    )
    def test_persist_job_runs_ignores_unsupported_result_shapes(
        self,
        result: object,
    ) -> None:
        """
        Test that unsupported result shapes do not emit any job-run records.
        """

        class _FakeHistoryStore:
            def __init__(self) -> None:
                self.records: list[object] = []

            def record_job_run(self, record: object) -> None:
                self.records.append(record)

        history_store = _FakeHistoryStore()

        run_mod._persist_job_runs(
            history_store,
            pipeline_name='pipeline-a',
            result=result,
            run_id='run-123',
        )

        assert history_store.records == []

    def test_persist_job_runs_records_supported_dag_entries(self) -> None:
        """
        Test that DAG execution summaries persist one job record per valid row.
        """

        class _FakeHistoryStore:
            def __init__(self) -> None:
                self.records: list[object] = []

            def record_job_run(self, record: object) -> None:
                self.records.append(record)

        history_store = _FakeHistoryStore()

        run_mod._persist_job_runs(
            history_store,
            pipeline_name='pipeline-a',
            result={
                'executed_jobs': [
                    {
                        'duration_ms': 25,
                        'job': 'seed',
                        'result': {'status': 'success', 'rows': 10},
                        'result_status': 'success',
                        'sequence_index': 0,
                        'started_at': '2026-03-23T00:00:00Z',
                        'finished_at': '2026-03-23T00:00:01Z',
                        'status': 'succeeded',
                    },
                    {
                        'job': 'publish',
                        'reason': 'upstream_failed',
                        'skipped_due_to': ['seed'],
                        'started_at': '2026-03-23T00:00:01Z',
                        'finished_at': '2026-03-23T00:00:01Z',
                        'status': 'skipped',
                    },
                    {
                        'job': '',
                        'status': 'succeeded',
                    },
                ],
            },
            run_id='run-123',
        )

        assert history_store.records == [
            run_mod.JobRunRecord(
                run_id='run-123',
                job_name='seed',
                pipeline_name='pipeline-a',
                sequence_index=0,
                started_at='2026-03-23T00:00:00Z',
                finished_at='2026-03-23T00:00:01Z',
                duration_ms=25,
                records_in=None,
                records_out=None,
                status='succeeded',
                result_status='success',
                error_type=None,
                error_message=None,
                skipped_due_to=None,
                result_summary={'status': 'success', 'rows': 10},
            ),
            run_mod.JobRunRecord(
                run_id='run-123',
                job_name='publish',
                pipeline_name='pipeline-a',
                sequence_index=1,
                started_at='2026-03-23T00:00:01Z',
                finished_at='2026-03-23T00:00:01Z',
                duration_ms=None,
                records_in=None,
                records_out=None,
                status='skipped',
                result_status=None,
                error_type=None,
                error_message=None,
                skipped_due_to=['seed'],
                result_summary={
                    'reason': 'upstream_failed',
                    'skipped_due_to': ['seed'],
                },
            ),
        ]
