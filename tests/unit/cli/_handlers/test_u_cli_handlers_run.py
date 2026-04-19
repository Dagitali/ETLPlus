"""
:mod:`tests.unit.cli._handlers.test_u_cli_handlers_run` module.

Direct unit tests for :mod:`etlplus.cli._handlers.run`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.cli._handlers import run as run_mod
from etlplus.history._config import ResolvedHistoryConfig

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
                self.records: list[run_mod.JobRunRecord] = []

            def record_job_run(self, record: run_mod.JobRunRecord) -> None:
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
                self.records: list[run_mod.JobRunRecord] = []

            def record_job_run(self, record: run_mod.JobRunRecord) -> None:
                self.records.append(record)

        history_store = _FakeHistoryStore()
        telemetry_calls: list[dict[str, object]] = []

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(
            run_mod.RuntimeTelemetry,
            'emit_history_record',
            classmethod(
                lambda _cls, record, *, record_level: telemetry_calls.append(
                    {'record': dict(record), 'record_level': record_level},
                ),
            ),
        )

        try:
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
        finally:
            monkeypatch.undo()

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
        assert telemetry_calls == [
            {
                'record': history_store.records[0].to_payload(),
                'record_level': 'job',
            },
            {
                'record': history_store.records[1].to_payload(),
                'record_level': 'job',
            },
        ]


class TestHistoryStoreSelection:
    """Unit tests for local history-store selection helpers."""

    def test_open_history_store_reuses_environment_store_when_settings_match(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Matching resolved settings should reuse the environment-backed store."""
        settings = ResolvedHistoryConfig(
            enabled=True,
            backend='sqlite',
            state_dir=tmp_path,
            capture_tracebacks=False,
        )
        sentinel = object()

        monkeypatch.setattr(
            run_mod.ResolvedHistoryConfig,
            'resolve',
            classmethod(lambda _cls, *_args, **_kwargs: settings),
        )
        monkeypatch.setattr(
            run_mod.HistoryStore,
            'from_environment',
            lambda: sentinel,
        )
        monkeypatch.setattr(
            run_mod.HistoryStore,
            'from_settings',
            lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError('from_settings should not be called'),
            ),
        )

        assert run_mod._open_history_store(settings) is sentinel

    def test_open_history_store_uses_explicit_settings_when_environment_differs(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Differing settings should open the store from explicit resolved values."""
        settings = ResolvedHistoryConfig(
            enabled=True,
            backend='jsonl',
            state_dir=tmp_path / 'cli',
            capture_tracebacks=False,
        )
        env_settings = ResolvedHistoryConfig(
            enabled=True,
            backend='sqlite',
            state_dir=tmp_path / 'env',
            capture_tracebacks=False,
        )
        captured: dict[str, object] = {}
        sentinel = object()

        monkeypatch.setattr(
            run_mod.ResolvedHistoryConfig,
            'resolve',
            classmethod(lambda _cls, *_args, **_kwargs: env_settings),
        )
        monkeypatch.setattr(
            run_mod.HistoryStore,
            'from_environment',
            lambda: (_ for _ in ()).throw(
                AssertionError('from_environment should not be called'),
            ),
        )

        def fake_from_settings(**kwargs: object) -> object:
            captured.update(kwargs)
            return sentinel

        monkeypatch.setattr(run_mod.HistoryStore, 'from_settings', fake_from_settings)

        assert run_mod._open_history_store(settings) is sentinel
        assert captured == {
            'backend': 'jsonl',
            'state_dir': tmp_path / 'cli',
        }


class TestPersistedRunSummary:
    """Unit tests for compact persisted run summaries."""

    def test_persisted_run_summary_compacts_dag_results(self) -> None:
        """DAG results should persist only the aggregate run-level summary."""
        assert run_mod._persisted_run_summary(
            {
                'continue_on_fail': True,
                'executed_job_count': 1,
                'executed_jobs': [
                    {
                        'duration_ms': 12,
                        'job': 'seed',
                        'result': {'rows': 10, 'status': 'success'},
                        'result_status': 'success',
                        'status': 'succeeded',
                    },
                    {
                        'duration_ms': 0,
                        'job': 'publish',
                        'reason': 'upstream_failed',
                        'skipped_due_to': ['seed'],
                        'status': 'skipped',
                    },
                ],
                'failed_job_count': 0,
                'failed_jobs': [],
                'final_job': 'publish',
                'final_result': {'rows': 10, 'status': 'success'},
                'final_result_status': None,
                'job_count': 2,
                'mode': 'all',
                'ordered_jobs': ['seed', 'publish'],
                'requested_job': None,
                'skipped_job_count': 1,
                'skipped_jobs': ['publish'],
                'status': 'partial_success',
                'succeeded_job_count': 1,
                'succeeded_jobs': ['seed'],
            },
        ) == {
            'continue_on_fail': True,
            'executed_job_count': 1,
            'failed_job_count': 0,
            'failed_jobs': [],
            'final_job': 'publish',
            'final_result_status': None,
            'job_count': 2,
            'mode': 'all',
            'ordered_jobs': ['seed', 'publish'],
            'requested_job': None,
            'skipped_job_count': 1,
            'skipped_jobs': ['publish'],
            'status': 'partial_success',
            'succeeded_job_count': 1,
            'succeeded_jobs': ['seed'],
        }

    def test_persisted_run_summary_preserves_non_dag_results(self) -> None:
        """Single-job results should keep their existing persisted summary."""
        result = {'job': 'seed', 'status': 'success'}
        assert run_mod._persisted_run_summary(result) == result


class TestRunSummaryHelpers:
    """Unit tests for compact DAG-summary helper functions."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(None, [], id='none'),
            pytest.param('seed', [], id='non-list'),
            pytest.param(['seed', 3, 'publish'], ['seed', 'publish'], id='mixed-list'),
        ],
    )
    def test_coerce_string_list_filters_to_strings(
        self,
        value: object,
        expected: list[str],
    ) -> None:
        """
        Test that string-list coercion discards non-list and non-string inputs.
        """
        assert run_mod._coerce_string_list(value) == expected

    @pytest.mark.parametrize(
        (
            'result',
            'continue_on_fail',
            'failed_jobs',
            'skipped_jobs',
            'succeeded_jobs',
            'expected',
        ),
        [
            pytest.param(
                {'status': 'custom'},
                False,
                [],
                [],
                [],
                'custom',
                id='explicit-status-wins',
            ),
            pytest.param(
                {},
                True,
                ['seed'],
                [],
                ['publish'],
                'partial_success',
                id='continue-on-fail-with-successes',
            ),
            pytest.param(
                {},
                False,
                ['seed'],
                ['notify'],
                [],
                'failed',
                id='failed-or-skipped-without-continue',
            ),
            pytest.param(
                {},
                False,
                [],
                [],
                ['seed'],
                'success',
                id='fallback-success',
            ),
        ],
    )
    def test_dag_run_status_normalizes_aggregate_status(
        self,
        result: dict[str, object],
        continue_on_fail: bool,
        failed_jobs: list[str],
        skipped_jobs: list[str],
        succeeded_jobs: list[str],
        expected: str,
    ) -> None:
        """
        Test that aggregate DAG status follows explicit, partial, failed, and
        success cases.
        """
        assert (
            run_mod._dag_run_status(
                result,
                continue_on_fail=continue_on_fail,
                failed_jobs=failed_jobs,
                skipped_jobs=skipped_jobs,
                succeeded_jobs=succeeded_jobs,
            )
            == expected
        )

    @pytest.mark.parametrize(
        ('executed_jobs', 'field_name', 'expected'),
        [
            pytest.param([], 'job', None, id='empty'),
            pytest.param(['bad-row'], 'job', None, id='all-non-mappings'),
            pytest.param(
                [{'job': 'seed'}, {'job': ''}],
                'job',
                None,
                id='last-mapping-missing-string',
            ),
            pytest.param(
                ['bad-row', {'result_status': 'success'}],
                'result_status',
                'success',
                id='skips-trailing-non-mappings',
            ),
        ],
    )
    def test_last_job_field_handles_empty_invalid_and_trailing_rows(
        self,
        executed_jobs: list[object],
        field_name: str,
        expected: str | None,
    ) -> None:
        """
        Test that trailing-job field lookup ignores invalid rows and requires
        non-empty strings.
        """
        assert run_mod._last_job_field(executed_jobs, field_name=field_name) == expected

    @pytest.mark.parametrize(
        ('result', 'expected'),
        [
            pytest.param(None, None, id='none'),
            pytest.param('plain-text', 'plain-text', id='non-mapping'),
        ],
    )
    def test_persisted_run_summary_preserves_none_and_non_mapping_inputs(
        self,
        result: object,
        expected: object,
    ) -> None:
        """
        Test that persisted run summaries preserve null and non-mapping shapes.
        """
        assert run_mod._persisted_run_summary(result) == expected


class TestTelemetryConfiguration:
    """Unit tests for telemetry setup in the run handler."""

    def test_run_handler_configures_telemetry_before_start_event(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Run handler should install config-backed telemetry before startup."""
        configure_calls: list[object] = []

        cfg = run_mod.Config.from_dict(
            {
                'name': 'Telemetry Pipeline',
                'telemetry': {
                    'enabled': True,
                    'exporter': 'opentelemetry',
                    'service_name': 'etlplus-run-tests',
                },
                'sources': [],
                'targets': [],
                'jobs': [],
            },
        )

        monkeypatch.setattr(run_mod.Config, 'from_yaml', lambda *_args, **_kwargs: cfg)
        monkeypatch.setattr(
            run_mod.RuntimeTelemetry,
            'configure',
            classmethod(
                lambda _cls, config=None, **_kwargs: configure_calls.append(config),
            ),
        )
        monkeypatch.setattr(
            run_mod._summary,
            'pipeline_summary',
            lambda _cfg: {'name': 'Telemetry Pipeline'},
        )
        monkeypatch.setattr(
            run_mod._output,
            'emit_json_payload',
            lambda _payload, pretty=True: 0,
        )

        assert run_mod.run_handler(config='pipeline.yml') == 0
        assert len(configure_calls) == 1
        assert getattr(configure_calls[0], 'service_name', None) == 'etlplus-run-tests'
