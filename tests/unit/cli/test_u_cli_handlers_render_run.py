"""
:mod:`tests.unit.cli.test_u_cli_handlers_render_run` module.

Unit tests for render and run entry points in :mod:`etlplus.cli._handlers`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast
from unittest.mock import ANY

import pytest

from etlplus import Config
from etlplus.cli._handlers import render as render_mod
from etlplus.cli._handlers import run as run_mod

from .conftest import CaptureIo
from .conftest import assert_emit_json
from .pytest_cli_handlers_support import handlers
from .pytest_cli_handlers_support import patch_config_from_yaml

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestRenderHandler:
    """Unit tests for :func:`render_handler`."""

    def test_errors_without_specs(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`render_handler` reports missing specs."""

        assert (
            handlers.render_handler(
                config=None,
                spec=None,
                table=None,
                template='ddl',
                template_path=None,
                output=None,
                pretty=True,
                quiet=False,
            )
            == 1
        )
        assert 'No table schemas found' in capsys.readouterr().err

    def test_output_file_respects_quiet_flag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test output-file rendering without a status log in quiet mode."""
        output_path = tmp_path / 'rendered.sql'
        monkeypatch.setattr(
            handlers._summary,
            'collect_table_specs',
            lambda _cfg, _spec: [{'table': 'Widget'}],
        )
        monkeypatch.setattr(
            render_mod,
            'render_tables',
            lambda specs, **kwargs: ['SELECT 1'],
        )

        assert (
            handlers.render_handler(
                config='pipeline.yml',
                spec=None,
                table=None,
                template='ddl',
                template_path='custom.sql.j2',
                output=str(output_path),
                pretty=True,
                quiet=True,
            )
            == 0
        )
        assert output_path.read_text(encoding='utf-8') == 'SELECT 1\n'
        assert 'Rendered 1 schema(s)' not in capsys.readouterr().out

    def test_uses_template_file_override_when_template_is_a_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that template-path auto-detection from the ``template`` argument.
        """
        template_path = tmp_path / 'ddl.sql.j2'
        template_path.write_text('CREATE TABLE {{ table }}', encoding='utf-8')
        monkeypatch.setattr(
            handlers._summary,
            'collect_table_specs',
            lambda _cfg, _spec: [{'table': 'Widget'}],
        )
        captured: dict[str, object] = {}

        def _render_tables(
            specs: list[dict[str, object]],
            *,
            template: str | None,
            template_path: str | None,
        ) -> list[str]:
            captured['specs'] = specs
            captured['template'] = template
            captured['template_path'] = template_path
            return ['SELECT 1']

        monkeypatch.setattr(render_mod, 'render_tables', _render_tables)

        assert (
            handlers.render_handler(
                config='pipeline.yml',
                spec=None,
                table='Widget',
                template=cast(Any, str(template_path)),
                template_path=None,
                output='-',
                pretty=False,
                quiet=True,
            )
            == 0
        )
        assert captured['template'] is None
        assert captured['template_path'] == str(template_path)
        assert captured['specs'] == [{'table': 'Widget'}]
        assert capsys.readouterr().out.strip() == 'SELECT 1'

    def test_writes_sql_from_spec(
        self,
        widget_spec_paths: tuple[Path, Path],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`render_handler` writes SQL for standalone specs."""
        spec_path, output_path = widget_spec_paths
        assert (
            handlers.render_handler(
                config=None,
                spec=str(spec_path),
                table=None,
                template='ddl',
                template_path=None,
                output=str(output_path),
                pretty=True,
                quiet=False,
            )
            == 0
        )

        sql_text = output_path.read_text(encoding='utf-8')
        assert 'CREATE TABLE [dbo].[Widget]' in sql_text

        captured = capsys.readouterr()
        assert f'Rendered 1 schema(s) to {output_path}' in captured.out


class TestRunHandler:
    """Unit tests for :func:`run_handler`."""

    def test_emits_pipeline_summary_without_job(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`run_handler` emits a summary when no job set."""
        patch_config_from_yaml(monkeypatch, dummy_cfg)

        assert (
            handlers.run_handler(
                config='pipeline.yml',
                job=None,
                pipeline=None,
                pretty=True,
            )
            == 0
        )

        assert_emit_json(
            capture_io,
            {
                'name': dummy_cfg.name,
                'version': dummy_cfg.version,
                'sources': ['s1'],
                'targets': ['t1'],
                'jobs': ['j1'],
            },
            pretty=True,
        )

    def test_runs_job_and_emits_result(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`run_handler` executes a named job and emits status.
        """
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            handlers.RuntimeEvents,
            'create_run_id',
            lambda: 'run-123',
        )

        history_calls: dict[str, object] = {}
        job_runs: list[object] = []

        class _FakeHistoryStore:
            def record_run_started(self, record: object) -> None:
                history_calls['started'] = record

            def record_job_run(self, record: object) -> None:
                job_runs.append(record)

            def record_run_finished(
                self,
                completion: object,
            ) -> None:
                completion_obj = cast(Any, completion)
                history_calls['finished'] = {
                    'duration_ms': completion_obj.state.duration_ms,
                    'error_message': completion_obj.state.error_message,
                    'error_traceback': completion_obj.state.error_traceback,
                    'error_type': completion_obj.state.error_type,
                    'finished_at': completion_obj.state.finished_at,
                    'result_summary': completion_obj.state.result_summary,
                    'run_id': completion_obj.run_id,
                    'status': completion_obj.state.status,
                }

        monkeypatch.setattr(
            handlers.HistoryStore,
            'from_environment',
            _FakeHistoryStore,
        )
        run_calls: dict[str, object] = {}

        def fake_run(
            *,
            job: str,
            config_path: str,
            run_all: bool = False,
            continue_on_fail: bool = False,
            max_concurrency: int | None = None,
        ) -> dict[str, object]:
            run_calls['params'] = (
                job,
                config_path,
                run_all,
                continue_on_fail,
                max_concurrency,
            )
            return {'job': job, 'ok': True}

        monkeypatch.setattr(run_mod, 'run', fake_run)

        assert (
            handlers.run_handler(
                config='pipeline.yml',
                job='job1',
                pretty=False,
            )
            == 0
        )
        assert run_calls['params'] == ('job1', 'pipeline.yml', False, False, None)
        assert cast(Any, history_calls['started']).run_id == 'run-123'
        assert history_calls['finished'] == {
            'duration_ms': ANY,
            'error_message': None,
            'error_traceback': None,
            'error_type': None,
            'finished_at': ANY,
            'result_summary': {'job': 'job1', 'ok': True},
            'run_id': 'run-123',
            'status': 'succeeded',
        }
        assert_emit_json(
            capture_io,
            {
                'run_id': 'run-123',
                'status': 'ok',
                'result': {'job': 'job1', 'ok': True},
            },
            pretty=False,
        )

    def test_run_all_failure_summary_returns_nonzero_and_records_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Handled DAG failures should emit JSON, record failure, and return 1."""
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            handlers.RuntimeEvents,
            'create_run_id',
            lambda: 'run-all-1',
        )

        history_calls: dict[str, object] = {}
        job_runs: list[object] = []

        class _FakeHistoryStore:
            def record_run_started(self, record: object) -> None:
                history_calls['started'] = record

            def record_job_run(self, record: object) -> None:
                job_runs.append(record)

            def record_run_finished(
                self,
                completion: object,
            ) -> None:
                completion_obj = cast(Any, completion)
                history_calls['finished'] = completion_obj.state

        monkeypatch.setattr(
            handlers.HistoryStore,
            'from_environment',
            _FakeHistoryStore,
        )
        lifecycle_calls: list[dict[str, object]] = []
        monkeypatch.setattr(
            run_mod._lifecycle,
            'emit_lifecycle_event',
            lambda **kwargs: lifecycle_calls.append(kwargs),
        )
        monkeypatch.setattr(
            run_mod,
            'run',
            lambda **kwargs: {
                'continue_on_fail': True,
                'executed_jobs': [
                    {'job': 'seed', 'status': 'failed'},
                    {
                        'job': 'publish',
                        'reason': 'upstream_failed',
                        'skipped_due_to': ['seed'],
                        'status': 'skipped',
                    },
                ],
                'failed_jobs': ['seed'],
                'mode': 'all',
                'ordered_jobs': ['seed', 'publish'],
                'skipped_jobs': ['publish'],
                'status': 'failed',
                'succeeded_jobs': [],
            },
        )

        assert (
            handlers.run_handler(
                config='pipeline.yml',
                run_all=True,
                continue_on_fail=True,
                pretty=False,
            )
            == 1
        )
        assert cast(Any, history_calls['started']).job_name is None
        finished = cast(Any, history_calls['finished'])
        assert finished.status == 'failed'
        assert finished.error_type == 'RunExecutionFailed'
        assert 'DAG execution' in cast(str, finished.error_message)
        assert finished.result_summary == {
            'continue_on_fail': True,
            'executed_job_count': 1,
            'failed_job_count': 1,
            'failed_jobs': ['seed'],
            'final_job': 'publish',
            'final_result_status': None,
            'job_count': 2,
            'mode': 'all',
            'ordered_jobs': ['seed', 'publish'],
            'requested_job': None,
            'skipped_job_count': 1,
            'skipped_jobs': ['publish'],
            'status': 'failed',
            'succeeded_job_count': 0,
            'succeeded_jobs': [],
        }
        assert len(job_runs) == 2
        assert [call['lifecycle'] for call in lifecycle_calls] == [
            'started',
            'failed',
        ]
        assert lifecycle_calls[1] == {
            'command': 'run',
            'lifecycle': 'failed',
            'run_id': 'run-all-1',
            'event_format': None,
            'continue_on_fail': True,
            'config_path': 'pipeline.yml',
            'duration_ms': ANY,
            'error_message': 'Job "seed" failed during DAG execution',
            'error_type': 'RunExecutionFailed',
            'etlplus_version': run_mod.__version__,
            'job': None,
            'pipeline_name': dummy_cfg.name,
            'result_status': 'failed',
            'run_all': True,
            'status': 'error',
        }
        assert_emit_json(
            capture_io,
            {
                'run_id': 'run-all-1',
                'status': 'error',
                'result': {
                    'continue_on_fail': True,
                    'executed_jobs': [
                        {'job': 'seed', 'status': 'failed'},
                        {
                            'job': 'publish',
                            'reason': 'upstream_failed',
                            'skipped_due_to': ['seed'],
                            'status': 'skipped',
                        },
                    ],
                    'failed_jobs': ['seed'],
                    'mode': 'all',
                    'ordered_jobs': ['seed', 'publish'],
                    'skipped_jobs': ['publish'],
                    'status': 'failed',
                    'succeeded_jobs': [],
                },
            },
            pretty=False,
        )

    def test_run_exception_captures_traceback_when_enabled(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
    ) -> None:
        """History completion should include a capped traceback when enabled."""
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            handlers.RuntimeEvents,
            'create_run_id',
            lambda: 'run-trace-1',
        )

        completions: list[Any] = []

        class _FakeHistoryStore:
            def record_run_started(self, record: object) -> None:
                _ = record

            def record_job_run(self, record: object) -> None:
                _ = record

            def record_run_finished(self, completion: object) -> None:
                completions.append(completion)

        monkeypatch.setattr(
            handlers.HistoryStore,
            'from_environment',
            _FakeHistoryStore,
        )

        def _raise_error(**_kwargs: object) -> object:
            raise ValueError('boom')

        monkeypatch.setattr(run_mod, 'run', _raise_error)

        with pytest.raises(ValueError, match='boom'):
            handlers.run_handler(
                config='pipeline.yml',
                job='job1',
                capture_tracebacks=True,
                pretty=False,
            )

        assert len(completions) == 1
        assert completions[0].state.error_type == 'ValueError'
        assert completions[0].state.error_message == 'boom'
        assert completions[0].state.error_traceback is not None
        assert 'ValueError: boom' in cast(str, completions[0].state.error_traceback)

    def test_run_exception_emits_failed_event_with_stable_context(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
    ) -> None:
        """
        Test that unhandled run exceptions still emit the stable failed event
        context.
        """
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            handlers.RuntimeEvents,
            'create_run_id',
            lambda: 'run-exc-1',
        )

        class _FakeHistoryStore:
            def record_run_started(self, record: object) -> None:
                _ = record

            def record_job_run(self, record: object) -> None:
                _ = record

            def record_run_finished(self, completion: object) -> None:
                _ = completion

        monkeypatch.setattr(
            handlers.HistoryStore,
            'from_environment',
            _FakeHistoryStore,
        )
        lifecycle_calls: list[dict[str, object]] = []
        monkeypatch.setattr(
            run_mod._lifecycle,
            'emit_lifecycle_event',
            lambda **kwargs: lifecycle_calls.append(kwargs),
        )
        monkeypatch.setattr(
            run_mod,
            'run',
            lambda **kwargs: (_ for _ in ()).throw(TypeError('bad connector path')),
        )

        with pytest.raises(TypeError, match='bad connector path'):
            handlers.run_handler(
                config='pipeline.yml',
                job='job1',
                pretty=False,
            )

        assert [call['lifecycle'] for call in lifecycle_calls] == [
            'started',
            'failed',
        ]
        assert lifecycle_calls[1] == {
            'command': 'run',
            'lifecycle': 'failed',
            'run_id': 'run-exc-1',
            'event_format': None,
            'config_path': 'pipeline.yml',
            'continue_on_fail': False,
            'duration_ms': ANY,
            'error_message': 'bad connector path',
            'error_type': 'TypeError',
            'etlplus_version': run_mod.__version__,
            'job': 'job1',
            'pipeline_name': dummy_cfg.name,
            'run_all': False,
            'status': 'error',
        }

    def test_run_handler_skips_local_history_when_disabled(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Disabling history should skip store creation and persistence."""
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            handlers.RuntimeEvents,
            'create_run_id',
            lambda: 'run-no-history',
        )
        monkeypatch.setattr(
            handlers.HistoryStore,
            'from_environment',
            lambda: (_ for _ in ()).throw(
                AssertionError('history store should not be opened'),
            ),
        )
        monkeypatch.setattr(
            handlers.HistoryStore,
            'from_settings',
            lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError('history store should not be opened'),
            ),
        )
        monkeypatch.setattr(
            run_mod,
            'run',
            lambda **_kwargs: {'job': 'job1', 'ok': True},
        )

        assert (
            handlers.run_handler(
                config='pipeline.yml',
                job='job1',
                history_enabled=False,
                pretty=False,
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {
                'run_id': 'run-no-history',
                'status': 'ok',
                'result': {'job': 'job1', 'ok': True},
            },
            pretty=False,
        )
