"""
:mod:`tests.unit.cli.test_u_cli_handlers_lifecycle` module.

Direct unit tests for :mod:`etlplus.cli._handlers._lifecycle`.
"""

from __future__ import annotations

import pytest

import etlplus.cli._handlers._lifecycle as lifecycle_mod
from etlplus.history import RunCompletion
from etlplus.history import RunState

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


# SECTION: HELPERS ========================================================== #


class _FakeHistoryStore:
    """Small test double that records persisted run completions."""

    def __init__(self) -> None:
        self.completions: list[RunCompletion] = []

    def record_run_finished(
        self,
        completion: RunCompletion,
    ) -> None:
        """Capture one completion object."""
        self.completions.append(completion)


def _command_context(
    *,
    command: str = 'run',
    event_format: str | None = None,
    run_id: str = 'run-123',
    started_at: str = '2026-04-01T12:00:00Z',
    started_perf: float = 1.0,
) -> lifecycle_mod.CommandContext:
    """Build one command context with stable defaults for test setup."""
    return lifecycle_mod.CommandContext(
        command=command,
        event_format=event_format,
        run_id=run_id,
        started_at=started_at,
        started_perf=started_perf,
    )


# SECTION: TESTS ============================================================ #


class TestCaptureTraceback:
    """Unit tests for :func:`etlplus.cli._handlers._lifecycle._capture_traceback`."""

    def test_truncates_long_traceback_strings(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Persisted failure tracebacks should be capped to the configured limit."""
        monkeypatch.setattr(
            lifecycle_mod,
            'format_exception',
            lambda *_args: ['x' * (16_000 + 250)],
        )

        text = lifecycle_mod._capture_traceback(RuntimeError('boom'))

        assert len(text) == 16_000
        assert text.endswith('\n...[truncated]\n')


class TestCommandContext:
    """
    Unit tests for :class:`etlplus.cli._handlers._lifecycle.CommandContext`.
    """

    def test_stores_supplied_fields(self) -> None:
        """Test that the dataclass preserves the supplied runtime values."""
        context = _command_context(
            command='run',
            event_format='jsonl',
            run_id='run-123',
            started_at='2026-04-01T12:00:00Z',
            started_perf=12.5,
        )

        assert context.command == 'run'
        assert context.event_format == 'jsonl'
        assert context.run_id == 'run-123'
        assert context.started_at == '2026-04-01T12:00:00Z'
        assert context.started_perf == 12.5


class TestCompleteCommand:
    """
    Unit tests for :func:`etlplus.cli._handlers._lifecycle.complete_command`.
    """

    def test_emits_completed_event(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that completed events forward context data and duration."""
        captured: dict[str, object] = {}
        context = _command_context(
            command='validate',
            event_format='jsonl',
            run_id='run-987',
            started_at='2026-04-01T14:00:00Z',
            started_perf=50.0,
        )

        monkeypatch.setattr(lifecycle_mod, 'elapsed_ms', lambda _started_perf: 654)

        def fake_emit_lifecycle_event(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(
            lifecycle_mod,
            'emit_lifecycle_event',
            fake_emit_lifecycle_event,
        )

        lifecycle_mod.complete_command(context, status='ok', valid=True)

        assert captured == {
            'command': 'validate',
            'lifecycle': 'completed',
            'run_id': 'run-987',
            'event_format': 'jsonl',
            'duration_ms': 654,
            'status': 'ok',
            'valid': True,
        }


class TestElapsedMs:
    """Unit tests for :func:`etlplus.cli._handlers._lifecycle.elapsed_ms`."""

    def test_uses_perf_counter_delta(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Test that elapsed milliseconds derive from the current perf counter.
        """
        monkeypatch.setattr(lifecycle_mod, 'perf_counter', lambda: 12.345)
        assert lifecycle_mod.elapsed_ms(10.0) == 2345


class TestEmitLifecycleEvent:
    """
    Unit tests for :func:`etlplus.cli._handlers._lifecycle.emit_lifecycle_event`.
    """

    def test_builds_and_emits_runtime_event(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that lifecycle emission builds one event and emits it verbatim.
        """
        captured: dict[str, object] = {}
        built_payload: dict[str, object] = {'built': True}

        def fake_build(**kwargs: object) -> dict[str, object]:
            captured['build_kwargs'] = kwargs
            return built_payload

        def fake_emit(payload: object, *, event_format: str | None) -> None:
            captured['emit_payload'] = payload
            captured['event_format'] = event_format

        monkeypatch.setattr(lifecycle_mod.RuntimeEvents, 'build', fake_build)
        monkeypatch.setattr(lifecycle_mod.RuntimeEvents, 'emit', fake_emit)

        lifecycle_mod.emit_lifecycle_event(
            command='extract',
            lifecycle='started',
            run_id='run-123',
            event_format='jsonl',
            source='input.json',
        )

        assert captured == {
            'build_kwargs': {
                'command': 'extract',
                'lifecycle': 'started',
                'run_id': 'run-123',
                'source': 'input.json',
            },
            'emit_payload': built_payload,
            'event_format': 'jsonl',
        }


class TestEmitFailureEvent:
    """
    Unit tests for :func:`etlplus.cli._handlers._lifecycle.emit_failure_event`.
    """

    def test_emits_failed_lifecycle_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that failed events include shared error metadata."""
        captured: dict[str, object] = {}

        monkeypatch.setattr(lifecycle_mod, 'elapsed_ms', lambda _started_perf: 321)

        def fake_emit_lifecycle_event(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(
            lifecycle_mod,
            'emit_lifecycle_event',
            fake_emit_lifecycle_event,
        )

        lifecycle_mod.emit_failure_event(
            command='load',
            run_id='run-456',
            started_perf=1.25,
            event_format='jsonl',
            exc=ValueError('bad payload'),
            target='out.json',
        )

        assert captured == {
            'command': 'load',
            'lifecycle': 'failed',
            'run_id': 'run-456',
            'event_format': 'jsonl',
            'duration_ms': 321,
            'error_message': 'bad payload',
            'error_type': 'ValueError',
            'status': 'error',
            'target': 'out.json',
        }


class TestFailCommand:
    """
    Unit tests for :func:`etlplus.cli._handlers._lifecycle.fail_command`.
    """

    def test_forwards_context_and_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that fail command delegates to the shared failure emitter."""
        captured: dict[str, object] = {}
        context = _command_context(
            command='extract',
            event_format=None,
            run_id='run-111',
            started_at='2026-04-01T15:00:00Z',
            started_perf=1.0,
        )
        exc = RuntimeError('boom')

        def fake_emit_failure_event(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(
            lifecycle_mod,
            'emit_failure_event',
            fake_emit_failure_event,
        )

        lifecycle_mod.fail_command(context, exc, source='stdin')

        assert captured == {
            'command': 'extract',
            'run_id': 'run-111',
            'started_perf': 1.0,
            'event_format': None,
            'exc': exc,
            'source': 'stdin',
        }


class TestFailureBoundary:
    """
    Unit tests for :func:`etlplus.cli._handlers._lifecycle.failure_boundary`.
    """

    def test_error_path_invokes_callback_before_fail_command(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that error callbacks run before the shared fail emission."""
        calls: list[tuple[str, object]] = []
        context = _command_context(
            command='load',
            event_format='jsonl',
            run_id='run-222',
            started_at='2026-04-01T17:00:00Z',
            started_perf=1.0,
        )

        def on_error(exc: Exception) -> None:
            calls.append(('on_error', exc))

        def fake_fail_command(
            context_arg: object,
            exc: Exception,
            **fields: object,
        ) -> None:
            calls.append(('fail_command', (context_arg, exc, dict(fields))))

        monkeypatch.setattr(lifecycle_mod, 'fail_command', fake_fail_command)

        with pytest.raises(RuntimeError, match='boom'):
            with lifecycle_mod.failure_boundary(
                context,
                on_error=on_error,
                step='load',
            ):
                raise RuntimeError('boom')

        assert calls[0][0] == 'on_error'
        assert isinstance(calls[0][1], RuntimeError)
        assert calls[1] == (
            'fail_command',
            (
                context,
                calls[0][1],
                {'step': 'load'},
            ),
        )

    def test_error_path_without_callback_still_fails_and_reraises(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that the no-callback branch still emits failure and re-raises."""
        captured: list[tuple[object, Exception, dict[str, object]]] = []
        context = _command_context(
            command='run',
            event_format=None,
            run_id='run-333',
            started_at='2026-04-01T18:00:00Z',
            started_perf=2.0,
        )

        def fake_fail_command(
            context_arg: object,
            exc: Exception,
            **fields: object,
        ) -> None:
            captured.append((context_arg, exc, dict(fields)))

        monkeypatch.setattr(lifecycle_mod, 'fail_command', fake_fail_command)

        with pytest.raises(ValueError, match='bad input'):
            with lifecycle_mod.failure_boundary(
                context,
                step='run',
            ):
                raise ValueError('bad input')

        assert len(captured) == 1
        context_arg, exc, fields = captured[0]
        assert context_arg == context
        assert isinstance(exc, ValueError)
        assert str(exc) == 'bad input'
        assert fields == {'step': 'run'}

    def test_success_path_skips_failures(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that successful blocks do not call fail or error callbacks."""
        fail_calls: list[tuple[object, Exception, dict[str, object]]] = []
        on_error_calls: list[Exception] = []
        context = _command_context(
            command='extract',
            event_format=None,
            run_id='run-123',
            started_at='2026-04-01T16:00:00Z',
            started_perf=0.0,
        )

        def fake_fail_command(
            context_arg: object,
            exc: Exception,
            **fields: object,
        ) -> None:
            fail_calls.append((context_arg, exc, dict(fields)))

        monkeypatch.setattr(lifecycle_mod, 'fail_command', fake_fail_command)

        with lifecycle_mod.failure_boundary(
            context,
            on_error=on_error_calls.append,
            step='extract',
        ):
            pass

        assert not fail_calls
        assert not on_error_calls


class TestRecordRunCompletion:
    """
    Unit tests for :func:`etlplus.cli._handlers._lifecycle.record_run_completion`.
    """

    @pytest.mark.parametrize(
        (
            'status',
            'context',
            'finished_at',
            'duration_ms',
            'result_summary',
            'exc',
            'expected_state',
        ),
        [
            pytest.param(
                'failed',
                _command_context(
                    run_id='run-555',
                    started_at='2026-04-01T20:00:00Z',
                    started_perf=4.0,
                ),
                '2026-04-01T20:45:00Z',
                888,
                None,
                RuntimeError('pipeline failed'),
                RunState(
                    status='failed',
                    finished_at='2026-04-01T20:45:00Z',
                    duration_ms=888,
                    result_summary=None,
                    error_type='RuntimeError',
                    error_message='pipeline failed',
                ),
                id='failure',
            ),
            pytest.param(
                'succeeded',
                _command_context(
                    run_id='run-444',
                    started_at='2026-04-01T19:00:00Z',
                    started_perf=3.0,
                ),
                '2026-04-01T19:30:00Z',
                777,
                {'rows': 10},
                None,
                RunState(
                    status='succeeded',
                    finished_at='2026-04-01T19:30:00Z',
                    duration_ms=777,
                    result_summary={'rows': 10},
                    error_type=None,
                    error_message=None,
                ),
                id='success',
            ),
        ],
    )
    def test_records_completion(
        self,
        monkeypatch: pytest.MonkeyPatch,
        status: str,
        context: lifecycle_mod.CommandContext,
        finished_at: str,
        duration_ms: int,
        result_summary: dict[str, object] | list[dict[str, object]] | None,
        exc: Exception | None,
        expected_state: RunState,
    ) -> None:
        """
        Test that run completions persist either success or failure metadata.
        """
        store = _FakeHistoryStore()
        telemetry_calls: list[dict[str, object]] = []
        monkeypatch.setattr(
            lifecycle_mod.RuntimeEvents,
            'utc_now_iso',
            lambda: finished_at,
        )
        monkeypatch.setattr(
            lifecycle_mod,
            'elapsed_ms',
            lambda _started_perf: duration_ms,
        )
        monkeypatch.setattr(
            lifecycle_mod.RuntimeTelemetry,
            'emit_history_record',
            classmethod(
                lambda _cls, record, *, record_level: telemetry_calls.append(
                    {'record': dict(record), 'record_level': record_level},
                ),
            ),
        )

        lifecycle_mod.record_run_completion(
            store,
            context,
            status=status,
            pipeline_name='customer-sync',
            job_name='daily',
            config_path='pipeline.yml',
            etlplus_version='1.2.3',
            result_summary=result_summary,
            exc=exc,
        )

        assert store.completions == [
            RunCompletion(
                run_id=context.run_id,
                state=expected_state,
            ),
        ]
        assert telemetry_calls == [
            {
                'record': {
                    'config_path': 'pipeline.yml',
                    'config_sha256': None,
                    'duration_ms': expected_state.duration_ms,
                    'error_message': expected_state.error_message,
                    'error_traceback': expected_state.error_traceback,
                    'error_type': expected_state.error_type,
                    'etlplus_version': '1.2.3',
                    'finished_at': expected_state.finished_at,
                    'host': None,
                    'job_name': 'daily',
                    'pid': None,
                    'pipeline_name': 'customer-sync',
                    'records_in': None,
                    'records_out': None,
                    'result_summary': expected_state.result_summary,
                    'run_id': context.run_id,
                    'started_at': context.started_at,
                    'status': expected_state.status,
                },
                'record_level': 'run',
            },
        ]

    def test_records_handled_failure_without_exception_from_explicit_fields(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that handled failures persist explicit error fields without an
        exception object.
        """
        store = _FakeHistoryStore()
        context = _command_context(
            run_id='run-handled-1',
            started_perf=5.0,
        )
        telemetry_calls: list[dict[str, object]] = []
        monkeypatch.setattr(
            lifecycle_mod.RuntimeEvents,
            'utc_now_iso',
            lambda: '2026-04-01T21:00:00Z',
        )
        monkeypatch.setattr(
            lifecycle_mod,
            'elapsed_ms',
            lambda _started_perf: 999,
        )
        monkeypatch.setattr(
            lifecycle_mod.RuntimeTelemetry,
            'emit_history_record',
            classmethod(
                lambda _cls, record, *, record_level: telemetry_calls.append(
                    {'record': dict(record), 'record_level': record_level},
                ),
            ),
        )

        lifecycle_mod.record_run_completion(
            store,
            context,
            status='failed',
            pipeline_name='customer-sync',
            error_message='DAG execution failed',
            error_type='RunExecutionFailed',
            result_summary={'status': 'failed'},
        )

        assert store.completions == [
            RunCompletion(
                run_id='run-handled-1',
                state=RunState(
                    status='failed',
                    finished_at='2026-04-01T21:00:00Z',
                    duration_ms=999,
                    result_summary={'status': 'failed'},
                    error_type='RunExecutionFailed',
                    error_message='DAG execution failed',
                ),
            ),
        ]
        assert telemetry_calls[0]['record_level'] == 'run'


class TestStartCommand:
    """
    Unit tests for :func:`etlplus.cli._handlers._lifecycle.start_command`.
    """

    def test_returns_context_and_emits_started_event(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that starting a command seeds the context from runtime helpers.
        """
        captured: dict[str, object] = {}

        monkeypatch.setattr(
            lifecycle_mod.RuntimeEvents,
            'create_run_id',
            lambda: 'run-789',
        )
        monkeypatch.setattr(
            lifecycle_mod.RuntimeEvents,
            'utc_now_iso',
            lambda: '2026-04-01T13:00:00Z',
        )
        monkeypatch.setattr(lifecycle_mod, 'perf_counter', lambda: 98.75)

        def fake_emit_lifecycle_event(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(
            lifecycle_mod,
            'emit_lifecycle_event',
            fake_emit_lifecycle_event,
        )

        context = lifecycle_mod.start_command(
            command='transform',
            event_format='jsonl',
            source='input.csv',
        )

        assert context == _command_context(
            command='transform',
            event_format='jsonl',
            run_id='run-789',
            started_at='2026-04-01T13:00:00Z',
            started_perf=98.75,
        )
        assert captured == {
            'command': 'transform',
            'lifecycle': 'started',
            'run_id': 'run-789',
            'event_format': 'jsonl',
            'timestamp': '2026-04-01T13:00:00Z',
            'source': 'input.csv',
        }
