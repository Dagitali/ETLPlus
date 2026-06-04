"""
:mod:`tests.unit.telemetry.test_u_telemetry_runtime` module.

Unit tests for :mod:`etlplus.telemetry.runtime`.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from types import ModuleType
from typing import Any
from typing import cast

import pytest

import etlplus.telemetry.runtime as telemetry_runtime_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: CONSTANTS ======================================================== #


DISABLED_CONFIG = telemetry_runtime_mod.TelemetryConfig(enabled=False)
DISABLED_SETTINGS = telemetry_runtime_mod.ResolvedTelemetryConfig(
    enabled=False,
    exporter='none',
    service_name='etlplus',
)

ENABLED_OTEL_CONFIG = telemetry_runtime_mod.TelemetryConfig(
    enabled=True,
    exporter='opentelemetry',
    service_name='etlplus-tests',
)
ENABLED_OTEL_SETTINGS = telemetry_runtime_mod.ResolvedTelemetryConfig(
    enabled=True,
    exporter='opentelemetry',
    service_name='etlplus-tests',
)


# SECTION: HELPERS ========================================================== #


class _FakeCounter:
    def __init__(self) -> None:
        self.calls: list[tuple[int, dict[str, object]]] = []

    def add(
        self,
        value: int,
        attributes: dict[str, object],
    ) -> None:
        """Record one counter increment call."""
        self.calls.append((value, dict(attributes)))


class _FakeHistogram:
    def __init__(self) -> None:
        self.calls: list[tuple[int, dict[str, object]]] = []

    def record(
        self,
        value: int,
        attributes: dict[str, object],
    ) -> None:
        """Record one histogram sample call."""
        self.calls.append((value, dict(attributes)))


class _FakeSpan:
    def __init__(self, name: str) -> None:
        self.name = name
        self.attributes: dict[str, object] = {}
        self.events: list[tuple[str, dict[str, object]]] = []
        self.exceptions: list[Exception] = []
        self.status: object | None = None
        self.ended = False

    def add_event(
        self,
        name: str,
        attributes: dict[str, object],
    ) -> None:
        """Record one span event."""
        self.events.append((name, dict(attributes)))

    def end(self) -> None:
        """Mark the fake span as ended."""
        self.ended = True

    def record_exception(
        self,
        exc: Exception,
    ) -> None:
        """Record one exception attached to the span."""
        self.exceptions.append(exc)

    def set_attributes(
        self,
        attributes: dict[str, object],
    ) -> None:
        """Merge span attributes into the fake span."""
        self.attributes.update(attributes)

    def set_status(
        self,
        status: object,
    ) -> None:
        """Store one final span status value."""
        self.status = status


class _FakeTracer:
    def __init__(self) -> None:
        self.spans: list[_FakeSpan] = []

    def start_span(
        self,
        name: str,
    ) -> _FakeSpan:
        """Create and return one fake span."""
        span = _FakeSpan(name)
        self.spans.append(span)
        return span


class _FakeMeter:
    def __init__(self) -> None:
        self.counters: list[_FakeCounter] = []
        self.histograms: list[_FakeHistogram] = []

    def create_counter(self, *_args: object, **_kwargs: object) -> _FakeCounter:
        """Create and return one fake counter."""
        counter = _FakeCounter()
        self.counters.append(counter)
        return counter

    def create_histogram(self, *_args: object, **_kwargs: object) -> _FakeHistogram:
        """Create and return one fake histogram."""
        histogram = _FakeHistogram()
        self.histograms.append(histogram)
        return histogram


@dataclass
class _FakeStatus:
    code: object
    description: str | None = None


@dataclass
class _FakeStatusCode:
    OK = 'ok'
    ERROR = 'error'


class _FakeTraceModule(ModuleType):
    """Typed fake ``opentelemetry.trace`` module."""

    get_tracer: object
    Status: type[_FakeStatus]
    StatusCode: type[_FakeStatusCode]


class _FakeMetricsModule(ModuleType):
    """Typed fake ``opentelemetry.metrics`` module."""

    get_meter: object


class _FakeOpenTelemetryModule(ModuleType):
    """Typed fake ``opentelemetry`` root module."""

    trace: _FakeTraceModule
    metrics: _FakeMetricsModule


class _FakeOpenTelemetryInstaller:
    """Install typed fake OpenTelemetry modules for telemetry tests."""

    @classmethod
    def install(
        cls,
        monkeypatch: pytest.MonkeyPatch,
    ) -> tuple[_FakeTracer, _FakeMeter]:
        """Install lightweight fake OpenTelemetry modules into ``sys.modules``."""
        tracer = _FakeTracer()
        meter = _FakeMeter()

        trace_mod = _FakeTraceModule('opentelemetry.trace')
        trace_mod.get_tracer = lambda *_args, **_kwargs: tracer
        trace_mod.Status = _FakeStatus  # noqa: N815
        trace_mod.StatusCode = _FakeStatusCode  # noqa: N815

        metrics_mod = _FakeMetricsModule('opentelemetry.metrics')
        metrics_mod.get_meter = lambda *_args, **_kwargs: meter

        root_mod = _FakeOpenTelemetryModule('opentelemetry')
        root_mod.trace = trace_mod
        root_mod.metrics = metrics_mod

        monkeypatch.setitem(sys.modules, 'opentelemetry', root_mod)
        monkeypatch.setitem(sys.modules, 'opentelemetry.trace', trace_mod)
        monkeypatch.setitem(sys.modules, 'opentelemetry.metrics', metrics_mod)
        return tracer, meter


# SECTION: TESTS ============================================================ #


class TestTelemetryRuntime:
    """Unit tests for the process-local telemetry bridge."""

    def teardown_method(self) -> None:
        """Reset process-local telemetry state between tests."""
        telemetry_runtime_mod.RuntimeTelemetry.reset()

    def test_build_adapter_returns_none_for_unsupported_exporter(self) -> None:
        """Unsupported exporters should degrade to a no-op adapter."""
        settings = telemetry_runtime_mod.ResolvedTelemetryConfig(
            enabled=True,
            exporter=cast(Any, 'custom-exporter'),
            service_name='etlplus-tests',
        )

        assert (
            telemetry_runtime_mod._OpenTelemetryAdapter.build_adapter(settings) is None
        )

    def test_configure_reuses_cached_settings_without_rebuilding_adapter(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Repeated configure calls should reuse identical settings by default."""
        initial = telemetry_runtime_mod.RuntimeTelemetry.configure(
            DISABLED_CONFIG,
            env={},
            force=True,
        )

        monkeypatch.setattr(
            telemetry_runtime_mod._OpenTelemetryAdapter,
            'build_adapter',
            lambda _settings: (_ for _ in ()).throw(AssertionError('unexpected')),
        )

        resolved = telemetry_runtime_mod.RuntimeTelemetry.configure(
            DISABLED_CONFIG,
            env={},
        )

        assert resolved == initial

    def test_configure_telemetry_drops_adapter_when_optional_dep_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Missing optional dependencies should degrade to a no-op adapter."""
        monkeypatch.delitem(sys.modules, 'opentelemetry', raising=False)
        monkeypatch.delitem(sys.modules, 'opentelemetry.trace', raising=False)
        monkeypatch.delitem(sys.modules, 'opentelemetry.metrics', raising=False)

        with caplog.at_level('WARNING'):
            telemetry_runtime_mod.RuntimeTelemetry.configure(
                telemetry_runtime_mod.TelemetryConfig(enabled=True),
                env={},
                force=True,
            )

        assert telemetry_runtime_mod.RuntimeTelemetry._adapter is None
        assert 'optional OpenTelemetry dependencies are not installed' in caplog.text

    def test_emit_event_auto_configures_from_process_env_when_uninitialized(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """First event export should lazily configure telemetry from ``os.environ``."""
        captured: dict[str, object] = {}

        def _configure(
            cls,
            config: object | None = None,
            *,
            env: object | None = None,
            enabled: bool | None = None,
            exporter: str | None = None,
            service_name: str | None = None,
            force: bool = False,
        ) -> telemetry_runtime_mod.ResolvedTelemetryConfig:
            del config, enabled, exporter, service_name, force
            captured['env'] = env
            cls._settings = DISABLED_SETTINGS
            cls._adapter = None
            return cls._settings

        monkeypatch.setattr(
            telemetry_runtime_mod.RuntimeTelemetry,
            'configure',
            classmethod(_configure),
        )
        telemetry_runtime_mod.RuntimeTelemetry.reset()

        telemetry_runtime_mod.RuntimeTelemetry.emit_event({'event': 'run.started'})

        assert captured['env'] is os.environ
        assert telemetry_runtime_mod.RuntimeTelemetry._settings == DISABLED_SETTINGS
        assert telemetry_runtime_mod.RuntimeTelemetry._adapter is None

    def test_emit_event_completed_without_started_span_creates_ok_span(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Terminal events without a prior start span should create and close one."""
        tracer, meter = _FakeOpenTelemetryInstaller.install(monkeypatch)
        adapter = telemetry_runtime_mod._OpenTelemetryAdapter(ENABLED_OTEL_SETTINGS)

        adapter.emit_event(
            {
                'command': 'run',
                'continue_on_fail': True,
                'event': 'run.completed',
                'lifecycle': 'completed',
                'run_all': False,
                'run_id': 'run-123',
                'schema': 'etlplus.event.v1',
                'schema_version': 1,
                'valid': True,
            },
        )

        assert len(tracer.spans) == 1
        span = tracer.spans[0]
        assert span.ended is True
        assert span.status == _FakeStatus(_FakeStatusCode.OK)
        assert span.attributes['etlplus.continue_on_fail'] is True
        assert span.attributes['etlplus.run_all'] is False
        assert span.attributes['etlplus.valid'] is True
        assert meter.histograms[0].calls == []

    def test_emit_event_exports_started_and_failed_lifecycle_data(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Enabled telemetry should bridge runtime events into spans and metrics."""
        tracer, meter = _FakeOpenTelemetryInstaller.install(monkeypatch)

        telemetry_runtime_mod.RuntimeTelemetry.configure(
            ENABLED_OTEL_CONFIG,
            env={},
            force=True,
        )

        telemetry_runtime_mod.RuntimeTelemetry.emit_event(
            {
                'command': 'run',
                'event': 'run.started',
                'lifecycle': 'started',
                'run_id': 'run-123',
                'schema': 'etlplus.event.v1',
                'schema_version': 1,
                'timestamp': '2026-04-18T00:00:00+00:00',
            },
        )
        telemetry_runtime_mod.RuntimeTelemetry.emit_event(
            {
                'command': 'run',
                'duration_ms': 42,
                'error_message': 'boom',
                'event': 'run.failed',
                'lifecycle': 'failed',
                'run_id': 'run-123',
                'schema': 'etlplus.event.v1',
                'schema_version': 1,
                'status': 'error',
                'timestamp': '2026-04-18T00:00:42+00:00',
            },
        )

        assert len(tracer.spans) == 1
        span = tracer.spans[0]
        assert span.name == 'etlplus.run'
        assert span.attributes['etlplus.service_name'] == 'etlplus-tests'
        assert span.exceptions
        assert span.ended is True
        assert meter.counters[0].calls == [
            (1, span.events[0][1]),
            (1, span.events[1][1]),
        ]
        assert meter.counters[1].calls == [
            (1, span.events[1][1]),
        ]
        assert meter.histograms[0].calls == [
            (42, span.events[1][1]),
        ]

    def test_emit_event_ignores_missing_or_non_terminal_lifecycle_values(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Event export should ignore incomplete and non-terminal lifecycle rows."""
        tracer, meter = _FakeOpenTelemetryInstaller.install(monkeypatch)
        adapter = telemetry_runtime_mod._OpenTelemetryAdapter(ENABLED_OTEL_SETTINGS)

        adapter.emit_event({'command': 'run', 'event': 'run.started'})
        adapter.emit_event(
            {
                'command': 'run',
                'event': 'run.progress',
                'lifecycle': 'progress',
                'run_id': 'run-123',
            },
        )

        assert tracer.spans == []
        assert len(meter.counters[0].calls) == 2

    def test_emit_event_defaults_invalid_schema_version(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Invalid schema versions should not prevent telemetry export."""
        _tracer, meter = _FakeOpenTelemetryInstaller.install(monkeypatch)
        adapter = telemetry_runtime_mod._OpenTelemetryAdapter(ENABLED_OTEL_SETTINGS)

        adapter.emit_event(
            {
                'command': 'run',
                'event': 'run.completed',
                'lifecycle': 'completed',
                'run_id': 'run-123',
                'schema_version': 'v1',
            },
        )

        assert meter.counters[0].calls[0][1]['etlplus.schema_version'] == 0

    def test_emit_event_is_noop_when_disabled(self) -> None:
        """Disabled telemetry should not create any runtime adapter."""
        settings = telemetry_runtime_mod.RuntimeTelemetry.configure(
            DISABLED_CONFIG,
            env={},
            force=True,
        )

        telemetry_runtime_mod.RuntimeTelemetry.emit_event({'event': 'run.started'})

        assert settings.enabled is False
        assert telemetry_runtime_mod.RuntimeTelemetry._adapter is None

    def test_emit_event_swallows_adapter_exceptions_and_logs_debug(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Adapter export failures should degrade to debug logging without raising."""

        class _BrokenAdapter:
            def emit_event(self, *_args: object, **_kwargs: object) -> None:
                raise TypeError('broken event exporter')

        telemetry_runtime_mod.RuntimeTelemetry._settings = ENABLED_OTEL_SETTINGS
        telemetry_runtime_mod.RuntimeTelemetry._adapter = cast(Any, _BrokenAdapter())

        with caplog.at_level('DEBUG'):
            telemetry_runtime_mod.RuntimeTelemetry.emit_event({'event': 'run.started'})

        assert 'failed to export event' in caplog.text

    def test_emit_history_record_exports_metrics_from_normalized_fields(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """History-derived telemetry should emit counters and histograms."""
        _tracer, meter = _FakeOpenTelemetryInstaller.install(monkeypatch)

        telemetry_runtime_mod.RuntimeTelemetry.configure(
            ENABLED_OTEL_CONFIG,
            env={},
            force=True,
        )

        telemetry_runtime_mod.RuntimeTelemetry.emit_history_record(
            {
                'duration_ms': 125,
                'job_name': 'publish',
                'pipeline_name': 'customer-sync',
                'records_in': 20,
                'records_out': 18,
                'result_status': 'success',
                'run_id': 'run-123',
                'sequence_index': 2,
                'status': 'succeeded',
            },
            record_level='job',
        )

        assert meter.counters[2].calls == [
            (
                1,
                {
                    'etlplus.history.job_name': 'publish',
                    'etlplus.history.level': 'job',
                    'etlplus.history.pipeline_name': 'customer-sync',
                    'etlplus.history.result_status': 'success',
                    'etlplus.history.sequence_index': 2,
                    'etlplus.history.status': 'succeeded',
                    'etlplus.run_id': 'run-123',
                    'etlplus.service_name': 'etlplus-tests',
                },
            ),
        ]
        assert meter.histograms[1].calls == [
            (125, meter.counters[2].calls[0][1]),
        ]
        assert meter.histograms[2].calls == [
            (20, meter.counters[2].calls[0][1]),
        ]
        assert meter.histograms[3].calls == [
            (18, meter.counters[2].calls[0][1]),
        ]

    def test_emit_history_record_skips_missing_numeric_metrics(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """History export should skip histogram writes for missing numeric fields."""
        _tracer, meter = _FakeOpenTelemetryInstaller.install(monkeypatch)
        adapter = telemetry_runtime_mod._OpenTelemetryAdapter(ENABLED_OTEL_SETTINGS)

        adapter.emit_history_record({'run_id': 'run-123'}, record_level='run')

        assert meter.counters[2].calls == [
            (
                1,
                {
                    'etlplus.history.level': 'run',
                    'etlplus.run_id': 'run-123',
                    'etlplus.service_name': 'etlplus-tests',
                },
            ),
        ]
        assert meter.histograms[1].calls == []
        assert meter.histograms[2].calls == []
        assert meter.histograms[3].calls == []

    def test_numeric_telemetry_fields_ignore_booleans(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Boolean payload fields should not be exported as integer metrics."""
        _tracer, meter = _FakeOpenTelemetryInstaller.install(monkeypatch)
        adapter = telemetry_runtime_mod._OpenTelemetryAdapter(ENABLED_OTEL_SETTINGS)

        adapter.emit_event(
            {
                'command': 'run',
                'duration_ms': True,
                'event': 'run.completed',
                'lifecycle': 'completed',
                'run_id': 'run-123',
            },
        )
        adapter.emit_history_record(
            {
                'duration_ms': True,
                'records_in': False,
                'records_out': True,
                'run_id': 'run-123',
                'sequence_index': True,
            },
            record_level='job',
        )

        assert 'etlplus.history.sequence_index' not in meter.counters[2].calls[0][1]
        assert meter.histograms[0].calls == []
        assert meter.histograms[1].calls == []
        assert meter.histograms[2].calls == []
        assert meter.histograms[3].calls == []
