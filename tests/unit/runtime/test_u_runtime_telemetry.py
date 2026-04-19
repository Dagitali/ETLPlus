"""
:mod:`tests.unit.runtime.test_u_runtime_telemetry` module.

Unit tests for :mod:`etlplus.runtime._telemetry`.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from types import ModuleType
from typing import Any
from typing import cast

import pytest

import etlplus.runtime._telemetry as telemetry_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


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


class TestTelemetryConfig:
    """Unit tests for telemetry configuration resolution."""

    def test_from_obj_parses_optional_mapping(self) -> None:
        """Telemetry config should parse supported fields tolerantly."""
        cfg = telemetry_mod.TelemetryConfig.from_obj(
            {
                'enabled': True,
                'exporter': 'opentelemetry',
                'service_name': 'etlplus-cli',
            },
        )

        assert cfg.enabled is True
        assert cfg.exporter == 'opentelemetry'
        assert cfg.service_name == 'etlplus-cli'

    def test_from_obj_returns_defaults_for_non_mapping_inputs(self) -> None:
        """Telemetry config should fall back to defaults for non-mapping input."""
        cfg = telemetry_mod.TelemetryConfig.from_obj(None)

        assert cfg == telemetry_mod.TelemetryConfig()

    def test_helper_coercions_cover_invalid_and_false_inputs(self) -> None:
        """Telemetry helper coercions should reject invalid inputs predictably."""
        assert telemetry_mod._coerce_exporter('bogus') is None
        assert telemetry_mod._coerce_flag('off', default=True) is False
        assert telemetry_mod._coerce_flag('maybe', default=True) is True

    def test_resolve_prefers_env_and_promotes_enabled_exporter(self) -> None:
        """Env values should override config and enabling telemetry picks OTel."""
        resolved = telemetry_mod.ResolvedTelemetryConfig.resolve(
            telemetry_mod.TelemetryConfig(enabled=False),
            env={
                'ETLPLUS_TELEMETRY_ENABLED': 'true',
                'ETLPLUS_TELEMETRY_SERVICE_NAME': 'env-service',
            },
        )

        assert resolved.enabled is True
        assert resolved.exporter == 'opentelemetry'
        assert resolved.service_name == 'env-service'


class TestRuntimeTelemetry:
    """Unit tests for the process-local telemetry bridge."""

    def teardown_method(self) -> None:
        """Reset process-local telemetry state between tests."""
        telemetry_mod.RuntimeTelemetry.reset()

    def test_build_adapter_returns_none_for_unsupported_exporter(self) -> None:
        """Unsupported exporters should degrade to a no-op adapter."""
        settings = telemetry_mod.ResolvedTelemetryConfig(
            enabled=True,
            exporter=cast(Any, 'custom-exporter'),
            service_name='etlplus-tests',
        )

        assert telemetry_mod._OpenTelemetryAdapter.build_adapter(settings) is None

    def test_configure_reuses_cached_settings_without_rebuilding_adapter(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Repeated configure calls should reuse identical settings by default."""
        initial = telemetry_mod.RuntimeTelemetry.configure(
            telemetry_mod.TelemetryConfig(enabled=False),
            env={},
            force=True,
        )

        monkeypatch.setattr(
            telemetry_mod._OpenTelemetryAdapter,
            'build_adapter',
            lambda _settings: (_ for _ in ()).throw(AssertionError('unexpected')),
        )

        resolved = telemetry_mod.RuntimeTelemetry.configure(
            telemetry_mod.TelemetryConfig(enabled=False),
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
            telemetry_mod.RuntimeTelemetry.configure(
                telemetry_mod.TelemetryConfig(enabled=True),
                env={},
                force=True,
            )

        assert telemetry_mod.RuntimeTelemetry._adapter is None
        assert 'optional OpenTelemetry dependencies are not installed' in caplog.text

    def test_emit_event_completed_without_started_span_creates_ok_span(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Terminal events without a prior start span should create and close one."""
        tracer, meter = _FakeOpenTelemetryInstaller.install(monkeypatch)
        adapter = telemetry_mod._OpenTelemetryAdapter(
            telemetry_mod.ResolvedTelemetryConfig(
                enabled=True,
                exporter='opentelemetry',
                service_name='etlplus-tests',
            ),
        )

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

        telemetry_mod.RuntimeTelemetry.configure(
            telemetry_mod.TelemetryConfig(
                enabled=True,
                exporter='opentelemetry',
                service_name='etlplus-tests',
            ),
            env={},
            force=True,
        )

        telemetry_mod.RuntimeTelemetry.emit_event(
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
        telemetry_mod.RuntimeTelemetry.emit_event(
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
        adapter = telemetry_mod._OpenTelemetryAdapter(
            telemetry_mod.ResolvedTelemetryConfig(
                enabled=True,
                exporter='opentelemetry',
                service_name='etlplus-tests',
            ),
        )

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

    def test_emit_event_is_noop_when_disabled(self) -> None:
        """Disabled telemetry should not create any runtime adapter."""
        settings = telemetry_mod.RuntimeTelemetry.configure(
            telemetry_mod.TelemetryConfig(enabled=False),
            env={},
            force=True,
        )

        telemetry_mod.RuntimeTelemetry.emit_event({'event': 'run.started'})

        assert settings.enabled is False
        assert telemetry_mod.RuntimeTelemetry._adapter is None

    def test_emit_event_swallows_adapter_exceptions_and_logs_debug(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Adapter export failures should degrade to debug logging without raising."""

        class _BrokenAdapter:
            def emit_event(self, *_args: object, **_kwargs: object) -> None:
                raise TypeError('broken event exporter')

        telemetry_mod.RuntimeTelemetry._settings = (
            telemetry_mod.ResolvedTelemetryConfig(
                enabled=True,
                exporter='opentelemetry',
                service_name='etlplus-tests',
            )
        )
        telemetry_mod.RuntimeTelemetry._adapter = cast(Any, _BrokenAdapter())

        with caplog.at_level('DEBUG'):
            telemetry_mod.RuntimeTelemetry.emit_event({'event': 'run.started'})

        assert 'failed to export event' in caplog.text

    def test_emit_history_record_exports_metrics_from_normalized_fields(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """History-derived telemetry should emit counters and histograms."""
        _tracer, meter = _FakeOpenTelemetryInstaller.install(monkeypatch)

        telemetry_mod.RuntimeTelemetry.configure(
            telemetry_mod.TelemetryConfig(
                enabled=True,
                exporter='opentelemetry',
                service_name='etlplus-tests',
            ),
            env={},
            force=True,
        )

        telemetry_mod.RuntimeTelemetry.emit_history_record(
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
        adapter = telemetry_mod._OpenTelemetryAdapter(
            telemetry_mod.ResolvedTelemetryConfig(
                enabled=True,
                exporter='opentelemetry',
                service_name='etlplus-tests',
            ),
        )

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
