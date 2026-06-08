"""
:mod:`tests.unit.telemetry.test_u_telemetry_runtime` module.

Unit tests for :mod:`etlplus.telemetry.runtime`.
"""

from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import Mock

import pytest

import etlplus.telemetry.runtime as telemetry_runtime_mod

from .pytest_telemetry_support import FakeStatus
from .pytest_telemetry_support import FakeStatusCode
from .pytest_telemetry_support import install_fake_opentelemetry

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
        configure = Mock(return_value=DISABLED_SETTINGS)

        monkeypatch.setattr(
            telemetry_runtime_mod.RuntimeTelemetry,
            'configure',
            configure,
        )
        telemetry_runtime_mod.RuntimeTelemetry.reset()

        telemetry_runtime_mod.RuntimeTelemetry.emit_event({'event': 'run.started'})

        configure.assert_called_once_with(env=os.environ)
        assert telemetry_runtime_mod.RuntimeTelemetry._adapter is None

    def test_emit_event_completed_without_started_span_creates_ok_span(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Terminal events without a prior start span should create and close one."""
        tracer, meter = install_fake_opentelemetry(monkeypatch)
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
        assert span.status == FakeStatus(FakeStatusCode.OK)
        assert span.attributes['etlplus.continue_on_fail'] is True
        assert span.attributes['etlplus.run_all'] is False
        assert span.attributes['etlplus.valid'] is True
        assert meter.histograms[0].calls == []

    def test_emit_event_exports_started_and_failed_lifecycle_data(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Enabled telemetry should bridge runtime events into spans and metrics."""
        tracer, meter = install_fake_opentelemetry(monkeypatch)

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
        tracer, meter = install_fake_opentelemetry(monkeypatch)
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
        _tracer, meter = install_fake_opentelemetry(monkeypatch)
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
        telemetry_runtime_mod.RuntimeTelemetry._settings = ENABLED_OTEL_SETTINGS
        telemetry_runtime_mod.RuntimeTelemetry._adapter = cast(
            Any,
            SimpleNamespace(
                emit_event=Mock(side_effect=TypeError('broken event exporter')),
            ),
        )

        with caplog.at_level('DEBUG'):
            telemetry_runtime_mod.RuntimeTelemetry.emit_event({'event': 'run.started'})

        assert 'failed to export event' in caplog.text

    @pytest.mark.parametrize(
        ('metric_name', 'expected_value'),
        [
            pytest.param('counter', 1, id='counter'),
            pytest.param('duration', 125, id='duration'),
            pytest.param('records-in', 20, id='records-in'),
            pytest.param('records-out', 18, id='records-out'),
        ],
    )
    def test_emit_history_record_exports_metrics_from_normalized_fields(
        self,
        monkeypatch: pytest.MonkeyPatch,
        metric_name: str,
        expected_value: int,
    ) -> None:
        """History-derived telemetry should emit counters and histograms."""
        _tracer, meter = install_fake_opentelemetry(monkeypatch)

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

        expected_attributes = {
            'etlplus.history.job_name': 'publish',
            'etlplus.history.level': 'job',
            'etlplus.history.pipeline_name': 'customer-sync',
            'etlplus.history.result_status': 'success',
            'etlplus.history.sequence_index': 2,
            'etlplus.history.status': 'succeeded',
            'etlplus.run_id': 'run-123',
            'etlplus.service_name': 'etlplus-tests',
        }
        match metric_name:
            case 'counter':
                assert meter.counters[2].calls == [
                    (expected_value, expected_attributes),
                ]
            case 'duration':
                assert meter.histograms[1].calls == [
                    (expected_value, meter.counters[2].calls[0][1]),
                ]
            case 'records-in':
                assert meter.histograms[2].calls == [
                    (expected_value, meter.counters[2].calls[0][1]),
                ]
            case 'records-out':
                assert meter.histograms[3].calls == [
                    (expected_value, meter.counters[2].calls[0][1]),
                ]
            case _:
                pytest.fail(f'unhandled metric: {metric_name}')

    def test_emit_history_record_skips_missing_numeric_metrics(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """History export should skip histogram writes for missing numeric fields."""
        _tracer, meter = install_fake_opentelemetry(monkeypatch)
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

    @pytest.mark.parametrize(
        'metric_name',
        [
            pytest.param('sequence-index-attribute', id='sequence-index-attribute'),
            pytest.param('duration', id='duration'),
            pytest.param('records-in', id='records-in'),
            pytest.param('records-out', id='records-out'),
            pytest.param('history-duration', id='history-duration'),
        ],
    )
    def test_numeric_telemetry_fields_ignore_booleans(
        self,
        monkeypatch: pytest.MonkeyPatch,
        metric_name: str,
    ) -> None:
        """Boolean payload fields should not be exported as integer metrics."""
        _tracer, meter = install_fake_opentelemetry(monkeypatch)
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

        match metric_name:
            case 'sequence-index-attribute':
                assert (
                    'etlplus.history.sequence_index'
                    not in meter.counters[2].calls[0][1]
                )
            case 'duration':
                assert meter.histograms[0].calls == []
            case 'history-duration':
                assert meter.histograms[1].calls == []
            case 'records-in':
                assert meter.histograms[2].calls == []
            case 'records-out':
                assert meter.histograms[3].calls == []
            case _:
                pytest.fail(f'unhandled metric: {metric_name}')
