"""
:mod:`etlplus.runtime._telemetry` module.

Optional runtime telemetry adapters built on top of structured event payloads.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any
from typing import Literal
from typing import Self
from typing import cast

from ..__version__ import __version__
from ..utils._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RuntimeTelemetry',
    # Data Classes
    'ResolvedTelemetryConfig',
    'TelemetryConfig',
    # Functions
    'configure_telemetry',
    'resolve_telemetry_settings',
]


# SECTION: TYPE ALIASES ===================================================== #


type TelemetryExporter = Literal['none', 'opentelemetry']


# SECTION: INTERNAL CONSTANTS =============================================== #


_DEFAULT_EXPORTER: TelemetryExporter = 'none'
_VALID_EXPORTERS = frozenset({'none', 'opentelemetry'})
_ENABLED_TRUE_VALUES = frozenset({'1', 'on', 'true', 'yes'})
_ENABLED_FALSE_VALUES = frozenset({'0', 'off', 'false', 'no'})
_ENV_TELEMETRY_ENABLED = 'ETLPLUS_TELEMETRY_ENABLED'
_ENV_TELEMETRY_EXPORTER = 'ETLPLUS_TELEMETRY_EXPORTER'
_ENV_TELEMETRY_SERVICE_NAME = 'ETLPLUS_TELEMETRY_SERVICE_NAME'
_LOGGER = logging.getLogger(__name__)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_exporter(
    value: object,
) -> TelemetryExporter | None:
    """Return one supported telemetry exporter name when valid."""
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if normalized in _VALID_EXPORTERS:
        return cast(TelemetryExporter, normalized)
    return None


def _coerce_flag(
    value: object,
    *,
    default: bool,
) -> bool:
    """Return one boolean flag or *default* when the input is invalid."""
    if isinstance(value, bool):
        return value
    if not isinstance(value, str):
        return default
    normalized = value.strip().lower()
    if normalized in _ENABLED_TRUE_VALUES:
        return True
    if normalized in _ENABLED_FALSE_VALUES:
        return False
    return default


def _event_attributes(
    event: Mapping[str, Any],
    *,
    service_name: str,
) -> dict[str, str | bool | int | float]:
    """Return one attribute mapping derived from the stable event envelope."""
    attrs: dict[str, str | bool | int | float] = {
        'etlplus.service_name': service_name,
        'etlplus.schema': str(event.get('schema', '')),
        'etlplus.schema_version': int(event.get('schema_version', 0) or 0),
        'etlplus.command': str(event.get('command', '')),
        'etlplus.lifecycle': str(event.get('lifecycle', '')),
        'etlplus.run_id': str(event.get('run_id', '')),
    }
    for field_name in (
        'config_path',
        'error_message',
        'error_type',
        'event',
        'job',
        'pipeline_name',
        'result_status',
        'source',
        'source_type',
        'status',
        'target',
        'target_type',
        'timestamp',
    ):
        value = event.get(field_name)
        if isinstance(value, str) and value:
            attrs[f'etlplus.{field_name}'] = value
    for field_name in ('continue_on_fail', 'run_all', 'valid'):
        value = event.get(field_name)
        if isinstance(value, bool):
            attrs[f'etlplus.{field_name}'] = value
    return attrs


def _span_name(
    event: Mapping[str, Any],
) -> str:
    """Return one stable span name for a runtime command event."""
    command = event.get('command')
    return f'etlplus.{command}' if isinstance(command, str) and command else 'etlplus'


def _build_adapter(
    settings: ResolvedTelemetryConfig,
) -> _OpenTelemetryAdapter | None:
    """Return one configured telemetry adapter when optional deps are present."""
    if not settings.enabled or settings.exporter == 'none':
        return None
    if settings.exporter != 'opentelemetry':
        return None
    try:
        return _OpenTelemetryAdapter(settings)
    except ImportError:
        _LOGGER.warning(
            'Telemetry is enabled but optional OpenTelemetry dependencies are '
            'not installed. Install the telemetry extra to activate export.',
        )
        return None


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True, frozen=True)
class ResolvedTelemetryConfig:
    """Resolved telemetry settings used at runtime."""

    # -- Instance Attributes -- #

    enabled: bool
    exporter: TelemetryExporter
    service_name: str

    # -- Class Methods -- #

    @classmethod
    def resolve(
        cls,
        config: TelemetryConfig | None,
        *,
        env: Mapping[str, str] | None = None,
        enabled: bool | None = None,
        exporter: str | None = None,
        service_name: str | None = None,
    ) -> Self:
        """
        Return effective telemetry settings from config and environment.

        This method considers the provided config, environment variables, and
        explicit overrides to determine the final telemetry settings.

        Parameters
        ----------
        config : TelemetryConfig | None
            Optional telemetry configuration.
        env : Mapping[str, str] | None, optional
            Environment variables to consider, by default None.
        enabled : bool | None, optional
            Explicit override for the enabled flag, by default None.
        exporter : str | None, optional
            Explicit override for the exporter, by default None.
        service_name : str | None, optional
            Explicit override for the service name, by default None.

        Returns
        -------
        Self
            The resolved telemetry configuration.
        """
        telemetry_cfg = config if config is not None else TelemetryConfig()
        env_map = env or {}

        resolved_exporter = (
            _coerce_exporter(exporter)
            or _coerce_exporter(env_map.get(_ENV_TELEMETRY_EXPORTER))
            or telemetry_cfg.exporter
            or _DEFAULT_EXPORTER
        )
        resolved_enabled = (
            enabled
            if enabled is not None
            else _coerce_flag(
                env_map.get(_ENV_TELEMETRY_ENABLED),
                default=(telemetry_cfg.enabled or resolved_exporter != 'none'),
            )
        )
        if resolved_enabled and resolved_exporter == 'none':
            resolved_exporter = 'opentelemetry'

        raw_service_name = (
            service_name
            or env_map.get(_ENV_TELEMETRY_SERVICE_NAME)
            or telemetry_cfg.service_name
            or 'etlplus'
        )
        cleaned_service_name = raw_service_name.strip() or 'etlplus'

        return cls(
            enabled=resolved_enabled,
            exporter=resolved_exporter,
            service_name=cleaned_service_name,
        )


@dataclass(kw_only=True, slots=True, frozen=True)
class TelemetryConfig:
    """Pipeline-level telemetry defaults."""

    # -- Instance Attributes -- #

    enabled: bool = False
    exporter: TelemetryExporter | None = None
    service_name: str | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap | None,
    ) -> Self:
        """Parse one optional telemetry config mapping."""
        if not isinstance(obj, Mapping):
            return cls()

        raw_service_name = obj.get('service_name')
        return cls(
            enabled=_coerce_flag(obj.get('enabled'), default=False),
            exporter=_coerce_exporter(obj.get('exporter')),
            service_name=(
                raw_service_name.strip()
                if isinstance(raw_service_name, str) and raw_service_name.strip()
                else None
            ),
        )


# SECTION: CLASSES ========================================================== #


class _OpenTelemetryAdapter:
    """In-process bridge from runtime events to OpenTelemetry primitives."""

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        settings: ResolvedTelemetryConfig,
    ) -> None:
        opentelemetry_root = import_module('opentelemetry')
        trace_module = import_module('opentelemetry.trace')
        metrics = opentelemetry_root.metrics
        trace = opentelemetry_root.trace
        Status = trace_module.Status
        StatusCode = trace_module.StatusCode

        self._settings = settings
        self._status = Status
        self._status_code = StatusCode
        self._spans: dict[str, Any] = {}
        self._tracer = trace.get_tracer('etlplus.runtime', __version__)
        meter = metrics.get_meter('etlplus.runtime', __version__)
        self._event_counter = meter.create_counter(
            'etlplus.command.events',
            unit='1',
            description='Count of ETLPlus structured runtime lifecycle events.',
        )
        self._failure_counter = meter.create_counter(
            'etlplus.command.failures',
            unit='1',
            description='Count of failed ETLPlus structured runtime lifecycle events.',
        )
        self._duration_histogram = meter.create_histogram(
            'etlplus.command.duration',
            unit='ms',
            description='Observed ETLPlus command duration in milliseconds.',
        )

    # -- Instance Methods -- #

    def emit_event(
        self,
        event: Mapping[str, Any],
    ) -> None:
        """Export one structured runtime event as spans and metrics."""
        attrs = _event_attributes(event, service_name=self._settings.service_name)
        self._event_counter.add(1, attrs)

        lifecycle = event.get('lifecycle')
        run_id = event.get('run_id')
        if not isinstance(lifecycle, str) or not isinstance(run_id, str):
            return

        if lifecycle == 'started':
            span = self._tracer.start_span(_span_name(event))
            span.set_attributes(attrs)
            span.add_event(str(event.get('event', 'started')), attributes=attrs)
            self._spans[run_id] = span
            return

        if lifecycle not in {'completed', 'failed'}:
            return

        span = self._spans.pop(run_id, None)
        if span is None:
            span = self._tracer.start_span(_span_name(event))

        span.set_attributes(attrs)
        span.add_event(str(event.get('event', lifecycle)), attributes=attrs)

        duration_ms = event.get('duration_ms')
        if isinstance(duration_ms, int):
            self._duration_histogram.record(duration_ms, attrs)

        if lifecycle == 'failed':
            self._failure_counter.add(1, attrs)
            error_message = event.get('error_message')
            error_text = (
                error_message
                if isinstance(error_message, str)
                else 'ETLPlus command failed'
            )
            span.record_exception(RuntimeError(error_text))
            span.set_status(self._status(self._status_code.ERROR, error_text))
        else:
            span.set_status(self._status(self._status_code.OK))

        span.end()


# SECTION: CLASSES ========================================================== #


class RuntimeTelemetry:
    """Global runtime telemetry bridge configured per CLI invocation."""

    # -- Internal Class Attributes -- #

    _adapter: _OpenTelemetryAdapter | None = None
    _settings: ResolvedTelemetryConfig | None = None

    # -- Class Methods -- #

    @classmethod
    def configure(
        cls,
        config: TelemetryConfig | ResolvedTelemetryConfig | None = None,
        *,
        env: Mapping[str, str] | None = None,
        enabled: bool | None = None,
        exporter: str | None = None,
        service_name: str | None = None,
        force: bool = False,
    ) -> ResolvedTelemetryConfig:
        """Resolve and install the active runtime telemetry settings."""
        resolved = (
            config
            if isinstance(config, ResolvedTelemetryConfig)
            else ResolvedTelemetryConfig.resolve(
                config,
                env=env,
                enabled=enabled,
                exporter=exporter,
                service_name=service_name,
            )
        )
        if not force and resolved == cls._settings:
            return resolved
        cls._settings = resolved
        cls._adapter = _build_adapter(resolved)
        return resolved

    @classmethod
    def emit_event(
        cls,
        event: Mapping[str, Any],
    ) -> None:
        """Export one runtime event through the active adapter when enabled."""
        if cls._settings is None:
            cls.configure(env=os.environ)
        if cls._adapter is None:
            return
        try:
            cls._adapter.emit_event(event)
        except (AttributeError, RuntimeError, TypeError, ValueError):
            _LOGGER.debug(
                'Runtime telemetry adapter failed to export event.',
                exc_info=True,
            )

    @classmethod
    def reset(
        cls,
    ) -> None:
        """Reset the process-local telemetry bridge state for tests."""
        cls._adapter = None
        cls._settings = None


# SECTION: FUNCTIONS ======================================================== #


# TODO: Replace with direct call to RuntimeTelemetry.configure.
def configure_telemetry(
    config: TelemetryConfig | ResolvedTelemetryConfig | None = None,
    *,
    env: Mapping[str, str] | None = None,
    enabled: bool | None = None,
    exporter: str | None = None,
    service_name: str | None = None,
    force: bool = False,
) -> ResolvedTelemetryConfig:
    """Resolve and install the active runtime telemetry settings."""
    return RuntimeTelemetry.configure(
        config,
        env=env,
        enabled=enabled,
        exporter=exporter,
        service_name=service_name,
        force=force,
    )


# TODO: Replace with direct call to ResolvedTelemetryConfig.resolve.
def resolve_telemetry_settings(
    config: TelemetryConfig | None,
    *,
    env: Mapping[str, str] | None = None,
    enabled: bool | None = None,
    exporter: str | None = None,
    service_name: str | None = None,
) -> ResolvedTelemetryConfig:
    """Return effective telemetry settings without mutating global state."""
    return ResolvedTelemetryConfig.resolve(
        config,
        env=env,
        enabled=enabled,
        exporter=exporter,
        service_name=service_name,
    )
