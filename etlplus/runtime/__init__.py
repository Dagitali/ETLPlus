"""
:mod:`etlplus.runtime` package.

Shared runtime policy helpers for CLI and future hosted execution surfaces.
"""

from __future__ import annotations

from ._events import EVENT_SCHEMA
from ._events import EVENT_SCHEMA_VERSION
from ._events import RuntimeEvents
from ._logging import configure_logging
from ._logging import resolve_log_level
from ._telemetry import ResolvedTelemetryConfig
from ._telemetry import RuntimeTelemetry
from ._telemetry import TelemetryConfig
from ._telemetry import configure_telemetry
from ._telemetry import resolve_telemetry_settings
from .readiness import ReadinessReportBuilder

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
    'RuntimeEvents',
    'RuntimeTelemetry',
    'TelemetryConfig',
    'ResolvedTelemetryConfig',
    # Constants
    'EVENT_SCHEMA',
    'EVENT_SCHEMA_VERSION',
    # Functions
    'configure_logging',
    'configure_telemetry',
    'resolve_log_level',
    'resolve_telemetry_settings',
]
