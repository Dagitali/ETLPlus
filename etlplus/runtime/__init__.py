"""
:mod:`etlplus.runtime` package.

Shared runtime policy helpers for CLI and future hosted execution surfaces.
"""

from __future__ import annotations

from ._events import EVENT_SCHEMA
from ._events import EVENT_SCHEMA_VERSION
from ._events import RuntimeEvents
from ._logging import RuntimeLoggingPolicy
from ._telemetry import ResolvedTelemetryConfig
from ._telemetry import RuntimeTelemetry
from ._telemetry import TelemetryConfig
from .readiness import ReadinessReportBuilder

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
    'RuntimeEvents',
    'RuntimeLoggingPolicy',
    'RuntimeTelemetry',
    'TelemetryConfig',
    'ResolvedTelemetryConfig',
    # Constants
    'EVENT_SCHEMA',
    'EVENT_SCHEMA_VERSION',
]
