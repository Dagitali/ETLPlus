"""
:mod:`etlplus.runtime` package.

Shared runtime policy helpers for CLI and future hosted execution surfaces.
"""

from __future__ import annotations

from .events import EVENT_SCHEMA
from .events import EVENT_SCHEMA_VERSION
from .events import RuntimeEvents
from .logging import configure_logging
from .logging import resolve_log_level
from .readiness import ReadinessReportBuilder
from .readiness import build_readiness_report

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
    'RuntimeEvents',
    # Constants
    'EVENT_SCHEMA',
    'EVENT_SCHEMA_VERSION',
    # Functions
    'build_readiness_report',
    'configure_logging',
    'resolve_log_level',
]
