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
from ._readiness import ReadinessReportBuilder

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
    'RuntimeEvents',
    # Constants
    'EVENT_SCHEMA',
    'EVENT_SCHEMA_VERSION',
    # Functions
    'configure_logging',
    'resolve_log_level',
]
