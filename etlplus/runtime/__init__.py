"""
:mod:`etlplus.runtime` package.

Shared runtime policy helpers for CLI and future hosted execution surfaces.
"""

from __future__ import annotations

from .events import EVENT_SCHEMA
from .events import EVENT_SCHEMA_VERSION
from .events import build_structured_event
from .events import create_run_id
from .logging import configure_logging
from .logging import resolve_log_level
from .readiness import build_readiness_report

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'EVENT_SCHEMA',
    'EVENT_SCHEMA_VERSION',
    # Functions
    'build_structured_event',
    'build_readiness_report',
    'configure_logging',
    'create_run_id',
    'resolve_log_level',
]
