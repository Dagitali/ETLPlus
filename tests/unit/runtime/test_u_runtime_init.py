"""
:mod:`tests.unit.runtime.test_u_runtime_init` module.

Unit tests for :mod:`etlplus.runtime` package exports.
"""

from __future__ import annotations

import etlplus.runtime as runtime_pkg
from etlplus.runtime._events import EVENT_SCHEMA
from etlplus.runtime._events import EVENT_SCHEMA_VERSION
from etlplus.runtime._events import RuntimeEvents
from etlplus.runtime._logging import configure_logging
from etlplus.runtime._logging import resolve_log_level
from etlplus.runtime._readiness import ReadinessReportBuilder

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


def test_runtime_package_exports_expected_symbols() -> None:
    """Test that the runtime package facade exports the documented helpers."""
    assert runtime_pkg.__all__ == [
        'ReadinessReportBuilder',
        'RuntimeEvents',
        'EVENT_SCHEMA',
        'EVENT_SCHEMA_VERSION',
        'configure_logging',
        'resolve_log_level',
    ]
    assert runtime_pkg.ReadinessReportBuilder is ReadinessReportBuilder
    assert runtime_pkg.RuntimeEvents is RuntimeEvents
    assert runtime_pkg.EVENT_SCHEMA == EVENT_SCHEMA
    assert runtime_pkg.EVENT_SCHEMA_VERSION == EVENT_SCHEMA_VERSION
    assert runtime_pkg.configure_logging is configure_logging
    assert runtime_pkg.resolve_log_level is resolve_log_level
