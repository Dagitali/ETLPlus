"""
:mod:`tests.unit.runtime.test_u_runtime_init` module.

Unit tests for :mod:`etlplus.runtime` package exports.
"""

from __future__ import annotations

import pytest

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


RUNTIME_EXPORTS = [
    ('ReadinessReportBuilder', ReadinessReportBuilder),
    ('RuntimeEvents', RuntimeEvents),
    ('EVENT_SCHEMA', EVENT_SCHEMA),
    ('EVENT_SCHEMA_VERSION', EVENT_SCHEMA_VERSION),
    ('configure_logging', configure_logging),
    ('resolve_log_level', resolve_log_level),
]


def test_runtime_package_exports_expected_symbols() -> None:
    """Test that the runtime package facade exports the documented helpers."""
    assert runtime_pkg.__all__ == [name for name, _value in RUNTIME_EXPORTS]


@pytest.mark.parametrize(('name', 'expected'), RUNTIME_EXPORTS)
def test_runtime_package_exports_bind_expected_symbols(
    name: str,
    expected: object,
) -> None:
    """Runtime package attributes should match the documented facade surface."""
    assert getattr(runtime_pkg, name) is expected
