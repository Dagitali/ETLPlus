"""
:mod:`tests.unit.runtime.test_u_runtime_init` module.

Unit tests for :mod:`etlplus.runtime` package facade exports.
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

# SECTION: HELPERS ========================================================== #


RUNTIME_EXPORTS = [
    ('ReadinessReportBuilder', ReadinessReportBuilder),
    ('RuntimeEvents', RuntimeEvents),
    ('EVENT_SCHEMA', EVENT_SCHEMA),
    ('EVENT_SCHEMA_VERSION', EVENT_SCHEMA_VERSION),
    ('configure_logging', configure_logging),
    ('resolve_log_level', resolve_log_level),
]

# SECTION: TESTS ============================================================ #


class TestRuntimePackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert runtime_pkg.__all__ == [name for name, _value in RUNTIME_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), RUNTIME_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(runtime_pkg, name) == expected
