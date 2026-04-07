"""
:mod:`tests.unit.runtime.test_u_runtime_init` module.

Unit tests for :mod:`etlplus.runtime` package facade exports.
"""

from __future__ import annotations

from types import ModuleType

import pytest

import etlplus.runtime as runtime_pkg
import etlplus.runtime._readiness as readiness_mod
import etlplus.runtime._readiness_checks as readiness_checks_mod
import etlplus.runtime._readiness_strict as readiness_strict_mod
from etlplus.runtime._events import EVENT_SCHEMA
from etlplus.runtime._events import EVENT_SCHEMA_VERSION
from etlplus.runtime._events import RuntimeEvents
from etlplus.runtime._logging import configure_logging
from etlplus.runtime._logging import resolve_log_level
from etlplus.runtime._readiness import ReadinessReportBuilder

from ..pytest_export_contracts import assert_helper_module_exports_match_facade_usage

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


EXPECTED_EXPORTS = [
    ('ReadinessReportBuilder', ReadinessReportBuilder),
    ('RuntimeEvents', RuntimeEvents),
    ('EVENT_SCHEMA', EVENT_SCHEMA),
    ('EVENT_SCHEMA_VERSION', EVENT_SCHEMA_VERSION),
    ('configure_logging', configure_logging),
    ('resolve_log_level', resolve_log_level),
]
HELPER_EXPORT_CASES = [
    ('_checks', readiness_checks_mod),
    ('_strict', readiness_strict_mod),
]

# SECTION: TESTS ============================================================ #


class TestRuntimePackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert runtime_pkg.__all__ == [name for name, _value in EXPECTED_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), EXPECTED_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(runtime_pkg, name) == expected

    @pytest.mark.parametrize(('alias', 'helper_module'), HELPER_EXPORT_CASES)
    def test_helper_module_exports_match_facade_usage(
        self,
        alias: str,
        helper_module: ModuleType,
    ) -> None:
        """
        Test that helper exports stay limited to the names consumed by the
        facade.
        """
        assert_helper_module_exports_match_facade_usage(
            facade_module=readiness_mod,
            helper_module=helper_module,
            alias=alias,
        )
