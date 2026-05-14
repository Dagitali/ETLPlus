"""
:mod:`tests.unit.telemetry.test_u_telemetry_init` module.

Unit tests for :mod:`etlplus.telemetry` package facade exports.
"""

from __future__ import annotations

import pytest

import etlplus.telemetry as telemetry_pkg
from etlplus.telemetry import ResolvedTelemetryConfig
from etlplus.telemetry import RuntimeTelemetry
from etlplus.telemetry import TelemetryConfig

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


EXPECTED_EXPORTS = [
    ('RuntimeTelemetry', RuntimeTelemetry),
    ('ResolvedTelemetryConfig', ResolvedTelemetryConfig),
    ('TelemetryConfig', TelemetryConfig),
]

# SECTION: TESTS ============================================================ #


class TestRuntimePackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert telemetry_pkg.__all__ == [name for name, _value in EXPECTED_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), EXPECTED_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(telemetry_pkg, name) == expected
