"""
:mod:`tests.unit.telemetry.test_u_telemetry_init` module.

Unit tests for :mod:`etlplus.telemetry` package facade exports.
"""

from __future__ import annotations

import pytest

import etlplus.telemetry as telemetry_pkg
import etlplus.telemetry.config as telemetry_config_mod
import etlplus.telemetry.runtime as telemetry_runtime_mod

from ..pytest_export_contracts import assert_package_exports

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


EXPECTED_EXPORTS: tuple[tuple[str, object], ...] = (
    ('RuntimeTelemetry', telemetry_runtime_mod.RuntimeTelemetry),
    ('ResolvedTelemetryConfig', telemetry_config_mod.ResolvedTelemetryConfig),
    ('TelemetryConfig', telemetry_config_mod.TelemetryConfig),
)


# SECTION: TESTS ============================================================ #


class TestTelemetryPackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert_package_exports(
            package_module=telemetry_pkg,
            expected_exports=EXPECTED_EXPORTS,
        )

    @pytest.mark.parametrize(('name', 'expected'), EXPECTED_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(telemetry_pkg, name) == expected
