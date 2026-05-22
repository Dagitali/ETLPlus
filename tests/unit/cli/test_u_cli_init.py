"""
:mod:`tests.unit.cli.test_u_cli_init` module.

Unit tests for :mod:`etlplus.cli` package facade exports.
"""

from __future__ import annotations

import pytest

import etlplus.cli as cli_pkg
from etlplus.cli import main as cli_main

from ..pytest_export_contracts import assert_package_exports

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


CLI_EXPORTS: tuple[tuple[str, object], ...] = (('main', cli_main),)


# SECTION: TESTS ============================================================ #


class TestCliPackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert_package_exports(package_module=cli_pkg, expected_exports=CLI_EXPORTS)

    @pytest.mark.parametrize(('name', 'expected'), CLI_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(cli_pkg, name) == expected
