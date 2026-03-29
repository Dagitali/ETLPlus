"""
:mod:`tests.unit.cli.test_u_cli_init` module.

Unit tests for :mod:`etlplus.cli` package exports.
"""

from __future__ import annotations

import etlplus.cli as cli_pkg
from etlplus.cli import main as cli_main

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


def test_cli_package_exports_main() -> None:
    """Test that package-level ``main`` export references CLI entrypoint."""
    assert cli_pkg.main is cli_main
    assert cli_pkg.__all__ == ['main']
