"""
:mod:`tests.unit.cli.test_u_cli_init` module.

Unit tests for :mod:`etlplus.cli` package exports.
"""

from __future__ import annotations

import etlplus.cli as cli_pkg
from etlplus.cli.main import main as cli_main

# SECTION: TESTS ============================================================ #


def test_cli_package_exports_main() -> None:
    """Package-level ``main`` export should reference CLI entrypoint."""
    assert cli_pkg.main is cli_main
    assert cli_pkg.__all__ == ['main']
