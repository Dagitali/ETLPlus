"""
:mod:`tests.unit.cli.test_u_cli_constants` module.

Unit tests for :mod:`etlplus.cli._constants`.
"""

from __future__ import annotations

import etlplus.cli._constants as constants_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


def test_constants_are_populated_and_consistent() -> None:
    """Test that CLI constants expose coherent default values."""
    assert constants_mod.DATA_CONNECTORS
    assert constants_mod.FILE_FORMATS
    assert constants_mod.DEFAULT_FILE_FORMAT in constants_mod.FILE_FORMATS
    assert constants_mod.PROJECT_URL.startswith('https://')
    assert 'ETLPlus' in constants_mod.CLI_DESCRIPTION
    assert '--source-format' in constants_mod.CLI_EPILOG
