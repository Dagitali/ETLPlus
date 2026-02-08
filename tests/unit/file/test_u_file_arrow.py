"""
:mod:`tests.unit.file.test_u_file_arrow` module.

Unit tests for :mod:`etlplus.file.arrow`.
"""

from __future__ import annotations

from etlplus.file import arrow as mod
from tests.unit.file.conftest import PyarrowGateOnlyModuleContract

# SECTION: TESTS ============================================================ #


class TestArrow(PyarrowGateOnlyModuleContract):
    """Unit tests for :mod:`etlplus.file.arrow`."""

    module = mod
    format_name = 'arrow'
