"""
:mod:`tests.unit.file.test_u_file_tab` module.

Unit tests for :mod:`etlplus.file.tab`.
"""

from __future__ import annotations

from etlplus.file import tab as mod
from tests.unit.file.conftest import DelimitedModuleContract

# SECTION: TESTS ============================================================ #


class TestTab(DelimitedModuleContract):
    """Unit tests for :mod:`etlplus.file.tab`."""

    module = mod
    format_name = 'tab'
    delimiter = '\t'
