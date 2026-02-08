"""
:mod:`tests.unit.file.test_u_file_psv` module.

Unit tests for :mod:`etlplus.file.psv`.
"""

from __future__ import annotations

from etlplus.file import psv as mod
from tests.unit.file.conftest import DelimitedModuleContract

# SECTION: TESTS ============================================================ #


class TestPsv(DelimitedModuleContract):
    """Unit tests for :mod:`etlplus.file.psv`."""

    module = mod
    format_name = 'psv'
    delimiter = '|'
