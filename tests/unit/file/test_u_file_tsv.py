"""
:mod:`tests.unit.file.test_u_file_tsv` module.

Unit tests for :mod:`etlplus.file.tsv`.
"""

from __future__ import annotations

from etlplus.file import tsv as mod
from tests.unit.file.conftest import DelimitedModuleContract

# SECTION: TESTS ============================================================ #


class TestTsv(DelimitedModuleContract):
    """Unit tests for :mod:`etlplus.file.tsv`."""

    module = mod
    format_name = 'tsv'
    delimiter = '\t'
