"""
:mod:`tests.unit.file.test_u_file_dta` module.

Unit tests for :mod:`etlplus.file.dta`.
"""

from __future__ import annotations

from etlplus.file import dta as mod
from tests.unit.file.conftest import SingleDatasetWritableContract

# SECTION: TESTS ============================================================ #


class TestDta(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.dta`."""

    module = mod
    handler_cls = mod.DtaFile
    format_name = 'dta'
