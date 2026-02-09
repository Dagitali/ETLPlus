"""
:mod:`tests.unit.file.test_u_file_sav` module.

Unit tests for :mod:`etlplus.file.sav`.
"""

from __future__ import annotations

from etlplus.file import sav as mod
from tests.unit.file.conftest import SingleDatasetWritableContract

# SECTION: TESTS ============================================================ #


class TestSav(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.sav`."""

    module = mod
    handler_cls = mod.SavFile
    format_name = 'sav'
