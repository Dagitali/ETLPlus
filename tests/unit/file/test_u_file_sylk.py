"""
:mod:`tests.unit.file.test_u_file_sylk` module.

Unit tests for :mod:`etlplus.file.sylk`.
"""

from __future__ import annotations

from etlplus.file import sylk as mod

from .pytest_file_contract_contracts import SingleDatasetPlaceholderContract

# SECTION: TESTS ============================================================ #


class TestSylk(SingleDatasetPlaceholderContract):
    """Unit tests for :mod:`etlplus.file.sylk`."""

    module = mod
    handler_cls = mod.SylkFile
    format_name = 'sylk'
