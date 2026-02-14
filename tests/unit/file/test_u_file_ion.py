"""
:mod:`tests.unit.file.test_u_file_ion` module.

Unit tests for :mod:`etlplus.file.ion`.
"""

from __future__ import annotations

from etlplus.file import ion as mod
from tests.unit.file.pytest_file_contract_contracts import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestIon(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.ion`."""

    module = mod
    handler_cls = mod.IonFile
    format_name = 'ion'
