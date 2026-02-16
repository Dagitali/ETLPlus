"""
:mod:`tests.unit.file.test_u_file_psv` module.

Unit tests for :mod:`etlplus.file.psv`.
"""

from __future__ import annotations

from etlplus.file import psv as mod

from .pytest_file_contract_mixins import RoundtripSpec
from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import DelimitedModuleContract

# SECTION: TESTS ============================================================ #


class TestPsv(
    DelimitedModuleContract,
    RoundtripUnitModuleContract,
):
    """Unit tests for :mod:`etlplus.file.psv`."""

    module = mod
    format_name = 'psv'
    delimiter = '|'
    roundtrip_spec = RoundtripSpec(
        payload=[{'id': 1, 'name': 'Ada'}],
        expected=[{'id': '1', 'name': 'Ada'}],
    )
