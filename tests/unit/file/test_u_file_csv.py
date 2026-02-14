"""
:mod:`tests.unit.file.test_u_file_csv` module.

Unit tests for :mod:`etlplus.file.csv`.
"""

from __future__ import annotations

from etlplus.file import csv as mod

from .pytest_file_contract_contracts import DelimitedModuleContract
from .pytest_file_contract_mixins import RoundtripSpec
from .pytest_file_contract_mixins import RoundtripUnitModuleContract

# SECTION: TESTS ============================================================ #


class TestCsv(
    DelimitedModuleContract,
    RoundtripUnitModuleContract,
):
    """Unit tests for :mod:`etlplus.file.csv`."""

    module = mod
    format_name = 'csv'
    delimiter = ','
    roundtrip_spec = RoundtripSpec(
        payload=[{'id': 1, 'name': 'Ada'}],
        expected=[{'id': '1', 'name': 'Ada'}],
    )
