"""
:mod:`tests.unit.file.test_u_file_csv` module.

Unit tests for :mod:`etlplus.file.csv`.
"""

from __future__ import annotations

from etlplus.file import csv as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import DelimitedModuleContract
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: TESTS ============================================================ #


class TestCsv(
    DelimitedModuleContract,
    RoundtripUnitModuleContract,
):
    """Unit tests for :mod:`etlplus.file.csv`."""

    module = mod
    format_name = 'csv'
    delimiter = ','
    roundtrip_spec = build_roundtrip_spec(
        shape='delimited',
        field_count=2,
        value_kind='mixed',
    )
