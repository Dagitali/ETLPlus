"""
:mod:`tests.unit.file.test_u_file_psv` module.

Unit tests for :mod:`etlplus.file.psv`.
"""

from __future__ import annotations

from etlplus.file import psv as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import DelimitedModuleContract
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: TESTS ============================================================ #


class TestPsv(
    DelimitedModuleContract,
    RoundtripUnitModuleContract,
):
    """Unit tests for :mod:`etlplus.file.psv`."""

    module = mod
    format_name = 'psv'
    delimiter = '|'
    roundtrip_spec = build_roundtrip_spec(
        shape='delimited',
        field_count=2,
        value_kind='mixed',
    )
