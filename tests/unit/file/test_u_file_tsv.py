"""
:mod:`tests.unit.file.test_u_file_tsv` module.

Unit tests for :mod:`etlplus.file.tsv`.
"""

from __future__ import annotations

from etlplus.file import tsv as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import DelimitedModuleContract
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: TESTS ============================================================ #


class TestTsv(
    DelimitedModuleContract,
    RoundtripUnitModuleContract,
):
    """Unit tests for :mod:`etlplus.file.tsv`."""

    module = mod
    format_name = 'tsv'
    delimiter = '\t'
    roundtrip_spec = build_roundtrip_spec(
        shape='delimited',
        field_count=2,
        value_kind='mixed',
    )
