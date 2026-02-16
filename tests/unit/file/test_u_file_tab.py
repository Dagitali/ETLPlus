"""
:mod:`tests.unit.file.test_u_file_tab` module.

Unit tests for :mod:`etlplus.file.tab`.
"""

from __future__ import annotations

from etlplus.file import tab as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import DelimitedModuleContract
from .pytest_file_roundtrip_cases import build_delimited_roundtrip_spec

# SECTION: TESTS ============================================================ #


class TestTab(
    DelimitedModuleContract,
    RoundtripUnitModuleContract,
):
    """Unit tests for :mod:`etlplus.file.tab`."""

    module = mod
    format_name = 'tab'
    delimiter = '\t'
    roundtrip_spec = build_delimited_roundtrip_spec()
