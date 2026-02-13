"""
:mod:`tests.unit.file.test_u_file_tab` module.

Unit tests for :mod:`etlplus.file.tab`.
"""

from __future__ import annotations

from etlplus.file import tab as mod
from tests.unit.file.conftest import DelimitedModuleContract
from tests.unit.file.conftest import RoundtripSpec
from tests.unit.file.conftest import RoundtripUnitModuleContract

# SECTION: TESTS ============================================================ #


class TestTab(
    DelimitedModuleContract,
    RoundtripUnitModuleContract,
):
    """Unit tests for :mod:`etlplus.file.tab`."""

    module = mod
    format_name = 'tab'
    delimiter = '\t'
    roundtrip_spec = RoundtripSpec(
        payload=[{'id': 1, 'name': 'Ada'}],
        expected=[{'id': '1', 'name': 'Ada'}],
    )
