"""
:mod:`tests.unit.file.test_u_file_conf` module.

Unit tests for :mod:`etlplus.file.conf`.
"""

from __future__ import annotations

from etlplus.file import conf as mod
from tests.unit.file.pytest_file_contract_contracts import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestConf(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.conf`."""

    module = mod
    handler_cls = mod.ConfFile
    format_name = 'conf'
