"""
:mod:`tests.unit.file.test_u_file_log` module.

Unit tests for :mod:`etlplus.file.log`.
"""

from __future__ import annotations

from etlplus.file import log as mod
from tests.unit.file.conftest import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestLog(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.log`."""

    module = mod
    handler_cls = mod.LogFile
    format_name = 'log'
