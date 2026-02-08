"""
:mod:`tests.unit.file.test_u_file_hbs` module.

Unit tests for :mod:`etlplus.file.hbs`.
"""

from __future__ import annotations

from etlplus.file import hbs as mod
from tests.unit.file.conftest import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestHbs(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.hbs`."""

    module = mod
    handler_cls = mod.HbsFile
    format_name = 'hbs'
