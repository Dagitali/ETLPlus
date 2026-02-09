"""
:mod:`tests.unit.file.test_u_file_vm` module.

Unit tests for :mod:`etlplus.file.vm`.
"""

from __future__ import annotations

from etlplus.file import vm as mod
from tests.unit.file.conftest import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestVm(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.vm`."""

    module = mod
    handler_cls = mod.VmFile
    format_name = 'vm'
