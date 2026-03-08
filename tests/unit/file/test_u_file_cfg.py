"""
:mod:`tests.unit.file.test_u_file_cfg` module.

Unit tests for :mod:`etlplus.file.cfg`.
"""

from __future__ import annotations

from etlplus.file import cfg as mod

from .pytest_file_contracts import StubModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCfg(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.cfg`."""

    module = mod
    handler_cls = mod.CfgFile
    format_name = 'cfg'
