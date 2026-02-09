"""
:mod:`tests.unit.file.test_u_file_xpt` module.

Unit tests for :mod:`etlplus.file.xpt`.
"""

from __future__ import annotations

from etlplus.file import xpt as mod
from tests.unit.file.conftest import SingleDatasetWritableContract

# SECTION: TESTS ============================================================ #


class TestXpt(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.xpt`."""

    module = mod
    handler_cls = mod.XptFile
    format_name = 'xpt'
