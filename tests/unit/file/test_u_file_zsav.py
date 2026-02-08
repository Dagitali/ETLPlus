"""
:mod:`tests.unit.file.test_u_file_zsav` module.

Unit tests for :mod:`etlplus.file.zsav`.
"""

from __future__ import annotations

from etlplus.file import zsav as mod
from tests.unit.file.conftest import SingleDatasetPlaceholderContract

# SECTION: TESTS ============================================================ #


class TestZsav(SingleDatasetPlaceholderContract):
    """Unit tests for :mod:`etlplus.file.zsav`."""

    module = mod
    handler_cls = mod.ZsavFile
    format_name = 'zsav'
