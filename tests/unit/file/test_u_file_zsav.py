"""
:mod:`tests.unit.file.test_u_file_zsav` module.

Unit tests for :mod:`etlplus.file.zsav`.
"""

from __future__ import annotations

from etlplus.file import zsav as mod

from .pytest_file_contracts import SingleDatasetPlaceholderContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestZsav(SingleDatasetPlaceholderContract):
    """Unit tests for :mod:`etlplus.file.zsav`."""

    module = mod
    handler_cls = mod.ZsavFile
    format_name = 'zsav'
