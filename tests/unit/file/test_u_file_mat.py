"""
:mod:`tests.unit.file.test_u_file_mat` module.

Unit tests for :mod:`etlplus.file.mat`.
"""

from __future__ import annotations

from etlplus.file import mat as mod

from .pytest_file_contracts import SingleDatasetPlaceholderContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestMat(SingleDatasetPlaceholderContract):
    """Unit tests for :mod:`etlplus.file.mat`."""

    module = mod
    handler_cls = mod.MatFile
    format_name = 'mat'
