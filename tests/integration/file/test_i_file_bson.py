"""
:mod:`tests.integration.file.test_i_file_bson` module.

Integration smoke tests for :mod:`etlplus.file.bson`.
"""

from __future__ import annotations

from etlplus.file import bson as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestBson(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.bson`."""

    module = mod
