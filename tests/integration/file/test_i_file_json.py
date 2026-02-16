"""
:mod:`tests.integration.file.test_i_file_json` module.

Integration tests for :mod:`etlplus.file.json`.
"""

from __future__ import annotations

from etlplus.file import json as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestJson(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.json`."""

    module = mod
    file_name = 'data.json'
