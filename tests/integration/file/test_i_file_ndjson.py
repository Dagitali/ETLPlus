"""
:mod:`tests.integration.file.test_i_file_ndjson` module.

Integration smoke tests for :mod:`etlplus.file.ndjson`.
"""

from __future__ import annotations

from etlplus.file import ndjson as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestNdjson(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.ndjson`."""

    module = mod
