"""
:mod:`tests.integration.file.test_i_file_toml` module.

Integration smoke tests for :mod:`etlplus.file.toml`.
"""

from __future__ import annotations

from etlplus.file import toml as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestToml(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.toml`."""

    module = mod
    use_sample_record = True
