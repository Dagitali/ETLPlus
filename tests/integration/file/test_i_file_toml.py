"""
:mod:`tests.integration.file.test_i_file_toml` module.

Integration tests for :mod:`etlplus.file.toml`.
"""

from __future__ import annotations

from etlplus.file import toml as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestToml(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.toml`."""

    module = mod
    file_name = 'data.toml'
    use_sample_record = True
