"""Smoke tests for etlplus.file.toml."""

from __future__ import annotations

from etlplus.file import toml as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestToml(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.toml."""

    module = mod
    file_name = 'data.toml'
    use_sample_record = True
