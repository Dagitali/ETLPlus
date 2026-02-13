"""
Smoke tests for etlplus.file.avro.
"""

from __future__ import annotations

from etlplus.file import avro as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestAvro(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.avro."""

    module = mod
    file_name = 'data.avro'
