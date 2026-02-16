"""
:mod:`tests.integration.file.test_i_file_avro` module.

Integration tests for :mod:`etlplus.file.avro`.
"""

from __future__ import annotations

from etlplus.file import avro as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestAvro(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.avro`."""

    module = mod
    file_name = 'data.avro'
