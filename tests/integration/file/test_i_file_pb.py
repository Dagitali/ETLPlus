"""
:mod:`tests.integration.file.test_i_file_pb` module.

Integration tests for :mod:`etlplus.file.pb`.
"""

from __future__ import annotations

from etlplus.file import pb as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestPb(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.pb`."""

    module = mod
    file_name = 'data.pb'
    payload = {'payload_base64': 'aGVsbG8='}
