"""
:mod:`tests.integration.file.test_i_file_msgpack` module.

Integration tests for :mod:`etlplus.file.msgpack`.
"""

from __future__ import annotations

from etlplus.file import msgpack as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestMsgpack(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.msgpack`."""

    module = mod
    file_name = 'data.msgpack'
