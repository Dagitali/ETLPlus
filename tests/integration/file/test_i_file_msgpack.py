"""
:mod:`tests.integration.file.test_i_file_msgpack` module.

Integration smoke tests for :mod:`etlplus.file.msgpack`.
"""

from __future__ import annotations

from etlplus.file import msgpack as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestMsgpack(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.msgpack`."""

    module = mod
