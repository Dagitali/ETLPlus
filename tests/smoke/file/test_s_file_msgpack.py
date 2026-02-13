"""
Smoke tests for etlplus.file.msgpack.
"""

from __future__ import annotations

from etlplus.file import msgpack as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestMsgpack(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.msgpack."""

    module = mod
    file_name = 'data.msgpack'
