"""Smoke tests for etlplus.file.proto."""

from __future__ import annotations

from etlplus.file import proto as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestProto(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.proto."""

    module = mod
    file_name = 'data.proto'
    payload = {
        'schema': """syntax = "proto3";
message Test { string name = 1; }
""",
    }
