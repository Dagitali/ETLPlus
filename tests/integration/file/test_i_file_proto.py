"""
:mod:`tests.integration.file.test_i_file_proto` module.

Integration smoke tests for :mod:`etlplus.file.proto`.
"""

from __future__ import annotations

from etlplus.file import proto as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestProto(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.proto`."""

    module = mod
    payload = {
        'schema': """syntax = "proto3";
message Test { string name = 1; }
""",
    }
