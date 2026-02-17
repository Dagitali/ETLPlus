"""
:mod:`tests.unit.file.test_u_file_proto` module.

Unit tests for :mod:`etlplus.file.proto`.
"""

from __future__ import annotations

from etlplus.file import proto as mod

from .pytest_file_contracts import BinaryKeyedPayloadModuleContract

# SECTION: TESTS ============================================================ #


class TestProtoReadWrite(BinaryKeyedPayloadModuleContract):
    """Unit tests for :mod:`etlplus.file.proto`."""

    module = mod
    format_name = 'proto'
    payload_key = 'schema'
    sample_payload_value = 'message Row { string id = 1; }'
    expected_bytes = sample_payload_value.encode('utf-8')
    invalid_payload = {'payload': 'message Row {}'}
