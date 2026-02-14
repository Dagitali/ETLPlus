"""
:mod:`tests.unit.file.test_u_file_pb` module.

Unit tests for :mod:`etlplus.file.pb`.
"""

from __future__ import annotations

import base64

from etlplus.file import pb as mod
from tests.unit.file.pytest_file_contract_contracts import (
    BinaryKeyedPayloadModuleContract,
)

# SECTION: TESTS ============================================================ #


class TestPbReadWrite(BinaryKeyedPayloadModuleContract):
    """Unit tests for :mod:`etlplus.file.pb`."""

    module = mod
    format_name = 'pb'
    payload_key = 'payload_base64'
    expected_bytes = b'\x00\x01hello'
    sample_payload_value = base64.b64encode(expected_bytes).decode('ascii')
    invalid_payload = {'payload': 'not-base64'}
