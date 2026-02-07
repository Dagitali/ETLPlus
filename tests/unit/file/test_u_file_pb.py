"""
:mod:`tests.unit.file.test_u_file_pb` module.

Unit tests for :mod:`etlplus.file.pb`.
"""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from etlplus.file import pb as mod

# SECTION: TESTS ============================================================ #


class TestPbReadWrite:
    """Unit tests for PB read/write functions."""

    def test_read_write_round_trip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that PB bytes are preserved through read/write."""
        payload = b'\x00\x01hello'
        data = {
            'payload_base64': base64.b64encode(payload).decode('ascii'),
        }
        path = tmp_path / 'data.pb'

        written = mod.write(path, data)

        assert written == 1
        assert path.read_bytes() == payload
        assert mod.read(path) == data

    def test_write_rejects_missing_payload_key(
        self,
        tmp_path: Path,
    ) -> None:
        """Test PB write requiring the payload_base64 key."""
        path = tmp_path / 'data.pb'

        with pytest.raises(TypeError, match='payload_base64'):
            mod.write(path, {'payload': 'not-base64'})
