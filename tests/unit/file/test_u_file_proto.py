"""
:mod:`tests.unit.file.test_u_file_proto` module.

Unit tests for :mod:`etlplus.file.proto`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import proto as mod

# SECTION: TESTS ============================================================ #


class TestProtoReadWrite:
    """Unit tests for PROTO read/write functions."""

    def test_read_write_round_trip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that schema text is preserved through read/write."""
        data = {'schema': 'message Row { string id = 1; }'}
        path = tmp_path / 'data.proto'

        written = mod.write(path, data)

        assert written == 1
        assert mod.read(path) == data

    def test_write_rejects_missing_schema_key(
        self,
        tmp_path: Path,
    ) -> None:
        """Test PROTO write requiring the schema key."""
        path = tmp_path / 'data.proto'

        with pytest.raises(TypeError, match='schema'):
            mod.write(path, {'payload': 'message Row {}'})
