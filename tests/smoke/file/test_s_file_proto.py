"""
:mod:`tests.smoke.file.test_s_file_proto` module.

Smoke tests for :mod:`etlplus.file.proto`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import proto as mod

# SECTION: TESTS ============================================================ #


class TestProto:
    """
    Smoke tests for :mod:`etlplus.file.proto`.
    """

    def test_read_write(self, tmp_path: Path) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.
        """
        path = tmp_path / 'data.proto'
        payload = {
            'schema': '''syntax = "proto3";
message Test { string name = 1; }
''',
        }

        try:
            written = mod.write(path, payload)
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
