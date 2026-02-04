"""
:mod:`tests.smoke.file.test_s_file_proto` module.

Smoke tests for :mod:`etlplus.file.proto`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import proto as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestProto:
    """
    Smoke tests for :mod:`etlplus.file.proto`.
    """

    def test_read_write(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.

        Parameters
        ----------
        tmp_path : Path
            Pytest temporary directory.
        """
        path = tmp_path / 'data.proto'
        payload = {
            'schema': """syntax = "proto3";
message Test { string name = 1; }
""",
        }
        run_file_smoke(mod, path, payload)
