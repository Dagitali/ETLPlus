"""
:mod:`tests.unit.utils.test_u_utils_paths` module.

Unit tests for :mod:`etlplus.utils._paths`.
"""

from __future__ import annotations

import pytest

from etlplus.utils import PathParser

# SECTION: TESTS ============================================================ #


class TestPathParser:
    """Unit tests for path parsing helpers."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('C:\\data\\file.csv', True, id='windows-backslash'),
            pytest.param('D:/data/file.csv', True, id='windows-forward-slash'),
            pytest.param('/tmp/data.csv', False, id='posix-absolute'),
            pytest.param('data/input.csv', False, id='relative'),
            pytest.param('s3://bucket/key', False, id='uri'),
            pytest.param('', False, id='empty'),
            pytest.param('1:/data', False, id='numeric-prefix'),
        ],
    )
    def test_is_windows_drive_path(self, value: str, expected: bool) -> None:
        """Test Windows drive-prefix detection."""
        assert PathParser.is_windows_drive_path(value) is expected
