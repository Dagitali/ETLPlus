"""
:mod:`tests.unit.utils.test_u_utils_data` module.

Unit tests for :mod:`etlplus.utils.data`.
"""

from __future__ import annotations

import pytest

from etlplus.utils import count_records
from etlplus.utils import print_json

# SECTION: TESTS ============================================================ #


class TestData:
    """Unit tests for :mod:`etlplus.utils.data`."""

    def test_count_records(self) -> None:
        """Test record counts differ for dicts vs. lists."""
        assert count_records({'a': 1}) == 1
        assert count_records([{'a': 1}, {'b': 2}]) == 2

    def test_print_json_uses_utf8(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`print_json` avoids ASCII escaping."""
        payload = {'emoji': '\u2603'}
        print_json(payload)
        captured = capsys.readouterr().out
        assert '\\u2603' not in captured
        assert 'emoji' in captured
