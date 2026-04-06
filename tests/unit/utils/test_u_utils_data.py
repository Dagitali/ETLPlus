"""
:mod:`tests.unit.utils.test_u_utils_data` module.

Unit tests for :mod:`etlplus.utils._data`.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from etlplus.utils import count_records
from etlplus.utils import print_json
from etlplus.utils._types import JSONData

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestDataHelpers:
    """Unit tests for general data helpers."""

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param({'id': 1}, 1, id='single-dict'),
            pytest.param({}, 1, id='empty-dict-still-single-record'),
            pytest.param([{'id': 1}], 1, id='single-item-list'),
            pytest.param([{'id': 1}, {'id': 2}], 2, id='two-item-list'),
            pytest.param([], 0, id='empty-list'),
        ],
    )
    def test_count_records(
        self,
        payload: JSONData,
        expected: int,
    ) -> None:
        """
        Test that :func:`count_records` counts dict and list payloads
        consistently.
        """
        assert count_records(payload) == expected

    def test_print_json_uses_utf8_without_ascii_escaping(
        self,
        unicode_payload: dict[str, str],
        capsys: pytest.CaptureFixture[str],
        assert_json_output: Callable[[str, object], None],
    ) -> None:
        """
        Test that :func:`print_json` preserves readable Unicode output.
        """
        print_json(unicode_payload)
        captured = capsys.readouterr().out

        assert '\\u2603' not in captured
        assert_json_output(captured, unicode_payload)
