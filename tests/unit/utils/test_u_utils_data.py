"""
:mod:`tests.unit.utils.test_u_utils_data` module.

Unit tests for :mod:`etlplus.utils.data`.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from etlplus.utils import count_records
from etlplus.utils import print_json
from etlplus.utils.types import JSONData

# SECTION: TESTS ============================================================ #


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
    payload: JSONData,
    expected: int,
) -> None:
    """Test record counts consistently for dict and list payloads."""
    assert count_records(payload) == expected


def test_print_json_uses_utf8_without_ascii_escaping(
    unicode_payload: dict[str, str],
    capsys: pytest.CaptureFixture[str],
    assert_json_output: Callable[[str, object], None],
) -> None:
    """
    Test that :func:`print_json` prints UTF-8 JSON and keeps Unicode
    characters readable.
    """
    print_json(unicode_payload)
    captured = capsys.readouterr().out
    assert '\\u2603' not in captured
    assert_json_output(captured, unicode_payload)
