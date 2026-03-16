"""
:mod:`tests.unit.utils.test_u_utils_text` module.

Unit tests for :mod:`etlplus.utils.text`.
"""

from __future__ import annotations

import pytest

from etlplus.utils import normalize_choice
from etlplus.utils import normalize_str

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        pytest.param('  HeLLo  ', 'hello', id='trim-and-lower'),
        pytest.param('', '', id='empty-string'),
        pytest.param(None, '', id='none'),
    ],
)
def test_normalize_str(
    value: str | None,
    expected: str,
) -> None:
    """
    Test that :func:`normalize_str` normalizes casing/whitespace and safely
    handles ``None``.
    """
    assert normalize_str(value) == expected


def test_normalize_choice_with_default_normalizer() -> None:
    """
    Test that :func:`normalize_choice` resolves known choices and falls back to
    default for unknown values.
    """
    mapping = {'file': 'file', 'api': 'api'}
    assert normalize_choice('  FILE  ', mapping=mapping, default='file') == 'file'
    assert normalize_choice('unknown', mapping=mapping, default='file') == 'file'
    assert normalize_choice(None, mapping=mapping, default='file') == 'file'


def test_normalize_choice_supports_custom_normalizer() -> None:
    """
    Test that :func:`normalize_choice` uses a caller-provided normalizer
    function before map lookup.
    """
    mapping = {'v1': 'version-1'}
    assert (
        normalize_choice(
            'V1',
            mapping=mapping,
            default='fallback',
            normalize=lambda value: (value or '').lower(),
        )
        == 'version-1'
    )
