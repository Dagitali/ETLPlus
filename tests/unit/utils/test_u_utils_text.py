"""
:mod:`tests.unit.utils.test_u_utils_text` module.

Unit tests for :mod:`etlplus.utils._text`.
"""

from __future__ import annotations

import pytest

from etlplus.utils import TextNormalizer

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


# SECTION: TESTS ============================================================ #


class TestNormalizeText:
    """Unit tests for text-normalization helpers."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('  HeLLo  ', 'hello', id='trim-and-lower'),
            pytest.param('', '', id='empty-string'),
            pytest.param(None, '', id='none'),
        ],
    )
    def test_normalize_str(
        self,
        value: str | None,
        expected: str,
    ) -> None:
        """
        Test that :meth:`TextNormalizer.normalize` trims, lowercases, and
        handles ``None`` safely.
        """
        assert TextNormalizer.normalize(value) == expected

    def test_normalize_choice_with_default_normalizer(self) -> None:
        """
        Test that :meth:`TextNormalizer.resolve_choice` resolves choices and
        falls back to defaults.
        """
        mapping = {'file': 'file', 'api': 'api'}

        assert (
            TextNormalizer.resolve_choice(
                '  FILE  ',
                mapping=mapping,
                default='file',
            )
            == 'file'
        )
        assert (
            TextNormalizer.resolve_choice(
                'unknown',
                mapping=mapping,
                default='file',
            )
            == 'file'
        )
        assert (
            TextNormalizer.resolve_choice(
                None,
                mapping=mapping,
                default='file',
            )
            == 'file'
        )

    def test_normalize_choice_supports_custom_normalizer(self) -> None:
        """
        Test that :meth:`TextNormalizer.resolve_choice` honors a caller-provided
        normalizer.
        """
        assert (
            TextNormalizer.resolve_choice(
                'V1',
                mapping={'v1': 'version-1'},
                default='fallback',
                normalize=lambda value: (value or '').lower(),
            )
            == 'version-1'
        )
