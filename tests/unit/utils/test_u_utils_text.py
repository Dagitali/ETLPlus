"""
:mod:`tests.unit.utils.test_u_utils_text` module.

Unit tests for :mod:`etlplus.utils._text`.
"""

from __future__ import annotations

import pytest

from etlplus.utils import TextChoiceResolver
from etlplus.utils import TextNormalizer

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


# SECTION: TESTS ============================================================ #


class TestNormalizeText:
    """Unit tests for text-normalization helpers."""

    def test_choice_resolver_is_frozen(self) -> None:
        """Test that choice policy cannot be reassigned after construction."""
        resolver = TextChoiceResolver({'file': 'file'}, 'file')

        with pytest.raises(AttributeError):
            resolver.default = 'api'  # type: ignore[misc]

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('  HeLLo  ', 'hello', id='trim-and-lower'),
            pytest.param('  STRAẞE  ', 'strasse', id='unicode-casefold'),
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
        Test that :meth:`TextChoiceResolver.resolve` resolves choices and
        falls back to defaults.
        """
        resolver = TextChoiceResolver({'file': 'file', 'api': 'api'}, 'file')

        assert resolver.resolve('  FILE  ') == 'file'
        assert resolver.resolve('unknown') == 'file'
        assert resolver.resolve(None) == 'file'

    def test_normalize_choice_supports_custom_normalizer(self) -> None:
        """
        Test that :class:`TextChoiceResolver` honors a caller-provided
        normalizer.
        """
        assert (
            TextChoiceResolver(
                {'v1': 'version-1'},
                'fallback',
                normalize=lambda value: (value or '').lower(),
            ).resolve('V1')
            == 'version-1'
        )

    def test_normalize_choice_wrapper_preserves_function_api(self) -> None:
        """Test compatibility wrapper delegates to the stateful resolver."""
        assert (
            TextNormalizer.resolve_choice(
                '  FILE  ',
                mapping={'file': 'file'},
                default='api',
            )
            == 'file'
        )

    @pytest.mark.parametrize(
        ('text', 'limit', 'expected'),
        [
            pytest.param('abcdef', 3, 'abc', id='truncated'),
            pytest.param('abc', 10, 'abc', id='shorter-than-limit'),
            pytest.param('', 3, '', id='empty'),
            pytest.param(None, 3, '', id='none'),
        ],
    )
    def test_truncate(
        self,
        text: str | None,
        limit: int,
        expected: str,
    ) -> None:
        """Test bounded text truncation."""
        assert TextNormalizer.truncate(text, limit=limit) == expected
