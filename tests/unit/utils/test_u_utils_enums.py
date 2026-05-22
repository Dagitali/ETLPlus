"""
:mod:`tests.unit.utils.test_u_utils_enums` module.

Unit tests for :mod:`etlplus.utils._enums`.
"""

from __future__ import annotations

import pytest

from etlplus.utils._enums import CoercibleStrEnum

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _German(CoercibleStrEnum):
    """Test enum with Unicode-sensitive aliases."""

    STREET = 'street'

    @classmethod
    def aliases(cls) -> dict[str, str]:
        return {'straße': cls.STREET}


class _Palette(CoercibleStrEnum):
    """Test enum with alias variants for branch coverage."""

    RED = 'red'
    GREEN = 'green'
    BLUE = 'blue'

    @classmethod
    def aliases(cls) -> dict[str, str]:
        return {
            'r': cls.RED,
            'verdant': 'green',
            'leaf': 'GREEN',
        }


class _Plain(CoercibleStrEnum):
    """Test enum without alias overrides."""

    ONE = 'one'


# SECTION: TESTS ============================================================ #


class TestCoercibleStrEnum:
    """Unit tests for coercion helpers on :class:`CoercibleStrEnum`."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('leaf', _Palette.GREEN, id='member-name'),
            pytest.param('verdant', _Palette.GREEN, id='member-value'),
            pytest.param('r', _Palette.RED, id='direct-member'),
        ],
    )
    def test_alias_resolving(self, value: str, expected: _Palette) -> None:
        """Test that aliases resolve through every supported alias target."""
        assert _Palette.coerce(value) is expected

    def test_alias_resolving_uses_casefolding(self) -> None:
        """Test that alias matching follows shared text normalization."""
        assert _German.coerce('STRASSE') is _German.STREET

    def test_choices_returns_member_values(self) -> None:
        """Test that choices expose enum values in declaration order."""
        assert _Palette.choices() == ('red', 'green', 'blue')

    def test_coerce_returns_member_input_unchanged(self) -> None:
        """Test that coerce returns member values as-is."""
        assert _Palette.coerce(_Palette.RED) is _Palette.RED

    def test_try_coerce_returns_none_for_invalid(self) -> None:
        """Test that try_coerce returns ``None`` for invalid inputs."""
        assert _Palette.try_coerce('unknown') is None

    def test_default_aliases_mapping_is_empty(self) -> None:
        """
        Test that aliases default to an empty mapping when not overridden.
        """
        assert _Plain.aliases() == {}

    def test_invalid_value_raises(self) -> None:
        """
        Test that invalid values raise :class:`ValueError` with allowed values.
        """
        with pytest.raises(ValueError, match='Invalid _Palette value'):
            _Palette.coerce('unknown')
