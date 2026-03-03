"""
:mod:`tests.unit.utils.test_u_utils_enums` module.

Unit tests for :mod:`etlplus.utils.enums`.
"""

from __future__ import annotations

import pytest

from etlplus.utils.enums import CoercibleStrEnum

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


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

    def test_alias_resolving_by_member_name(self) -> None:
        """Aliases may resolve by member names as a fallback."""
        assert _Palette.coerce('leaf') is _Palette.GREEN

    def test_alias_resolving_by_member_value(self) -> None:
        """Aliases may resolve by value strings."""
        assert _Palette.coerce('verdant') is _Palette.GREEN

    def test_alias_resolving_direct_member(self) -> None:
        """Aliases may resolve directly to enum members."""
        assert _Palette.coerce('r') is _Palette.RED

    def test_choices_returns_member_values(self) -> None:
        """choices should expose enum values in declaration order."""
        assert _Palette.choices() == ('red', 'green', 'blue')

    def test_coerce_returns_member_input_unchanged(self) -> None:
        """coerce should return member values as-is."""
        assert _Palette.coerce(_Palette.RED) is _Palette.RED

    def test_try_coerce_returns_none_for_invalid(self) -> None:
        """try_coerce should return ``None`` for invalid inputs."""
        assert _Palette.try_coerce('unknown') is None

    def test_default_aliases_mapping_is_empty(self) -> None:
        """aliases defaults to an empty mapping when not overridden."""
        assert _Plain.aliases() == {}

    def test_invalid_value_raises(self) -> None:
        """Invalid values should raise ``ValueError`` with allowed values."""
        with pytest.raises(ValueError, match='Invalid _Palette value'):
            _Palette.coerce('unknown')
