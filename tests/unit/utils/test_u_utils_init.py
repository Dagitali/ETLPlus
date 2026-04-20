"""
:mod:`tests.unit.utils.test_u_utils_init` module.

Unit tests for :mod:`etlplus.utils` package facade exports.
"""

from __future__ import annotations

import pytest

from etlplus import utils as utils_pkg
from etlplus.utils._data import JsonCodec
from etlplus.utils._data import RecordCounter
from etlplus.utils._enums import CoercibleStrEnum
from etlplus.utils._mapping import MappingParser
from etlplus.utils._mixins import BoundsWarningsMixin
from etlplus.utils._numbers import FloatParser
from etlplus.utils._numbers import IntParser
from etlplus.utils._parsing import MappingFieldParser
from etlplus.utils._parsing import SequenceParser
from etlplus.utils._parsing import ValueParser
from etlplus.utils._substitution import SubstitutionResolver
from etlplus.utils._text import TextNormalizer

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


UTILS_EXPORTS = [
    ('MappingFieldParser', MappingFieldParser),
    ('SequenceParser', SequenceParser),
    ('ValueParser', ValueParser),
    ('FloatParser', FloatParser),
    ('IntParser', IntParser),
    ('JsonCodec', JsonCodec),
    ('MappingParser', MappingParser),
    ('RecordCounter', RecordCounter),
    ('SubstitutionResolver', SubstitutionResolver),
    ('TextNormalizer', TextNormalizer),
    ('CoercibleStrEnum', CoercibleStrEnum),
    ('BoundsWarningsMixin', BoundsWarningsMixin),
]


# SECTION: TESTS ============================================================ #


class TestUtilsPackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert utils_pkg.__all__ == [name for name, _value in UTILS_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), UTILS_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(utils_pkg, name) == expected
