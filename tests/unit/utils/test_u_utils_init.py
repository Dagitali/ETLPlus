"""
:mod:`tests.unit.utils.test_u_utils_init` module.

Unit tests for :mod:`etlplus.utils` package facade exports.
"""

from __future__ import annotations

import pytest

from etlplus import utils as utils_pkg
from etlplus.utils._data import JsonCodec
from etlplus.utils._data import RecordPayloadParser
from etlplus.utils._data import count_records
from etlplus.utils._data import stringify_value
from etlplus.utils._enums import CoercibleStrEnum
from etlplus.utils._graph import NamedDependencyGraph
from etlplus.utils._graph import topological_sort_names
from etlplus.utils._mapping import MappingParser
from etlplus.utils._mixins import BoundsWarningsMixin
from etlplus.utils._numbers import FloatParser
from etlplus.utils._numbers import IntParser
from etlplus.utils._numbers import finite_decimal_or_none
from etlplus.utils._numbers import is_integer_value
from etlplus.utils._numbers import is_number_value
from etlplus.utils._parsing import MappingFieldParser
from etlplus.utils._parsing import SequenceParser
from etlplus.utils._parsing import ValueParser
from etlplus.utils._paths import PathParser
from etlplus.utils._substitution import SubstitutionResolver
from etlplus.utils._text import TextChoiceResolver
from etlplus.utils._text import TextNormalizer
from etlplus.utils._types import NonEmptyStr
from etlplus.utils._types import NonEmptyStrList

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
    ('NamedDependencyGraph', NamedDependencyGraph),
    ('PathParser', PathParser),
    ('RecordPayloadParser', RecordPayloadParser),
    ('SubstitutionResolver', SubstitutionResolver),
    ('TextChoiceResolver', TextChoiceResolver),
    ('TextNormalizer', TextNormalizer),
    ('CoercibleStrEnum', CoercibleStrEnum),
    ('count_records', count_records),
    ('finite_decimal_or_none', finite_decimal_or_none),
    ('is_integer_value', is_integer_value),
    ('is_number_value', is_number_value),
    ('stringify_value', stringify_value),
    ('topological_sort_names', topological_sort_names),
    ('BoundsWarningsMixin', BoundsWarningsMixin),
    ('NonEmptyStr', NonEmptyStr),
    ('NonEmptyStrList', NonEmptyStrList),
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
