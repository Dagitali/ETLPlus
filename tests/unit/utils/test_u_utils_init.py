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
from etlplus.utils._graph import topological_sort_named_items
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
from etlplus.utils._paths import PathHasher
from etlplus.utils._paths import PathParser
from etlplus.utils._secrets import SecretResolver
from etlplus.utils._substitution import SubstitutionResolver
from etlplus.utils._substitution import TokenReferenceCollector
from etlplus.utils._text import TextChoiceResolver
from etlplus.utils._text import TextNormalizer
from etlplus.utils._types import NonEmptyStr
from etlplus.utils._types import NonEmptyStrList

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


STABLE_UTILS_EXPORTS = [
    ('FloatParser', FloatParser),
    ('IntParser', IntParser),
    ('MappingFieldParser', MappingFieldParser),
    ('MappingParser', MappingParser),
    ('NamedDependencyGraph', NamedDependencyGraph),
    ('PathParser', PathParser),
    ('SecretResolver', SecretResolver),
    ('SequenceParser', SequenceParser),
    ('SubstitutionResolver', SubstitutionResolver),
    ('TextChoiceResolver', TextChoiceResolver),
    ('TextNormalizer', TextNormalizer),
    ('ValueParser', ValueParser),
    ('CoercibleStrEnum', CoercibleStrEnum),
    ('count_records', count_records),
    ('finite_decimal_or_none', finite_decimal_or_none),
    ('is_integer_value', is_integer_value),
    ('is_number_value', is_number_value),
    ('stringify_value', stringify_value),
    ('topological_sort_named_items', topological_sort_named_items),
    ('topological_sort_names', topological_sort_names),
]

TRANSITIONAL_UTILS_EXPORTS = [
    ('JsonCodec', JsonCodec),
    ('PathHasher', PathHasher),
    ('RecordPayloadParser', RecordPayloadParser),
    ('TokenReferenceCollector', TokenReferenceCollector),
    ('BoundsWarningsMixin', BoundsWarningsMixin),
    ('NonEmptyStr', NonEmptyStr),
    ('NonEmptyStrList', NonEmptyStrList),
]

UTILS_EXPORTS = [*STABLE_UTILS_EXPORTS, *TRANSITIONAL_UTILS_EXPORTS]


# SECTION: TESTS ============================================================ #


class TestUtilsPackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert utils_pkg.__all__ == [name for name, _value in UTILS_EXPORTS]

    def test_stable_symbols_lead_public_export_order(self) -> None:
        """Test stable exports stay grouped first in the package facade."""
        assert utils_pkg.__all__[: len(STABLE_UTILS_EXPORTS)] == [
            name for name, _value in STABLE_UTILS_EXPORTS
        ]

    def test_transitional_symbols_remain_public_for_now(self) -> None:
        """
        Test transitional exports remain available until a feature release
        narrows them.
        """
        assert utils_pkg.__all__[len(STABLE_UTILS_EXPORTS):] == [
            name for name, _value in TRANSITIONAL_UTILS_EXPORTS
        ]

    @pytest.mark.parametrize(('name', 'expected'), UTILS_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(utils_pkg, name) == expected
