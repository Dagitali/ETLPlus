"""
:mod:`etlplus.utils` package.

Small shared helpers used across modules.
"""

from __future__ import annotations

from ._data import JsonCodec
from ._data import RecordPayloadParser
from ._data import count_records
from ._data import stringify_value
from ._enums import CoercibleStrEnum
from ._graph import NamedDependencyGraph
from ._graph import topological_sort_named_items
from ._graph import topological_sort_names
from ._mapping import MappingParser
from ._mixins import BoundsWarningsMixin
from ._numbers import FloatParser
from ._numbers import IntParser
from ._numbers import finite_decimal_or_none
from ._numbers import is_integer_value
from ._numbers import is_number_value
from ._parsing import MappingFieldParser
from ._parsing import SequenceParser
from ._parsing import ValueParser
from ._paths import PathHasher
from ._paths import PathParser
from ._secrets import SecretResolver
from ._substitution import SubstitutionResolver
from ._substitution import TokenReferenceCollector
from ._text import TextChoiceResolver
from ._text import TextNormalizer
from ._types import NonEmptyStr
from ._types import NonEmptyStrList

# SECTION: EXPORTS ========================================================== #


# NOTE: For the public-surface narrowing work, the utils facade is split into
# NOTE: stable and transitional groups and in the package export contract test
# NOTE: at test_u_utils_init.py. That preserves the full current facade, but it
# NOTE: clearly marks JsonCodec, PathHasher, RecordPayloadParser,
# NOTE: TokenReferenceCollector, BoundsWarningsMixin, NonEmptyStr, and
# NOTE: NonEmptyStrList as transitional exports to revisit in a future feature
# NOTE: release.
__all__ = [
    # Stable exports that are expected to remain public in the long term
    # Classes
    'FloatParser',
    'IntParser',
    'MappingFieldParser',
    'MappingParser',
    'NamedDependencyGraph',
    'PathParser',
    'SecretResolver',
    'SequenceParser',
    'SubstitutionResolver',
    'TextChoiceResolver',
    'TextNormalizer',
    'ValueParser',
    # Enums
    'CoercibleStrEnum',
    # Functions
    'count_records',
    'finite_decimal_or_none',
    'is_integer_value',
    'is_number_value',
    'stringify_value',
    'topological_sort_named_items',
    'topological_sort_names',
    # Transitional exports kept public for v1 compatibility
    # Data Classes
    'JsonCodec',
    'PathHasher',
    'RecordPayloadParser',
    'TokenReferenceCollector',
    # Mixins
    'BoundsWarningsMixin',
    # Type Aliases
    'NonEmptyStr',
    'NonEmptyStrList',
]
