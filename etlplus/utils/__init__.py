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
from ._substitution import SubstitutionResolver
from ._substitution import TokenReferenceCollector
from ._text import TextChoiceResolver
from ._text import TextNormalizer
from ._types import NonEmptyStr
from ._types import NonEmptyStrList

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MappingFieldParser',
    'SequenceParser',
    'ValueParser',
    'FloatParser',
    'IntParser',
    'JsonCodec',
    'MappingParser',
    'NamedDependencyGraph',
    'PathHasher',
    'PathParser',
    'RecordPayloadParser',
    'SubstitutionResolver',
    'TextChoiceResolver',
    'TextNormalizer',
    'TokenReferenceCollector',
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
    # Mixins
    'BoundsWarningsMixin',
    # Type Aliases
    'NonEmptyStr',
    'NonEmptyStrList',
]
