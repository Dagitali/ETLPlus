"""
:mod:`etlplus.utils` package.

Small shared helpers used across modules.
"""

from __future__ import annotations

from ._data import count_records
from ._data import print_json
from ._data import serialize_json
from ._enums import CoercibleStrEnum
from ._mapping import cast_str_dict
from ._mapping import coerce_dict
from ._mapping import maybe_mapping
from ._mixins import BoundsWarningsMixin
from ._numbers import to_float
from ._numbers import to_int
from ._numbers import to_maximum_float
from ._numbers import to_maximum_int
from ._numbers import to_minimum_float
from ._numbers import to_minimum_int
from ._numbers import to_number
from ._numbers import to_positive_float
from ._numbers import to_positive_int
from ._parsing import ValueParser
from ._substitution import deep_substitute
from ._text import normalize_choice
from ._text import normalize_str

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ValueParser',
    # Enums
    'CoercibleStrEnum',
    # Functions (data utilities)
    'count_records',
    'print_json',
    'serialize_json',
    # Functions (mapping utilities)
    'cast_str_dict',
    'coerce_dict',
    'deep_substitute',
    'maybe_mapping',
    # Functions (float coercion)
    'to_float',
    'to_maximum_float',
    'to_minimum_float',
    'to_positive_float',
    # Functions (int coercion)
    'to_int',
    'to_maximum_int',
    'to_minimum_int',
    'to_positive_int',
    # Functions (generic number coercion)
    'to_number',
    # Functions (text processing)
    'normalize_choice',
    'normalize_str',
    # Mixins
    'BoundsWarningsMixin',
]
