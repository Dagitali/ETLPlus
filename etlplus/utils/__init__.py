"""
:mod:`etlplus.utils` package.

Small shared helpers used across modules.
"""

from __future__ import annotations

from .data import count_records
from .data import print_json
from .data import serialize_json
from .mapping import cast_str_dict
from .mapping import coerce_dict
from .mapping import maybe_mapping
from .numbers import to_float
from .numbers import to_int
from .numbers import to_maximum_float
from .numbers import to_maximum_int
from .numbers import to_minimum_float
from .numbers import to_minimum_int
from .numbers import to_number
from .numbers import to_positive_float
from .numbers import to_positive_int
from .substitution import deep_substitute
from .text import normalize_choice
from .text import normalize_str

# SECTION: EXPORTS ========================================================== #


__all__ = [
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
]
