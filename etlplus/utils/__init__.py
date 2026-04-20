"""
:mod:`etlplus.utils` package.

Small shared helpers used across modules.
"""

from __future__ import annotations

from ._data import count_records
from ._data import parse_json
from ._data import print_json
from ._data import serialize_json
from ._enums import CoercibleStrEnum
from ._mapping import cast_str_dict
from ._mapping import coerce_dict
from ._mapping import maybe_mapping
from ._mixins import BoundsWarningsMixin
from ._numbers import FloatParser
from ._numbers import IntParser
from ._parsing import MappingFieldParser
from ._parsing import SequenceParser
from ._parsing import ValueParser
from ._substitution import deep_substitute
from ._text import normalize_choice
from ._text import normalize_str

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MappingFieldParser',
    'SequenceParser',
    'ValueParser',
    'FloatParser',
    'IntParser',
    # Enums
    'CoercibleStrEnum',
    # Functions (data utilities)
    'count_records',
    'parse_json',
    'print_json',
    'serialize_json',
    # Functions (mapping utilities)
    'cast_str_dict',
    'coerce_dict',
    'deep_substitute',
    'maybe_mapping',
    # Functions (text processing)
    'normalize_choice',
    'normalize_str',
    # Mixins
    'BoundsWarningsMixin',
]
