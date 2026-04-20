"""
:mod:`tests.unit.utils.test_u_utils_init` module.

Unit tests for :mod:`etlplus.utils` package facade exports.
"""

from __future__ import annotations

import pytest

from etlplus import utils as utils_pkg
from etlplus.utils._data import count_records
from etlplus.utils._data import parse_json
from etlplus.utils._data import print_json
from etlplus.utils._data import serialize_json
from etlplus.utils._enums import CoercibleStrEnum
from etlplus.utils._mapping import cast_str_dict
from etlplus.utils._mapping import coerce_dict
from etlplus.utils._mapping import maybe_mapping
from etlplus.utils._mixins import BoundsWarningsMixin
from etlplus.utils._numbers import FloatParser
from etlplus.utils._numbers import IntParser
from etlplus.utils._parsing import MappingFieldParser
from etlplus.utils._parsing import SequenceParser
from etlplus.utils._parsing import ValueParser
from etlplus.utils._substitution import deep_substitute
from etlplus.utils._text import normalize_choice
from etlplus.utils._text import normalize_str

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


UTILS_EXPORTS = [
    ('MappingFieldParser', MappingFieldParser),
    ('SequenceParser', SequenceParser),
    ('ValueParser', ValueParser),
    ('FloatParser', FloatParser),
    ('IntParser', IntParser),
    ('CoercibleStrEnum', CoercibleStrEnum),
    ('count_records', count_records),
    ('parse_json', parse_json),
    ('print_json', print_json),
    ('serialize_json', serialize_json),
    ('cast_str_dict', cast_str_dict),
    ('coerce_dict', coerce_dict),
    ('deep_substitute', deep_substitute),
    ('maybe_mapping', maybe_mapping),
    ('normalize_choice', normalize_choice),
    ('normalize_str', normalize_str),
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
