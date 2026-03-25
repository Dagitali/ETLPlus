"""
:mod:`tests.unit.utils.test_u_utils_init` module.

Unit tests for :mod:`etlplus.utils.__init__`.
"""

from __future__ import annotations

from etlplus import utils as mod
from etlplus.utils.data import count_records
from etlplus.utils.data import print_json
from etlplus.utils.data import serialize_json
from etlplus.utils.mapping import cast_str_dict
from etlplus.utils.mapping import coerce_dict
from etlplus.utils.mapping import maybe_mapping
from etlplus.utils.numbers import to_float
from etlplus.utils.numbers import to_int
from etlplus.utils.numbers import to_maximum_float
from etlplus.utils.numbers import to_maximum_int
from etlplus.utils.numbers import to_minimum_float
from etlplus.utils.numbers import to_minimum_int
from etlplus.utils.numbers import to_number
from etlplus.utils.numbers import to_positive_float
from etlplus.utils.numbers import to_positive_int
from etlplus.utils.substitution import deep_substitute
from etlplus.utils.text import normalize_choice
from etlplus.utils.text import normalize_str

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestUtilsPackageExports:
    """Unit tests for public package exports."""

    def test_all_exports_are_expected_and_importable(self) -> None:
        """Test ``__all__`` and top-level package symbol wiring."""
        assert mod.__all__ == [
            'count_records',
            'print_json',
            'serialize_json',
            'cast_str_dict',
            'coerce_dict',
            'deep_substitute',
            'maybe_mapping',
            'to_float',
            'to_maximum_float',
            'to_minimum_float',
            'to_positive_float',
            'to_int',
            'to_maximum_int',
            'to_minimum_int',
            'to_positive_int',
            'to_number',
            'normalize_choice',
            'normalize_str',
        ]
        assert mod.count_records is count_records
        assert mod.print_json is print_json
        assert mod.serialize_json is serialize_json
        assert mod.cast_str_dict is cast_str_dict
        assert mod.coerce_dict is coerce_dict
        assert mod.deep_substitute is deep_substitute
        assert mod.maybe_mapping is maybe_mapping
        assert mod.to_float is to_float
        assert mod.to_maximum_float is to_maximum_float
        assert mod.to_minimum_float is to_minimum_float
        assert mod.to_positive_float is to_positive_float
        assert mod.to_int is to_int
        assert mod.to_maximum_int is to_maximum_int
        assert mod.to_minimum_int is to_minimum_int
        assert mod.to_positive_int is to_positive_int
        assert mod.to_number is to_number
        assert mod.normalize_choice is normalize_choice
        assert mod.normalize_str is normalize_str
