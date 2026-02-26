"""
:mod:`tests.unit.utils.test_u_utils_mapping` module.

Unit tests for :mod:`etlplus.utils.mapping`.
"""

from __future__ import annotations

from etlplus.utils import cast_str_dict
from etlplus.utils import coerce_dict
from etlplus.utils import maybe_mapping

# SECTION: TESTS ============================================================ #


class TestMapping:
    """Unit tests for :mod:`etlplus.utils.mapping`."""

    def test_cast_and_coerce_dict_helpers(self) -> None:
        """Test mapping helpers that normalize dictionaries."""
        assert cast_str_dict(None) == {}
        assert cast_str_dict({'a': 1}) == {'a': '1'}
        assert coerce_dict({'k': 'v'}) == {'k': 'v'}
        assert not coerce_dict('not-mapping')

    def test_maybe_mapping(self) -> None:
        """Test mapping detection helper returns None for non-mappings."""
        mapping = {'x': 1}
        assert maybe_mapping(mapping) is mapping
        assert maybe_mapping(5) is None
