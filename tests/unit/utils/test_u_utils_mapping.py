"""
:mod:`tests.unit.utils.test_u_utils_mapping` module.

Unit tests for :mod:`etlplus.utils._mapping`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import cast

import pytest

from etlplus.utils import cast_str_dict
from etlplus.utils import coerce_dict
from etlplus.utils import maybe_mapping

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestMappingHelpers:
    """Unit tests for mapping coercion helpers."""

    @pytest.mark.parametrize(
        ('mapping', 'expected'),
        [
            pytest.param(None, {}, id='none'),
            pytest.param({}, {}, id='empty-dict'),
            pytest.param({'a': 1}, {'a': '1'}, id='coerce-values-to-str'),
            pytest.param(
                cast(Any, {1: 2}),
                {'1': '2'},
                id='coerce-keys-and-values',
            ),
        ],
    )
    def test_cast_str_dict(
        self,
        mapping: Mapping[str, Any] | None,
        expected: dict[str, str],
    ) -> None:
        """
        Test that :func:`cast_str_dict` coerces keys and values to strings.
        """
        assert cast_str_dict(mapping) == expected

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param({'k': 'v'}, {'k': 'v'}, id='dict'),
            pytest.param({}, {}, id='empty-dict'),
            pytest.param('not-mapping', {}, id='string-is-not-mapping'),
            pytest.param([('k', 'v')], {}, id='list-of-pairs-is-not-mapping'),
        ],
    )
    def test_coerce_dict(
        self,
        value: object,
        expected: dict[str, Any],
    ) -> None:
        """
        Test that :func:`coerce_dict` copies mappings and rejects non-mappings.
        """
        assert coerce_dict(value) == expected

    def test_maybe_mapping_returns_same_object_for_mappings(self) -> None:
        """
        Test that :func:`maybe_mapping` preserves mapping identity when
        possible.
        """
        mapping: Mapping[str, int] = {'x': 1}
        assert maybe_mapping(mapping) is mapping

    def test_maybe_mapping_returns_none_for_non_mappings(
        self,
        non_mapping_value: object,
    ) -> None:
        """
        Test that :func:`maybe_mapping` returns ``None`` for non-mapping
        values.
        """
        assert maybe_mapping(non_mapping_value) is None
