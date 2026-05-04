"""
:mod:`tests.unit.utils.test_u_utils_mapping` module.

Unit tests for :mod:`etlplus.utils._mapping`.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from etlplus.utils import MappingParser

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
        Test that :meth:`MappingParser.to_str_dict` coerces keys and values to
        strings.
        """
        assert MappingParser.to_str_dict(mapping) == expected

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
        Test that :meth:`MappingParser.to_dict` copies mappings and rejects
        non-mappings.
        """
        assert MappingParser.to_dict(value) == expected

    def test_index_named_items_normalizes_names(self) -> None:
        """Test that usable names are stripped before indexing."""
        item = SimpleNamespace(name=' valid ')

        assert MappingParser.index_named_items([item], item_label='job') == {
            'valid': item,
        }

    def test_index_named_items_rejects_duplicates(self) -> None:
        """Test that duplicate named items raise a descriptive error."""
        items = [SimpleNamespace(name='dup'), SimpleNamespace(name='dup')]

        with pytest.raises(ValueError, match='Duplicate source connector name'):
            MappingParser.index_named_items(items, item_label='source connector')

    def test_index_named_items_rejects_duplicates_after_stripping(self) -> None:
        """Test duplicate detection after name normalization."""
        items = [
            SimpleNamespace(name='dup'),
            SimpleNamespace(name=' dup '),
        ]

        with pytest.raises(ValueError, match='Duplicate job name: dup'):
            MappingParser.index_named_items(items, item_label='job')

    @pytest.mark.parametrize(
        'item',
        [
            pytest.param(SimpleNamespace(name=''), id='empty-string'),
            pytest.param(SimpleNamespace(name='   '), id='blank-string'),
            pytest.param(SimpleNamespace(), id='missing-name'),
            pytest.param(SimpleNamespace(name=123), id='non-string-name'),
        ],
    )
    def test_index_named_items_skips_unusable_names(self, item: object) -> None:
        """Test that objects without usable names are not indexed."""
        assert MappingParser.index_named_items([item], item_label='job') == {}

    def test_merge_to_dict_excludes_reserved_keys(self) -> None:
        """Test that later mappings win and excluded keys are removed."""
        merged = MappingParser.merge_to_dict(
            'not-a-mapping',
            {'encoding': 'utf-8', 'path': '/tmp/a.json'},
            {'delimiter': ';', 'path': '/tmp/b.json', 'format': 'csv'},
            excluded_keys=frozenset({'path', 'format'}),
        )

        assert merged == {
            'delimiter': ';',
            'encoding': 'utf-8',
        }

    def test_maybe_mapping_returns_same_object_for_mappings(self) -> None:
        """
        Test that :meth:`MappingParser.optional` preserves mapping identity when
        possible.
        """
        mapping: Mapping[str, int] = {'x': 1}
        assert MappingParser.optional(mapping) is mapping

    def test_maybe_mapping_returns_none_for_non_mappings(
        self,
        non_mapping_value: object,
    ) -> None:
        """
        Test that :meth:`MappingParser.optional` returns ``None`` for non-mapping
        values.
        """
        assert MappingParser.optional(non_mapping_value) is None
