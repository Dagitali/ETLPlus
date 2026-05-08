"""
:mod:`tests.unit.utils.test_u_utils_graph` module.

Unit tests for :mod:`etlplus.utils._graph`.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from etlplus.utils import NamedDependencyGraph
from etlplus.utils import topological_sort_named_items
from etlplus.utils import topological_sort_names

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


# SECTION: HELPERS ========================================================== #


@dataclass(slots=True)
class _Node:
    """Minimal named node used by object-level graph helper tests."""

    name: str
    depends_on: tuple[str, ...] = ()


# SECTION: TESTS ============================================================ #


class TestGraphHelpers:
    """Unit tests for generic graph-ordering helpers."""

    def test_named_dependency_graph_zero_indegree_names_returns_sorted_nodes(
        self,
    ) -> None:
        """Test that the class-level zero-indegree helper remains stable."""
        assert NamedDependencyGraph.zero_indegree_names({'b': 0, 'a': 0, 'c': 1}) == [
            'a',
            'b',
        ]

    @pytest.mark.parametrize(
        ('dependencies_by_name', 'items', 'match'),
        [
            pytest.param(
                {'a': ('b',), 'b': ('a',)},
                [
                    _Node(name='a', depends_on=('b',)),
                    _Node(name='b', depends_on=('a',)),
                ],
                'Dependency cycle detected',
                id='cycle',
            ),
            pytest.param(
                {'a': ('a',)},
                [_Node(name='a', depends_on=('a',))],
                'depends on itself',
                id='self-dependency',
            ),
            pytest.param(
                {'a': (), 'b': ('missing',)},
                [_Node(name='a'), _Node(name='b', depends_on=('missing',))],
                'Unknown dependency',
                id='unknown-dependency',
            ),
        ],
    )
    def test_topological_sort_helpers_reject_invalid_graphs(
        self,
        dependencies_by_name: dict[str, tuple[str, ...]],
        items: list[_Node],
        match: str,
    ) -> None:
        """Test that invalid generic graphs raise consistent value errors."""
        with pytest.raises(ValueError, match=match):
            topological_sort_names(dependencies_by_name)
        with pytest.raises(ValueError, match=match):
            topological_sort_named_items(
                items,
                dependency_getter=lambda item: item.depends_on,
                item_label='node',
            )

    def test_topological_sort_named_items_orders_objects_by_dependencies(
        self,
    ) -> None:
        """Test that object-level topological sorting honors dependencies."""
        items = [
            _Node(name='publish', depends_on=('transform',)),
            _Node(name='extract'),
            _Node(name='transform', depends_on=('extract', 'extract')),
        ]

        ordered = topological_sort_named_items(
            items,
            dependency_getter=lambda item: item.depends_on,
            item_label='node',
        )

        assert [item.name for item in ordered] == ['extract', 'transform', 'publish']

    @pytest.mark.parametrize(
        ('dependencies_by_name', 'expected'),
        [
            pytest.param(
                {'a': (), 'b': ('a', 'a')},
                ['a', 'b'],
                id='duplicate-edges',
            ),
            pytest.param(
                {'b': ('a',), 'c': ('b',), 'a': ()},
                ['a', 'b', 'c'],
                id='dependency-chain',
            ),
            pytest.param(
                {'a': (), 'b': (), 'c': ('a', 'b')},
                ['a', 'b', 'c'],
                id='multi-parent',
            ),
        ],
    )
    def test_topological_sort_names_orders_nodes_by_dependencies(
        self,
        dependencies_by_name: dict[str, tuple[str, ...]],
        expected: list[str],
    ) -> None:
        """Test that generic topological ordering honors dependencies."""
        assert topological_sort_names(dependencies_by_name) == expected
        assert (
            NamedDependencyGraph.from_dependencies(dependencies_by_name).ordered_names()
            == expected
        )

    def test_topological_sort_named_items_rejects_duplicate_names(
        self,
    ) -> None:
        """Test that object-level sorting rejects duplicate item names."""
        items = [_Node(name='a'), _Node(name='a')]

        with pytest.raises(ValueError, match='Duplicate node name'):
            topological_sort_named_items(
                items,
                dependency_getter=lambda item: item.depends_on,
                item_label='node',
            )
