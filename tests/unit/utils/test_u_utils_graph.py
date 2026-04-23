"""
:mod:`tests.unit.utils.test_u_utils_graph` module.

Unit tests for :mod:`etlplus.utils._graph`.
"""

from __future__ import annotations

import pytest

from etlplus.utils import NamedDependencyGraph
from etlplus.utils import topological_sort_names

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


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

    @pytest.mark.parametrize(
        ('dependencies_by_name', 'match'),
        [
            pytest.param(
                {'a': ('b',), 'b': ('a',)},
                'Dependency cycle detected',
                id='cycle',
            ),
            pytest.param(
                {'a': ('a',)},
                'depends on itself',
                id='self-dependency',
            ),
            pytest.param(
                {'a': (), 'b': ('missing',)},
                'Unknown dependency',
                id='unknown-dependency',
            ),
        ],
    )
    def test_topological_sort_names_rejects_invalid_graphs(
        self,
        dependencies_by_name: dict[str, tuple[str, ...]],
        match: str,
    ) -> None:
        """Test that invalid generic graphs raise consistent value errors."""
        with pytest.raises(ValueError, match=match):
            topological_sort_names(dependencies_by_name)
