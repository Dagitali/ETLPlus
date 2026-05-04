"""
:mod:`etlplus.utils._graph` module.

Generic graph-ordering helpers.
"""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from heapq import heapify
from heapq import heappop
from heapq import heappush
from typing import Self

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'NamedDependencyGraph',
    # Functions
    'topological_sort_names',
]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class NamedDependencyGraph:
    """Dependency graph state for name-based topological sorting."""

    # -- Instance Attributes -- #

    names: tuple[str, ...]
    edges: dict[str, set[str]]
    indegree: dict[str, int]

    # -- Class Methods -- #

    @classmethod
    def from_dependencies(
        cls,
        dependencies_by_name: Mapping[str, Iterable[str]],
    ) -> Self:
        """
        Build one validated graph from node dependency mappings.

        Parameters
        ----------
        dependencies_by_name : Mapping[str, Iterable[str]]
            Mapping of node name to the names it depends on.

        Returns
        -------
        Self
            Validated graph instance.
        """
        names = tuple(dict.fromkeys(dependencies_by_name))
        graph = cls(
            names=names,
            edges={name: set() for name in names},
            indegree={name: 0 for name in names},
        )

        known_names = frozenset(names)
        for name, dependencies in dependencies_by_name.items():
            for dependency_name in dict.fromkeys(dependencies):
                graph._add_dependency(
                    name,
                    dependency_name,
                    known_names=known_names,
                )
        return graph

    # -- Static Methods -- #

    @staticmethod
    def zero_indegree_names(
        indegree: Mapping[str, int],
    ) -> list[str]:
        """
        Return the sorted node names whose indegree is zero.

        Parameters
        ----------
        indegree : Mapping[str, int]
            Mapping of node name to its indegree.

        Returns
        -------
        list[str]
            Sorted node names with zero indegree.
        """
        return sorted(name for name, degree in indegree.items() if degree == 0)

    # -- Internal Instance Methods -- #

    def _add_dependency(
        self,
        name: str,
        dependency_name: str,
        *,
        known_names: frozenset[str],
    ) -> None:
        """Validate and register one dependency edge."""
        if dependency_name not in known_names:
            raise ValueError(
                f'Unknown dependency "{dependency_name}" for node "{name}"',
            )
        if dependency_name == name:
            raise ValueError(f'Node "{name}" depends on itself')
        self.edges[dependency_name].add(name)
        self.indegree[name] += 1

    # -- Instance Methods -- #

    def ordered_names(
        self,
    ) -> list[str]:
        """
        Return node names in dependency-respecting order.

        Returns
        -------
        list[str]
            Node names in dependency-respecting order.

        Raises
        ------
        ValueError
            If a dependency cycle is detected.
        """
        remaining_indegree = dict(self.indegree)
        queue = self.zero_indegree_names(remaining_indegree)
        heapify(queue)
        ordered: list[str] = []

        while queue:
            name = heappop(queue)
            ordered.append(name)
            for child in sorted(self.edges[name]):
                remaining_indegree[child] -= 1
                if remaining_indegree[child] == 0:
                    heappush(queue, child)

        if len(ordered) != len(self.names):
            raise ValueError('Dependency cycle detected')
        return ordered


# SECTION: FUNCTIONS ======================================================== #


def topological_sort_names(
    dependencies_by_name: Mapping[str, Iterable[str]],
) -> list[str]:
    """
    Return node names in topological order.

    Parameters
    ----------
    dependencies_by_name : Mapping[str, Iterable[str]]
        Mapping of node name to the names it depends on.

    Returns
    -------
    list[str]
        Node names in dependency-respecting order.
    """
    return NamedDependencyGraph.from_dependencies(
        dependencies_by_name,
    ).ordered_names()
