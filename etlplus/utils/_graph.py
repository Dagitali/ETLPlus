"""
:mod:`etlplus.utils._graph` module.

Generic graph-ordering helpers.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'topological_sort_names',
    'zero_indegree_names',
]


# SECTION: FUNCTIONS ======================================================== #


def zero_indegree_names(
    indegree: Mapping[str, int],
) -> list[str]:
    """
    Return the sorted node names whose indegree is zero.

    Parameters
    ----------
    indegree : Mapping[str, int]
        Mapping of node name to indegree.

    Returns
    -------
    list[str]
        Sorted ready-to-process node names.
    """
    return sorted(name for name, degree in indegree.items() if degree == 0)


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

    Raises
    ------
    ValueError
        If the graph references unknown dependencies, contains a self-edge, or
        contains a dependency cycle.
    """
    names: dict[str, Any | None] = dict.fromkeys(dependencies_by_name)
    edges: dict[str, set[str]] = {name: set() for name in names}
    indegree: dict[str, int] = {name: 0 for name in names}

    for name, dependencies in dependencies_by_name.items():
        for dependency_name in dict.fromkeys(dependencies):
            if dependency_name not in names:
                raise ValueError(
                    f'Unknown dependency "{dependency_name}" for node "{name}"',
                )
            if dependency_name == name:
                raise ValueError(f'Node "{name}" depends on itself')
            if name in edges[dependency_name]:
                continue
            edges[dependency_name].add(name)
            indegree[name] += 1

    remaining_indegree = dict(indegree)
    queue = deque(zero_indegree_names(remaining_indegree))
    ordered: list[str] = []

    while queue:
        name = queue.popleft()
        ordered.append(name)
        for child in sorted(edges[name]):
            remaining_indegree[child] -= 1
            if remaining_indegree[child] == 0:
                queue.append(child)

    if len(ordered) != len(names):
        raise ValueError('Dependency cycle detected')
    return ordered
