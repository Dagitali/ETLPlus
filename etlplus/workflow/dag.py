"""
:mod:`etlplus.workflow.dag` module.

Lightweight directed acyclic graph (DAG) helpers for ordering jobs based on
:attr:`depends_on`.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from .jobs import JobConfig

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Errors
    'DagError',
    # Functions
    'topological_sort_jobs',
]


# SECTION: ERRORS =========================================================== #


@dataclass(slots=True)
class DagError(ValueError):
    """
    Raised when the job dependency graph is invalid.

    Attributes
    ----------
    message : str
        Error message.
    """

    # -- Attributes -- #

    message: str

    # -- Magic Methods (Object Representation) -- #

    def __str__(self) -> str:
        return self.message


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _ready(
    indegree: dict[str, int],
) -> list[str]:
    """
    Return a sorted list of nodes with zero indegree.

    Parameters
    ----------
    indegree : dict[str, int]
        Mapping of node name to indegree.

    Returns
    -------
    list[str]
        Sorted list of node names ready to process.
    """
    return sorted(name for name, deg in indegree.items() if deg == 0)


# SECTION: FUNCTIONS ======================================================== #


def topological_sort_jobs(
    jobs: list[JobConfig],
) -> list[JobConfig]:
    """
    Return jobs in topological order based on :attr:`depends_on`.

    Parameters
    ----------
    jobs : list[JobConfig]
        List of job configurations to sort.

    Returns
    -------
    list[JobConfig]
        Jobs sorted in topological order.

    Raises
    ------
    DagError
        If a dependency is missing, self-referential, or when a cycle is
        detected.
    """
    index = {job.name: job for job in jobs}
    edges: dict[str, set[str]] = {name: set() for name in index}
    indegree: dict[str, int] = {name: 0 for name in index}

    for job in jobs:
        for dep in job.depends_on:
            if dep not in index:
                raise DagError(
                    f'Unknown dependency "{dep}" in job "{job.name}"',
                )
            if dep == job.name:
                raise DagError(f'Job "{job.name}" depends on itself')
            if job.name not in edges[dep]:
                edges[dep].add(job.name)
                indegree[job.name] += 1

    queue = deque(_ready(indegree))
    ordered: list[str] = []

    while queue:
        name = queue.popleft()
        ordered.append(name)
        for child in sorted(edges[name]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(ordered) != len(jobs):
        raise DagError('Dependency cycle detected')

    return [index[name] for name in ordered]
