"""
:mod:`etlplus.workflow._dag` module.

Lightweight directed acyclic graph (DAG) helpers for ordering jobs based on
:attr:`depends_on`.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from dataclasses import field

from ._errors import DagError
from ._jobs import JobConfig

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'topological_sort_jobs',
]


# SECTION: INTERNAL CLASSES ================================================= #


@dataclass(slots=True)
class _DagTopology:
    """Internal dependency graph state for one topological sort operation."""

    # -- Instance Attributes -- #

    jobs_by_name: dict[str, JobConfig]
    edges: dict[str, set[str]] = field(default_factory=dict)
    indegree: dict[str, int] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    def from_jobs(
        cls,
        jobs: list[JobConfig],
    ) -> _DagTopology:
        """Build validated DAG state from parsed workflow jobs."""
        jobs_by_name = {job.name: job for job in jobs}
        topology = cls(
            jobs_by_name=jobs_by_name,
            edges={name: set() for name in jobs_by_name},
            indegree={name: 0 for name in jobs_by_name},
        )

        for job in jobs:
            for dep in job.depends_on:
                topology._add_dependency(job, dep)
        return topology

    # -- Internal Instance Methods -- #

    def _add_dependency(
        self,
        job: JobConfig,
        dependency_name: str,
    ) -> None:
        """Validate and register one job dependency edge."""
        if dependency_name not in self.jobs_by_name:
            raise DagError(
                message=(f'Unknown dependency "{dependency_name}" in job "{job.name}"'),
            )
        if dependency_name == job.name:
            # raise DagError(f'Job "{job.name}" depends on itself')
            raise DagError(message=f'Job "{job.name}" depends on itself')
        if job.name in self.edges[dependency_name]:
            return
        self.edges[dependency_name].add(job.name)
        self.indegree[job.name] += 1

    # -- Instance Methods -- #

    def ordered_names(
        self,
    ) -> list[str]:
        """
        Return job names in topological order.

        Returns
        -------
        list[str]
            Job names in topological order.

        Raises
        ------
        DagError
            If a dependency cycle is detected.
        """
        remaining_indegree = dict(self.indegree)
        queue = deque(_ready(remaining_indegree))
        ordered: list[str] = []

        while queue:
            name = queue.popleft()
            ordered.append(name)
            for child in sorted(self.edges[name]):
                remaining_indegree[child] -= 1
                if remaining_indegree[child] == 0:
                    queue.append(child)

        if len(ordered) != len(self.jobs_by_name):
            # raise DagError('Dependency cycle detected')
            raise DagError(message='Dependency cycle detected')
        return ordered


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

    Notes
    -----
    - Propagates :class:`DagError` when dependency validation fails or a cycle
      is detected.
    """
    topology = _DagTopology.from_jobs(jobs)
    ordered_names = topology.ordered_names()
    return [topology.jobs_by_name[name] for name in ordered_names]
