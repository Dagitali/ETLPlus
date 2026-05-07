"""
:mod:`etlplus.workflow._dag` module.

Lightweight directed acyclic graph (DAG) helpers for ordering jobs based on
:attr:`depends_on`.
"""

from __future__ import annotations

from ..utils import MappingParser
from ..utils import topological_sort_names
from ._errors import DagError
from ._jobs import JobConfig

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'topological_sort_jobs',
]


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
        If dependency validation fails or a dependency cycle is detected.

    Notes
    -----
    - Propagates :class:`DagError` when dependency validation fails or a cycle
        is detected.
    """
    try:
        jobs_by_name = MappingParser.index_named_items(jobs, item_label='job')
        dependencies_by_name = {
            job.name: tuple(dep for dep in job.depends_on if isinstance(dep, str))
            for job in jobs
        }
        ordered_names = topological_sort_names(dependencies_by_name)
    except ValueError as exc:
        message = str(exc).replace(' for node ', ' in job ')
        message = message.replace('Node "', 'Job "')
        raise DagError(message=message) from exc
    return [jobs_by_name[name] for name in ordered_names]
