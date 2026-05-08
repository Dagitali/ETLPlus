"""
:mod:`etlplus.workflow._dag` module.

Lightweight directed acyclic graph (DAG) helpers for ordering jobs based on
:attr:`depends_on`.
"""

from __future__ import annotations

from ..utils import topological_sort_named_items
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
        return topological_sort_named_items(
            jobs,
            dependency_getter=lambda job: job.depends_on,
            item_label='job',
        )
    except ValueError as exc:
        message = str(exc).replace(' for node ', ' in job ')
        message = message.replace('Node "', 'Job "')
        raise DagError(message=message) from exc
