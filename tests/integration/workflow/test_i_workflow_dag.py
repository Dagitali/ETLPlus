"""
:mod:`tests.integration.workflow.test_i_workflow_dag` module.

Integration smoke tests for :mod:`etlplus.workflow.dag`.
"""

from __future__ import annotations

import pytest

from etlplus.workflow import JobConfig
from etlplus.workflow import topological_sort_jobs

# SECTION: MARKERS ========================================================== #


pytestmark = pytest.mark.integration


# SECTION: TESTS ============================================================ #


def test_topological_sort_jobs_from_jobconfig_payloads() -> None:
    """Topological sorting should honor dependencies from parsed job blocks."""
    raw_jobs = [
        {
            'name': 'publish',
            'depends_on': ['transform'],
        },
        {
            'name': 'extract',
        },
        {
            'name': 'transform',
            'depends_on': ['extract'],
        },
    ]

    jobs = [JobConfig.from_obj(obj) for obj in raw_jobs]
    assert all(job is not None for job in jobs)
    ordered = topological_sort_jobs([job for job in jobs if job is not None])
    assert [job.name for job in ordered] == ['extract', 'transform', 'publish']
