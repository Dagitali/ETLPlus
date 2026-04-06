"""
:mod:`tests.unit.workflow.test_u_workflow_dag` module.

Unit tests for :mod:`etlplus.workflow._dag`.
"""

from __future__ import annotations

import pytest

from etlplus.workflow._dag import DagError
from etlplus.workflow._dag import _ready
from etlplus.workflow._dag import topological_sort_jobs
from etlplus.workflow._jobs import JobConfig

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _job(name: str, *depends_on: str) -> JobConfig:
    """Build a minimal `JobConfig` for DAG ordering tests."""
    return JobConfig(name=name, depends_on=list(depends_on))


def _ordered_names(jobs: list[JobConfig]) -> list[str]:
    """Return job names after topological sorting."""
    return [job.name for job in topological_sort_jobs(jobs)]


# SECTION: TESTS ============================================================ #


class TestDagHelpers:
    """Unit tests for DAG helpers and sorting behavior."""

    def test_dag_error_string_representation(self) -> None:
        """
        Test that :class:`DagError` string conversion returns the original
        message.
        """
        assert str(DagError('boom')) == 'boom'

    def test_ready_returns_sorted_zero_indegree_nodes(self) -> None:
        """Test that :func:`_ready` sorts node names with zero indegree."""
        assert _ready({'b': 0, 'a': 0, 'c': 1}) == ['a', 'b']

    @pytest.mark.parametrize(
        ('jobs', 'expected'),
        [
            pytest.param(
                [_job('a'), _job('b', 'a', 'a')], ['a', 'b'], id='duplicate-edges',
            ),
            pytest.param(
                [_job('b', 'a'), _job('c', 'b'), _job('a')],
                ['a', 'b', 'c'],
                id='dependency-chain',
            ),
            pytest.param(
                [_job('a'), _job('b'), _job('c', 'a', 'b')],
                ['a', 'b', 'c'],
                id='multi-parent',
            ),
        ],
    )
    def test_topological_sort_orders_jobs_by_dependencies(
        self,
        jobs: list[JobConfig],
        expected: list[str],
    ) -> None:
        """
        Test that :func:`topological_sort_jobs` honors declared dependency
        ordering.
        """
        assert _ordered_names(jobs) == expected

    @pytest.mark.parametrize(
        ('jobs', 'match'),
        [
            pytest.param(
                [_job('a', 'b'), _job('b', 'a')],
                'Dependency cycle detected',
                id='cycle',
            ),
            pytest.param([_job('a', 'a')], 'depends on itself', id='self-dependency'),
            pytest.param(
                [_job('a'), _job('b', 'missing')],
                'Unknown dependency',
                id='unknown-dependency',
            ),
        ],
    )
    def test_topological_sort_rejects_invalid_graphs(
        self,
        jobs: list[JobConfig],
        match: str,
    ) -> None:
        """
        Test that :func:`topological_sort_jobs` raises `DagError` for invalid
        dependency graphs.
        """
        with pytest.raises(DagError, match=match):
            topological_sort_jobs(jobs)
