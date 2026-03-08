"""
:mod:`tests.unit.workflow.test_u_workflow_dag` module.

Unit tests for :mod:`etlplus.workflow.dag`.
"""

from __future__ import annotations

import pytest

from etlplus.workflow.dag import DagError
from etlplus.workflow.dag import _ready
from etlplus.workflow.dag import topological_sort_jobs
from etlplus.workflow.jobs import JobConfig

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _job(name: str, *depends_on: str) -> JobConfig:
    return JobConfig(name=name, depends_on=list(depends_on))


# SECTION: TESTS ============================================================ #


def test_dag_error_string_representation() -> None:
    """
    Test that :class:`DagError` string conversion returns its message.
    """
    assert str(DagError('boom')) == 'boom'


def test_ready_returns_sorted_zero_indegree_nodes() -> None:
    """Test that :func:`_ready` sorts node names with zero indegree."""
    assert _ready({'b': 0, 'a': 0, 'c': 1}) == ['a', 'b']


def test_topological_sort_dedupes_duplicate_dependency_edges() -> None:
    """Test that duplicate dependency entries do not double-count indegrees."""
    jobs = [
        _job('a'),
        _job('b', 'a', 'a'),
    ]
    ordered = topological_sort_jobs(jobs)
    assert [job.name for job in ordered] == ['a', 'b']


def test_topological_sort_detects_cycles() -> None:
    """Test that cyclic dependencies raise :class:`DagError`."""
    jobs = [_job('a', 'b'), _job('b', 'a')]
    with pytest.raises(DagError, match='Dependency cycle detected'):
        topological_sort_jobs(jobs)


def test_topological_sort_handles_multi_parent_dependencies() -> None:
    """
    Test that nodes with multiple parents enqueue after all are resolved.
    """
    jobs = [
        _job('a'),
        _job('b'),
        _job('c', 'a', 'b'),
    ]
    ordered = topological_sort_jobs(jobs)
    assert ordered[-1].name == 'c'
    assert {ordered[0].name, ordered[1].name} == {'a', 'b'}


def test_topological_sort_orders_jobs_by_dependencies() -> None:
    """Test that sorting places dependencies before their dependents."""
    jobs = [
        _job('b', 'a'),
        _job('c', 'b'),
        _job('a'),
    ]
    ordered = topological_sort_jobs(jobs)
    assert [job.name for job in ordered] == ['a', 'b', 'c']


def test_topological_sort_rejects_self_dependencies() -> None:
    """Test that self-referential dependencies raise :class:`DagError`."""
    with pytest.raises(DagError, match='depends on itself'):
        topological_sort_jobs([_job('a', 'a')])


def test_topological_sort_rejects_unknown_dependencies() -> None:
    """Test that unknown dependency names raise :class:`DagError`."""
    jobs = [_job('a'), _job('b', 'missing')]
    with pytest.raises(DagError, match='Unknown dependency'):
        topological_sort_jobs(jobs)
