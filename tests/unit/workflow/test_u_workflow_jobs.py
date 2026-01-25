"""
:mod:`tests.unit.workflow.test_u_workflow_jobs` module.

Unit tests for :mod:`etlplus.workflow.jobs`.

Covers dataclass parsing, from_obj methods, and edge cases.
"""

from __future__ import annotations

import importlib
from typing import Protocol
from typing import TypeVar

import pytest

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


jobs = importlib.import_module('etlplus.workflow.jobs')


# SECTION: TESTS ============================================================ #

T = TypeVar('T', covariant=True)


class SupportsFromObj(Protocol[T]):
    """Protocol for dataclasses exposing a ``from_obj`` constructor."""

    @classmethod
    def from_obj(cls, obj: dict[str, object] | None) -> T | None: ...


@pytest.mark.parametrize(
    'ref_cls, obj, expected',
    [
        pytest.param(
            jobs.ExtractRef,
            {'source': 'my_source', 'options': {'foo': 1}},
            {'source': 'my_source', 'options': {'foo': 1}},
            id='extract-ref',
        ),
        pytest.param(
            jobs.LoadRef,
            {'target': 'my_target', 'overrides': {'foo': 2}},
            {'target': 'my_target', 'overrides': {'foo': 2}},
            id='load-ref',
        ),
        pytest.param(
            jobs.TransformRef,
            {'pipeline': 'my_pipeline'},
            {'pipeline': 'my_pipeline'},
            id='transform-ref',
        ),
        pytest.param(
            jobs.ValidationRef,
            {'ruleset': 'rs', 'severity': 'warn', 'phase': 'both'},
            {'ruleset': 'rs', 'severity': 'warn', 'phase': 'both'},
            id='validation-ref',
        ),
    ],
)
def test_ref_from_obj_valid(
    ref_cls: type[SupportsFromObj[object]],
    obj: dict[str, object],
    expected: dict[str, object],
) -> None:
    """Test valid dict input yields the expected reference object."""
    ref = ref_cls.from_obj(obj)
    assert ref is not None
    for field, value in expected.items():
        assert getattr(ref, field) == value


@pytest.mark.parametrize(
    'ref_cls, obj',
    [
        pytest.param(jobs.ExtractRef, None, id='extract-none'),
        pytest.param(jobs.ExtractRef, {'source': 123}, id='extract-bad'),
        pytest.param(jobs.LoadRef, {'target': 123}, id='load-bad'),
        pytest.param(jobs.TransformRef, {'pipeline': 123}, id='transform-bad'),
        pytest.param(jobs.ValidationRef, None, id='validation-none'),
        pytest.param(
            jobs.ValidationRef,
            {'ruleset': 123},
            id='validation-bad',
        ),
    ],
)
def test_ref_from_obj_invalid(
    ref_cls: type[SupportsFromObj[object]],
    obj: dict[str, object] | None,
) -> None:
    """Test invalid dict input yields None for reference objects."""
    assert ref_cls.from_obj(obj) is None


def test_jobconfig_from_obj_valid() -> None:
    """Test valid dict input yields expected :class:`JobConfig` instance."""
    obj = {
        'name': 'job1',
        'description': 'desc',
        'extract': {'source': 'src'},
        'validate': {'ruleset': 'rs'},
        'transform': {'pipeline': 'p'},
        'load': {'target': 't'},
    }
    cfg = jobs.JobConfig.from_obj(obj)
    assert cfg is not None
    assert cfg.name == 'job1'
    assert cfg.description == 'desc'
    assert cfg.extract is not None
    assert cfg.validate is not None
    assert cfg.transform is not None
    assert cfg.load is not None


@pytest.mark.parametrize(
    'obj',
    [
        pytest.param(None, id='none'),
        pytest.param({'name': 123}, id='bad-name'),
    ],
)
def test_jobconfig_from_obj_invalid(
    obj: dict[str, object] | None,
) -> None:
    """Test invalid dict input yields None for :class:`JobConfig`."""
    assert jobs.JobConfig.from_obj(obj) is None


def test_jobconfig_description_coercion() -> None:
    """
    Test that :class:`JobConfig` coerces description to a string.
    """
    cfg = jobs.JobConfig.from_obj({'name': 'x', 'description': 5})
    assert cfg is not None
    assert cfg.name == 'x'
    assert cfg.description == '5'
