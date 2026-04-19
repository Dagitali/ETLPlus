"""
:mod:`tests.unit.workflow.test_u_workflow_jobs` module.

Unit tests for :mod:`etlplus.workflow._jobs`.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from etlplus.workflow._jobs import ExtractRef
from etlplus.workflow._jobs import JobConfig
from etlplus.workflow._jobs import JobRetryConfig
from etlplus.workflow._jobs import LoadRef
from etlplus.workflow._jobs import TransformRef
from etlplus.workflow._jobs import ValidationRef

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


# SECTION: HELPERS ========================================================== #


type RefClass = (
    type[ExtractRef]
    | type[JobRetryConfig]
    | type[LoadRef]
    | type[TransformRef]
    | type[ValidationRef]
)


def _assert_fields(actual: object, expected: Mapping[str, object]) -> None:
    """Assert that *actual* exposes the expected field values."""
    for field, value in expected.items():
        assert getattr(actual, field) == value


# SECTION: TESTS ============================================================ #


class TestReferenceParsing:
    """Unit tests for reference dataclass parsing."""

    @pytest.mark.parametrize(
        ('ref_cls', 'obj', 'expected'),
        [
            pytest.param(
                ExtractRef,
                {'source': 'my_source', 'options': {'foo': 1}},
                {'source': 'my_source', 'options': {'foo': 1}},
                id='extract-ref',
            ),
            pytest.param(
                LoadRef,
                {'target': 'my_target', 'overrides': {'foo': 2}},
                {'target': 'my_target', 'overrides': {'foo': 2}},
                id='load-ref',
            ),
            pytest.param(
                TransformRef,
                {'pipeline': 'my_pipeline'},
                {'pipeline': 'my_pipeline'},
                id='transform-ref',
            ),
            pytest.param(
                JobRetryConfig,
                {'max_attempts': 3, 'backoff_seconds': 1.5},
                {'max_attempts': 3, 'backoff_seconds': 1.5},
                id='retry-ref',
            ),
            pytest.param(
                ValidationRef,
                {'ruleset': 'rs', 'severity': 'warn', 'phase': 'both'},
                {'ruleset': 'rs', 'severity': 'warn', 'phase': 'both'},
                id='validation-ref',
            ),
        ],
    )
    def test_ref_from_obj_valid(
        self,
        ref_cls: RefClass,
        obj: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        """
        Test that valid reference payloads produce the expected dataclasses.
        """
        ref = ref_cls.from_obj(obj)
        assert ref is not None
        _assert_fields(ref, expected)

    @pytest.mark.parametrize(
        ('ref_cls', 'obj'),
        [
            pytest.param(ExtractRef, None, id='extract-none'),
            pytest.param(ExtractRef, {'source': 123}, id='extract-bad'),
            pytest.param(LoadRef, {'target': 123}, id='load-bad'),
            pytest.param(JobRetryConfig, None, id='retry-none'),
            pytest.param(TransformRef, {'pipeline': 123}, id='transform-bad'),
            pytest.param(ValidationRef, None, id='validation-none'),
            pytest.param(ValidationRef, {'ruleset': 123}, id='validation-bad'),
        ],
    )
    def test_ref_from_obj_invalid(
        self,
        ref_cls: RefClass,
        obj: dict[str, object] | None,
    ) -> None:
        """Test that invalid reference payloads yield `None`."""
        assert ref_cls.from_obj(obj) is None


class TestJobConfigParsing:
    """Unit tests for job configuration parsing."""

    def test_jobconfig_from_obj_valid(self) -> None:
        """
        Test that valid job payloads produce populated :class:`JobConfig`
        instances.
        """
        cfg = JobConfig.from_obj(
            {
                'name': 'job1',
                'description': 'desc',
                'extract': {'source': 'src'},
                'validate': {'ruleset': 'rs'},
                'retry': {'max_attempts': 3, 'backoff_seconds': 0.25},
                'transform': {'pipeline': 'p'},
                'load': {'target': 't'},
            },
        )

        assert cfg is not None
        _assert_fields(cfg, {'name': 'job1', 'description': 'desc'})
        assert cfg.extract is not None
        assert cfg.validate is not None
        assert cfg.retry == JobRetryConfig(max_attempts=3, backoff_seconds=0.25)
        assert cfg.transform is not None
        assert cfg.load is not None

    @pytest.mark.parametrize(
        'obj',
        [
            pytest.param(None, id='none'),
            pytest.param({}, id='empty-mapping'),
            pytest.param({'name': 123}, id='bad-name'),
        ],
    )
    def test_jobconfig_from_obj_invalid(
        self,
        obj: dict[str, object] | None,
    ) -> None:
        """Test that invalid job payloads yield ``None``."""
        assert JobConfig.from_obj(obj) is None

    @pytest.mark.parametrize(
        ('obj', 'expected'),
        [
            pytest.param(
                {'name': 'x', 'description': 5},
                {'name': 'x', 'description': '5', 'depends_on': []},
                id='coerces-description',
            ),
            pytest.param(
                {'name': 'x', 'depends_on': ['a', 1, None, 'b']},
                {'name': 'x', 'description': None, 'depends_on': ['a', 'b']},
                id='filters-non-string-dependencies',
            ),
            pytest.param(
                {'name': 'x', 'depends_on': 'prepare'},
                {'name': 'x', 'description': None, 'depends_on': ['prepare']},
                id='wraps-string-dependency',
            ),
            pytest.param(
                {
                    'name': 'x',
                    'retry': {'max_attempts': '4', 'backoff_seconds': '1.25'},
                },
                {'name': 'x', 'description': None, 'depends_on': []},
                id='coerces-retry-settings',
            ),
        ],
    )
    def test_jobconfig_from_obj_normalizes_optional_fields(
        self,
        obj: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        """
        Test that job config parsing normalizes optional fields consistently.
        """
        cfg = JobConfig.from_obj(obj)
        assert cfg is not None
        _assert_fields(cfg, expected)
        if 'retry' in obj:
            assert cfg.retry == JobRetryConfig(max_attempts=4, backoff_seconds=1.25)
