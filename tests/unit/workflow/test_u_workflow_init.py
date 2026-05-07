"""
:mod:`tests.unit.workflow.test_u_workflow_init` module.

Unit tests for :mod:`etlplus.workflow` package facade exports.
"""

from __future__ import annotations

import pytest

import etlplus.workflow as workflow_pkg
from etlplus.workflow._dag import topological_sort_jobs
from etlplus.workflow._errors import DagError
from etlplus.workflow._jobs import ExtractRef
from etlplus.workflow._jobs import JobConfig
from etlplus.workflow._jobs import JobRetryConfig
from etlplus.workflow._jobs import LoadRef
from etlplus.workflow._jobs import TransformRef
from etlplus.workflow._jobs import ValidationRef
from etlplus.workflow._profile import ProfileConfig
from etlplus.workflow._schedule import ScheduleBackfillConfig
from etlplus.workflow._schedule import ScheduleConfig
from etlplus.workflow._schedule import ScheduleIntervalConfig
from etlplus.workflow._schedule import ScheduleTargetConfig
from etlplus.workflow._schedule import schedule_validation_issues

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


WORKFLOW_EXPORTS = [
    ('ExtractRef', ExtractRef),
    ('JobConfig', JobConfig),
    ('JobRetryConfig', JobRetryConfig),
    ('LoadRef', LoadRef),
    ('ProfileConfig', ProfileConfig),
    ('ScheduleBackfillConfig', ScheduleBackfillConfig),
    ('ScheduleConfig', ScheduleConfig),
    ('ScheduleIntervalConfig', ScheduleIntervalConfig),
    ('ScheduleTargetConfig', ScheduleTargetConfig),
    ('TransformRef', TransformRef),
    ('ValidationRef', ValidationRef),
    ('DagError', DagError),
    ('schedule_validation_issues', schedule_validation_issues),
    ('topological_sort_jobs', topological_sort_jobs),
]

# SECTION: TESTS ============================================================ #


class TestWorkflowPackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert workflow_pkg.__all__ == [name for name, _value in WORKFLOW_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), WORKFLOW_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(workflow_pkg, name) == expected
