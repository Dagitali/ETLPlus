"""
:mod:`tests.unit.workflow.test_u_workflow_init` module.

Unit tests for :mod:`etlplus.workflow` package exports.
"""

from __future__ import annotations

import etlplus.workflow as workflow_pkg
from etlplus.workflow.dag import topological_sort_jobs
from etlplus.workflow.jobs import ExtractRef
from etlplus.workflow.jobs import JobConfig
from etlplus.workflow.jobs import LoadRef
from etlplus.workflow.jobs import TransformRef
from etlplus.workflow.jobs import ValidationRef
from etlplus.workflow.profile import ProfileConfig

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


def test_workflow_package_exports_expected_symbols() -> None:
    """Test package re-exports should match documented ``__all__``."""
    assert workflow_pkg.ExtractRef is ExtractRef
    assert workflow_pkg.JobConfig is JobConfig
    assert workflow_pkg.LoadRef is LoadRef
    assert workflow_pkg.TransformRef is TransformRef
    assert workflow_pkg.ValidationRef is ValidationRef
    assert workflow_pkg.ProfileConfig is ProfileConfig
    assert workflow_pkg.topological_sort_jobs is topological_sort_jobs

    assert set(workflow_pkg.__all__) == {
        'ExtractRef',
        'JobConfig',
        'LoadRef',
        'ProfileConfig',
        'TransformRef',
        'ValidationRef',
        'topological_sort_jobs',
    }
