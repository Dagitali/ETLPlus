"""
:mod:`tests.unit.ops.test_u_ops_init` module.

Unit tests for :mod:`etlplus.ops.__init__`.
"""

from __future__ import annotations

import etlplus.ops as mod
from etlplus.ops._enums import AggregateName
from etlplus.ops._enums import OperatorName
from etlplus.ops._enums import PipelineStep
from etlplus.ops._utils import ValidationResultDict
from etlplus.ops._utils import ValidationSettings
from etlplus.ops._utils import maybe_validate

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


def test_public_exports_are_wired_to_internal_modules() -> None:
    """Test that stable package exports resolve to the underscored modules."""
    assert mod.AggregateName is AggregateName
    assert mod.OperatorName is OperatorName
    assert mod.PipelineStep is PipelineStep
    assert mod.ValidationResultDict is ValidationResultDict
    assert mod.ValidationSettings is ValidationSettings
    assert mod.maybe_validate is maybe_validate
    assert mod.__all__ == [
        'AggregateName',
        'OperatorName',
        'PipelineStep',
        'extract',
        'load',
        'maybe_validate',
        'run',
        'run_pipeline',
        'transform',
        'validate',
        'ValidationResultDict',
        'ValidationSettings',
    ]
