"""
:mod:`tests.unit.ops.test_u_ops_init` module.

Unit tests for :mod:`etlplus.ops.__init__`.
"""

from __future__ import annotations

import pytest

import etlplus.ops as mod
from etlplus.ops._enums import AggregateName
from etlplus.ops._enums import OperatorName
from etlplus.ops._enums import PipelineStep
from etlplus.ops._utils import ValidationResultDict
from etlplus.ops._utils import ValidationSettings
from etlplus.ops._utils import maybe_validate
from etlplus.ops.extract import extract
from etlplus.ops.load import load
from etlplus.ops.run import run
from etlplus.ops.run import run_pipeline
from etlplus.ops.transform import transform
from etlplus.ops.validate import FieldRulesDict
from etlplus.ops.validate import FieldValidationDict
from etlplus.ops.validate import ValidationDict
from etlplus.ops.validate import validate

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


OPS_EXPORTS = [
    ('AggregateName', AggregateName),
    ('OperatorName', OperatorName),
    ('PipelineStep', PipelineStep),
    ('extract', extract),
    ('load', load),
    ('maybe_validate', maybe_validate),
    ('run', run),
    ('run_pipeline', run_pipeline),
    ('transform', transform),
    ('validate', validate),
    ('FieldRulesDict', FieldRulesDict),
    ('FieldValidationDict', FieldValidationDict),
    ('ValidationDict', ValidationDict),
    ('ValidationResultDict', ValidationResultDict),
    ('ValidationSettings', ValidationSettings),
]


# SECTION: TESTS ============================================================ #

class TestOpsPackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert mod.__all__ == [name for name, _value in OPS_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), OPS_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(mod, name) == expected
