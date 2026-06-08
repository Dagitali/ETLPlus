"""
:mod:`tests.unit.test_u_ops_enums` module.

Unit tests for :mod:`etlplus.ops._enums` coercion helpers and behaviors.
"""

from __future__ import annotations

from typing import Any
from typing import cast

import pytest

from etlplus.ops._enums import AggregateName
from etlplus.ops._enums import OperatorName
from etlplus.ops._enums import PipelineStep

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestAggregateName:
    """Unit tests for :class:`etlplus.ops._enums.AggregateName`."""

    @pytest.mark.parametrize(
        'nums',
        [
            pytest.param([1, 2, 3], id='ints'),
            pytest.param([1.0, 2.0, 3.0], id='floats'),
        ],
    )
    @pytest.mark.parametrize(
        ('aggregate', 'expected'),
        [
            pytest.param(AggregateName.SUM, 6, id='sum'),
            pytest.param(AggregateName.MAX, 3, id='max'),
            pytest.param(AggregateName.MIN, 1, id='min'),
            pytest.param(AggregateName.COUNT, 3, id='count'),
            pytest.param(AggregateName.AVG, pytest.approx(2.0), id='avg'),
        ],
    )
    def test_funcs(
        self,
        nums: list[int | float],
        aggregate: AggregateName,
        expected: object,
    ) -> None:
        """Test the aggregate functions across numeric inputs."""
        assert aggregate.func(nums, len(nums)) == expected


class TestOperatorName:
    """Unit tests for :class:`etlplus.ops._enums.OperatorName`."""

    def test_func_property_defensive_fallthrough(self) -> None:
        """Test descriptor fallback when called with a non-enum self."""
        descriptor = cast(property, OperatorName.__dict__['func'])
        raw_func = descriptor.fget
        assert raw_func is not None
        assert raw_func(cast(Any, object())) is None

    @pytest.mark.parametrize(
        ('operator', 'left', 'right'),
        [
            pytest.param(OperatorName.EQ, 5, 5, id='eq'),
            pytest.param(OperatorName.NE, 5, 6, id='ne'),
            pytest.param(OperatorName.GT, 5, 2, id='gt'),
            pytest.param(OperatorName.LT, 2, 5, id='lt'),
            pytest.param(OperatorName.LTE, 5, 5, id='lte'),
            pytest.param(OperatorName.IN, 'a', 'abc', id='in'),
            pytest.param(OperatorName.CONTAINS, 'alphabet', 'bet', id='contains'),
        ],
    )
    def test_funcs(
        self,
        operator: OperatorName,
        left: object,
        right: object,
    ) -> None:
        """Test the operator functions."""
        assert operator.func(left, right) is True


class TestPipelineStep:
    """Unit tests for :class:`etlplus.ops._enums.PipelineStep`."""

    @pytest.mark.parametrize(
        ('step', 'expected'),
        [
            pytest.param(PipelineStep.FILTER, 0, id='filter'),
            pytest.param(PipelineStep.AGGREGATE, 4, id='aggregate'),
        ],
    )
    def test_order(self, step: PipelineStep, expected: int) -> None:
        """Test the order values."""
        assert step.order == expected
