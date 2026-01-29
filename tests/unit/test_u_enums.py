"""
:mod:`tests.unit.test_u_enums` module.

Unit tests for :mod:`etlplus.enums` coercion helpers and behaviors.
"""

from __future__ import annotations

import pytest

from etlplus.enums import AggregateName
from etlplus.enums import OperatorName
from etlplus.enums import PipelineStep

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestAggregateName:
    """Unit test suite for :class:`etlplus.enums.AggregateName`."""

    @pytest.mark.parametrize(
        'nums',
        [
            pytest.param([1, 2, 3], id='ints'),
            pytest.param([1.0, 2.0, 3.0], id='floats'),
        ],
    )
    def test_funcs(self, nums: list[int | float]) -> None:
        """Test the aggregate functions across numeric inputs."""
        assert AggregateName.SUM.func(nums, len(nums)) == 6
        assert AggregateName.MAX.func(nums, len(nums)) == 3
        assert AggregateName.MIN.func(nums, len(nums)) == 1
        assert AggregateName.COUNT.func(nums, len(nums)) == 3
        assert AggregateName.AVG.func(nums, len(nums)) == pytest.approx(2.0)


class TestOperatorName:
    """Unit test suite for :class:`etlplus.enums.OperatorName`."""

    def test_funcs(self) -> None:
        """Test the operator functions."""
        assert OperatorName.EQ.func(5, 5) is True
        assert OperatorName.NE.func(5, 6) is True
        assert OperatorName.GT.func(5, 2) is True
        assert OperatorName.LTE.func(5, 5) is True
        assert OperatorName.IN.func('a', 'abc') is True
        assert OperatorName.CONTAINS.func('alphabet', 'bet') is True


class TestPipelineStep:
    """Unit test suite for :class:`etlplus.enums.PipelineStep`."""

    def test_order(self) -> None:
        """Test the order values."""
        assert PipelineStep.FILTER.order == 0
        assert PipelineStep.AGGREGATE.order == 4
