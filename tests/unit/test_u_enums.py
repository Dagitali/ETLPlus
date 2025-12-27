"""
:mod:`tests.unit.test_u_enums` module.

Unit tests for :mod:`etlplus.enums` coercion helpers and behaviors.
"""

from __future__ import annotations

import pytest

from etlplus.enums import AggregateName
from etlplus.enums import DataConnectorType
from etlplus.enums import FileFormat
from etlplus.enums import HttpMethod
from etlplus.enums import OperatorName
from etlplus.enums import PipelineStep
from etlplus.enums import coerce_data_connector_type
from etlplus.enums import coerce_file_format
from etlplus.enums import coerce_http_method

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestAggregateName:
    """Unit test suite for :class:`etlplus.enums.AggregateName`."""

    # TODO: Fix type hints to allow both int and float inputs.
    def test_funcs(self) -> None:
        """Test the aggregate functions."""
        # nums = [1, 2, 3]
        nums = [1.0, 2.0, 3.0]
        assert AggregateName.SUM.func(nums, len(nums)) == 6
        assert AggregateName.MAX.func(nums, len(nums)) == 3
        assert AggregateName.MIN.func(nums, len(nums)) == 1
        assert AggregateName.COUNT.func(nums, len(nums)) == 3
        assert AggregateName.AVG.func(nums, len(nums)) == pytest.approx(2.0)


@pytest.mark.unit
class TestDataConnectorType:
    """Unit test suite for :class:`etlplus.enums.DataConnectorType`."""

    @pytest.mark.parametrize(
        'value,expected',
        [
            ('API', DataConnectorType.API),
            (' rest ', DataConnectorType.API),
            ('db', DataConnectorType.DATABASE),
            ('Fs', DataConnectorType.FILE),
        ],
    )
    def test_coerce_aliases(
        self,
        value: str,
        expected: DataConnectorType,
    ) -> None:
        """Test alias coercions."""
        assert coerce_data_connector_type(value) is expected

    def test_invalid_value(self) -> None:
        """Test that invalid values raise ValueError."""
        with pytest.raises(ValueError, match='Invalid DataConnectorType'):
            coerce_data_connector_type('queue')


@pytest.mark.unit
class TestFileFormat:
    """Unit test suite for :class:`etlplus.enums.FileFormat`."""

    @pytest.mark.parametrize(
        'value,expected',
        [
            ('JSON', FileFormat.JSON),
            ('application/xml', FileFormat.XML),
            ('yml', FileFormat.YAML),
        ],
    )
    def test_aliases(
        self,
        value: str,
        expected: FileFormat,
    ) -> None:
        """Test alias coercions."""
        assert coerce_file_format(value) is expected

    def test_invalid_value(self) -> None:
        with pytest.raises(ValueError, match='Invalid FileFormat'):
            coerce_file_format('ini')


@pytest.mark.unit
class TestHttpMethod:
    """Unit test suite for :class:`etlplus.enums.HttpMethod`."""

    def test_allows_body(self) -> None:
        """Test the `allows_body` property."""
        assert HttpMethod.POST.allows_body is True
        assert HttpMethod.PUT.allows_body is True
        assert HttpMethod.PATCH.allows_body is True
        assert HttpMethod.GET.allows_body is False

    def test_coerce_wrapper(self) -> None:
        """Test the :func:`coerce_http_method` helper."""
        assert coerce_http_method('delete') is HttpMethod.DELETE


@pytest.mark.unit
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


@pytest.mark.unit
class TestPipelineStep:
    """Unit test suite for :class:`etlplus.enums.PipelineStep`."""

    def test_order(self) -> None:
        """Test the order values."""
        assert PipelineStep.FILTER.order == 0
        assert PipelineStep.AGGREGATE.order == 4
