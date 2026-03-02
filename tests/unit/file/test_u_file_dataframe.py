"""
:mod:`tests.unit.file.test_u_file_dataframe` module.

Unit tests for :mod:`etlplus.file._dataframe`.
"""

from __future__ import annotations

from etlplus.file import _dataframe as mod

from .pytest_file_support import DictRecordsFrameStub
from .pytest_file_support import RDataPandasStub

# SECTION: TESTS ============================================================ #


class TestDataframeHelpers:
    """Unit tests for dataframe helper functions."""

    def test_dataframe_from_records(self) -> None:
        """Test frame construction from record payloads."""
        records = [{'id': 1}, {'id': 2}]
        frame = mod.dataframe_from_records(
            RDataPandasStub(),
            records,
        )
        assert isinstance(frame, DictRecordsFrameStub)
        assert frame.to_dict(orient='records') == records

    def test_dataframe_from_data(self) -> None:
        """Test data normalization and frame construction."""
        frame = mod.dataframe_from_data(
            RDataPandasStub(),
            {'id': 1},
            format_name='TEST',
        )
        assert isinstance(frame, DictRecordsFrameStub)
        assert frame.to_dict(orient='records') == [{'id': 1}]

    def test_dataframe_and_count_from_data(self) -> None:
        """Test combined frame construction and normalized count output."""
        frame, count = mod.dataframe_and_count_from_data(
            RDataPandasStub(),
            [{'id': 1}, {'id': 2}],
            format_name='TEST',
        )
        assert isinstance(frame, DictRecordsFrameStub)
        assert frame.to_dict(orient='records') == [{'id': 1}, {'id': 2}]
        assert count == 2
