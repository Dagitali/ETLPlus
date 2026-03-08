"""
:mod:`tests.unit.file.test_u_file_statistical_handlers` module.

Unit tests for :mod:`etlplus.file._statistical_handlers`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import _statistical_handlers as mod

from .pytest_file_support import DictRecordsFrameStub
from .pytest_file_support import PandasReadSasStub
from .pytest_file_support import PyreadstatTabularStub

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _PyreadstatReadWriteHandler(mod.PyreadstatReadWriteFrameMixin):
    """Concrete read/write pyreadstat handler for unit tests."""

    format_name = 'SAV'
    pyreadstat_read_method = 'read_sav'
    pyreadstat_write_method = 'write_sav'


class _PyreadstatRequiredWriteHandler(mod.PyreadstatRequiredWriteFrameMixin):
    """Concrete required-write pyreadstat handler for unit tests."""

    format_name = 'XPT'
    pyreadstat_write_method = 'write_xport'


class _PyreadstatSasFallbackHandler(mod.PyreadstatReadSasFallbackFrameMixin):
    """Concrete SAS fallback handler for unit tests."""

    format_name = 'SAS7BDAT'
    pyreadstat_read_method = 'read_sas7bdat'
    sas_format_hint = 'sas7bdat'


# SECTION: TESTS ============================================================ #


class TestStatisticalHandlers:
    """Unit tests for shared statistical-handler mixin behavior."""

    def test_pyreadstat_read_write_mixin_reads_and_writes_frame(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that direct :mod:`pyreadstat` read/write methods dispatch."""
        path = tmp_path / 'sample.sav'
        frame = DictRecordsFrameStub([{'id': 1}])
        pyreadstat = PyreadstatTabularStub(
            frame=frame,
            read_method_name='read_sav',
            write_method_name='write_sav',
        )
        handler = _PyreadstatReadWriteHandler()

        read_result = handler.read_frame(
            path,
            pandas=object(),
            pyreadstat=pyreadstat,
        )
        handler.write_frame(
            path,
            frame,
            pandas=object(),
            pyreadstat=pyreadstat,
        )

        assert read_result is frame
        pyreadstat.assert_single_read_path(path)
        pyreadstat.assert_last_write_path(path)

    def test_required_write_mixin_raises_when_writer_is_missing(self) -> None:
        """
        Test that required-write path raises for missing :mod:`pyreadstat`
        writers.
        """
        handler = _PyreadstatRequiredWriteHandler()
        with pytest.raises(
            ImportError,
            match=(
                'XPT write support requires "pyreadstat" '
                'with write_xport\\(\\)'
            ),
        ):
            handler.write_frame(
                Path('sample.xpt'),
                DictRecordsFrameStub([]),
                pandas=object(),
                pyreadstat=object(),
            )

    def test_sas_fallback_prefers_pyreadstat_reader_when_available(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that SAS fallback handler uses :mod:`pyreadstat` first when
        available.
        """
        path = tmp_path / 'sample.sas7bdat'
        frame = DictRecordsFrameStub([{'id': 1}])
        pyreadstat = PyreadstatTabularStub(
            frame=frame,
            read_method_name='read_sas7bdat',
        )
        pandas = PandasReadSasStub(frame)
        handler = _PyreadstatSasFallbackHandler()

        result = handler.read_frame(
            path,
            pandas=pandas,
            pyreadstat=pyreadstat,
        )

        assert result is frame
        pyreadstat.assert_single_read_path(path)
        assert pandas.read_calls == []

    def test_sas_fallback_uses_pandas_when_pyreadstat_reader_is_missing(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that SAS fallback path delegates to :mod:`pandas` when needed.
        """
        path = tmp_path / 'sample.sas7bdat'
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = PandasReadSasStub(frame)
        handler = _PyreadstatSasFallbackHandler()

        result = handler.read_frame(
            path,
            pandas=pandas,
            pyreadstat=object(),
        )

        assert result is frame
        pandas.assert_single_read_call(path, format_name='sas7bdat')
