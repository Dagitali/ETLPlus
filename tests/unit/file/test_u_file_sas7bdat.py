"""
:mod:`tests.unit.file.test_u_file_sas7bdat` module.

Unit tests for :mod:`etlplus.file.sas7bdat`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import sas7bdat as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for SAS7BDAT helpers."""

    # pylint: disable=unused-argument

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = list(records)

    def to_dict(
        self,
        *,
        orient: str,  # noqa: ARG002
    ) -> list[dict[str, object]]:
        """Simulate frame-to-record conversion."""
        return list(self._records)


class _PandasStub:
    """Stub for pandas module with configurable ``read_sas`` behavior."""

    def __init__(
        self,
        frame: _Frame,
        *,
        fail_on_format_kwarg: bool = False,
    ) -> None:
        self._frame = frame
        self._fail_on_format_kwarg = fail_on_format_kwarg
        self.read_calls: list[dict[str, object]] = []

    def read_sas(
        self,
        path: Path,
        **kwargs: object,
    ) -> _Frame:
        """Simulate pandas.read_sas with optional format rejection."""
        format_name = kwargs.get('format')
        self.read_calls.append({'path': path, 'format': format_name})
        if self._fail_on_format_kwarg and format_name is not None:
            raise TypeError('format not supported')
        return self._frame


# SECTION: TESTS ============================================================ #


class TestSas7bdatRead:
    """Unit tests for :func:`etlplus.file.sas7bdat.read`."""

    def test_read_uses_format_hint(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that read requests the SAS7BDAT format hint when supported."""
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        monkeypatch.setattr(mod, 'get_dependency', lambda *_, **__: object())
        monkeypatch.setattr(mod, 'get_pandas', lambda *_: pandas)

        result = mod.read(tmp_path / 'data.sas7bdat')

        assert result == [{'id': 1}]
        assert pandas.read_calls == [
            {'path': tmp_path / 'data.sas7bdat', 'format': 'sas7bdat'},
        ]

    def test_read_falls_back_when_format_kwarg_not_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read fallback when pandas rejects the format keyword."""
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame, fail_on_format_kwarg=True)
        monkeypatch.setattr(mod, 'get_dependency', lambda *_, **__: object())
        monkeypatch.setattr(mod, 'get_pandas', lambda *_: pandas)

        result = mod.read(tmp_path / 'data.sas7bdat')

        assert result == [{'id': 1}]
        assert pandas.read_calls == [
            {'path': tmp_path / 'data.sas7bdat', 'format': 'sas7bdat'},
            {'path': tmp_path / 'data.sas7bdat', 'format': None},
        ]


class TestSas7bdatWrite:
    """Unit tests for :func:`etlplus.file.sas7bdat.write`."""

    def test_write_not_supported(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that write raises read-only RuntimeError."""
        with pytest.raises(RuntimeError, match='read-only'):
            mod.write(tmp_path / 'data.sas7bdat', [{'id': 1}])
