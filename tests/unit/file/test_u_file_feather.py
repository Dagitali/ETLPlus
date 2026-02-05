"""
:mod:`tests.unit.file.test_u_file_feather` module.

Unit tests for :mod:`etlplus.file.feather`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import feather as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for Feather helpers."""

    # pylint: disable=unused-argument

    def __init__(
        self,
        records: list[dict[str, object]],
    ) -> None:
        self._records = records
        self.to_feather_calls: list[dict[str, object]] = []

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Simulate converting a frame to a list of records."""
        return list(self._records)

    def to_feather(
        self,
        path: Path,
    ) -> None:
        """Simulate writing the frame to a Feather file."""
        self.to_feather_calls.append({'path': path})


class _PandasStub:
    """Stub for pandas module."""

    def __init__(self, frame: _Frame) -> None:
        # pylint: disable=invalid-name

        self._frame = frame
        self.read_calls: list[dict[str, object]] = []
        self.last_frame: _Frame | None = None

        def _from_records(records: list[dict[str, object]]) -> _Frame:
            frame = _Frame(records)
            self.last_frame = frame
            return frame

        self.DataFrame = type(  # type: ignore[assignment]
            'DataFrame',
            (),
            {'from_records': staticmethod(_from_records)},
        )

    def read_feather(self, path: Path) -> _Frame:
        """Simulate reading a Feather file into a frame."""
        self.read_calls.append({'path': path})
        return self._frame


class _PyarrowStub:
    """Stub module to satisfy Feather dependency."""


# SECTION: TESTS ============================================================ #


class TestFeatherRead:
    """Unit tests for :func:`etlplus.file.feather.read`."""

    def test_read_missing_pyarrow_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _missing(*_args: object, **_kwargs: object) -> object:
            raise ImportError('missing')

        monkeypatch.setattr(mod, 'get_dependency', _missing)

        with pytest.raises(ImportError):
            mod.read(tmp_path / 'data.feather')

    def test_read_uses_pandas(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that :func:`read` uses pandas to read Feather files."""
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pyarrow': _PyarrowStub(), 'pandas': pandas})

        result = mod.read(tmp_path / 'data.feather')

        assert result == [{'id': 1}]
        assert pandas.read_calls


class TestFeatherWrite:
    """Unit tests for :func:`etlplus.file.feather.write`."""

    def test_write_missing_pyarrow_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _missing(*_args: object, **_kwargs: object) -> object:
            raise ImportError('missing')

        monkeypatch.setattr(mod, 'get_dependency', _missing)

        with pytest.raises(ImportError):
            mod.write(tmp_path / 'data.feather', [{'id': 1}])

    def test_write_calls_to_feather(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` calls the frame's :meth:`to_feather` method.
        """
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pyarrow': _PyarrowStub(), 'pandas': pandas})
        path = tmp_path / 'data.feather'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_feather_calls == [{'path': path}]
