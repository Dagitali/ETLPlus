"""
:mod:`tests.unit.file.test_u_file_feather` module.

Unit tests for :mod:`etlplus.file.feather`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import cast

import pytest

from etlplus.file import feather as mod

# SECTION: HELPERS ========================================================== #


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
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test that :func:`read` uses pandas to read Feather files."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pyarrow': _PyarrowStub(), 'pandas': pandas})

        result = mod.read(tmp_path / 'data.feather')

        assert result == [{'id': 1}]
        assert pandas.read_calls


class TestFeatherWrite:
    """Unit tests for :func:`etlplus.file.feather.write`."""

    def test_write_calls_to_feather(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """
        Test that :func:`write` calls the frame's :meth:`to_feather` method.
        """
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pyarrow': _PyarrowStub(), 'pandas': pandas})
        path = tmp_path / 'data.feather'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_feather_calls == [{'path': path}]

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
