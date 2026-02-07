"""
:mod:`tests.unit.file.test_u_file_arrow` module.

Unit tests for :mod:`etlplus.file.arrow`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import arrow as mod

# SECTION: TESTS ============================================================ #


class TestArrowRead:
    """Unit tests for :func:`etlplus.file.arrow.read`."""

    def test_read_missing_pyarrow_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`read` raises when dependency resolution fails."""

        def _missing(*_args: object, **_kwargs: object) -> object:
            raise ImportError('missing pyarrow')

        monkeypatch.setattr(mod, 'get_dependency', _missing)

        with pytest.raises(ImportError, match='missing pyarrow'):
            mod.read(tmp_path / 'data.arrow')


class TestArrowWrite:
    """Unit tests for :func:`etlplus.file.arrow.write`."""

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that empty payloads return zero without creating a file."""
        path = tmp_path / 'data.arrow'

        assert mod.write(path, []) == 0
        assert not path.exists()

    def test_write_missing_pyarrow_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`write` raises when dependency resolution fails."""

        def _missing(*_args: object, **_kwargs: object) -> object:
            raise ImportError('missing pyarrow')

        monkeypatch.setattr(mod, 'get_dependency', _missing)

        with pytest.raises(ImportError, match='missing pyarrow'):
            mod.write(tmp_path / 'data.arrow', [{'id': 1}])
