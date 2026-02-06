"""
:mod:`tests.unit.file.test_u_file_gz` module.

Unit tests for :mod:`etlplus.file.gz`.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from etlplus.file import core
from etlplus.file import gz as mod
from etlplus.file.enums import FileFormat

# SECTION: HELPERS ========================================================== #


class _StubFile:
    """Minimal stand-in for :class:`etlplus.file.core.File`."""

    # pylint: disable=unused-argument

    def __init__(self, path: Path, fmt: FileFormat) -> None:
        self.path = Path(path)
        self.fmt = fmt

    def read(self) -> dict[str, str]:
        """Return deterministic payload for smoke reads."""
        return {'fmt': self.fmt.value, 'name': self.path.name}

    def write(self, data: object) -> int:  # noqa: ARG002
        """Persist dummy bytes so gzip write can read them."""
        self.path.write_text('payload', encoding='utf-8')
        return 1


# SECTION: TESTS ============================================================ #


class TestGzRead:
    """Unit tests for :func:`etlplus.file.gz.read`."""

    def test_read_raises_on_missing_inner_format(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that reading a gzip file without an inner format fails."""
        path = tmp_path / 'payload.gz'

        with pytest.raises(ValueError, match='Cannot infer file format'):
            mod.read(path)

    def test_read_raises_on_non_gzip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that reading a non-gzip file raises an error."""
        path = tmp_path / 'payload.json'
        path.write_text('irrelevant', encoding='utf-8')

        with pytest.raises(ValueError, match='Not a gzip file'):
            mod.read(path)

    def test_read_uses_file_helper(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that reading uses the File helper."""
        monkeypatch.setattr(core, 'File', _StubFile)
        path = tmp_path / 'payload.json.gz'
        with gzip.open(path, 'wb') as handle:
            handle.write(b'payload')

        result = mod.read(path)

        assert result == {'fmt': 'json', 'name': 'payload.json'}


class TestGzWrite:
    """Unit tests for :func:`etlplus.file.gz.write`."""

    def test_write_raises_on_missing_inner_format(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing a gzip file without an inner format fails."""
        path = tmp_path / 'payload.gz'

        with pytest.raises(ValueError, match='Cannot infer file format'):
            mod.write(path, [{'id': 1}])

    def test_write_creates_gzip_payload(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that writing creates a gzip file with the expected payload."""
        monkeypatch.setattr(core, 'File', _StubFile)
        path = tmp_path / 'payload.json.gz'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        with gzip.open(path, 'rb') as handle:
            assert handle.read() == b'payload'
