"""
:mod:`tests.unit.file.test_u_file_core_dispatch` module.

Unit tests for :mod:`etlplus.file._core_dispatch`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from etlplus.file import _core_dispatch as mod
from etlplus.file import core as core_mod
from etlplus.file.enums import FileFormat

# SECTION: TESTS ============================================================ #


class TestCoreDispatchHelpers:
    """Unit tests for temporary-file core dispatch helpers."""

    def test_read_payload_with_core_uses_filename_basename(
        self,
        monkeypatch,
    ) -> None:
        """Test read payload routing through core File wrapper."""
        seen: dict[str, Any] = {}

        class FileStub:
            """Stub core File wrapper for read dispatch tests."""

            def __init__(self, path: Path, fmt: FileFormat) -> None:
                seen['path'] = path
                seen['fmt'] = fmt

            def read(self) -> dict[str, object]:
                path = seen['path']
                seen['payload'] = path.read_bytes()
                return {'name': path.name}

        monkeypatch.setattr(core_mod, 'File', FileStub)

        result = mod.read_payload_with_core(
            fmt=FileFormat.JSON,
            payload=b'{"a": 1}',
            filename='../nested/input.json',
        )

        assert result == {'name': 'input.json'}
        assert seen['fmt'] is FileFormat.JSON
        assert seen['path'].name == 'input.json'
        assert seen['payload'] == b'{"a": 1}'

    def test_write_payload_with_core_returns_count_and_bytes(
        self,
        monkeypatch,
    ) -> None:
        """Test write payload routing through core File wrapper."""
        seen: dict[str, Any] = {}

        class FileStub:
            """Stub core File wrapper for write dispatch tests."""

            def __init__(self, path: Path, fmt: FileFormat) -> None:
                seen['path'] = path
                seen['fmt'] = fmt

            def write(self, data: dict[str, object]) -> int:
                path = seen['path']
                seen['data'] = data
                path.write_bytes(b'written')
                return 7

        monkeypatch.setattr(core_mod, 'File', FileStub)

        count, payload = mod.write_payload_with_core(
            fmt=FileFormat.JSON,
            data={'x': 1},
            filename='nested/output.json',
        )

        assert count == 7
        assert payload == b'written'
        assert seen['fmt'] is FileFormat.JSON
        assert seen['path'].as_posix().endswith('nested/output.json')
        assert seen['data'] == {'x': 1}
