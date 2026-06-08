"""
:mod:`tests.unit.file.test_u_file_core_dispatch` module.

Unit tests for :mod:`etlplus.file._core_dispatch`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from etlplus.file import _core as core_mod
from etlplus.file import _core_dispatch as mod
from etlplus.file._enums import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCoreDispatchHelpers:
    """Unit tests for temporary-file core dispatch helpers."""

    def test_read_payload_with_core_uses_filename_basename(
        self,
        monkeypatch,
    ) -> None:
        """
        Test that payload reads route through the core :class:`File` wrapper.
        """
        seen: dict[str, Any] = {}

        class FileStub:
            """Stub core :class:`File` wrapper for read dispatch tests."""

            def __init__(self, path: Path, fmt: FileFormat) -> None:
                seen['path'] = path
                seen['fmt'] = fmt

            def read(self) -> dict[str, object]:
                """Stub read method that captures the payload bytes."""
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

    @pytest.mark.parametrize('filename', ['../escape.json', '/tmp/escape.json'])
    def test_write_payload_with_core_rejects_unsafe_paths(
        self,
        monkeypatch,
        filename: str,
    ) -> None:
        """
        Test that write payload rejecting paths that escape the temp root.
        """

        class FileStub:
            """Fail fast if unsafe filenames ever reach File dispatch."""

            def __init__(self, path: Path, fmt: FileFormat) -> None:
                _ = (path, fmt)
                raise AssertionError('File dispatch should not be reached')

        monkeypatch.setattr(core_mod, 'File', FileStub)

        with pytest.raises(ValueError, match='relative path|temporary'):
            mod.write_payload_with_core(
                fmt=FileFormat.JSON,
                data={'x': 1},
                filename=filename,
            )

    @pytest.mark.parametrize(
        ('check_name', 'expected'),
        [
            pytest.param('count', 7, id='count'),
            pytest.param('payload', b'written', id='payload'),
            pytest.param('fmt', FileFormat.JSON, id='fmt'),
            pytest.param('path-suffix', 'nested/output.json', id='path-suffix'),
            pytest.param('data', {'x': 1}, id='data'),
        ],
    )
    def test_write_payload_with_core_returns_count_and_bytes(
        self,
        monkeypatch,
        check_name: str,
        expected: object,
    ) -> None:
        """Test that write payload routing through core File wrapper."""
        seen: dict[str, Any] = {}

        class FileStub:
            """Stub core File wrapper for write dispatch tests."""

            def __init__(self, path: Path, fmt: FileFormat) -> None:
                seen['path'] = path
                seen['fmt'] = fmt

            def write(self, data: dict[str, object]) -> int:
                """
                Stub write method that captures the data dict and simulates
                writing bytes.
                """
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

        match check_name:
            case 'count':
                assert count == expected
            case 'payload':
                assert payload == expected
            case 'fmt':
                assert seen['fmt'] is expected
            case 'path-suffix':
                assert seen['path'].as_posix().endswith(str(expected))
            case 'data':
                assert seen['data'] == expected
            case _:
                pytest.fail(f'unhandled check: {check_name}')
