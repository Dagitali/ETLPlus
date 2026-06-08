"""
:mod:`tests.unit.storage.test_u_storage_remote_buffer` module.

Unit tests for :mod:`etlplus.storage._remote_buffer`.
"""

from __future__ import annotations

from io import BytesIO
from io import TextIOWrapper

import pytest

from etlplus.storage import _remote_buffer as remote_buffer_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestRemoteBufferHelpers:
    """Unit tests for the shared in-memory remote buffer helpers."""

    @pytest.mark.parametrize(
        ('kwargs', 'match'),
        [
            pytest.param(
                {'kind': 'read', 'text_mode': False},
                'payload is required',
                id='read-requires-payload',
            ),
            pytest.param(
                {'kind': 'write', 'text_mode': False},
                'uploader is required',
                id='write-requires-uploader',
            ),
        ],
    )
    def test_open_remote_buffer_requires_required_inputs(
        self,
        kwargs: dict[str, object],
        match: str,
    ) -> None:
        """Test that remote buffers require mode-specific inputs."""
        with pytest.raises(ValueError, match=match):
            remote_buffer_mod.open_remote_buffer(**kwargs)

    def test_parse_remote_open_mode_rejects_invalid_mode(self) -> None:
        """Test that invalid remote open modes are rejected."""
        with pytest.raises(ValueError, match='support only'):
            remote_buffer_mod.parse_remote_open_mode('a')

    @pytest.mark.parametrize(
        ('check_name', 'expected'),
        [
            pytest.param('type', BytesIO, id='type'),
            pytest.param('payload', b'payload', id='payload'),
        ],
    )
    def test_read_buffer_binary_mode_returns_bytes_buffer(
        self,
        check_name: str,
        expected: object,
    ) -> None:
        """Test that binary read mode returns the raw bytes buffer."""
        handle = remote_buffer_mod.open_remote_buffer(
            kind='read',
            text_mode=False,
            payload=b'payload',
        )

        match check_name:
            case 'type':
                assert isinstance(handle, expected)
            case 'payload':
                assert handle.read() == expected
            case _:
                pytest.fail(f'unhandled check: {check_name}')

    def test_upload_buffer_uploads_only_once_on_double_close(self) -> None:
        """Test that upload-on-close buffers do not upload twice."""
        uploads: list[bytes] = []
        handle = remote_buffer_mod.open_remote_buffer(
            kind='write',
            text_mode=False,
            uploader=uploads.append,
        )

        handle.write(b'payload')
        handle.close()
        handle.close()

        assert uploads == [b'payload']

    @pytest.mark.parametrize(
        ('check_name', 'expected'),
        [
            pytest.param('type', TextIOWrapper, id='type'),
            pytest.param('upload', [b'payload'], id='upload'),
        ],
    )
    def test_write_buffer_text_mode_returns_text_wrapper(
        self,
        check_name: str,
        expected: object,
    ) -> None:
        """Test that text write mode wraps the upload buffer in text I/O."""
        uploads: list[bytes] = []
        handle = remote_buffer_mod.open_remote_buffer(
            kind='write',
            text_mode=True,
            uploader=uploads.append,
        )

        if check_name == 'type':
            assert isinstance(handle, expected)
            return

        handle.write('payload')
        handle.close()
        assert uploads == expected
