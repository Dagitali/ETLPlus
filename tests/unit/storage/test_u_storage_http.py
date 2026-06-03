"""
:mod:`tests.unit.storage.test_u_storage_http` module.

Unit tests for :mod:`etlplus.storage._http`.
"""

from __future__ import annotations

import pytest

from etlplus.storage import HttpStorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import _http as http_mod

from .pytest_storage_support import FakeHttpSession

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHttpStorageBackend:
    """Unit tests for :class:`etlplus.storage.HttpStorageBackend`."""

    def test_delete_rejects_cleanup(self) -> None:
        """Test that HTTP backend explicitly rejects deletion."""
        backend = HttpStorageBackend(session=FakeHttpSession(get_status=200))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(ValueError, match='read-only'):
            backend.delete(location)

    def test_exists_falls_back_to_get_when_head_not_supported(self) -> None:
        """Test that HTTP exists falls back to GET when HEAD is unsupported."""
        session = FakeHttpSession(head_status=405, get_status=200)
        backend = HttpStorageBackend(session=session)
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        assert backend.exists(location) is True
        assert session.calls == [
            ('head', 'https://example.com/files/data.csv', True),
            ('get', 'https://example.com/files/data.csv', True),
        ]

    def test_exists_returns_false_for_not_found(self) -> None:
        """Test that HTTP exists returns false for 404 responses."""
        backend = HttpStorageBackend(session=FakeHttpSession(head_status=404))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        assert backend.exists(location) is False

    def test_exists_returns_true_for_successful_head(self) -> None:
        """Test that HTTP exists returns true for successful HEAD calls."""
        session = FakeHttpSession(head_status=200)
        backend = HttpStorageBackend(session=session)
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        assert backend.exists(location) is True
        assert session.calls == [
            ('head', 'https://example.com/files/data.csv', True),
        ]

    def test_exists_returns_false_when_get_fallback_is_not_found(self) -> None:
        """Test that HTTP exists treats a 404 fallback GET as missing."""
        session = FakeHttpSession(head_status=405, get_status=404)
        backend = HttpStorageBackend(session=session)
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        assert backend.exists(location) is False

    def test_open_raises_file_not_found_for_missing_resource(self) -> None:
        """Test that HTTP open maps 404 responses to FileNotFoundError."""
        backend = HttpStorageBackend(session=FakeHttpSession(get_status=404))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(FileNotFoundError, match='File not found'):
            backend.open(location)

    def test_open_reads_text_payload(self) -> None:
        """Test that HTTP open returns a readable text buffer."""
        backend = HttpStorageBackend(
            session=FakeHttpSession(get_status=200, payload=b'name\nAda\n'),
        )
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with backend.open(location, encoding='utf-8') as handle:
            assert handle.read() == 'name\nAda\n'

    def test_open_rejects_unexpected_kwargs(self) -> None:
        """Test that HTTP open rejects unsupported keyword arguments."""
        backend = HttpStorageBackend(session=FakeHttpSession(get_status=200))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(TypeError, match='Unsupported HTTP open'):
            backend.open(location, unsupported=True)

    @pytest.mark.parametrize('mode', ['w', 'wb', 'wt'])
    def test_open_rejects_write_modes(self, mode: str) -> None:
        """Test that HTTP backend is explicitly read-only."""
        backend = HttpStorageBackend(session=FakeHttpSession(get_status=200))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(ValueError, match='read-only'):
            backend.open(location, mode)

    def test_session_scope_closes_owned_session(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that owned sessions are closed when the scope exits."""
        closed: list[bool] = []

        class FakeOwnedSession(FakeHttpSession):
            def close(self) -> None:
                closed.append(True)

        session = FakeOwnedSession()
        monkeypatch.setattr(http_mod.requests, 'Session', lambda: session)
        backend = HttpStorageBackend()

        with backend._session_scope() as scoped_session:
            assert scoped_session is session

        assert closed == [True]
