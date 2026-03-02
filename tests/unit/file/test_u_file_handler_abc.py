"""
:mod:`tests.unit.file.test_u_file_handler_abc` module.

Unit tests for :mod:`etlplus.file._handler_abc`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import _handler_abc as mod

# SECTION: TESTS ============================================================ #


class TestHandlerAbcHelpers:
    """Unit tests for internal helper functions in ``_handler_abc``."""

    def test_prepare_rows_for_write_returns_none_for_empty_rows(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test empty payload writes short-circuiting without directory create.
        """
        path = tmp_path / 'nested' / 'data.csv'

        result = mod._prepare_rows_for_write(
            path,
            [],
            format_name='CSV',
        )

        assert result is None
        assert not path.parent.exists()

    def test_prepare_rows_for_write_normalizes_rows_and_creates_parent(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test non-empty payload writes normalizing rows and creating parent.
        """
        path = tmp_path / 'nested' / 'data.csv'

        result = mod._prepare_rows_for_write(
            path,
            {'id': 1},
            format_name='CSV',
        )

        assert result == [{'id': 1}]
        assert path.parent.exists()

    def test_use_connection_closes_on_success(self) -> None:
        """
        Test connection helper closing resources after successful operation.
        """
        calls: list[str] = []
        connection = object()

        def _connect(path: Path) -> object:
            calls.append(f'connect:{path.name}')
            return connection

        def _close(conn: object) -> None:
            assert conn is connection
            calls.append('close')

        def _operation(conn: object) -> str:
            assert conn is connection
            calls.append('op')
            return 'ok'

        result = mod._use_connection(
            Path('data.db'),
            connect=_connect,
            close=_close,
            operation=_operation,
        )

        assert result == 'ok'
        assert calls == ['connect:data.db', 'op', 'close']

    def test_use_connection_closes_on_failure(self) -> None:
        """
        Test connection helper closing resources even when operation fails.
        """
        calls: list[str] = []
        connection = object()

        def _connect(path: Path) -> object:
            calls.append(f'connect:{path.name}')
            return connection

        def _close(conn: object) -> None:
            assert conn is connection
            calls.append('close')

        def _operation(conn: object) -> str:
            assert conn is connection
            calls.append('op')
            raise ValueError('boom')

        with pytest.raises(ValueError, match='boom'):
            mod._use_connection(
                Path('data.db'),
                connect=_connect,
                close=_close,
                operation=_operation,
            )

        assert calls == ['connect:data.db', 'op', 'close']
