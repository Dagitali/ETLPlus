"""
:mod:`tests.unit.file.test_u_file_orc` module.

Unit tests for :mod:`etlplus.file.orc`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import cast

from etlplus.file import orc as mod

# SECTION: HELPERS ========================================================== #


class _PyarrowStub:
    """Stub module to satisfy ORC dependency."""


# SECTION: TESTS ============================================================ #


class TestOrcRead:
    """Unit tests for :func:`etlplus.file.orc.read`."""

    def test_read_uses_pandas(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """
        Test that :func:`read` uses the :mod:`pandas` module to read the file.
        """
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pyarrow': _PyarrowStub(), 'pandas': pandas})

        result = mod.read(tmp_path / 'data.orc')

        assert result == [{'id': 1}]
        assert pandas.read_calls


class TestOrcWrite:
    """Unit tests for :func:`etlplus.file.orc.write`."""

    def test_write_calls_to_orc(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """
        Test that :func:`write` calls :meth:`to_orc` on the
        :class:`pandas.DataFrame`.
        """
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pyarrow': _PyarrowStub(), 'pandas': pandas})
        path = tmp_path / 'data.orc'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_orc_calls == [
            {'path': path, 'index': False},
        ]
