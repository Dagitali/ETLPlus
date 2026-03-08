"""
:mod:`tests.unit.file.test_u_file_r_handlers` module.

Unit tests for :mod:`etlplus.file._r_handlers`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from etlplus.file import _r_handlers as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _PyreadrStub:
    """Minimal pyreadr-like stub for shared mixin tests."""

    def __init__(self, result: dict[str, object]) -> None:
        self._result = result
        self.read_calls: list[str] = []
        self.write_calls: list[tuple[str, object]] = []
        self.write_with_kwargs_calls: list[
            tuple[str, object, dict[str, object]]
        ] = []

    def read_r(self, path: str) -> dict[str, object]:
        """Record read calls and return configured mapping."""
        self.read_calls.append(path)
        return dict(self._result)

    def write_plain(self, path: str, frame: object) -> None:
        """Record writer calls without kwargs."""
        self.write_calls.append((path, frame))

    def write_with_kwargs(
        self,
        path: str,
        frame: object,
        **kwargs: object,
    ) -> None:
        """Record writer calls with kwargs."""
        self.write_with_kwargs_calls.append((path, frame, dict(kwargs)))


class _FallbackWriter:
    """Writer callable that raises on kwargs to exercise fallback behavior."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object, dict[str, object]]] = []

    def __call__(self, path: str, frame: object, **kwargs: object) -> None:
        self.calls.append((path, frame, dict(kwargs)))
        if kwargs:
            raise TypeError('kwargs unsupported')


class _PandasStub:
    """Minimal pandas-like stub for R coercion helpers."""

    DataFrame = type('DataFrame', (), {})


class _RDataHandler(mod.RDataHandlerMixin):
    """Concrete handler stub exposing shared R-data mixin behavior."""

    format_name = 'RDA'
    dataset_key = 'data'

    def __init__(self, pyreadr: object) -> None:
        self._pyreadr = pyreadr

    def resolve_format_dependency(
        self,
        dependency_name: str,
        *,
        pip_name: str | None = None,
    ) -> Any:
        _ = pip_name
        assert dependency_name == 'pyreadr'
        return self._pyreadr

    def resolve_pandas(self) -> object:
        """Return minimal pandas-like stub for value coercion checks."""
        return _PandasStub()


# SECTION: TESTS ============================================================ #


class TestRHandlers:
    """Unit tests for shared R-data handler mixin behavior."""

    def test_resolve_pyreadr_writer_returns_first_available_candidate(
        self,
    ) -> None:
        """Test that writer resolution uses candidate method priority order."""
        pyreadr = _PyreadrStub({'data': []})
        handler = _RDataHandler(pyreadr)

        writer = handler.resolve_pyreadr_writer(
            'missing_writer',
            'write_with_kwargs',
            error_message='writer missing',
        )

        writer('sample.rda', {'id': 1}, df_name='data')
        assert pyreadr.write_with_kwargs_calls == [
            ('sample.rda', {'id': 1}, {'df_name': 'data'}),
        ]

    def test_resolve_pyreadr_writer_raises_for_missing_methods(self) -> None:
        """
        Test that writer resolution failures raises the provided error text.
        """
        handler = _RDataHandler(_PyreadrStub({'data': []}))
        with pytest.raises(ImportError, match='writer missing'):
            handler.resolve_pyreadr_writer(
                'missing_writer',
                error_message='writer missing',
            )

    def test_call_pyreadr_writer_falls_back_after_type_error(self) -> None:
        """Test writer invocation fallback path when kwargs are unsupported."""
        writer = _FallbackWriter()
        handler = _RDataHandler(_PyreadrStub({'data': []}))
        path = Path('data.rda')
        frame = object()

        handler.call_pyreadr_writer(
            writer,
            path=path,
            frame=frame,
            kwargs={'df_name': 'data'},
        )

        assert writer.calls == [
            ('data.rda', frame, {'df_name': 'data'}),
            ('data.rda', frame, {}),
        ]

    def test_coerce_r_dataset_delegates_to_read_and_coercion(self) -> None:
        """Test dataset coercion path with deterministic pyreadr payloads."""
        pyreadr = _PyreadrStub({'data': [{'id': 1}]})
        handler = _RDataHandler(pyreadr)

        result = handler.coerce_r_dataset(
            Path('sample.rda'),
            dataset=None,
        )

        assert result == [{'id': 1}]
        assert pyreadr.read_calls == ['sample.rda']
