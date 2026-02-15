"""
:mod:`tests.smoke.file.pytest_smoke_file_contracts` module.

Reusable smoke-test contracts for :mod:`etlplus.file` modules.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Any
from typing import Protocol
from typing import cast

import pytest

from ...pytest_file_common import normalize_write_kwargs
from ...pytest_file_common import resolve_module_handler

__all__ = [
    'FileModule',
    'SmokeRoundtripModuleContract',
    'run_file_smoke',
]


class FileModule(Protocol):
    """Protocol for file format modules exposing a singleton handler."""

    __name__: str


class SmokeRoundtripModuleContract:
    """Reusable write/read smoke contract for file-format modules."""

    module: FileModule
    file_name: str
    payload: object | None = None
    use_sample_record: bool = False
    write_kwargs: dict[str, object] | None = None
    expect_write_error: type[Exception] | None = None
    error_match: str | None = None

    def build_payload(
        self,
        *,
        sample_record: dict[str, Any],
        sample_records: list[dict[str, Any]],
    ) -> object:
        """Return the roundtrip payload for one smoke test module."""
        if self.payload is not None:
            return self.payload
        if self.use_sample_record:
            return sample_record
        return sample_records

    def test_roundtrip_smoke(
        self,
        tmp_path: Path,
        sample_record: dict[str, Any],
        sample_records: list[dict[str, Any]],
    ) -> None:
        """Test that read/write can be invoked with minimal payloads."""
        path = tmp_path / self.file_name
        payload = self.build_payload(
            sample_record=sample_record,
            sample_records=sample_records,
        )
        run_file_smoke(
            self.module,
            path,
            payload,
            write_kwargs=self.write_kwargs,
            expect_write_error=self.expect_write_error,
            error_match=self.error_match,
        )


def run_file_smoke(
    module: FileModule,
    path: Path,
    payload: object,
    *,
    write_kwargs: dict[str, object] | None = None,
    expect_write_error: type[Exception] | None = None,
    error_match: str | None = None,
) -> None:
    """
    Run a minimal read/write smoke cycle for file modules.

    Parameters
    ----------
    module : FileModule
        File module exposing ``read``/``write`` functions.
    path : Path
        Target path for the test file.
    payload : object
        Payload passed to ``write``.
    write_kwargs : dict[str, object] | None, optional
        Keyword arguments forwarded to ``write``.
    expect_write_error : type[Exception] | None, optional
        Expected exception type for write failures.
    error_match : str | None, optional
        Regex message to assert when ``expect_write_error`` is provided.

    Raises
    ------
    OSError
        If the smoke test encounters unexpected file system errors.
    """
    write_kwargs = normalize_write_kwargs(write_kwargs)
    handler = resolve_module_handler(cast(ModuleType, module))
    untyped_handler = cast(Any, handler)
    try:
        if expect_write_error is not None:
            match = error_match or ''
            with pytest.raises(expect_write_error, match=match):
                untyped_handler.write(path, payload, **write_kwargs)
            return
        written = untyped_handler.write(path, payload, **write_kwargs)
        assert written
        result = handler.read(path)
        assert result
    except OSError as exc:
        if module.__name__.endswith('.orc') and 'sysctlbyname' in str(exc):
            pytest.skip('ORC read failed due to sysctl limitations')
        raise
    except ImportError as exc:
        pytest.skip(str(exc))
