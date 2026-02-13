"""
:mod:`tests.smoke.file.conftest` module.

Define shared fixtures and helpers for pytest-based smoke tests of
:mod:`etlplus.file`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import Protocol

import pytest

from etlplus.file.base import WriteOptions

# SECTION: MARKERS ========================================================== #


# Directory-level marker for smoke tests.
pytestmark = pytest.mark.smoke


# SECTION: TYPES ============================================================ #


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


# SECTION: FUNCTIONS ======================================================== #


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
    """
    write_kwargs = dict(write_kwargs or {})
    handlers = [
        value
        for name, value in vars(module).items()
        if name.endswith('_HANDLER')
    ]
    assert len(handlers) == 1
    handler = handlers[0]

    if 'root_tag' in write_kwargs and 'options' not in write_kwargs:
        root_tag = write_kwargs.pop('root_tag')
        if not isinstance(root_tag, str):
            raise TypeError('root_tag must be a string')
        write_kwargs['options'] = WriteOptions(
            root_tag=root_tag,
        )
    try:
        if expect_write_error is not None:
            match = error_match or ''
            with pytest.raises(expect_write_error, match=match):
                handler.write(path, payload, **write_kwargs)
            return
        written = handler.write(path, payload, **write_kwargs)
        assert written
        result = handler.read(path)
        assert result
    except OSError as exc:
        if (
            module.__name__.endswith('.orc')
            and 'sysctlbyname' in str(exc)
        ):
            pytest.skip('ORC read failed due to sysctl limitations')
        raise
    except ImportError as exc:
        pytest.skip(str(exc))
