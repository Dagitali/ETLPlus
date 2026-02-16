"""
:mod:`tests.integration.file.pytest_smoke_file_contracts` module.

Reusable smoke-test contracts for :mod:`etlplus.file` modules.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import ModuleType

import pytest

from etlplus.types import JSONData
from etlplus.types import JSONDict
from etlplus.types import JSONList

from ...pytest_file_common import call_handler_operation
from ...pytest_file_common import skip_on_known_file_io_error

__all__ = [
    'SmokeRoundtripModuleContract',
    'run_file_smoke',
]


class SmokeRoundtripModuleContract:
    """Reusable write/read smoke contract for file-format modules."""

    module: ModuleType
    file_name: str
    payload: JSONData | None = None
    use_sample_record: bool = False
    write_kwargs: Mapping[str, object] | None = None
    expect_write_error: type[Exception] | None = None
    error_match: str | None = None

    def test_roundtrip_smoke(
        self,
        tmp_path: Path,
        sample_record: JSONDict,
        sample_records: JSONList,
    ) -> None:
        """Test that read/write can be invoked with minimal payloads."""
        path = tmp_path / self.file_name
        payload: JSONData
        if self.payload is not None:
            payload = self.payload
        elif self.use_sample_record:
            payload = sample_record
        else:
            payload = sample_records
        run_file_smoke(
            self.module,
            path,
            payload,
            write_kwargs=self.write_kwargs,
            expect_write_error=self.expect_write_error,
            error_match=self.error_match,
        )


def run_file_smoke(
    module: ModuleType,
    path: Path,
    payload: JSONData,
    *,
    write_kwargs: Mapping[str, object] | None = None,
    expect_write_error: type[Exception] | None = None,
    error_match: str | None = None,
) -> None:
    """
    Run a minimal read/write smoke cycle for file modules.

    Parameters
    ----------
    module : ModuleType
        File module exposing a singleton ``*_HANDLER`` instance.
    path : Path
        Target path for the test file.
    payload : JSONData
        Payload passed to ``write``.
    write_kwargs : Mapping[str, object] | None, optional
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
    try:
        if expect_write_error is not None:
            match = error_match or ''
            with pytest.raises(expect_write_error, match=match):
                call_handler_operation(
                    module,
                    operation='write',
                    path=path,
                    payload=payload,
                    write_kwargs=write_kwargs,
                )
            return
        written = call_handler_operation(
            module,
            operation='write',
            path=path,
            payload=payload,
            write_kwargs=write_kwargs,
        )
        assert isinstance(written, int)
        assert written >= 0
        result = call_handler_operation(
            module,
            operation='read',
            path=path,
        )
        assert result is not None
    except OSError as exc:
        skip_on_known_file_io_error(
            error=exc,
            module_name=module.__name__,
        )
        raise
    except ImportError as exc:
        pytest.skip(str(exc))
