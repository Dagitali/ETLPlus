"""
:mod:`tests.integration.file.pytest_smoke_file_contracts` module.

Reusable smoke-test contracts for :mod:`etlplus.file` modules.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from types import ModuleType

import pytest

from etlplus.utils._types import JSONDict
from etlplus.utils._types import JSONList

from ...pytest_file_common import call_handler_operation
from ...pytest_file_common import resolve_module_handler
from ...pytest_file_common import skip_on_known_file_io_error

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FileSmokeCase',
    # Constants
    'FILE_SMOKE_CASES',
    'FILE_SMOKE_EXCEPTION_CASES',
    'FILE_SMOKE_EXCEPTION_OVERRIDES',
    'FILE_SMOKE_OVERRIDE_ATTRS',
    # Functions
    'run_file_smoke',
]


# SECTION: CONSTANTS ======================================================== #


FILE_SMOKE_OVERRIDE_ATTRS = frozenset(
    {
        'error_match',
        'expect_write_error',
        'file_name',
    },
)
FILE_SMOKE_EXCEPTION_OVERRIDES = {
    'gz': frozenset({'file_name'}),
    'hdf5': frozenset(
        {'expect_write_error', 'error_match'},
    ),
    'sas7bdat': frozenset(
        {'expect_write_error', 'error_match'},
    ),
    'xls': frozenset(
        {'expect_write_error', 'error_match'},
    ),
    'zip': frozenset({'file_name'}),
}
FILE_SMOKE_EXCEPTION_CASES = frozenset(
    FILE_SMOKE_EXCEPTION_OVERRIDES,
)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class FileSmokeCase:
    """File-format smoke test case."""

    module_name: str
    file_name: str | None = None
    payload: object | None = None
    use_sample_record: bool = False
    write_kwargs: Mapping[str, object] | None = None
    expect_write_error: type[Exception] | None = None
    error_match: str | None = None

    @property
    def id(self) -> str:
        """Return the pytest parameter id for this case."""
        return self.module_name

    def import_module(self) -> ModuleType:
        """Import and return the ETLPlus file module for this case."""
        return import_module(f'etlplus.file.{self.module_name}')

    def path_for(self, tmp_path: Path) -> Path:
        """Return the smoke-test path for this case."""
        if self.file_name is not None:
            return tmp_path / self.file_name
        suffix = resolve_module_handler(self.import_module()).format.value
        return tmp_path / f'data.{suffix}'

    def payload_for(
        self,
        *,
        sample_record: JSONDict,
        sample_records: JSONList,
    ) -> object:
        """Return the write payload for this case."""
        if self.payload is not None:
            return self.payload
        if self.use_sample_record:
            return sample_record
        return sample_records

    def override_attrs(self) -> frozenset[str]:
        """Return structural override attributes configured for this case."""
        return frozenset(
            attr
            for attr in FILE_SMOKE_OVERRIDE_ATTRS
            if getattr(self, attr) is not None
        )


# SECTION: CONSTANTS ======================================================== #


FILE_SMOKE_CASES = (
    FileSmokeCase('arrow'),
    FileSmokeCase('avro'),
    FileSmokeCase('bson'),
    FileSmokeCase('cbor'),
    FileSmokeCase('csv'),
    FileSmokeCase('dat'),
    FileSmokeCase('dta'),
    FileSmokeCase('duckdb'),
    FileSmokeCase('feather'),
    FileSmokeCase('fwf'),
    FileSmokeCase('gz', file_name='data.json.gz'),
    FileSmokeCase('hdf5', expect_write_error=RuntimeError, error_match='read-only'),
    FileSmokeCase(
        'hbs',
        payload={'template': 'Hello {{ name }}', 'context': {'name': 'Ada'}},
    ),
    FileSmokeCase('ini', payload={'DEFAULT': {'name': 'Ada'}, 'main': {'age': '36'}}),
    FileSmokeCase(
        'jinja2',
        payload={'template': 'Hello {{ name }}', 'context': {'name': 'Ada'}},
    ),
    FileSmokeCase('json'),
    FileSmokeCase('log'),
    FileSmokeCase('msgpack'),
    FileSmokeCase(
        'mustache',
        payload={'template': 'Hello {{ name }}', 'context': {'name': 'Ada'}},
    ),
    FileSmokeCase('nc'),
    FileSmokeCase('ndjson'),
    FileSmokeCase('ods'),
    FileSmokeCase('orc'),
    FileSmokeCase('parquet'),
    FileSmokeCase('pb', payload={'payload_base64': 'aGVsbG8='}),
    FileSmokeCase('properties', payload={'id': '99', 'name': 'Grace'}),
    FileSmokeCase(
        'proto',
        payload={
            'schema': """syntax = "proto3"; message Test { string name = 1; } """,
        },
    ),
    FileSmokeCase('psv'),
    FileSmokeCase('rda'),
    FileSmokeCase('rds'),
    FileSmokeCase(
        'sas7bdat',
        expect_write_error=RuntimeError,
        error_match='read-only',
    ),
    FileSmokeCase('sav'),
    FileSmokeCase('sqlite'),
    FileSmokeCase('tab'),
    FileSmokeCase('toml', use_sample_record=True),
    FileSmokeCase('tsv'),
    FileSmokeCase('txt', payload='99\nGrace'),
    FileSmokeCase(
        'vm',
        payload={'template': 'Hello $name', 'context': {'name': 'Ada'}},
    ),
    FileSmokeCase('xls', expect_write_error=RuntimeError, error_match='read-only'),
    FileSmokeCase('xlsm'),
    FileSmokeCase('xlsx'),
    FileSmokeCase(
        'xml',
        payload={'root': {'text': 'hello'}},
        write_kwargs={'root_tag': 'root'},
    ),
    FileSmokeCase('xpt'),
    FileSmokeCase('yaml'),
    FileSmokeCase('zip', file_name='data.json.zip'),
)


# SECTION: FUNCTIONS ======================================================== #


def run_file_smoke(
    module: ModuleType,
    path: Path,
    payload: object,
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
    payload : object
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
